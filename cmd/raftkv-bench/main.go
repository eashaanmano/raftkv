// Command raftkv-bench measures throughput, latency and failover.
//
// Modes:
//
//	-mode grpc-local  start N servers in this process on 127.0.0.1 over real
//	                  gRPC with file WALs (fsync), then load them.
//	-mode grpc        load an existing cluster described by -config
//	                  (e.g. the docker-compose deployment).
//	-mode inmem       in-process cluster on the simulated network (no gRPC);
//	                  isolates Raft/KV/storage cost from RPC cost.
//
// Example: raftkv-bench -mode grpc-local -nodes 5 -clients 128 -duration 20s -kill-leader-at 10s
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	"raftkv/bench"
	"raftkv/cluster"
	"raftkv/internal/clusterconf"
	"raftkv/kv"
	"raftkv/raft"
	"raftkv/shard"
	"raftkv/transport/grpcx"
)

func main() {
	var (
		mode      = flag.String("mode", "grpc-local", "grpc-local | grpc | inmem")
		confPath  = flag.String("config", "cluster.json", "cluster config (mode=grpc)")
		nodes     = flag.Int("nodes", 5, "servers per group (local modes)")
		groups    = flag.Int("groups", 1, "replica groups (local modes)")
		memStore  = flag.Bool("mem", false, "in-memory storage instead of fsynced WAL (local modes; unsafe, for comparison only)")
		dataDir   = flag.String("data", "", "WAL directory for local modes (default: a temp dir)")
		netDelay  = flag.Duration("net-delay", 0, "inmem: max one-way delay per message")
		clients   = flag.Int("clients", 64, "concurrent closed-loop clients")
		duration  = flag.Duration("duration", 10*time.Second, "measured duration")
		warmup    = flag.Duration("warmup", 2*time.Second, "warmup before measuring")
		valueSize = flag.Int("value-size", 100, "value size in bytes")
		keys      = flag.Int("keys", 100000, "key space size")
		readRatio = flag.Float64("read-ratio", 0, "fraction of reads (0 = write-only)")
		killAt    = flag.Duration("kill-leader-at", 0, "crash the leader this long into the measured window (0 = never)")
		election  = flag.Duration("election-timeout", 300*time.Millisecond, "base election timeout (local modes)")
		heartbeat = flag.Duration("heartbeat", 50*time.Millisecond, "heartbeat interval (local modes)")
	)
	flag.Parse()

	var target bench.Target
	var cleanup func()
	switch *mode {
	case "inmem":
		dir := *dataDir
		if !*memStore && dir == "" {
			dir = tempDir()
		}
		if *memStore {
			dir = ""
		}
		c, err := cluster.New(cluster.Options{Groups: *groups, NodesPerGroup: *nodes, DataDir: dir,
			MaxRaftState: 64 << 20, ElectionTimeout: *election, HeartbeatInterval: *heartbeat})
		check(err)
		if *netDelay > 0 {
			c.Net.SetUnreliable(0, 0, *netDelay)
		}
		for _, g := range c.Ring.Groups() {
			_, err := c.WaitLeader(g, 10*time.Second)
			check(err)
		}
		target = bench.Target{Caller: cluster.CallerFor(c), Ring: c.Ring, Groups: c.Groups,
			KillLeader: func() (string, error) {
				l := c.Leader("g1")
				if l == "" {
					return "", fmt.Errorf("no leader")
				}
				c.Crash(l)
				return l, nil
			}}
		cleanup = c.Shutdown
		describe(*mode, *groups, *nodes, *memStore, dir)

	case "grpc-local":
		dir := *dataDir
		if dir == "" {
			dir = tempDir()
		}
		target, cleanup = startLocal(*groups, *nodes, *memStore, dir, *election, *heartbeat)
		describe(*mode, *groups, *nodes, *memStore, dir)

	case "grpc":
		conf, err := clusterconf.Load(*confPath)
		check(err)
		client := grpcx.NewClient()
		target = bench.Target{Caller: client, Ring: conf.Ring(), Groups: conf.Groups}
		cleanup = client.Close
		if *killAt > 0 {
			log.Printf("note: -kill-leader-at is ignored in grpc mode; kill a container yourself (scripts/nemesis.sh) and read 'longest client-visible stall'")
		}
		fmt.Printf("mode=grpc config=%s\n", *confPath)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
	defer cleanup()

	fmt.Printf("clients=%d duration=%v warmup=%v value=%dB keys=%d read-ratio=%.2f\n",
		*clients, *duration, *warmup, *valueSize, *keys, *readRatio)
	res, err := bench.Run(target, bench.Options{
		Clients: *clients, Duration: *duration, Warmup: *warmup, ValueSize: *valueSize,
		Keys: *keys, ReadRatio: *readRatio, KillLeaderAt: *killAt,
	})
	fmt.Print(res)
	check(err)
}

func describe(mode string, groups, nodes int, mem bool, dir string) {
	storage := "fsynced WAL in " + dir
	if mem {
		storage = "IN-MEMORY storage (no durability)"
	}
	fmt.Printf("mode=%s groups=%d nodes/group=%d storage=%s\n", mode, groups, nodes, storage)
}

func tempDir() string {
	d, err := os.MkdirTemp("", "raftkv-bench-")
	check(err)
	return d
}

type localNode struct {
	srv  *kv.Server
	grpc *grpc.Server
}

// startLocal runs a real cluster in this process: every node has its own
// gRPC server on a loopback port and talks to its peers over TCP.
func startLocal(groups, nodes int, mem bool, dir string, election, heartbeat time.Duration) (bench.Target, func()) {
	conf := map[string][]raft.Member{}
	var names []string
	lis := map[string]net.Listener{}
	for g := 1; g <= groups; g++ {
		gname := fmt.Sprintf("g%d", g)
		names = append(names, gname)
		for i := 1; i <= nodes; i++ {
			id := fmt.Sprintf("%s-n%d", gname, i)
			l, err := net.Listen("tcp", "127.0.0.1:0")
			check(err)
			lis[id] = l
			conf[gname] = append(conf[gname], raft.Member{ID: id, Addr: l.Addr().String()})
		}
	}
	ring := shard.NewRing(names, shard.DefaultVNodes)
	running := map[string]*localNode{}
	var clients []*grpcx.Client
	for g, ms := range conf {
		for _, m := range ms {
			var storage raft.Storage
			if mem {
				storage = raft.NewMemStorage()
			} else {
				fs, err := raft.OpenFileStorage(filepath.Join(dir, m.ID))
				check(err)
				storage = fs
			}
			tc := grpcx.NewClient()
			clients = append(clients, tc)
			srv, err := kv.NewServer(kv.ServerConfig{
				Raft: raft.Config{ID: m.ID, Bootstrap: conf[g], Storage: storage, Transport: tc,
					ElectionTimeout: election, HeartbeatInterval: heartbeat},
				Group: g, Ring: ring, MaxRaftState: 64 << 20,
			})
			check(err)
			gs := grpcx.NewServer(grpcx.Adapter{Server: srv})
			go gs.Serve(lis[m.ID])
			running[m.ID] = &localNode{srv: srv, grpc: gs}
		}
	}
	// Wait for leaders.
	deadline := time.Now().Add(10 * time.Second)
	for _, g := range names {
		for leaderOf(running, conf[g]) == "" {
			if time.Now().After(deadline) {
				log.Fatalf("no leader in %s", g)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	client := grpcx.NewClient()
	target := bench.Target{Caller: client, Ring: ring, Groups: conf,
		KillLeader: func() (string, error) {
			l := leaderOf(running, conf["g1"])
			if l == "" {
				return "", fmt.Errorf("no leader")
			}
			n := running[l]
			n.grpc.Stop() // connections drop immediately, like a crashed process
			n.srv.Stop()
			delete(running, l)
			return l, nil
		}}
	cleanup := func() {
		for _, n := range running {
			n.grpc.Stop()
			n.srv.Stop()
		}
		for _, c := range clients {
			c.Close()
		}
		client.Close()
	}
	return target, cleanup
}

func leaderOf(running map[string]*localNode, ms []raft.Member) string {
	for _, m := range ms {
		if n := running[m.ID]; n != nil {
			if _, ok := n.srv.Raft().IsLeader(); ok {
				return m.ID
			}
		}
	}
	return ""
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
