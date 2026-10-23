// Command raftkv-server runs one RaftKV replica.
//
//	raftkv-server -config cluster.json -id n1 -data /var/lib/raftkv
//
// A server listed in the config file bootstraps with its group's membership.
// A server started with -join begins empty and waits to be added with
// `raftkv-cli add-server`.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"raftkv/internal/clusterconf"
	"raftkv/kv"
	"raftkv/raft"
	"raftkv/transport/grpcx"
)

func main() {
	var (
		confPath  = flag.String("config", "cluster.json", "cluster config file")
		id        = flag.String("id", "", "this server's ID")
		listen    = flag.String("listen", "", "listen address (default: this server's addr from the config, on all interfaces)")
		dataDir   = flag.String("data", "data", "directory for the WAL and snapshots")
		join      = flag.Bool("join", false, "start empty and wait to be added via add-server")
		group     = flag.String("group", "", "group to join (only with -join, if the id is not in the config)")
		election  = flag.Duration("election-timeout", 300*time.Millisecond, "base election timeout")
		heartbeat = flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
		snapBytes = flag.Int("snapshot-bytes", 64<<20, "snapshot when the raft log exceeds this size (0 = never)")
		verbose   = flag.Bool("v", false, "log raft state changes")
	)
	flag.Parse()
	if *id == "" {
		log.Fatal("-id is required")
	}
	conf, err := clusterconf.Load(*confPath)
	if err != nil {
		log.Fatal(err)
	}
	g, self, found := conf.Find(*id)
	if !found {
		if !*join || *group == "" || *listen == "" {
			log.Fatalf("id %q is not in %s (use -join -group G -listen ADDR for a new server)", *id, *confPath)
		}
		g = *group
	}
	var bootstrap []raft.Member
	if !*join {
		bootstrap = conf.Groups[g]
	}
	addr := *listen
	if addr == "" {
		_, port, err := net.SplitHostPort(self.Addr)
		if err != nil {
			log.Fatalf("bad addr %q for %s: %v", self.Addr, *id, err)
		}
		addr = ":" + port
	}

	storage, err := raft.OpenFileStorage(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	logf := func(string, ...any) {}
	if *verbose {
		logf = log.Printf
	}
	client := grpcx.NewClient()
	srv, err := kv.NewServer(kv.ServerConfig{
		Raft: raft.Config{
			ID: *id, Bootstrap: bootstrap, Storage: storage, Transport: client,
			ElectionTimeout: *election, HeartbeatInterval: *heartbeat, Logf: logf,
		},
		Group: g, Ring: conf.Ring(), MaxRaftState: *snapBytes,
	})
	if err != nil {
		log.Fatal(err)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	gs := grpcx.NewServer(grpcx.Adapter{Server: srv})
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()
	log.Printf("raftkv-server %s (group %s) listening on %s, data in %s", *id, g, addr, *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
	gs.Stop()
	srv.Stop()
	client.Close()
}
