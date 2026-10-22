// Command raftkv-jepsen runs the fault-injection harness repeatedly and prints
// one line per run. Exit status is non-zero if any run is not linearizable.
//
//	raftkv-jepsen -runs 10 -duration 60s -nodes 5
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"raftkv/cluster"
	"raftkv/jepsen"
)

func main() {
	var (
		runs     = flag.Int("runs", 5, "number of independent runs")
		duration = flag.Duration("duration", 60*time.Second, "duration of each run")
		groups   = flag.Int("groups", 1, "replica groups")
		nodes    = flag.Int("nodes", 5, "servers per group")
		clients  = flag.Int("clients", 10, "concurrent clients")
		faults   = flag.String("faults", strings.Join(jepsen.AllFaults, ","), "comma-separated nemeses")
		disk     = flag.Bool("disk", true, "use the file WAL (fsync) instead of in-memory storage")
		unsafe   = flag.Bool("unsafe-local-reads", false, "negative control: skip ReadIndex (should FAIL)")
		seed     = flag.Int64("seed", 0, "base seed (0 = time)")
		verbose  = flag.Bool("v", false, "log nemesis actions")
	)
	flag.Parse()
	bad := 0
	for i := 0; i < *runs; i++ {
		opts := jepsen.Options{
			Cluster: cluster.Options{Groups: *groups, NodesPerGroup: *nodes, MaxRaftState: 64 << 10,
				ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond,
				UnsafeLocalReads: *unsafe},
			Clients: *clients, Duration: *duration, Faults: strings.Split(*faults, ","),
		}
		if *seed != 0 {
			opts.Seed = *seed + int64(i)
		}
		if *disk {
			dir, err := os.MkdirTemp("", "raftkv-jepsen-")
			if err != nil {
				log.Fatal(err)
			}
			opts.Cluster.DataDir = dir
			defer os.RemoveAll(dir)
		}
		if *verbose {
			opts.Logf = log.Printf
		}
		rep, err := jepsen.Run(opts)
		if err != nil {
			log.Fatal(err)
		}
		if !rep.Check.OK || rep.Check.TimedOut || !rep.Recovered {
			bad++
		}
		fmt.Printf("run %d: %v\n", i+1, rep)
	}
	fmt.Printf("%d/%d runs clean\n", *runs-bad, *runs)
	if bad > 0 {
		os.Exit(1)
	}
}
