// Command raftkv-cli talks to a RaftKV cluster.
//
//	raftkv-cli -config cluster.json put k v
//	raftkv-cli -config cluster.json get k
//	raftkv-cli -config cluster.json append k v
//	raftkv-cli -config cluster.json del k
//	raftkv-cli -config cluster.json status
//	raftkv-cli -config cluster.json add-server g1 n6 n6:7001
//	raftkv-cli -config cluster.json remove-server g1 n6
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"raftkv/internal/clusterconf"
	"raftkv/kv"
	"raftkv/transport/grpcx"
)

func main() {
	confPath := flag.String("config", "cluster.json", "cluster config file")
	timeout := flag.Duration("timeout", 5*time.Second, "overall deadline")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	conf, err := clusterconf.Load(*confPath)
	check(err)
	client := grpcx.NewClient()
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	ck := kv.NewClerk(client, conf.Ring(), conf.Groups)

	need := func(n int) {
		if len(args) != n {
			fmt.Fprintf(os.Stderr, "%s needs %d arguments\n", args[0], n-1)
			os.Exit(2)
		}
	}
	switch args[0] {
	case "get":
		need(2)
		v, err := ck.Get(ctx, args[1])
		check(err)
		fmt.Println(v)
	case "put":
		need(3)
		check(ck.Put(ctx, args[1], args[2]))
	case "append":
		need(3)
		check(ck.Append(ctx, args[1], args[2]))
	case "del":
		need(2)
		check(ck.Delete(ctx, args[1]))
	case "status":
		for _, g := range conf.GroupNames() {
			for _, m := range conf.Groups[g] {
				st, err := client.Status(ctx, m.Addr)
				if err != nil {
					fmt.Printf("%-4s %-6s %-22s unreachable: %v\n", g, m.ID, m.Addr, err)
					continue
				}
				b, _ := json.Marshal(st)
				fmt.Printf("%-4s %-6s %-22s %s\n", g, m.ID, m.Addr, b)
			}
		}
	case "add-server", "remove-server":
		if args[0] == "add-server" {
			need(4)
		} else {
			need(3)
		}
		g := args[1]
		var lastErr string
		for _, m := range conf.Groups[g] {
			var r *kv.AdminReply
			var err error
			if args[0] == "add-server" {
				r, err = client.AddServer(ctx, m.Addr, &kv.AddServerArgs{ID: args[2], Addr: args[3]})
			} else {
				r, err = client.RemoveServer(ctx, m.Addr, &kv.RemoveServerArgs{ID: args[2]})
			}
			if err == nil && r.Err == "" {
				fmt.Println("ok")
				return
			}
			if err != nil {
				lastErr = err.Error()
			} else {
				lastErr = r.Err
			}
		}
		check(fmt.Errorf("%s failed on every server of %s: %s", args[0], g, lastErr))
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		os.Exit(2)
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
