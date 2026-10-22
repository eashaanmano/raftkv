// Package jepsen is a Jepsen-style fault-injection harness. Concurrent clients
// run random reads, writes and appends against an in-process cluster while a
// nemesis injects partitions, crashes, clock skew, message loss/reordering and
// membership changes. Every operation is recorded with call/return times and
// the full history is checked for linearizability afterwards.
//
// The cluster runs inside one process on a simulated network, so this tests
// the Raft/KV logic under faults. It does not test the OS, the real network
// stack or the disk (see scripts/nemesis.sh for faults against Docker).
package jepsen

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/cluster"
	"raftkv/kv"
	"raftkv/lincheck"
	"raftkv/raft"
)

var AllFaults = []string{"partition", "isolate-leader", "crash", "clock-skew", "lossy-net", "membership"}

type Options struct {
	Cluster   cluster.Options
	Clients   int           // default 10
	Keys      int           // keys per window (default 4)
	KeyWindow time.Duration // keys rotate every window, bounding per-key history (default 3s)
	Duration  time.Duration // default 30s
	OpTimeout time.Duration // client deadline per op (default 1.5s)
	Faults    []string      // default AllFaults
	Seed      int64
	Logf      func(string, ...any)
}

type Report struct {
	Ops, OK, FailedReads, Unknown int
	Faults                        map[string]int
	Check                         lincheck.Result
	CheckTime                     time.Duration
	Recovered                     bool
}

func (r Report) String() string {
	var fs []string
	for f, n := range r.Faults {
		fs = append(fs, fmt.Sprintf("%s=%d", f, n))
	}
	sort.Strings(fs)
	verdict := "LINEARIZABLE"
	switch {
	case r.Check.TimedOut:
		verdict = "INCONCLUSIVE (checker timed out on some keys)"
	case !r.Check.OK:
		verdict = "NOT LINEARIZABLE (key " + r.Check.BadKey + ")"
	}
	return fmt.Sprintf("ops=%d ok=%d failed-reads=%d unknown-writes=%d keys=%d faults=%v recovered=%v check=%s (%v)",
		r.Ops, r.OK, r.FailedReads, r.Unknown, r.Check.Keys, fs, r.Recovered, verdict, r.CheckTime.Round(time.Millisecond))
}

func Run(opts Options) (Report, error) {
	if opts.Clients == 0 {
		opts.Clients = 10
	}
	if opts.Keys == 0 {
		opts.Keys = 4
	}
	if opts.KeyWindow == 0 {
		opts.KeyWindow = 3 * time.Second
	}
	if opts.Duration == 0 {
		opts.Duration = 30 * time.Second
	}
	if opts.OpTimeout == 0 {
		opts.OpTimeout = 1500 * time.Millisecond
	}
	if opts.Faults == nil {
		opts.Faults = AllFaults
	}
	if opts.Seed == 0 {
		opts.Seed = time.Now().UnixNano()
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	c, err := cluster.New(opts.Cluster)
	if err != nil {
		return Report{}, err
	}
	defer c.Shutdown()
	for _, g := range c.Ring.Groups() {
		if _, err := c.WaitLeader(g, 5*time.Second); err != nil {
			return Report{}, err
		}
	}
	baseDelay := 2 * time.Millisecond
	c.Net.SetUnreliable(0, 0, baseDelay) // always some reordering

	var (
		clock   atomic.Int64
		histMu  sync.Mutex
		history []lincheck.Op
		rep     = Report{Faults: map[string]int{}}
	)
	start := time.Now()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for ci := 0; ci < opts.Clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(opts.Seed + int64(ci)))
			clerk := c.Clerk()
			clerk.RandomReadReplica = opts.Cluster.UnsafeLocalReads
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				window := int(time.Since(start) / opts.KeyWindow)
				key := fmt.Sprintf("w%d-k%d", window, rng.Intn(opts.Keys))
				op := lincheck.Op{Client: ci, Key: key}
				ctx, cancel := context.WithTimeout(context.Background(), opts.OpTimeout)
				op.Call = clock.Add(1)
				var err error
				switch r := rng.Intn(10); {
				case r < 4:
					op.Kind = lincheck.Get
					op.Value, err = clerk.Get(ctx, key)
				case r < 6:
					op.Kind = lincheck.Put
					op.Value = fmt.Sprintf("%d.%d", ci, n)
					err = clerk.Put(ctx, key, op.Value)
				default:
					op.Kind = lincheck.Append
					op.Value = fmt.Sprintf("[%d.%d]", ci, n) // unique, so appends are distinguishable
					err = clerk.Append(ctx, key, op.Value)
				}
				op.Return = clock.Add(1)
				cancel()
				histMu.Lock()
				rep.Ops++
				switch {
				case err == nil:
					rep.OK++
					history = append(history, op)
				case op.Kind == lincheck.Get:
					rep.FailedReads++ // a failed read has no effect; drop it
				default:
					rep.Unknown++
					op.Unknown = true
					history = append(history, op)
				}
				histMu.Unlock()
				if err != nil && !errors.Is(err, kv.ErrUnknown) {
					opts.Logf("client %d: %v", ci, err)
				}
			}
		}(ci)
	}

	// Nemesis.
	nem := &nemesis{c: c, rng: rand.New(rand.NewSource(opts.Seed)), logf: opts.Logf, baseDelay: baseDelay}
	deadline := time.After(opts.Duration)
loop:
	for {
		if !sleepOr(deadline, time.Duration(500+nem.rng.Intn(1000))*time.Millisecond) {
			break
		}
		f := opts.Faults[nem.rng.Intn(len(opts.Faults))]
		rep.Faults[f]++
		nem.inject(f)
		ok := sleepOr(deadline, time.Duration(1000+nem.rng.Intn(2500))*time.Millisecond)
		nem.heal()
		if !ok {
			break loop
		}
	}
	nem.heal()
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()

	// Liveness after healing: every group must accept a write again.
	rep.Recovered = true
	clerk := c.Clerk()
	for _, g := range c.Ring.Groups() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		key := keyFor(c, g)
		if err := clerk.Put(ctx, key, "final"); err != nil {
			rep.Recovered = false
		}
		cancel()
	}

	t0 := time.Now()
	rep.Check = lincheck.CheckKV(history, 2*time.Minute)
	rep.CheckTime = time.Since(t0)
	return rep, nil
}

func keyFor(c *cluster.Cluster, group string) string {
	for i := 0; ; i++ {
		k := fmt.Sprintf("liveness-%d", i)
		if c.Ring.Lookup(k) == group {
			return k
		}
	}
}

func sleepOr(deadline <-chan time.Time, d time.Duration) bool {
	select {
	case <-deadline:
		return false
	case <-time.After(d):
		return true
	}
}

type nemesis struct {
	c         *cluster.Cluster
	rng       *rand.Rand
	logf      func(string, ...any)
	baseDelay time.Duration
	crashed   []string
	skewed    []string
	removed   []string
}

func (n *nemesis) randomGroup() string {
	gs := n.c.Ring.Groups()
	return gs[n.rng.Intn(len(gs))]
}

func (n *nemesis) groupNodes(g string) []string {
	var out []string
	for _, id := range n.c.Nodes() {
		if n.c.GroupOf(id) == g {
			out = append(out, id)
		}
	}
	return out
}

func (n *nemesis) inject(f string) {
	nodes := n.c.Nodes()
	switch f {
	case "partition":
		// Random two-way split of all servers.
		n.rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
		cut := 1 + n.rng.Intn(len(nodes)-1)
		n.c.Net.Partition(nodes[:cut], nodes[cut:])
		n.logf("nemesis: partition %v | %v", nodes[:cut], nodes[cut:])
	case "isolate-leader":
		g := n.randomGroup()
		l := n.c.Leader(g)
		if l == "" {
			return
		}
		var rest []string
		for _, id := range nodes {
			if id != l {
				rest = append(rest, id)
			}
		}
		n.c.Net.Partition([]string{l}, rest)
		n.logf("nemesis: isolate leader %s", l)
	case "crash":
		g := n.randomGroup()
		gn := n.groupNodes(g)
		n.rng.Shuffle(len(gn), func(i, j int) { gn[i], gn[j] = gn[j], gn[i] })
		k := 1 + n.rng.Intn(len(gn)/2+1) // sometimes a majority: unavailable but must stay safe
		for _, id := range gn[:k] {
			n.c.Crash(id)
			n.crashed = append(n.crashed, id)
		}
		n.logf("nemesis: crash %v", gn[:k])
	case "clock-skew":
		for _, id := range nodes {
			if n.rng.Intn(2) == 0 {
				clk := n.c.Clock(id)
				rate := []float64{0.25, 0.5, 2, 4}[n.rng.Intn(4)]
				clk.SetRate(rate)
				clk.Jump(time.Duration(n.rng.Intn(4000)-2000) * time.Millisecond)
				n.skewed = append(n.skewed, id)
				n.logf("nemesis: clock %s rate=%.2fx", id, rate)
			}
		}
	case "lossy-net":
		n.c.Net.SetUnreliable(0.2, 0, 40*time.Millisecond)
		n.logf("nemesis: 20%% loss, 0-40ms delay")
	case "membership":
		g := n.randomGroup()
		gn := n.groupNodes(g)
		id := gn[n.rng.Intn(len(gn))]
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := n.c.RemoveNode(ctx, id); err == nil {
			n.removed = append(n.removed, id)
			n.logf("nemesis: removed %s from %s", id, g)
		}
	}
}

func (n *nemesis) heal() {
	n.c.Net.Heal()
	n.c.Net.SetUnreliable(0, 0, n.baseDelay)
	for _, id := range n.crashed {
		if err := n.c.Start(id); err != nil {
			n.logf("nemesis: restart %s: %v", id, err)
		}
	}
	n.crashed = nil
	for _, id := range n.skewed {
		n.c.Clock(id).Reset()
	}
	n.skewed = nil
	for _, id := range n.removed {
		g := n.c.GroupOf(id)
		for attempt := 0; attempt < 50; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			l := n.c.Leader(g)
			var err error = errors.New("no leader")
			if l != "" {
				if srv := n.c.Server(l); srv != nil {
					err = srv.Raft().AddServer(ctx, raft.Member{ID: id, Addr: id})
				}
			}
			cancel()
			if err == nil {
				n.logf("nemesis: re-added %s", id)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	n.removed = nil
}
