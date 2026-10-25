// Package bench is a closed-loop load generator: N clients each issue one
// request at a time, back to back, and every latency is recorded. It can kill
// the leader mid-run to measure failover as seen by clients.
package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/kv"
	"raftkv/raft"
	"raftkv/shard"
)

type Target struct {
	Caller kv.Caller
	Ring   *shard.Ring
	Groups map[string][]raft.Member
	// KillLeader crashes the leader of the first group and returns its ID.
	KillLeader func() (string, error)
}

type Options struct {
	Clients      int
	Duration     time.Duration
	Warmup       time.Duration
	ValueSize    int
	Keys         int
	ReadRatio    float64       // fraction of Gets (0 = write-only)
	KillLeaderAt time.Duration // 0 = never
	OpTimeout    time.Duration
}

type Result struct {
	Ops, Writes, Reads, Errors int
	Elapsed                    time.Duration
	Throughput                 float64 // ops/s over the measured window
	P50, P90, P99, P999, Max   time.Duration
	MaxStall                   time.Duration // longest gap between successful completions (any client)
	KilledLeader               string
	FailoverClientVisible      time.Duration // kill -> first success of an op issued after the kill
}

func (r Result) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "ops=%d (writes=%d reads=%d) errors=%d elapsed=%v\n", r.Ops, r.Writes, r.Reads, r.Errors, r.Elapsed.Round(time.Millisecond))
	fmt.Fprintf(&b, "throughput: %.0f ops/s\n", r.Throughput)
	fmt.Fprintf(&b, "latency: p50=%v p90=%v p99=%v p99.9=%v max=%v\n", r.P50, r.P90, r.P99, r.P999, r.Max)
	fmt.Fprintf(&b, "longest client-visible stall: %v\n", r.MaxStall.Round(time.Millisecond))
	if r.KilledLeader != "" {
		fmt.Fprintf(&b, "killed leader %s; first write issued after the kill completed %v later\n", r.KilledLeader, r.FailoverClientVisible.Round(time.Millisecond))
	}
	return b.String()
}

func Run(t Target, o Options) (Result, error) {
	if o.Clients == 0 {
		o.Clients = 64
	}
	if o.Duration == 0 {
		o.Duration = 10 * time.Second
	}
	if o.ValueSize == 0 {
		o.ValueSize = 100
	}
	if o.Keys == 0 {
		o.Keys = 100000
	}
	if o.OpTimeout == 0 {
		o.OpTimeout = 5 * time.Second
	}
	value := strings.Repeat("x", o.ValueSize)
	start := time.Now()
	measureFrom := start.Add(o.Warmup)
	end := measureFrom.Add(o.Duration)

	var (
		mu         sync.Mutex
		lat        []time.Duration
		res        Result
		lastOK     atomic.Int64 // unix nanos of the latest successful completion
		maxStall   atomic.Int64
		killAt     atomic.Int64
		firstAfter atomic.Int64
	)
	lastOK.Store(time.Now().UnixNano())

	var wg sync.WaitGroup
	for ci := 0; ci < o.Clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(ci) + time.Now().UnixNano()))
			ck := kv.NewClerk(t.Caller, t.Ring, t.Groups)
			local := make([]time.Duration, 0, 1<<16)
			var writes, reads, errs int
			for time.Now().Before(end) {
				key := fmt.Sprintf("key-%d", rng.Intn(o.Keys))
				isRead := rng.Float64() < o.ReadRatio
				ctx, cancel := context.WithTimeout(context.Background(), o.OpTimeout)
				t0 := time.Now()
				var err error
				if isRead {
					_, err = ck.Get(ctx, key)
				} else {
					err = ck.Put(ctx, key, value)
				}
				t1 := time.Now()
				cancel()
				if err != nil {
					errs++
					continue
				}
				// stall tracking
				now := t1.UnixNano()
				prev := lastOK.Swap(now)
				if gap := now - prev; gap > maxStall.Load() && t0.After(measureFrom) {
					for {
						cur := maxStall.Load()
						if gap <= cur || maxStall.CompareAndSwap(cur, gap) {
							break
						}
					}
				}
				if k := killAt.Load(); k != 0 && t0.UnixNano() > k {
					firstAfter.CompareAndSwap(0, now)
				}
				if t0.After(measureFrom) && t1.Before(end) {
					local = append(local, t1.Sub(t0))
					if isRead {
						reads++
					} else {
						writes++
					}
				}
			}
			mu.Lock()
			lat = append(lat, local...)
			res.Writes += writes
			res.Reads += reads
			res.Errors += errs
			mu.Unlock()
		}(ci)
	}

	if o.KillLeaderAt > 0 && t.KillLeader != nil {
		go func() {
			time.Sleep(o.Warmup + o.KillLeaderAt)
			id, err := t.KillLeader()
			// Mark the kill once the leader is actually down, so ops that were
			// issued while we were still locating it don't count as post-kill.
			killAt.Store(time.Now().UnixNano())
			mu.Lock()
			if err == nil {
				res.KilledLeader = id
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	res.Ops = len(lat)
	res.Elapsed = o.Duration
	res.Throughput = float64(res.Ops) / o.Duration.Seconds()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		i := int(p * float64(len(lat)))
		if i >= len(lat) {
			i = len(lat) - 1
		}
		return lat[i]
	}
	res.P50, res.P90, res.P99, res.P999 = pct(0.50), pct(0.90), pct(0.99), pct(0.999)
	if len(lat) > 0 {
		res.Max = lat[len(lat)-1]
	}
	res.MaxStall = time.Duration(maxStall.Load())
	if k, f := killAt.Load(), firstAfter.Load(); k != 0 && f != 0 {
		res.FailoverClientVisible = time.Duration(f - k)
	}
	if res.Ops == 0 {
		return res, fmt.Errorf("no successful operations")
	}
	return res, nil
}
