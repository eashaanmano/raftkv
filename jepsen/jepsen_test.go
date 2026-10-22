package jepsen

import (
	"os"
	"strconv"
	"testing"
	"time"

	"raftkv/cluster"
)

func duration(t *testing.T, def time.Duration) time.Duration {
	if s := os.Getenv("JEPSEN_SECONDS"); s != "" {
		n, _ := strconv.Atoi(s)
		return time.Duration(n) * time.Second
	}
	if testing.Short() {
		return 8 * time.Second
	}
	return def
}

// TestLinearizableUnderFaults is the main safety test: all nemeses, a 5-node
// group with file-backed WALs and snapshots enabled, then a full
// linearizability check of the history.
func TestLinearizableUnderFaults(t *testing.T) {
	rep, err := Run(Options{
		Cluster: cluster.Options{Groups: 1, NodesPerGroup: 5, DataDir: t.TempDir(),
			MaxRaftState: 64 << 10, ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond},
		Duration: duration(t, 30*time.Second),
		Logf:     t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rep)
	if !rep.Check.OK {
		t.Fatalf("history is not linearizable: %v", rep)
	}
	if rep.Check.TimedOut {
		t.Fatalf("checker inconclusive: %v", rep)
	}
	if !rep.Recovered {
		t.Fatalf("cluster did not recover after faults healed: %v", rep)
	}
	if rep.OK < 100 {
		t.Fatalf("too few successful ops (%d) to be meaningful", rep.OK)
	}
}

// TestShardedLinearizableUnderFaults runs the same check across two replica
// groups of 3, routed by the consistent-hash ring.
func TestShardedLinearizableUnderFaults(t *testing.T) {
	rep, err := Run(Options{
		Cluster: cluster.Options{Groups: 2, NodesPerGroup: 3, MaxRaftState: 64 << 10,
			ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond},
		Duration: duration(t, 20*time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rep)
	if !rep.Check.OK || rep.Check.TimedOut || !rep.Recovered {
		t.Fatalf("failed: %v", rep)
	}
}

// TestCheckerCatchesStaleReads is a negative control. It disables ReadIndex
// (reads served from any replica's local state) and expects the checker to
// find a violation. If this test ever passes with a clean history, the
// harness is not exercising what we think it is.
func TestCheckerCatchesStaleReads(t *testing.T) {
	for attempt := 0; attempt < 3; attempt++ {
		rep, err := Run(Options{
			Cluster: cluster.Options{Groups: 1, NodesPerGroup: 5, UnsafeLocalReads: true,
				ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond},
			Faults:   []string{"partition", "isolate-leader", "lossy-net"},
			Duration: 10 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Log(rep)
		if !rep.Check.OK {
			return // violation detected, as expected
		}
	}
	t.Fatal("checker never detected stale reads with ReadIndex disabled")
}
