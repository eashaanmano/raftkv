package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestKVBasic(t *testing.T) {
	c, err := New(Options{Groups: 1, NodesPerGroup: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	ck := c.Clerk()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(ck.Put(ctx(t), "a", "1"))
	must(ck.Append(ctx(t), "a", "2"))
	if v, err := ck.Get(ctx(t), "a"); err != nil || v != "12" {
		t.Fatalf("get a = %q, %v", v, err)
	}
	must(ck.Delete(ctx(t), "a"))
	if v, _ := ck.Get(ctx(t), "a"); v != "" {
		t.Fatalf("deleted key returned %q", v)
	}
}

func TestKVExactlyOnceAcrossLeaderCrash(t *testing.T) {
	c, err := New(Options{Groups: 1, NodesPerGroup: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	ck := c.Clerk()
	for i := 0; i < 20; i++ {
		if i == 10 {
			l, _ := c.WaitLeader("g1", 5*time.Second)
			c.Crash(l)
		}
		if err := ck.Append(ctx(t), "x", "."); err != nil {
			t.Fatal(err)
		}
	}
	v, err := ck.Get(ctx(t), "x")
	if err != nil || len(v) != 20 {
		t.Fatalf("expected 20 appends exactly once, got %d (%v)", len(v), err)
	}
}

func TestKVShardedSnapshotsAndRestart(t *testing.T) {
	c, err := New(Options{Groups: 2, NodesPerGroup: 3, DataDir: t.TempDir(), MaxRaftState: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	ck := c.Clerk()
	perGroup := map[string]int{}
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("key%d", i)
		perGroup[c.Ring.Lookup(k)]++
		if err := ck.Put(ctx(t), k, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	if len(perGroup) != 2 {
		t.Fatalf("keys not spread across groups: %v", perGroup)
	}
	compacted := false
	for _, id := range c.Nodes() {
		if c.Server(id).Raft().Status().SnapIndex > 0 {
			compacted = true
		}
	}
	if !compacted {
		t.Fatal("no node took a snapshot")
	}
	// Restart everything: state must come back from snapshot + WAL.
	for _, id := range c.Nodes() {
		c.Crash(id)
	}
	for _, id := range c.Nodes() {
		if err := c.Start(id); err != nil {
			t.Fatal(err)
		}
	}
	ck2 := c.Clerk()
	for i := 0; i < 300; i += 7 {
		k := fmt.Sprintf("key%d", i)
		if v, err := ck2.Get(ctx(t), k); err != nil || v != fmt.Sprint(i) {
			t.Fatalf("after restart %s = %q, %v", k, v, err)
		}
	}
}

func TestKVMembershipChange(t *testing.T) {
	c, err := New(Options{Groups: 1, NodesPerGroup: 3, MaxRaftState: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	ck := c.Clerk()
	for i := 0; i < 100; i++ {
		if err := ck.Put(ctx(t), fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.AddNode(ctx(t), "g1", "g1-n4"); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveNode(ctx(t), "g1-n1"); err != nil {
		t.Fatal(err)
	}
	c.Crash("g1-n1")
	if err := ck.Put(ctx(t), "after", "yes"); err != nil {
		t.Fatal(err)
	}
	// The new node must hold the data (it got it via snapshot + log).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st := c.Server("g1-n4").Raft().Status()
		if st.LastApplied >= c.Server(c.Leader("g1")).Raft().Status().CommitIndex {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("new member never caught up")
}
