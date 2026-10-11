package raft_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"raftkv/raft"
	"raftkv/transport/inmem"
)

// harness runs an in-process cluster on the simulated network and checks, on
// every applied entry, that no two nodes ever apply different commands at the
// same index (State Machine Safety).
type harness struct {
	t        *testing.T
	net      *inmem.Network
	ids      []string
	members  []raft.Member
	nodes    map[string]*raft.Node
	stores   map[string]*raft.MemStorage
	snapEach int // >0: state machine snapshots every snapEach entries

	mu        sync.Mutex
	committed map[uint64][]byte          // index -> command (first applier wins)
	applied   map[string]uint64          // per-node last applied index
	logs      map[string]map[uint64]bool // per-node set of applied indices
}

func newHarness(t *testing.T, n int, snapEach int) *harness {
	h := &harness{t: t, net: inmem.New(), nodes: map[string]*raft.Node{}, stores: map[string]*raft.MemStorage{},
		snapEach: snapEach, committed: map[uint64][]byte{}, applied: map[string]uint64{}, logs: map[string]map[uint64]bool{}}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		h.ids = append(h.ids, id)
		h.members = append(h.members, raft.Member{ID: id})
	}
	for _, id := range h.ids {
		h.stores[id] = raft.NewMemStorage()
		h.start(id, h.members)
	}
	t.Cleanup(h.shutdown)
	return h
}

func (h *harness) start(id string, bootstrap []raft.Member) {
	ch := make(chan raft.ApplyMsg, 256)
	node, err := raft.NewNode(raft.Config{
		ID: id, Bootstrap: bootstrap, Storage: h.stores[id], ApplyCh: ch,
		Transport:       &inmem.RaftTransport{Net: h.net, From: id},
		ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.mu.Lock()
	h.nodes[id] = node
	h.applied[id] = 0
	h.logs[id] = map[uint64]bool{}
	h.mu.Unlock()
	h.net.Register(id, func(ctx context.Context, method string, args any) (any, error) {
		r, err, _ := inmem.DispatchRaft(node, method, args)
		return r, err
	})
	go h.consume(id, node, ch)
}

func (h *harness) consume(id string, node *raft.Node, ch chan raft.ApplyMsg) {
	for msg := range ch {
		h.mu.Lock()
		if msg.Snapshot != nil {
			h.applied[id] = msg.Snapshot.Index
			h.mu.Unlock()
			continue
		}
		e := msg.Entry
		if e.Index != h.applied[id]+1 {
			h.t.Errorf("%s applied index %d out of order (last %d)", id, e.Index, h.applied[id])
		}
		h.applied[id] = e.Index
		if e.Type == raft.EntryNormal {
			if prev, ok := h.committed[e.Index]; ok && !bytes.Equal(prev, e.Data) {
				h.t.Errorf("SAFETY VIOLATION: index %d applied as %q on one node and %q on %s", e.Index, prev, e.Data, id)
			}
			h.committed[e.Index] = e.Data
		}
		h.logs[id][e.Index] = true
		snap := h.snapEach > 0 && e.Index%uint64(h.snapEach) == 0
		h.mu.Unlock()
		if snap {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], e.Index)
			node.Snapshot(e.Index, b[:])
		}
	}
}

func (h *harness) crash(id string) {
	h.net.SetUp(id, false)
	h.mu.Lock()
	n := h.nodes[id]
	h.mu.Unlock()
	n.Stop()
}

func (h *harness) restart(id string) {
	h.start(id, h.members)
}

func (h *harness) disconnect(id string) {
	var rest []string
	for _, x := range h.ids {
		if x != id {
			rest = append(rest, x)
		}
	}
	h.net.Partition([]string{id}, rest)
}

func (h *harness) shutdown() {
	for _, n := range h.nodes {
		n.Stop()
	}
}

// checkOneLeader waits for exactly one leader in the highest term among up nodes.
func (h *harness) checkOneLeader(among []string) string {
	for iter := 0; iter < 30; iter++ {
		time.Sleep(100 * time.Millisecond)
		leaders := map[uint64][]string{}
		var top uint64
		for _, id := range among {
			st := h.nodes[id].Status()
			if st.State == "leader" {
				leaders[st.Term] = append(leaders[st.Term], id)
				if st.Term > top {
					top = st.Term
				}
			}
		}
		for term, ls := range leaders {
			if len(ls) > 1 {
				h.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
		}
		if len(leaders) > 0 {
			return leaders[top][0]
		}
	}
	h.t.Fatalf("no leader elected")
	return ""
}

func (h *harness) nCommitted(idx uint64) (int, []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, l := range h.logs {
		if l[idx] {
			count++
		}
	}
	return count, h.committed[idx]
}

// one submits cmd and waits until at least expected nodes applied it.
func (h *harness) one(cmd string, expected int, among []string) uint64 {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range among {
			idx, _, ok := h.nodes[id].Start([]byte(cmd))
			if !ok {
				continue
			}
			for t0 := time.Now(); time.Since(t0) < 2*time.Second; {
				if c, v := h.nCommitted(idx); c >= expected && string(v) == cmd {
					return idx
				}
				time.Sleep(20 * time.Millisecond)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("one(%q) failed to reach agreement", cmd)
	return 0
}

// ---------------------------------------------------------------------------

func TestInitialElection(t *testing.T) {
	h := newHarness(t, 3, 0)
	l := h.checkOneLeader(h.ids)
	term := h.nodes[l].Status().Term
	time.Sleep(1 * time.Second)
	if t2 := h.nodes[h.checkOneLeader(h.ids)].Status().Term; t2 != term {
		t.Fatalf("term changed with no failures: %d -> %d", term, t2)
	}
}

func TestReElection(t *testing.T) {
	h := newHarness(t, 3, 0)
	l1 := h.checkOneLeader(h.ids)
	h.disconnect(l1)
	var rest []string
	for _, id := range h.ids {
		if id != l1 {
			rest = append(rest, id)
		}
	}
	l2 := h.checkOneLeader(rest)
	if l2 == l1 {
		t.Fatal("old leader still leading")
	}
	// Old leader must have stepped down via CheckQuorum.
	time.Sleep(500 * time.Millisecond)
	if h.nodes[l1].Status().State == "leader" {
		t.Fatal("partitioned leader did not step down")
	}
	h.net.Heal()
	h.checkOneLeader(h.ids)

	// No quorum -> no leader.
	h.net.Partition([]string{h.ids[0]}, []string{h.ids[1]}, []string{h.ids[2]})
	time.Sleep(1 * time.Second)
	for _, id := range h.ids {
		if h.nodes[id].Status().State == "leader" {
			t.Fatalf("%s is leader without a quorum", id)
		}
	}
	h.net.Heal()
	h.checkOneLeader(h.ids)
}

func TestBasicAgreeAndFollowerFailure(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.checkOneLeader(h.ids)
	for i := 0; i < 5; i++ {
		h.one(fmt.Sprintf("a%d", i), 3, h.ids)
	}
	l := h.checkOneLeader(h.ids)
	f := h.ids[0]
	if f == l {
		f = h.ids[1]
	}
	h.disconnect(f)
	var rest []string
	for _, id := range h.ids {
		if id != f {
			rest = append(rest, id)
		}
	}
	for i := 0; i < 5; i++ {
		h.one(fmt.Sprintf("b%d", i), 2, rest)
	}
	h.net.Heal()
	h.one("c", 3, h.ids) // the lagging follower catches up
}

func TestNoAgreementWithoutMajority(t *testing.T) {
	h := newHarness(t, 5, 0)
	l := h.checkOneLeader(h.ids)
	h.one("x", 5, h.ids)
	var others []string
	for _, id := range h.ids {
		if id != l {
			others = append(others, id)
		}
	}
	// Leader + one follower on one side, three on the other.
	h.net.Partition([]string{l, others[0]}, others[1:])
	idx, _, ok := h.nodes[l].Start([]byte("lost"))
	if ok {
		time.Sleep(1 * time.Second)
		// The majority side may commit its own entries at idx; what must never
		// happen is our "lost" command being applied anywhere.
		if c, v := h.nCommitted(idx); c > 0 && string(v) == "lost" {
			t.Fatalf("entry committed without a majority")
		}
	}
	h.net.Heal()
	h.one("after", 5, h.ids)
}

func TestPersistenceCrashAll(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.checkOneLeader(h.ids)
	idx := h.one("durable", 3, h.ids)
	for _, id := range h.ids {
		h.crash(id)
	}
	h.mu.Lock()
	h.logs = map[string]map[uint64]bool{}
	h.mu.Unlock()
	for _, id := range h.ids {
		h.restart(id)
	}
	h.checkOneLeader(h.ids)
	h.one("after-restart", 3, h.ids)
	if c, v := h.nCommitted(idx); c != 3 || string(v) != "durable" {
		t.Fatalf("entry %d not re-applied after restart: count=%d v=%q", idx, c, v)
	}
}

func TestSnapshotInstallAndRestart(t *testing.T) {
	h := newHarness(t, 3, 10)
	l := h.checkOneLeader(h.ids)
	lag := h.ids[0]
	if lag == l {
		lag = h.ids[1]
	}
	h.crash(lag)
	var rest []string
	for _, id := range h.ids {
		if id != lag {
			rest = append(rest, id)
		}
	}
	var last uint64
	for i := 0; i < 50; i++ {
		last = h.one(fmt.Sprintf("s%d", i), 2, rest)
	}
	if st := h.nodes[rest[0]].Status(); st.SnapIndex == 0 {
		t.Fatalf("expected log compaction, snapIndex=0")
	}
	h.restart(lag) // must catch up via InstallSnapshot: its missing entries are gone
	h.one("post-snap", 3, h.ids)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		a := h.applied[lag]
		h.mu.Unlock()
		if a >= last {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("lagging node never caught up to %d", last)
}

func TestFigure8Unreliable(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	h := newHarness(t, 5, 25)
	h.net.SetUnreliable(0.1, 0, 15*time.Millisecond)
	h.checkOneLeader(h.ids)
	rng := rand.New(rand.NewSource(1))
	down := map[string]bool{}
	for iter := 0; iter < 60; iter++ {
		for _, id := range h.ids {
			if !down[id] {
				h.nodes[id].Start([]byte(fmt.Sprintf("f%d", iter)))
			}
		}
		time.Sleep(time.Duration(rng.Intn(80)) * time.Millisecond)
		if rng.Intn(3) == 0 {
			// crash the leader, if any
			for _, id := range h.ids {
				if !down[id] && h.nodes[id].Status().State == "leader" {
					h.crash(id)
					down[id] = true
					break
				}
			}
		}
		if len(down) >= 3 || rng.Intn(4) == 0 {
			for id := range down {
				h.restart(id)
				delete(down, id)
				break
			}
		}
	}
	for id := range down {
		h.restart(id)
	}
	h.net.SetUnreliable(0, 0, 0)
	h.one("final", 5, h.ids)
}

func TestMembershipChanges(t *testing.T) {
	h := newHarness(t, 3, 20)
	l := h.checkOneLeader(h.ids)
	for i := 0; i < 30; i++ {
		h.one(fmt.Sprintf("m%d", i), 3, h.ids)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Add n4 and n5, which start empty and must catch up (via snapshot).
	for _, id := range []string{"n4", "n5"} {
		h.stores[id] = raft.NewMemStorage()
		h.start(id, nil)
		if err := h.nodes[h.checkOneLeader(h.ids)].AddServer(ctx, raft.Member{ID: id}); err != nil {
			t.Fatalf("AddServer(%s): %v", id, err)
		}
		h.ids = append(h.ids, id)
		h.members = append(h.members, raft.Member{ID: id})
	}
	if got := len(h.nodes[h.checkOneLeader(h.ids)].Status().Config.Members); got != 5 {
		t.Fatalf("config has %d members, want 5", got)
	}
	h.one("five", 5, h.ids)

	// Remove the current leader; it must step down and the rest elect a new one.
	l = h.checkOneLeader(h.ids)
	if err := h.nodes[l].RemoveServer(ctx, l); err != nil {
		t.Fatalf("RemoveServer(leader): %v", err)
	}
	var rest []string
	for _, id := range h.ids {
		if id != l {
			rest = append(rest, id)
		}
	}
	l2 := h.checkOneLeader(rest)
	if l2 == l {
		t.Fatal("removed leader still leading")
	}
	h.one("four", 4, rest)
	// The removed server must not disrupt the cluster (PreVote): term stays put.
	term := h.nodes[l2].Status().Term
	time.Sleep(1 * time.Second)
	if t2 := h.nodes[l2].Status().Term; t2 != term {
		t.Fatalf("removed server disrupted the cluster: term %d -> %d", term, t2)
	}
}

func TestReadIndexRequiresQuorum(t *testing.T) {
	h := newHarness(t, 5, 0)
	l := h.checkOneLeader(h.ids)
	h.one("r", 5, h.ids)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if _, err := h.nodes[l].ReadIndex(ctx); err != nil {
		t.Fatalf("ReadIndex on healthy leader: %v", err)
	}
	cancel()
	var others []string
	for _, id := range h.ids {
		if id != l {
			others = append(others, id)
		}
	}
	h.net.Partition([]string{l, others[0]}, others[1:])
	ctx, cancel = context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if _, err := h.nodes[l].ReadIndex(ctx); err == nil {
		t.Fatal("minority leader served a ReadIndex: stale read possible")
	}
}

func TestPreVotePreventsDisruption(t *testing.T) {
	h := newHarness(t, 3, 0)
	l := h.checkOneLeader(h.ids)
	term := h.nodes[l].Status().Term
	f := h.ids[0]
	if f == l {
		f = h.ids[1]
	}
	h.disconnect(f)
	time.Sleep(1500 * time.Millisecond) // many election timeouts for f
	h.net.Heal()
	time.Sleep(500 * time.Millisecond)
	if st := h.nodes[f].Status(); st.Term != term {
		t.Fatalf("isolated follower inflated its term %d -> %d despite PreVote", term, st.Term)
	}
	if h.checkOneLeader(h.ids) != l {
		t.Fatal("leadership changed after a follower rejoined")
	}
}

func TestFileStorageRecovery(t *testing.T) {
	dir := t.TempDir()
	fs, err := raft.OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := fs.Load(); err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(fs.Save(&raft.HardState{Term: 3, VotedFor: "n2"}, 0, []raft.Entry{{Index: 1, Term: 1, Data: []byte("a")}, {Index: 2, Term: 1, Data: []byte("b")}, {Index: 3, Term: 2, Data: []byte("c")}}))
	must(fs.Save(nil, 3, []raft.Entry{{Index: 3, Term: 3, Data: []byte("C")}, {Index: 4, Term: 3, Data: []byte("d")}}))
	fs.Close()

	// Simulate a torn write at the tail.
	f, err := os.OpenFile(filepath.Join(dir, "wal"), os.O_APPEND|os.O_WRONLY, 0)
	must(err)
	f.Write([]byte{0x10, 0, 0, 0, 0xde, 0xad})
	f.Close()

	fs, _ = raft.OpenFileStorage(dir)
	hs, snap, es, err := fs.Load()
	must(err)
	if hs.Term != 3 || hs.VotedFor != "n2" || snap != nil || len(es) != 4 || string(es[2].Data) != "C" || es[2].Term != 3 {
		t.Fatalf("bad recovery: hs=%+v snap=%v entries=%+v", hs, snap, es)
	}
	must(fs.Save(nil, 0, []raft.Entry{{Index: 5, Term: 3, Data: []byte("e")}}))
	must(fs.SaveSnapshot(raft.HardState{Term: 4}, &raft.Snapshot{Index: 4, Term: 3, Config: raft.Configuration{Members: []raft.Member{{ID: "n1"}}}, Data: []byte("state")},
		[]raft.Entry{{Index: 5, Term: 3, Data: []byte("e")}}))
	must(fs.Save(nil, 0, []raft.Entry{{Index: 6, Term: 4, Data: []byte("f")}}))
	fs.Close()

	fs, _ = raft.OpenFileStorage(dir)
	hs, snap, es, err = fs.Load()
	must(err)
	if hs.Term != 4 || snap == nil || snap.Index != 4 || string(snap.Data) != "state" || len(es) != 2 || es[1].Index != 6 {
		t.Fatalf("bad recovery after snapshot: hs=%+v snap=%+v entries=%+v", hs, snap, es)
	}
	fs.Close()
}
