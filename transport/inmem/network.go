// Package inmem is a simulated network for tests and the fault injector. It can
// partition nodes, crash them, drop messages, and delay (and therefore reorder)
// requests and replies — the failure modes Raft must tolerate.
package inmem

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/raft"
)

// Handler serves one endpoint's RPCs.
type Handler func(ctx context.Context, method string, args any) (any, error)

var ErrUnreachable = errors.New("inmem: endpoint unreachable")

type Network struct {
	mu        sync.Mutex
	handlers  map[string]Handler
	up        map[string]bool
	partition map[string]int // nil = fully connected; endpoints not listed reach everyone
	dropRate  float64
	minDelay  time.Duration
	maxDelay  time.Duration
	rng       *rand.Rand

	Sent, Dropped atomic.Int64
}

func New() *Network {
	return &Network{
		handlers: map[string]Handler{},
		up:       map[string]bool{},
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Register installs (or replaces) the handler for an endpoint and marks it up.
func (n *Network) Register(name string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[name] = h
	n.up[name] = true
}

// SetUp marks an endpoint as running (true) or crashed (false).
func (n *Network) SetUp(name string, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.up[name] = up
}

// Partition splits the listed endpoints into isolated groups. Endpoints not
// listed (e.g. clients) can still reach every group.
func (n *Network) Partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partition = map[string]int{}
	for i, g := range groups {
		for _, name := range g {
			n.partition[name] = i
		}
	}
}

// Heal removes all partitions.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partition = nil
}

// SetUnreliable sets the drop probability and the per-direction delay range.
func (n *Network) SetUnreliable(dropRate float64, minDelay, maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate, n.minDelay, n.maxDelay = dropRate, minDelay, maxDelay
}

func (n *Network) connected(a, b string) bool {
	if n.partition == nil {
		return true
	}
	ga, oka := n.partition[a]
	gb, okb := n.partition[b]
	return !oka || !okb || ga == gb
}

// hop decides the fate of one direction of a message.
func (n *Network) hop(from, to string) (deliver bool, delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.connected(from, to) || n.rng.Float64() < n.dropRate {
		return false, 0
	}
	if n.maxDelay > n.minDelay {
		delay = n.minDelay + time.Duration(n.rng.Int63n(int64(n.maxDelay-n.minDelay)))
	} else {
		delay = n.minDelay
	}
	return true, delay
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Call sends an RPC from one endpoint to another. A dropped or partitioned
// message behaves like a real lost packet: the caller waits for its timeout.
func (n *Network) Call(ctx context.Context, from, to, method string, args any) (any, error) {
	n.Sent.Add(1)
	n.mu.Lock()
	h := n.handlers[to]
	alive := h != nil && n.up[to]
	if fromRegistered := n.handlers[from] != nil; fromRegistered && !n.up[from] {
		alive = false // a crashed node cannot send
	}
	n.mu.Unlock()
	if !alive {
		return nil, ErrUnreachable // connection refused: fails fast
	}
	ok, delay := n.hop(from, to)
	if !ok {
		n.Dropped.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := sleepCtx(ctx, delay); err != nil {
		return nil, err
	}
	type result struct {
		v   any
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := h(ctx, method, args)
		ch <- result{v, err}
	}()
	var r result
	select {
	case r = <-ch:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	n.mu.Lock()
	stillUp := n.up[to]
	n.mu.Unlock()
	if !stillUp {
		// Crashed mid-request. Modelled as a process crash (kill -9): the OS
		// resets the connection, so the caller learns immediately. A powered-off
		// machine would instead look like a dropped reply (timeout).
		return nil, ErrUnreachable
	}
	ok, delay = n.hop(to, from)
	if !ok {
		n.Dropped.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := sleepCtx(ctx, delay); err != nil {
		return nil, err
	}
	return r.v, r.err
}

// ---------------------------------------------------------------------------
// Raft adapter
// ---------------------------------------------------------------------------

// RaftTransport implements raft.Transport over the simulated network.
type RaftTransport struct {
	Net  *Network
	From string
}

func (t *RaftTransport) RequestVote(ctx context.Context, to raft.Member, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	v, err := t.Net.Call(ctx, t.From, to.ID, "Raft.RequestVote", a)
	if err != nil {
		return nil, err
	}
	return v.(*raft.RequestVoteReply), nil
}

func (t *RaftTransport) AppendEntries(ctx context.Context, to raft.Member, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	v, err := t.Net.Call(ctx, t.From, to.ID, "Raft.AppendEntries", a)
	if err != nil {
		return nil, err
	}
	return v.(*raft.AppendEntriesReply), nil
}

func (t *RaftTransport) InstallSnapshot(ctx context.Context, to raft.Member, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	v, err := t.Net.Call(ctx, t.From, to.ID, "Raft.InstallSnapshot", a)
	if err != nil {
		return nil, err
	}
	return v.(*raft.InstallSnapshotReply), nil
}

// DispatchRaft routes a Raft RPC to a node. handled=false for non-Raft methods.
func DispatchRaft(node *raft.Node, method string, args any) (reply any, err error, handled bool) {
	switch method {
	case "Raft.RequestVote":
		r, err := node.HandleRequestVote(args.(*raft.RequestVoteArgs))
		return r, err, true
	case "Raft.AppendEntries":
		r, err := node.HandleAppendEntries(args.(*raft.AppendEntriesArgs))
		return r, err, true
	case "Raft.InstallSnapshot":
		r, err := node.HandleInstallSnapshot(args.(*raft.InstallSnapshotArgs))
		return r, err, true
	}
	return nil, nil, false
}
