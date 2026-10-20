// Package cluster runs a complete sharded RaftKV deployment inside one process
// on the simulated network. The fault injector, the benchmark's in-process
// mode and the KV tests all build on it.
package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/kv"
	"raftkv/raft"
	"raftkv/shard"
	"raftkv/transport/inmem"
)

type Options struct {
	Groups        int    // replica groups (default 1)
	NodesPerGroup int    // servers per group (default 3)
	DataDir       string // if set, nodes use the file WAL here; otherwise in-memory storage
	MaxRaftState  int    // snapshot threshold in bytes (0 = never)
	MaxBatch      int

	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration

	UnsafeLocalReads bool // negative control, see kv.ServerConfig
	Logf             func(string, ...any)
}

type Cluster struct {
	Net    *inmem.Network
	Ring   *shard.Ring
	Groups map[string][]raft.Member
	opts   Options

	mu       sync.Mutex
	servers  map[string]*kv.Server
	mem      map[string]*raft.MemStorage
	clocks   map[string]*SkewClock
	groupOf  map[string]string
	booted   map[string]bool // part of the initial configuration
	clientNo atomic.Int64
}

func New(opts Options) (*Cluster, error) {
	if opts.Groups == 0 {
		opts.Groups = 1
	}
	if opts.NodesPerGroup == 0 {
		opts.NodesPerGroup = 3
	}
	c := &Cluster{
		Net: inmem.New(), Groups: map[string][]raft.Member{}, opts: opts,
		servers: map[string]*kv.Server{}, mem: map[string]*raft.MemStorage{},
		clocks: map[string]*SkewClock{}, groupOf: map[string]string{}, booted: map[string]bool{},
	}
	var groups []string
	for g := 1; g <= opts.Groups; g++ {
		name := fmt.Sprintf("g%d", g)
		groups = append(groups, name)
		for i := 1; i <= opts.NodesPerGroup; i++ {
			id := fmt.Sprintf("%s-n%d", name, i)
			c.Groups[name] = append(c.Groups[name], raft.Member{ID: id, Addr: id})
			c.groupOf[id] = name
			c.booted[id] = true
		}
	}
	c.Ring = shard.NewRing(groups, shard.DefaultVNodes)
	for _, id := range c.Nodes() {
		if err := c.Start(id); err != nil {
			c.Shutdown()
			return nil, err
		}
	}
	return c, nil
}

// Nodes returns every node ID, sorted.
func (c *Cluster) Nodes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ids []string
	for id := range c.groupOf {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (c *Cluster) GroupOf(id string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groupOf[id]
}

// Start (re)starts a node from whatever its storage holds.
func (c *Cluster) Start(id string) error {
	c.mu.Lock()
	group := c.groupOf[id]
	var storage raft.Storage
	if c.opts.DataDir != "" {
		fs, err := raft.OpenFileStorage(filepath.Join(c.opts.DataDir, id))
		if err != nil {
			c.mu.Unlock()
			return err
		}
		storage = fs
	} else {
		if c.mem[id] == nil {
			c.mem[id] = raft.NewMemStorage()
		}
		storage = c.mem[id]
	}
	if c.clocks[id] == nil {
		c.clocks[id] = NewSkewClock()
	}
	var bootstrap []raft.Member
	if c.booted[id] {
		bootstrap = c.Groups[group]
	}
	clock := c.clocks[id]
	c.mu.Unlock()

	srv, err := kv.NewServer(kv.ServerConfig{
		Raft: raft.Config{
			ID: id, Bootstrap: bootstrap, Storage: storage, Clock: clock,
			Transport:         &inmem.RaftTransport{Net: c.Net, From: id},
			ElectionTimeout:   c.opts.ElectionTimeout,
			HeartbeatInterval: c.opts.HeartbeatInterval,
			Logf:              c.opts.Logf,
		},
		Group: group, Ring: c.Ring, MaxRaftState: c.opts.MaxRaftState, MaxBatch: c.opts.MaxBatch,
		UnsafeLocalReads: c.opts.UnsafeLocalReads,
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.servers[id] = srv
	c.mu.Unlock()
	c.Net.Register(id, func(ctx context.Context, method string, args any) (any, error) {
		if r, err, ok := inmem.DispatchRaft(srv.Raft(), method, args); ok {
			return r, err
		}
		switch method {
		case "KV.Get":
			return srv.Get(ctx, args.(*kv.GetArgs))
		case "KV.Write":
			return srv.Write(ctx, args.(*kv.WriteArgs))
		}
		return nil, fmt.Errorf("unknown method %s", method)
	})
	return nil
}

// Crash stops a node abruptly. Its storage survives, as a disk would.
func (c *Cluster) Crash(id string) {
	c.Net.SetUp(id, false)
	c.mu.Lock()
	srv := c.servers[id]
	delete(c.servers, id)
	c.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
}

func (c *Cluster) Server(id string) *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[id]
}

func (c *Cluster) Clock(id string) *SkewClock {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clocks[id]
}

// Leader returns the current leader of group, or "" if none is known.
func (c *Cluster) Leader(group string) string {
	var best string
	var bestTerm uint64
	for _, id := range c.Nodes() {
		if c.GroupOf(id) != group {
			continue
		}
		if s := c.Server(id); s != nil {
			if term, ok := s.Raft().IsLeader(); ok && term >= bestTerm {
				best, bestTerm = id, term
			}
		}
	}
	return best
}

// WaitLeader polls until group has a leader.
func (c *Cluster) WaitLeader(group string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(group); l != "" {
			return l, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader in %s after %v", group, timeout)
}

// AddNode creates an empty server and adds it to group via a membership change.
func (c *Cluster) AddNode(ctx context.Context, group, id string) error {
	c.mu.Lock()
	c.groupOf[id] = group
	c.mu.Unlock()
	if err := c.Start(id); err != nil {
		return err
	}
	for ctx.Err() == nil {
		if l := c.Leader(group); l != "" {
			err := c.Server(l).Raft().AddServer(ctx, raft.Member{ID: id, Addr: id})
			if err == nil {
				c.mu.Lock()
				c.Groups[group] = append(c.Groups[group], raft.Member{ID: id, Addr: id})
				c.mu.Unlock()
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ctx.Err()
}

// RemoveNode removes id from its group's configuration (the server keeps running).
func (c *Cluster) RemoveNode(ctx context.Context, id string) error {
	group := c.GroupOf(id)
	for ctx.Err() == nil {
		if l := c.Leader(group); l != "" {
			if err := c.Server(l).Raft().RemoveServer(ctx, id); err == nil {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ctx.Err()
}

// Clerk returns a new client whose requests come from its own endpoint.
func (c *Cluster) Clerk() *kv.Clerk {
	name := fmt.Sprintf("client-%d", c.clientNo.Add(1))
	c.mu.Lock()
	groups := map[string][]raft.Member{}
	for g, ms := range c.Groups {
		groups[g] = append([]raft.Member(nil), ms...)
	}
	c.mu.Unlock()
	return kv.NewClerk(&caller{net: c.Net, from: name}, c.Ring, groups)
}

func (c *Cluster) Shutdown() {
	for _, id := range c.Nodes() {
		c.Crash(id)
	}
}

// CallerFor returns a kv.Caller that sends requests over the cluster's
// simulated network from a fresh client endpoint.
func CallerFor(c *Cluster) kv.Caller {
	return &caller{net: c.Net, from: fmt.Sprintf("client-%d", c.clientNo.Add(1))}
}

type caller struct {
	net  *inmem.Network
	from string
}

func (k *caller) Get(ctx context.Context, to raft.Member, a *kv.GetArgs) (*kv.GetReply, error) {
	v, err := k.net.Call(ctx, k.from, to.ID, "KV.Get", a)
	if err != nil {
		return nil, err
	}
	return v.(*kv.GetReply), nil
}

func (k *caller) Write(ctx context.Context, to raft.Member, a *kv.WriteArgs) (*kv.WriteReply, error) {
	v, err := k.net.Call(ctx, k.from, to.ID, "KV.Write", a)
	if err != nil {
		return nil, err
	}
	return v.(*kv.WriteReply), nil
}

// ---------------------------------------------------------------------------
// SkewClock: a per-node clock whose rate and offset the nemesis can change.
// ---------------------------------------------------------------------------

type SkewClock struct {
	mu         sync.Mutex
	anchorReal time.Time
	anchorVirt time.Time
	rate       float64
}

func NewSkewClock() *SkewClock {
	now := time.Now()
	return &SkewClock{anchorReal: now, anchorVirt: now, rate: 1}
}

func (s *SkewClock) nowLocked() time.Time {
	elapsed := time.Since(s.anchorReal)
	return s.anchorVirt.Add(time.Duration(float64(elapsed) * s.rate))
}

func (s *SkewClock) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nowLocked()
}

func (s *SkewClock) After(d time.Duration) <-chan time.Time {
	s.mu.Lock()
	r := s.rate
	s.mu.Unlock()
	return time.After(time.Duration(float64(d) / r))
}

// SetRate makes this node's clock run at rate × real time (e.g. 2 = twice as fast).
func (s *SkewClock) SetRate(rate float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.anchorVirt = s.nowLocked()
	s.anchorReal = time.Now()
	s.rate = rate
}

// Jump moves this node's clock by d (may be negative).
func (s *SkewClock) Jump(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.anchorVirt = s.nowLocked().Add(d)
	s.anchorReal = time.Now()
}

func (s *SkewClock) Reset() { s.SetRate(1) }
