package kv

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"time"

	"raftkv/raft"
	"raftkv/shard"
)

type ServerConfig struct {
	Raft  raft.Config // ApplyCh is set by NewServer
	Group string
	Ring  *shard.Ring // nil: this group owns every key
	// MaxRaftState triggers a snapshot when the Raft log exceeds this many
	// bytes. 0 disables snapshots.
	MaxRaftState int
	// MaxBatch bounds how many client writes are group-committed per fsync.
	MaxBatch int
	// UnsafeLocalReads serves Gets from local state on ANY replica, skipping
	// ReadIndex. It exists only as a negative control: the fault-injection
	// test turns it on to prove the linearizability checker catches stale reads.
	UnsafeLocalReads bool
}

type proposal struct {
	cmd  command
	done chan Err
}

type waiter struct {
	p    *proposal
	term uint64
}

type dedupEntry struct {
	Seq uint64
}

type Server struct {
	cfg ServerConfig
	rf  *raft.Node

	mu          sync.Mutex
	appliedCond *sync.Cond
	data        map[string]string
	dedup       map[uint64]dedupEntry
	lastApplied uint64
	waiters     map[uint64]*waiter
	sinceCheck  int

	proposeCh chan *proposal
	applyCh   chan raft.ApplyMsg
	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 1024
	}
	s := &Server{
		cfg:       cfg,
		data:      map[string]string{},
		dedup:     map[uint64]dedupEntry{},
		waiters:   map[uint64]*waiter{},
		proposeCh: make(chan *proposal, 4096),
		applyCh:   make(chan raft.ApplyMsg, 4096),
		stopCh:    make(chan struct{}),
	}
	s.appliedCond = sync.NewCond(&s.mu)
	cfg.Raft.ApplyCh = s.applyCh
	rf, err := raft.NewNode(cfg.Raft)
	if err != nil {
		return nil, err
	}
	s.rf = rf
	s.wg.Add(3)
	go s.applyLoop()
	go s.proposer()
	go s.sweeper()
	return s, nil
}

func (s *Server) Raft() *raft.Node { return s.rf }

func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.rf.Stop()
		s.mu.Lock()
		for idx, w := range s.waiters {
			w.p.done <- ErrShutdown
			delete(s.waiters, idx)
		}
		s.appliedCond.Broadcast()
		s.mu.Unlock()
		s.wg.Wait()
	})
}

func (s *Server) owns(key string) bool {
	return s.cfg.Ring == nil || s.cfg.Ring.Lookup(key) == s.cfg.Group
}

func (s *Server) leaderHint() (string, string) {
	m := s.rf.LeaderHint()
	return m.ID, m.Addr
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

func (s *Server) Get(ctx context.Context, args *GetArgs) (*GetReply, error) {
	if !s.owns(args.Key) {
		return &GetReply{Err: ErrWrongGroup}, nil
	}
	if s.cfg.UnsafeLocalReads {
		s.mu.Lock()
		defer s.mu.Unlock()
		return &GetReply{Value: s.data[args.Key]}, nil
	}
	idx, err := s.rf.ReadIndex(ctx)
	if err != nil {
		r := &GetReply{Err: ErrWrongLeader}
		if ctx.Err() != nil {
			r.Err = ErrTimeout
		}
		r.LeaderID, r.LeaderAddr = s.leaderHint()
		return r, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.waitApplied(ctx, idx); err != nil {
		return &GetReply{Err: ErrTimeout}, nil
	}
	return &GetReply{Value: s.data[args.Key]}, nil
}

func (s *Server) waitApplied(ctx context.Context, idx uint64) error {
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.appliedCond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	for s.lastApplied < idx {
		select {
		case <-s.stopCh:
			return errors.New("stopped")
		default:
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.appliedCond.Wait()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

func (s *Server) Write(ctx context.Context, args *WriteArgs) (*WriteReply, error) {
	if !s.owns(args.Key) {
		return &WriteReply{Err: ErrWrongGroup}, nil
	}
	s.mu.Lock()
	if d, ok := s.dedup[args.ClientID]; ok && args.Seq <= d.Seq {
		s.mu.Unlock()
		return &WriteReply{}, nil // already applied: a retry of a write that succeeded
	}
	s.mu.Unlock()
	if _, leader := s.rf.IsLeader(); !leader {
		r := &WriteReply{Err: ErrWrongLeader}
		r.LeaderID, r.LeaderAddr = s.leaderHint()
		return r, nil
	}
	p := &proposal{
		cmd:  command{Op: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, Seq: args.Seq},
		done: make(chan Err, 1),
	}
	select {
	case s.proposeCh <- p:
	case <-ctx.Done():
		return &WriteReply{Err: ErrTimeout}, nil
	case <-s.stopCh:
		return &WriteReply{Err: ErrShutdown}, nil
	}
	select {
	case e := <-p.done:
		r := &WriteReply{Err: e}
		if e == ErrWrongLeader {
			r.LeaderID, r.LeaderAddr = s.leaderHint()
		}
		return r, nil
	case <-ctx.Done():
		return &WriteReply{Err: ErrTimeout}, nil
	}
}

// proposer drains queued writes and appends them to Raft in batches: one
// fsync and one replication round for many client requests (group commit).
// Under load the queue fills while the previous batch is being fsynced, so
// batches grow automatically with concurrency.
func (s *Server) proposer() {
	defer s.wg.Done()
	batch := make([]*proposal, 0, s.cfg.MaxBatch)
	cmds := make([][]byte, 0, s.cfg.MaxBatch)
	for {
		batch, cmds = batch[:0], cmds[:0]
		select {
		case p := <-s.proposeCh:
			batch = append(batch, p)
		case <-s.stopCh:
			return
		}
	drain:
		for len(batch) < s.cfg.MaxBatch {
			select {
			case p := <-s.proposeCh:
				batch = append(batch, p)
			default:
				break drain
			}
		}
		for _, p := range batch {
			cmds = append(cmds, encodeCommand(p.cmd))
		}
		// Hold s.mu across StartBatch so the apply loop cannot apply these
		// entries before their waiters are registered.
		s.mu.Lock()
		first, term, ok := s.rf.StartBatch(cmds)
		if ok {
			for i, p := range batch {
				s.waiters[first+uint64(i)] = &waiter{p: p, term: term}
			}
		}
		s.mu.Unlock()
		if !ok {
			for _, p := range batch {
				p.done <- ErrWrongLeader
			}
		}
	}
}

// sweeper fails waiters promptly when leadership is lost, so clients retry
// against the new leader instead of waiting out their timeout.
func (s *Server) sweeper() {
	defer s.wg.Done()
	t := time.NewTicker(25 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
		}
		term, leader := s.rf.IsLeader()
		s.mu.Lock()
		for idx, w := range s.waiters {
			if !leader || w.term != term {
				w.p.done <- ErrWrongLeader
				delete(s.waiters, idx)
			}
		}
		s.mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// Applying committed entries
// ---------------------------------------------------------------------------

func (s *Server) applyLoop() {
	defer s.wg.Done()
	for {
		var msg raft.ApplyMsg
		select {
		case msg = <-s.applyCh:
		case <-s.stopCh:
			return
		}
		s.mu.Lock()
		if snap := msg.Snapshot; snap != nil {
			if snap.Index > s.lastApplied {
				s.restore(snap.Data)
				s.lastApplied = snap.Index
				for idx, w := range s.waiters {
					if idx <= snap.Index {
						w.p.done <- ErrWrongLeader // outcome unknown; client retries, dedup makes it safe
						delete(s.waiters, idx)
					}
				}
			}
		} else if e := msg.Entry; e.Index > s.lastApplied {
			if e.Type == raft.EntryNormal {
				cmd, err := decodeCommand(e.Data)
				if err != nil {
					panic(fmt.Sprintf("kv: corrupt command at %d: %v", e.Index, err))
				}
				s.apply(cmd)
				if w := s.waiters[e.Index]; w != nil {
					delete(s.waiters, e.Index)
					if w.p.cmd.ClientID == cmd.ClientID && w.p.cmd.Seq == cmd.Seq {
						w.p.done <- OK
					} else {
						w.p.done <- ErrWrongLeader // a different leader's entry won this index
					}
				}
			}
			s.lastApplied = e.Index
		}
		s.appliedCond.Broadcast()
		s.maybeSnapshot()
		s.mu.Unlock()
	}
}

func (s *Server) apply(c command) {
	if d, ok := s.dedup[c.ClientID]; ok && c.Seq <= d.Seq {
		return // duplicate of an already-applied request
	}
	switch c.Op {
	case OpPut:
		s.data[c.Key] = c.Value
	case OpAppend:
		s.data[c.Key] += c.Value
	case OpDelete:
		delete(s.data, c.Key)
	}
	s.dedup[c.ClientID] = dedupEntry{Seq: c.Seq}
}

func (s *Server) maybeSnapshot() {
	if s.cfg.MaxRaftState <= 0 {
		return
	}
	if s.sinceCheck++; s.sinceCheck < 64 {
		return
	}
	s.sinceCheck = 0
	if s.rf.LogSize() >= s.cfg.MaxRaftState {
		s.rf.Snapshot(s.lastApplied, s.encode())
	}
}

type snapshotState struct {
	Data  map[string]string
	Dedup map[uint64]dedupEntry
}

func (s *Server) encode() []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshotState{Data: s.data, Dedup: s.dedup}); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (s *Server) restore(b []byte) {
	if len(b) == 0 {
		s.data, s.dedup = map[string]string{}, map[uint64]dedupEntry{}
		return
	}
	var st snapshotState
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&st); err != nil {
		panic(fmt.Sprintf("kv: corrupt snapshot: %v", err))
	}
	if st.Data == nil {
		st.Data = map[string]string{}
	}
	if st.Dedup == nil {
		st.Dedup = map[uint64]dedupEntry{}
	}
	s.data, s.dedup = st.Data, st.Dedup
}

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------

func (s *Server) AddServer(ctx context.Context, a *AddServerArgs) (*AdminReply, error) {
	return s.adminReply(s.rf.AddServer(ctx, raft.Member{ID: a.ID, Addr: a.Addr})), nil
}

func (s *Server) RemoveServer(ctx context.Context, a *RemoveServerArgs) (*AdminReply, error) {
	return s.adminReply(s.rf.RemoveServer(ctx, a.ID)), nil
}

func (s *Server) Status(ctx context.Context, _ *StatusArgs) (*raft.Status, error) {
	st := s.rf.Status()
	return &st, nil
}

func (s *Server) adminReply(err error) *AdminReply {
	r := &AdminReply{}
	if err != nil {
		r.Err = err.Error()
		r.LeaderID, r.LeaderAddr = s.leaderHint()
	}
	return r
}
