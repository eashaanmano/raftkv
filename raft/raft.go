package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Config configures a Node.
type Config struct {
	ID string
	// Bootstrap is the initial configuration for a brand-new cluster. Leave nil
	// for a server that will be added later with AddServer. Ignored if the
	// node already has persisted state.
	Bootstrap []Member
	Transport Transport
	Storage   Storage
	ApplyCh   chan<- ApplyMsg
	Clock     Clock

	ElectionTimeout   time.Duration // base; each timeout is random in [T, 2T). Default 300ms.
	HeartbeatInterval time.Duration // default 50ms
	RPCTimeout        time.Duration // default 250ms
	MaxEntriesPerMsg  int           // default 4096

	DisablePreVote     bool
	DisableCheckQuorum bool
	Logf               func(format string, args ...any)
}

type replicator struct {
	member  Member
	trigger chan struct{}
	stop    chan struct{}
}

// Node is one Raft server. All exported methods are safe for concurrent use.
type Node struct {
	mu      sync.Mutex
	cfg     Config
	id      string
	rng     *rand.Rand
	stopped bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	state    State
	term     uint64
	votedFor string
	leaderID string

	// log[0] is a sentinel holding the snapshot's index/term; real entries follow.
	log      []Entry
	logBytes int
	snap     *Snapshot

	commitIndex uint64
	lastApplied uint64
	pendingSnap *Snapshot // installed from the leader, not yet delivered on ApplyCh

	config      Configuration // latest configuration in the log (used immediately, per §4.1)
	configIndex uint64
	snapConfig  Configuration

	electionDeadline  time.Time
	lastLeaderContact time.Time

	// Leader-only state.
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	lastAck     map[string]time.Time
	ackSeq      map[string]uint64
	hbSeq       uint64
	noopIndex   uint64
	leaderSince time.Time
	repl        map[string]*replicator
	catchup     map[string]Member

	applyCond *sync.Cond
	stateCond *sync.Cond
}

// NewNode restores persisted state and starts the node's background goroutines.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Clock == nil {
		cfg.Clock = SystemClock
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 300 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = 250 * time.Millisecond
	}
	if cfg.MaxEntriesPerMsg == 0 {
		cfg.MaxEntriesPerMsg = 4096
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	n := &Node{
		cfg:    cfg,
		id:     cfg.ID,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(len(cfg.ID))*7919)),
		stopCh: make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.stateCond = sync.NewCond(&n.mu)

	hs, snap, entries, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}
	n.term, n.votedFor = hs.Term, hs.VotedFor
	n.log = []Entry{{}}
	if snap != nil {
		n.snap = snap
		n.log[0] = Entry{Index: snap.Index, Term: snap.Term}
		n.snapConfig = snap.Config.Clone()
		n.commitIndex = snap.Index
		n.pendingSnap = snap // state machine must be restored from it on startup
	} else if len(entries) == 0 && len(cfg.Bootstrap) > 0 {
		// Brand-new cluster: every bootstrap member writes the identical
		// configuration entry at index 1, term 0, so the initial membership is
		// durable and replicates to servers added later (same as etcd).
		boot := Entry{Index: 1, Term: 0, Type: EntryConfig,
			Data: encodeConfig(Configuration{Members: append([]Member(nil), cfg.Bootstrap...)})}
		entries = []Entry{boot}
		if err := cfg.Storage.Save(nil, 0, entries); err != nil {
			return nil, fmt.Errorf("raft: bootstrap: %w", err)
		}
	}
	n.log = append(n.log, entries...)
	for _, e := range entries {
		n.logBytes += len(e.Data) + 32
	}
	n.recomputeConfig()
	n.resetElectionTimer()

	n.wg.Add(2)
	go n.ticker()
	go n.applier()
	return n, nil
}

// Stop halts the node. It does not wait for in-flight RPCs to the node.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.stopReplicators()
	n.state = Follower
	n.applyCond.Broadcast()
	n.stateCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
	n.cfg.Storage.Close()
}

// ---------------------------------------------------------------------------
// Log helpers (caller holds mu)
// ---------------------------------------------------------------------------

func (n *Node) snapIndex() uint64 { return n.log[0].Index }
func (n *Node) lastIndex() uint64 { return n.log[len(n.log)-1].Index }
func (n *Node) lastTerm() uint64  { return n.log[len(n.log)-1].Term }

// termAt returns the term of entry i, or 0 if i is unknown (compacted or beyond the end).
func (n *Node) termAt(i uint64) uint64 {
	if i < n.snapIndex() || i > n.lastIndex() {
		return 0
	}
	return n.log[i-n.snapIndex()].Term
}

func (n *Node) entry(i uint64) *Entry { return &n.log[i-n.snapIndex()] }

// entriesFrom copies up to max entries starting at index from.
func (n *Node) entriesFrom(from uint64, max int) []Entry {
	if from > n.lastIndex() {
		return nil
	}
	s := n.log[from-n.snapIndex():]
	if len(s) > max {
		s = s[:max]
	}
	return append([]Entry(nil), s...)
}

func (n *Node) mustSave(hs *HardState, truncateFrom uint64, entries []Entry) {
	if n.stopped {
		return
	}
	if err := n.cfg.Storage.Save(hs, truncateFrom, entries); err != nil {
		// Fail-stop: continuing after a failed fsync could violate safety.
		panic(fmt.Sprintf("raft %s: storage write failed: %v", n.id, err))
	}
}

func (n *Node) persistHardState() {
	n.mustSave(&HardState{Term: n.term, VotedFor: n.votedFor}, 0, nil)
}

func (n *Node) resetElectionTimer() {
	t := n.cfg.ElectionTimeout
	n.electionDeadline = n.cfg.Clock.Now().Add(t + time.Duration(n.rng.Int63n(int64(t))))
}

func encodeConfig(c Configuration) []byte {
	b, _ := json.Marshal(c)
	return b
}

func decodeConfig(b []byte) Configuration {
	var c Configuration
	if err := json.Unmarshal(b, &c); err != nil {
		panic("raft: corrupt config entry: " + err.Error())
	}
	return c
}

// configAt returns the configuration in effect at index i.
func (n *Node) configAt(i uint64) Configuration {
	if i > n.lastIndex() {
		i = n.lastIndex()
	}
	for j := i; j > n.snapIndex(); j-- {
		if e := n.entry(j); e.Type == EntryConfig {
			return decodeConfig(e.Data)
		}
	}
	return n.snapConfig.Clone()
}

// recomputeConfig sets the active configuration to the latest one in the log.
// Called after any append that contains a config entry and after truncation.
func (n *Node) recomputeConfig() {
	for j := n.lastIndex(); j > n.snapIndex(); j-- {
		if e := n.entry(j); e.Type == EntryConfig {
			n.config, n.configIndex = decodeConfig(e.Data), j
			n.onConfigChange()
			return
		}
	}
	n.config, n.configIndex = n.snapConfig.Clone(), n.snapIndex()
	n.onConfigChange()
}

func (n *Node) onConfigChange() {
	if n.state == Leader {
		for _, m := range n.config.Members {
			if _, ok := n.nextIndex[m.ID]; !ok && m.ID != n.id {
				n.nextIndex[m.ID] = n.lastIndex() + 1
				n.matchIndex[m.ID] = 0
			}
		}
		n.startReplicators()
	}
}

// ---------------------------------------------------------------------------
// Role transitions (caller holds mu)
// ---------------------------------------------------------------------------

func (n *Node) becomeFollower(term uint64, leader string) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.persistHardState()
	}
	if n.state == Leader {
		n.cfg.Logf("[%s] stepping down in term %d", n.id, n.term)
		n.stopReplicators()
		n.catchup = nil
	}
	n.state = Follower
	n.leaderID = leader
	n.stateCond.Broadcast()
}

func (n *Node) becomeLeader() {
	n.cfg.Logf("[%s] became leader in term %d", n.id, n.term)
	n.state = Leader
	n.leaderID = n.id
	n.leaderSince = n.cfg.Clock.Now()
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	n.lastAck = map[string]time.Time{}
	n.ackSeq = map[string]uint64{}
	n.catchup = map[string]Member{}
	for _, m := range n.config.Members {
		if m.ID != n.id {
			n.nextIndex[m.ID] = n.lastIndex() + 1
			n.matchIndex[m.ID] = 0
		}
	}
	// A no-op in the new term lets the leader learn the commit index (needed
	// before serving ReadIndex) and makes single-server changes safe (§4.1 bug fix).
	noop := Entry{Index: n.lastIndex() + 1, Term: n.term, Type: EntryNoop}
	n.appendLocal([]Entry{noop})
	n.noopIndex = noop.Index
	n.startReplicators()
	n.advanceCommit()
	n.stateCond.Broadcast()
}

// appendLocal appends entries created by this leader and persists them.
func (n *Node) appendLocal(es []Entry) {
	n.log = append(n.log, es...)
	cfgChanged := false
	for _, e := range es {
		n.logBytes += len(e.Data) + 32
		if e.Type == EntryConfig {
			cfgChanged = true
		}
	}
	n.mustSave(nil, 0, es)
	if cfgChanged {
		n.recomputeConfig()
	}
}

func (n *Node) startReplicators() {
	if n.repl == nil {
		n.repl = map[string]*replicator{}
	}
	start := func(m Member) {
		if m.ID == n.id || n.repl[m.ID] != nil {
			return
		}
		r := &replicator{member: m, trigger: make(chan struct{}, 1), stop: make(chan struct{})}
		n.repl[m.ID] = r
		n.wg.Add(1)
		go n.runReplicator(r, n.term)
	}
	for _, m := range n.config.Members {
		start(m)
	}
	for _, m := range n.catchup {
		start(m)
	}
}

func (n *Node) stopReplicators() {
	for id, r := range n.repl {
		close(r.stop)
		delete(n.repl, id)
	}
}

func (n *Node) triggerReplication() {
	for _, r := range n.repl {
		select {
		case r.trigger <- struct{}{}:
		default:
		}
	}
}

// ---------------------------------------------------------------------------
// Timers and elections
// ---------------------------------------------------------------------------

const tick = 10 * time.Millisecond

func (n *Node) ticker() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.cfg.Clock.After(tick):
		}
		n.mu.Lock()
		now := n.cfg.Clock.Now()
		switch {
		case n.stopped:
		case n.state == Leader:
			// CheckQuorum (§6.2): a leader that cannot reach a majority steps
			// down, so clients stop waiting on it and a partitioned old leader
			// does not linger.
			if !n.cfg.DisableCheckQuorum && now.Sub(n.leaderSince) >= n.cfg.ElectionTimeout && !n.quorumActive(now) {
				n.cfg.Logf("[%s] lost contact with quorum", n.id)
				n.becomeFollower(n.term, "")
				n.resetElectionTimer()
			}
		case now.After(n.electionDeadline):
			if !n.config.Contains(n.id) {
				n.resetElectionTimer() // non-voters never campaign
			} else if n.cfg.DisablePreVote {
				n.startElection()
			} else {
				n.startPreVote()
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) quorumActive(now time.Time) bool {
	count := 0
	for _, m := range n.config.Members {
		if m.ID == n.id || now.Sub(n.lastAck[m.ID]) < n.cfg.ElectionTimeout {
			count++
		}
	}
	return count >= n.config.Quorum()
}

// startPreVote (§9.6): ask whether we *would* win before bumping our term, so
// a node rejoining after a partition cannot disrupt a healthy leader.
func (n *Node) startPreVote() {
	n.state = PreCandidate
	n.leaderID = ""
	n.resetElectionTimer()
	n.runVote(&RequestVoteArgs{Term: n.term + 1, CandidateID: n.id,
		LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(), PreVote: true})
}

func (n *Node) startElection() {
	n.state = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.persistHardState()
	n.resetElectionTimer()
	n.cfg.Logf("[%s] starting election for term %d", n.id, n.term)
	n.runVote(&RequestVoteArgs{Term: n.term, CandidateID: n.id,
		LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm()})
}

func (n *Node) runVote(args *RequestVoteArgs) {
	cfg := n.config.Clone()
	granted := map[string]bool{n.id: true}
	won := false
	stillValid := func() bool {
		if args.PreVote {
			return n.state == PreCandidate && n.term == args.Term-1
		}
		return n.state == Candidate && n.term == args.Term
	}
	win := func() {
		if won || len(granted) < cfg.Quorum() {
			return
		}
		won = true
		if args.PreVote {
			n.startElection()
		} else {
			n.becomeLeader()
		}
	}
	win()
	for _, m := range cfg.Members {
		if m.ID == n.id {
			continue
		}
		go func(m Member) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			reply, err := n.cfg.Transport.RequestVote(ctx, m, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped {
				return
			}
			if reply.Term > n.term && !reply.Granted {
				n.becomeFollower(reply.Term, "")
				return
			}
			if !stillValid() || !reply.Granted {
				return
			}
			granted[m.ID] = true
			win()
		}(m)
	}
}

func (n *Node) logUpToDate(lastIndex, lastTerm uint64) bool {
	return lastTerm > n.lastTerm() || (lastTerm == n.lastTerm() && lastIndex >= n.lastIndex())
}

// HandleRequestVote is the RequestVote RPC handler.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	reply := &RequestVoteReply{Term: n.term}
	if args.PreVote {
		// Grant only if the candidate's term would be current, its log is
		// up to date, and we have not heard from a live leader recently.
		now := n.cfg.Clock.Now()
		heardRecently := n.state == Leader ||
			(n.leaderID != "" && now.Sub(n.lastLeaderContact) < n.cfg.ElectionTimeout)
		reply.Granted = args.Term >= n.term && !heardRecently &&
			n.logUpToDate(args.LastLogIndex, args.LastLogTerm)
		return reply, nil
	}
	if args.Term > n.term {
		n.becomeFollower(args.Term, "")
	}
	reply.Term = n.term
	if args.Term < n.term {
		return reply, nil
	}
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && n.logUpToDate(args.LastLogIndex, args.LastLogTerm) {
		n.votedFor = args.CandidateID
		n.persistHardState() // must be durable before the vote leaves this node
		n.resetElectionTimer()
		reply.Granted = true
	}
	return reply, nil
}

// ---------------------------------------------------------------------------
// Log replication (leader side)
// ---------------------------------------------------------------------------

func (n *Node) runReplicator(r *replicator, term uint64) {
	defer n.wg.Done()
	for {
		more, alive := n.replicateOnce(r, term)
		if !alive {
			return
		}
		if more {
			select {
			case <-r.stop:
				return
			default:
				continue
			}
		}
		select {
		case <-r.stop:
			return
		case <-r.trigger:
		case <-n.cfg.Clock.After(n.cfg.HeartbeatInterval):
		}
	}
}

// replicateOnce sends one AppendEntries (or InstallSnapshot) to a follower.
// more=true means the follower is still behind and we should send again now.
func (n *Node) replicateOnce(r *replicator, term uint64) (more, alive bool) {
	id := r.member.ID
	n.mu.Lock()
	if n.stopped || n.state != Leader || n.term != term {
		n.mu.Unlock()
		return false, false
	}
	if _, isCatchup := n.catchup[id]; !isCatchup && !n.config.Contains(id) {
		if n.repl[id] == r {
			delete(n.repl, id)
		}
		n.mu.Unlock()
		return false, false
	}
	seq := n.hbSeq
	next := n.nextIndex[id]
	if next == 0 {
		next = n.lastIndex() + 1
		n.nextIndex[id] = next
	}

	if next <= n.snapIndex() {
		// The follower needs entries we have compacted away: send the snapshot.
		snap := *n.snap
		args := &InstallSnapshotArgs{Term: n.term, LeaderID: n.id, Snapshot: snap}
		n.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 4*n.cfg.RPCTimeout)
		reply, err := n.cfg.Transport.InstallSnapshot(ctx, r.member, args)
		cancel()
		if err != nil {
			return false, true
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if reply.Term > n.term {
			n.becomeFollower(reply.Term, "")
			return false, false
		}
		if n.state != Leader || n.term != term {
			return false, false
		}
		n.recordAck(id, seq)
		if snap.Index > n.matchIndex[id] {
			n.matchIndex[id] = snap.Index
		}
		n.nextIndex[id] = n.matchIndex[id] + 1
		n.advanceCommit()
		return n.nextIndex[id] <= n.lastIndex(), true
	}

	args := &AppendEntriesArgs{
		Term:         n.term,
		LeaderID:     n.id,
		PrevLogIndex: next - 1,
		PrevLogTerm:  n.termAt(next - 1),
		Entries:      n.entriesFrom(next, n.cfg.MaxEntriesPerMsg),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
	reply, err := n.cfg.Transport.AppendEntries(ctx, r.member, args)
	cancel()
	if err != nil {
		return false, true
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return false, false
	}
	if reply.Term > n.term {
		n.becomeFollower(reply.Term, "")
		return false, false
	}
	if n.state != Leader || n.term != term {
		return false, false
	}
	// Any same-term reply proves the follower still recognises us as leader,
	// which is what ReadIndex and CheckQuorum need.
	n.recordAck(id, seq)
	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > n.matchIndex[id] {
			n.matchIndex[id] = match
		}
		if match+1 > n.nextIndex[id] {
			n.nextIndex[id] = match + 1
		}
		n.advanceCommit()
		n.stateCond.Broadcast() // AddServer waits on matchIndex
	} else {
		next := reply.ConflictIndex
		if reply.ConflictTerm != 0 {
			for i := n.lastIndex(); i > n.snapIndex(); i-- {
				if t := n.termAt(i); t == reply.ConflictTerm {
					next = i + 1
					break
				} else if t < reply.ConflictTerm {
					break
				}
			}
		}
		if next < 1 {
			next = 1
		}
		if next > n.lastIndex()+1 {
			next = n.lastIndex() + 1
		}
		if next <= n.matchIndex[id] {
			next = n.matchIndex[id] + 1
		}
		n.nextIndex[id] = next
		return true, true
	}
	return n.nextIndex[id] <= n.lastIndex(), true
}

func (n *Node) recordAck(id string, seq uint64) {
	n.lastAck[id] = n.cfg.Clock.Now()
	if seq > n.ackSeq[id] {
		n.ackSeq[id] = seq
	}
	n.stateCond.Broadcast()
}

// advanceCommit moves commitIndex to the highest index stored on a majority,
// but only for entries from the current term (§5.4.2 — the "Figure 8" rule).
func (n *Node) advanceCommit() {
	if n.state != Leader || len(n.config.Members) == 0 {
		return
	}
	matches := make([]uint64, 0, len(n.config.Members))
	for _, m := range n.config.Members {
		if m.ID == n.id {
			matches = append(matches, n.lastIndex())
		} else {
			matches = append(matches, n.matchIndex[m.ID])
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	N := matches[n.config.Quorum()-1]
	if N > n.commitIndex && n.termAt(N) == n.term {
		n.commitIndex = N
		n.applyCond.Signal()
		n.stateCond.Broadcast()
		n.triggerReplication() // propagate the new commit index promptly
		// A leader removed from the configuration steps down once the change commits.
		if n.configIndex <= n.commitIndex && !n.config.Contains(n.id) {
			n.becomeFollower(n.term, "")
		}
	}
}

// ---------------------------------------------------------------------------
// Log replication (follower side)
// ---------------------------------------------------------------------------

// HandleAppendEntries is the AppendEntries RPC handler.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	reply := &AppendEntriesReply{Term: n.term}
	if args.Term < n.term {
		return reply, nil
	}
	if args.Term > n.term || n.state != Follower {
		if n.state == Leader && args.Term == n.term {
			panic(fmt.Sprintf("raft: two leaders in term %d (%s, %s)", n.term, n.id, args.LeaderID))
		}
		n.becomeFollower(args.Term, args.LeaderID)
	}
	reply.Term = n.term
	n.leaderID = args.LeaderID
	n.lastLeaderContact = n.cfg.Clock.Now()
	n.resetElectionTimer()

	prev, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if prev < n.snapIndex() {
		// Everything up to snapIndex is committed and already in our snapshot.
		k := 0
		for k < len(entries) && entries[k].Index <= n.snapIndex() {
			k++
		}
		entries = entries[k:]
		prev, prevTerm = n.snapIndex(), n.log[0].Term
	}
	if prev > n.lastIndex() {
		reply.ConflictIndex = n.lastIndex() + 1
		return reply, nil
	}
	if t := n.termAt(prev); t != prevTerm {
		reply.ConflictTerm = t
		ci := prev
		for ci > n.snapIndex()+1 && n.termAt(ci-1) == t {
			ci--
		}
		reply.ConflictIndex = ci
		return reply, nil
	}

	var truncateFrom uint64
	var toAppend []Entry
	for i, e := range entries {
		if e.Index > n.lastIndex() {
			toAppend = entries[i:]
			break
		}
		if n.termAt(e.Index) != e.Term {
			if e.Index <= n.commitIndex {
				panic(fmt.Sprintf("raft %s: refusing to truncate committed entry %d", n.id, e.Index))
			}
			truncateFrom = e.Index
			n.log = n.log[:e.Index-n.snapIndex()]
			toAppend = entries[i:]
			break
		}
	}
	if truncateFrom > 0 || len(toAppend) > 0 {
		cfgChanged := truncateFrom > 0 && truncateFrom <= n.configIndex
		n.log = append(n.log, toAppend...)
		for _, e := range toAppend {
			if e.Type == EntryConfig {
				cfgChanged = true
			}
		}
		if truncateFrom > 0 {
			n.logBytes = 0
			for _, e := range n.log[1:] {
				n.logBytes += len(e.Data) + 32
			}
		} else {
			for _, e := range toAppend {
				n.logBytes += len(e.Data) + 32
			}
		}
		n.mustSave(nil, truncateFrom, toAppend)
		if cfgChanged {
			n.recomputeConfig()
		}
	}

	lastNew := args.PrevLogIndex + uint64(len(args.Entries))
	if args.LeaderCommit > n.commitIndex {
		c := args.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Signal()
		}
	}
	reply.Success = true
	return reply, nil
}

// HandleInstallSnapshot is the InstallSnapshot RPC handler.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	reply := &InstallSnapshotReply{Term: n.term}
	if args.Term < n.term {
		return reply, nil
	}
	if args.Term > n.term || n.state != Follower {
		n.becomeFollower(args.Term, args.LeaderID)
	}
	reply.Term = n.term
	n.leaderID = args.LeaderID
	n.lastLeaderContact = n.cfg.Clock.Now()
	n.resetElectionTimer()

	s := args.Snapshot
	if s.Index <= n.commitIndex {
		return reply, nil // we already have everything it contains
	}
	// Keep any log suffix that follows the snapshot and agrees with it.
	var rest []Entry
	if n.termAt(s.Index) == s.Term {
		rest = append(rest, n.log[s.Index-n.snapIndex()+1:]...)
	}
	n.log = append([]Entry{{Index: s.Index, Term: s.Term}}, rest...)
	n.logBytes = 0
	for _, e := range rest {
		n.logBytes += len(e.Data) + 32
	}
	n.snap = &s
	n.snapConfig = s.Config.Clone()
	n.commitIndex = s.Index
	n.pendingSnap = &s
	if err := n.cfg.Storage.SaveSnapshot(HardState{Term: n.term, VotedFor: n.votedFor}, &s, rest); err != nil {
		panic(fmt.Sprintf("raft %s: save snapshot: %v", n.id, err))
	}
	n.recomputeConfig()
	n.applyCond.Signal()
	return reply, nil
}

// ---------------------------------------------------------------------------
// Applying committed entries
// ---------------------------------------------------------------------------

func (n *Node) applier() {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		for !n.stopped && n.pendingSnap == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		if s := n.pendingSnap; s != nil {
			n.pendingSnap = nil
			n.mu.Unlock()
			select {
			case n.cfg.ApplyCh <- ApplyMsg{Snapshot: s}:
			case <-n.stopCh:
				return
			}
			n.mu.Lock()
			if s.Index > n.lastApplied {
				n.lastApplied = s.Index
			}
			n.stateCond.Broadcast()
			n.mu.Unlock()
			continue
		}
		if n.lastApplied < n.snapIndex() {
			n.lastApplied = n.snapIndex()
		}
		end := n.commitIndex
		if n.lastApplied >= end {
			n.mu.Unlock()
			continue
		}
		batch := n.entriesFrom(n.lastApplied+1, int(end-n.lastApplied))
		n.mu.Unlock()
		for i := range batch {
			select {
			case n.cfg.ApplyCh <- ApplyMsg{Entry: &batch[i]}:
			case <-n.stopCh:
				return
			}
		}
		n.mu.Lock()
		if end > n.lastApplied {
			n.lastApplied = end
		}
		n.stateCond.Broadcast()
		n.mu.Unlock()
	}
}

// Snapshot is called by the state machine after it has applied index; Raft
// discards the log through index (log compaction).
func (n *Node) Snapshot(index uint64, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || index <= n.snapIndex() || index > n.commitIndex || index > n.lastIndex() {
		return
	}
	s := &Snapshot{Index: index, Term: n.termAt(index), Config: n.configAt(index), Data: data}
	rest := append([]Entry(nil), n.log[index-n.snapIndex()+1:]...)
	n.log = append([]Entry{{Index: s.Index, Term: s.Term}}, rest...)
	n.logBytes = 0
	for _, e := range rest {
		n.logBytes += len(e.Data) + 32
	}
	n.snap = s
	n.snapConfig = s.Config.Clone()
	if err := n.cfg.Storage.SaveSnapshot(HardState{Term: n.term, VotedFor: n.votedFor}, s, rest); err != nil {
		panic(fmt.Sprintf("raft %s: save snapshot: %v", n.id, err))
	}
}

// ---------------------------------------------------------------------------
// Client-facing API
// ---------------------------------------------------------------------------

// StartBatch appends commands to the leader's log with a single fsync (group
// commit). It returns the index of the first command and the current term.
func (n *Node) StartBatch(cmds [][]byte) (first, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.state != Leader || len(cmds) == 0 {
		return 0, n.term, false
	}
	first = n.lastIndex() + 1
	es := make([]Entry, len(cmds))
	for i, c := range cmds {
		es[i] = Entry{Index: first + uint64(i), Term: n.term, Type: EntryNormal, Data: c}
	}
	n.appendLocal(es)
	n.advanceCommit() // single-node clusters commit immediately
	n.triggerReplication()
	return first, n.term, true
}

// Start appends a single command.
func (n *Node) Start(cmd []byte) (index, term uint64, isLeader bool) {
	return n.StartBatch([][]byte{cmd})
}

// ReadIndex implements linearizable reads without writing to the log
// (dissertation §6.4). It returns an index; the caller must wait until its
// state machine has applied at least that index, then read locally.
//  1. The leader must have committed an entry in its current term (the no-op),
//     so its commitIndex is at least as large as any previously committed index.
//  2. Record readIndex = commitIndex.
//  3. Confirm it is still leader by getting a heartbeat round acknowledged by a
//     majority. Concurrent ReadIndex calls coalesce onto the same round.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, ErrNotLeader
	}
	term := n.term
	if err := n.waitUntil(ctx, func() bool {
		return n.state != Leader || n.term != term || n.commitIndex >= n.noopIndex
	}); err != nil {
		return 0, err
	}
	if n.state != Leader || n.term != term {
		return 0, ErrNotLeader
	}
	readIndex := n.commitIndex
	n.hbSeq++
	seq := n.hbSeq
	n.triggerReplication()
	if err := n.waitUntil(ctx, func() bool {
		return n.state != Leader || n.term != term || n.heartbeatQuorum(seq)
	}); err != nil {
		return 0, err
	}
	if n.state != Leader || n.term != term {
		return 0, ErrNotLeader
	}
	return readIndex, nil
}

func (n *Node) heartbeatQuorum(seq uint64) bool {
	count := 0
	for _, m := range n.config.Members {
		if m.ID == n.id || n.ackSeq[m.ID] >= seq {
			count++
		}
	}
	return count >= n.config.Quorum()
}

// WaitApplied blocks until Raft has delivered index on ApplyCh.
func (n *Node) WaitApplied(ctx context.Context, index uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.waitUntil(ctx, func() bool { return n.lastApplied >= index })
}

// waitUntil waits on stateCond (mu held) until cond() or ctx is done.
func (n *Node) waitUntil(ctx context.Context, cond func() bool) error {
	stop := context.AfterFunc(ctx, func() {
		n.mu.Lock()
		n.stateCond.Broadcast()
		n.mu.Unlock()
	})
	defer stop()
	for !cond() {
		if n.stopped {
			return ErrStopped
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		n.stateCond.Wait()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Membership changes: one server at a time (dissertation §4.1-4.2).
// Any two majorities of configurations that differ by one server overlap, so
// no joint consensus is needed. A new configuration takes effect as soon as it
// is appended (not committed), and only one change may be uncommitted at once.
// ---------------------------------------------------------------------------

func (n *Node) prepareConfigChange(ctx context.Context) (uint64, error) {
	if n.state != Leader {
		return 0, ErrNotLeader
	}
	term := n.term
	// Wait for this term's no-op to commit; otherwise a change could be
	// applied on top of an uncommitted change from a previous leader.
	if err := n.waitUntil(ctx, func() bool {
		return n.state != Leader || n.term != term || n.commitIndex >= n.noopIndex
	}); err != nil {
		return 0, err
	}
	if n.state != Leader || n.term != term {
		return 0, ErrNotLeader
	}
	if n.configIndex > n.commitIndex || len(n.catchup) > 0 {
		return 0, ErrConfigInProgress
	}
	return term, nil
}

func (n *Node) commitConfig(ctx context.Context, term uint64, c Configuration) error {
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Type: EntryConfig, Data: encodeConfig(c)}
	n.appendLocal([]Entry{e})
	n.advanceCommit()
	n.triggerReplication()
	if err := n.waitUntil(ctx, func() bool {
		return n.commitIndex >= e.Index || n.term != term
	}); err != nil {
		return err
	}
	if n.commitIndex >= e.Index && n.termAt(e.Index) == term {
		return nil
	}
	return ErrNotLeader // outcome unknown; caller should check Status
}

// AddServer adds m as a voting member after bringing its log up to date, so
// adding it does not stall commits while it catches up.
func (n *Node) AddServer(ctx context.Context, m Member) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	term, err := n.prepareConfigChange(ctx)
	if err != nil {
		return err
	}
	if n.config.Contains(m.ID) {
		return nil
	}
	n.catchup[m.ID] = m
	n.nextIndex[m.ID] = n.lastIndex() + 1
	n.matchIndex[m.ID] = 0
	n.startReplicators()
	cleanup := func() { delete(n.catchup, m.ID) }

	// Catch-up rounds (§4.2.1): replicate up to the leader's last index as of
	// the round's start; once a round takes less than an election timeout the
	// new server is close enough to join.
	caught := false
	for round := 0; round < 10 && !caught; round++ {
		target := n.lastIndex()
		start := n.cfg.Clock.Now()
		if err := n.waitUntil(ctx, func() bool {
			return n.state != Leader || n.term != term || n.matchIndex[m.ID] >= target
		}); err != nil {
			cleanup()
			return err
		}
		if n.state != Leader || n.term != term {
			return ErrNotLeader
		}
		caught = n.cfg.Clock.Now().Sub(start) < n.cfg.ElectionTimeout
	}
	if !caught {
		cleanup()
		return ErrCatchUpTimeout
	}
	c := n.config.Clone()
	c.Members = append(c.Members, m)
	cleanup()
	return n.commitConfig(ctx, term, c)
}

// RemoveServer removes a voting member. Removing the leader itself is
// allowed; it keeps leading until the change commits, then steps down.
func (n *Node) RemoveServer(ctx context.Context, id string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	term, err := n.prepareConfigChange(ctx)
	if err != nil {
		return err
	}
	if !n.config.Contains(id) {
		return nil
	}
	if len(n.config.Members) == 1 {
		return fmt.Errorf("raft: cannot remove the last member")
	}
	c := Configuration{}
	for _, m := range n.config.Members {
		if m.ID != id {
			c.Members = append(c.Members, m)
		}
	}
	return n.commitConfig(ctx, term, c)
}

// ---------------------------------------------------------------------------
// Introspection
// ---------------------------------------------------------------------------

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.id, State: n.state.String(), Term: n.term, Leader: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied, LastIndex: n.lastIndex(),
		SnapIndex: n.snapIndex(), Config: n.config.Clone(),
	}
}

// IsLeader reports whether this node believes it is leader, and its term.
func (n *Node) IsLeader() (uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term, n.state == Leader
}

// LeaderHint returns the ID and address of the last known leader, if any.
func (n *Node) LeaderHint() Member {
	n.mu.Lock()
	defer n.mu.Unlock()
	m, _ := n.config.Get(n.leaderID)
	return m
}

// LogSize approximates the in-memory log size in bytes (for snapshot triggers).
func (n *Node) LogSize() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.logBytes
}
