// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", and Ongaro's PhD
// dissertation for PreVote, CheckQuorum, ReadIndex and single-server
// membership changes).
package raft

import (
	"context"
	"errors"
	"time"
)

// EntryType distinguishes client commands from Raft-internal entries.
type EntryType uint8

const (
	EntryNormal EntryType = iota // client command, delivered to the state machine
	EntryNoop                    // appended by every new leader (needed for ReadIndex + safe config changes)
	EntryConfig                  // cluster membership configuration
)

// Entry is a single log entry. Data is never mutated after creation, so
// entries can be shared between goroutines without copying the payload.
type Entry struct {
	Index uint64    `json:"i"`
	Term  uint64    `json:"t"`
	Type  EntryType `json:"y,omitempty"`
	Data  []byte    `json:"d,omitempty"`
}

// Member is one voting server in a configuration.
type Member struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Configuration is the set of voting members.
type Configuration struct {
	Members []Member `json:"members"`
}

func (c Configuration) Contains(id string) bool {
	for _, m := range c.Members {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (c Configuration) Get(id string) (Member, bool) {
	for _, m := range c.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// Quorum is the number of votes/acks needed for a majority.
func (c Configuration) Quorum() int { return len(c.Members)/2 + 1 }

func (c Configuration) Clone() Configuration {
	return Configuration{Members: append([]Member(nil), c.Members...)}
}

// HardState must be persisted before answering any RPC that depends on it.
type HardState struct {
	Term     uint64
	VotedFor string
}

// Snapshot captures the state machine up to and including Index.
type Snapshot struct {
	Index  uint64        `json:"index"`
	Term   uint64        `json:"term"`
	Config Configuration `json:"config"`
	Data   []byte        `json:"data"`
}

// ---- RPC messages ----

type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate"`
	LastLogIndex uint64 `json:"lli"`
	LastLogTerm  uint64 `json:"llt"`
	PreVote      bool   `json:"pre,omitempty"`
}

type RequestVoteReply struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
}

type AppendEntriesArgs struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader"`
	PrevLogIndex uint64  `json:"pli"`
	PrevLogTerm  uint64  `json:"plt"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"commit"`
}

type AppendEntriesReply struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	// Fast log backtracking (one RPC per conflicting term instead of per entry).
	ConflictIndex uint64 `json:"ci,omitempty"`
	ConflictTerm  uint64 `json:"ct,omitempty"`
}

// InstallSnapshotArgs ships the whole snapshot in one message. Chunking (as in
// Figure 13 of the paper) is unnecessary at the snapshot sizes this store targets.
type InstallSnapshotArgs struct {
	Term     uint64   `json:"term"`
	LeaderID string   `json:"leader"`
	Snapshot Snapshot `json:"snapshot"`
}

type InstallSnapshotReply struct {
	Term uint64 `json:"term"`
}

// ApplyMsg is delivered, in log order, on Config.ApplyCh. Exactly one of
// Entry / Snapshot is set. Noop and Config entries are delivered too so the
// state machine can track the applied index (ReadIndex waits on it).
type ApplyMsg struct {
	Entry    *Entry
	Snapshot *Snapshot
}

// Transport sends Raft RPCs. Implementations must be safe for concurrent use.
type Transport interface {
	RequestVote(ctx context.Context, to Member, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to Member, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to Member, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Clock lets the fault injector skew a node's notion of time. All of Raft's
// timers go through it; safety never depends on it (only liveness does).
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// SystemClock is the real wall clock.
var SystemClock Clock = systemClock{}

var (
	ErrNotLeader        = errors.New("raft: not leader")
	ErrStopped          = errors.New("raft: node stopped")
	ErrConfigInProgress = errors.New("raft: another membership change is in progress")
	ErrCatchUpTimeout   = errors.New("raft: new server did not catch up in time")
)

// State is a node's role.
type State uint8

const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"follower", "precandidate", "candidate", "leader"}[s]
}

// Status is a point-in-time view of a node, for tests, metrics and the CLI.
type Status struct {
	ID          string        `json:"id"`
	State       string        `json:"state"`
	Term        uint64        `json:"term"`
	Leader      string        `json:"leader"`
	CommitIndex uint64        `json:"commit_index"`
	LastApplied uint64        `json:"last_applied"`
	LastIndex   uint64        `json:"last_index"`
	SnapIndex   uint64        `json:"snapshot_index"`
	Config      Configuration `json:"config"`
}
