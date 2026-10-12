package raft

import (
	"context"
	"errors"
	"testing"
)

// deadTransport fails every RPC, so the node stays isolated and we control its state.
type deadTransport struct{}

var errDead = errors.New("unreachable")

func (deadTransport) RequestVote(context.Context, Member, *RequestVoteArgs) (*RequestVoteReply, error) {
	return nil, errDead
}
func (deadTransport) AppendEntries(context.Context, Member, *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return nil, errDead
}
func (deadTransport) InstallSnapshot(context.Context, Member, *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return nil, errDead
}

// TestFigure8CommitRule: a leader must not commit an entry from an earlier
// term just because a majority stores it (Raft paper §5.4.2, Figure 8).
func TestFigure8CommitRule(t *testing.T) {
	n, err := NewNode(Config{
		ID:        "n1",
		Bootstrap: []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
		Storage:   NewMemStorage(),
		Transport: deadTransport{},
		ApplyCh:   make(chan ApplyMsg, 16),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	n.mu.Lock()
	defer n.mu.Unlock()
	// Log: [1: bootstrap config, term 0] [2: term 2]. We are leader of term 4.
	n.log = append(n.log, Entry{Index: 2, Term: 2, Data: []byte("old")})
	n.term, n.state = 4, Leader
	n.matchIndex = map[string]uint64{"n2": 2, "n3": 0} // index 2 is on n1+n2: a majority
	n.nextIndex = map[string]uint64{"n2": 3, "n3": 1}

	n.advanceCommit()
	if n.commitIndex >= 2 {
		t.Fatalf("committed a term-2 entry by counting replicas in term 4 (commitIndex=%d)", n.commitIndex)
	}

	// Once an entry from the current term is on a majority, everything before it commits too.
	n.log = append(n.log, Entry{Index: 3, Term: 4})
	n.matchIndex["n2"] = 3
	n.advanceCommit()
	if n.commitIndex != 3 {
		t.Fatalf("commitIndex=%d, want 3", n.commitIndex)
	}
}
