package kv

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	mrand "math/rand"
	"sync"
	"time"

	"raftkv/raft"
	"raftkv/shard"
)

// ErrUnknown means the request's deadline expired before an answer came back.
// For a write the outcome is unknown: it may or may not have been applied.
// (The linearizability checker treats such writes as "maybe happened".)
var ErrUnknown = errors.New("kv: request timed out, outcome unknown")

// Clerk is a client. It routes each key to its replica group with the
// consistent-hash ring, follows leader hints, and retries until its context
// expires. A Clerk issues one request at a time (that is what makes the
// per-client sequence numbers sufficient for exactly-once writes); use one
// Clerk per goroutine.
type Clerk struct {
	caller Caller
	ring   *shard.Ring

	mu     sync.Mutex
	groups map[string][]raft.Member
	leader map[string]int

	clientID uint64
	seq      uint64

	// RPCTimeout bounds each attempt (default 500ms).
	RPCTimeout time.Duration
	// RandomReadReplica sends Gets to a random replica first. Only useful with
	// the UnsafeLocalReads negative control; real reads always end on the leader.
	RandomReadReplica bool
}

// NewClerk creates a client. groups maps group name to its servers.
func NewClerk(caller Caller, ring *shard.Ring, groups map[string][]raft.Member) *Clerk {
	var b [8]byte
	rand.Read(b[:])
	c := &Clerk{
		caller: caller, ring: ring, groups: map[string][]raft.Member{}, leader: map[string]int{},
		clientID: binary.LittleEndian.Uint64(b[:]), RPCTimeout: 500 * time.Millisecond,
	}
	for g, ms := range groups {
		c.groups[g] = append([]raft.Member(nil), ms...)
	}
	return c
}

func (c *Clerk) ClientID() uint64 { return c.clientID }

func (c *Clerk) group(key string) string {
	if c.ring == nil {
		for g := range c.groups {
			return g
		}
	}
	return c.ring.Lookup(key)
}

// next picks which server to try after a failure, following a leader hint
// (and learning servers added by membership changes) when one is given.
func (c *Clerk) next(g string, cur int, hintID, hintAddr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	servers := c.groups[g]
	if hintID != "" {
		for i, m := range servers {
			if m.ID == hintID && i != cur {
				return i
			}
		}
		if hintAddr != "" {
			found := false
			for _, m := range servers {
				found = found || m.ID == hintID
			}
			if !found {
				c.groups[g] = append(servers, raft.Member{ID: hintID, Addr: hintAddr})
				return len(c.groups[g]) - 1
			}
		}
	}
	return (cur + 1) % len(servers)
}

func (c *Clerk) start(g string, random bool) (int, []raft.Member) {
	c.mu.Lock()
	defer c.mu.Unlock()
	servers := c.groups[g]
	if len(servers) == 0 {
		return 0, nil
	}
	if random {
		return mrand.Intn(len(servers)), servers
	}
	return c.leader[g] % len(servers), servers
}

func (c *Clerk) server(g string, i int) raft.Member {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groups[g][i]
}

func (c *Clerk) remember(g string, i int) {
	c.mu.Lock()
	c.leader[g] = i
	c.mu.Unlock()
}

// Get returns the value of key ("" if absent).
func (c *Clerk) Get(ctx context.Context, key string) (string, error) {
	g := c.group(key)
	i, servers := c.start(g, c.RandomReadReplica)
	if servers == nil {
		return "", fmt.Errorf("kv: no servers for group %q", g)
	}
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return "", ErrUnknown
		}
		rctx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
		reply, err := c.caller.Get(rctx, c.server(g, i), &GetArgs{Key: key})
		cancel()
		switch {
		case err == nil && reply.Err == OK:
			c.remember(g, i)
			return reply.Value, nil
		case err == nil && reply.Err == ErrWrongGroup:
			return "", fmt.Errorf("kv: key %q not owned by group %s (ring mismatch)", key, g)
		case err == nil:
			i = c.next(g, i, reply.LeaderID, reply.LeaderAddr)
		default:
			i = c.next(g, i, "", "")
		}
		backoff(ctx, attempt)
	}
}

func (c *Clerk) Put(ctx context.Context, key, value string) error {
	return c.write(ctx, OpPut, key, value)
}

func (c *Clerk) Append(ctx context.Context, key, value string) error {
	return c.write(ctx, OpAppend, key, value)
}

func (c *Clerk) Delete(ctx context.Context, key string) error {
	return c.write(ctx, OpDelete, key, "")
}

func (c *Clerk) write(ctx context.Context, op OpType, key, value string) error {
	c.seq++
	args := &WriteArgs{Op: op, Key: key, Value: value, ClientID: c.clientID, Seq: c.seq}
	g := c.group(key)
	i, servers := c.start(g, false)
	if servers == nil {
		return fmt.Errorf("kv: no servers for group %q", g)
	}
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return ErrUnknown
		}
		rctx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
		reply, err := c.caller.Write(rctx, c.server(g, i), args)
		cancel()
		switch {
		case err == nil && reply.Err == OK:
			c.remember(g, i)
			return nil
		case err == nil && reply.Err == ErrWrongGroup:
			return fmt.Errorf("kv: key %q not owned by group %s (ring mismatch)", key, g)
		case err == nil && reply.Err == ErrTimeout:
			// The leader may still commit it; retrying the same seq is safe.
		case err == nil:
			i = c.next(g, i, reply.LeaderID, reply.LeaderAddr)
		default:
			i = c.next(g, i, "", "")
		}
		backoff(ctx, attempt)
	}
}

func backoff(ctx context.Context, attempt int) {
	d := time.Duration(attempt) * 5 * time.Millisecond
	if d > 50*time.Millisecond {
		d = 50 * time.Millisecond
	}
	if d == 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
