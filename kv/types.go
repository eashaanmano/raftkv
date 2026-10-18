// Package kv is a linearizable key-value state machine replicated with Raft.
//
// Writes go through the Raft log; exactly-once semantics come from per-client
// sequence numbers (a retried write is recognised and not applied twice).
// Reads use Raft's ReadIndex protocol instead of the log, so they cost one
// heartbeat round rather than a disk write.
package kv

import (
	"context"
	"encoding/binary"
	"errors"

	"raftkv/raft"
)

type Err string

const (
	OK             Err = ""
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrTimeout     Err = "ErrTimeout"
	ErrShutdown    Err = "ErrShutdown"
)

type OpType uint8

const (
	OpPut OpType = iota + 1
	OpAppend
	OpDelete
)

type GetArgs struct {
	Key string `json:"key"`
}

type GetReply struct {
	Value      string `json:"value"`
	Err        Err    `json:"err,omitempty"`
	LeaderID   string `json:"leader_id,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

type WriteArgs struct {
	Op       OpType `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID uint64 `json:"client_id"`
	Seq      uint64 `json:"seq"`
}

type WriteReply struct {
	Err        Err    `json:"err,omitempty"`
	LeaderID   string `json:"leader_id,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

type AddServerArgs struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type RemoveServerArgs struct {
	ID string `json:"id"`
}

type AdminReply struct {
	Err        string `json:"err,omitempty"`
	LeaderID   string `json:"leader_id,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

type StatusArgs struct{}

// Caller is the client side of the KV RPCs (implemented by each transport).
type Caller interface {
	Get(ctx context.Context, to raft.Member, args *GetArgs) (*GetReply, error)
	Write(ctx context.Context, to raft.Member, args *WriteArgs) (*WriteReply, error)
}

// ---- command encoding (what goes into the Raft log) ----

type command struct {
	Op       OpType
	Key      string
	Value    string
	ClientID uint64
	Seq      uint64
}

func encodeCommand(c command) []byte {
	b := make([]byte, 0, 1+8+8+4+len(c.Key)+4+len(c.Value))
	b = append(b, byte(c.Op))
	b = binary.LittleEndian.AppendUint64(b, c.ClientID)
	b = binary.LittleEndian.AppendUint64(b, c.Seq)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(c.Key)))
	b = append(b, c.Key...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(c.Value)))
	return append(b, c.Value...)
}

func decodeCommand(b []byte) (command, error) {
	var c command
	if len(b) < 21 {
		return c, errors.New("kv: short command")
	}
	c.Op = OpType(b[0])
	c.ClientID = binary.LittleEndian.Uint64(b[1:])
	c.Seq = binary.LittleEndian.Uint64(b[9:])
	kl := int(binary.LittleEndian.Uint32(b[17:]))
	b = b[21:]
	if len(b) < kl+4 {
		return c, errors.New("kv: short key")
	}
	c.Key = string(b[:kl])
	b = b[kl:]
	vl := int(binary.LittleEndian.Uint32(b))
	b = b[4:]
	if len(b) < vl {
		return c, errors.New("kv: short value")
	}
	c.Value = string(b[:vl])
	return c, nil
}
