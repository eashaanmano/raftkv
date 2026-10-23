// Package grpcx carries Raft and KV RPCs over gRPC (HTTP/2).
//
// The services are registered by hand with a JSON codec instead of
// protoc-generated stubs, so the repo builds with nothing but `go build`.
// Trade-off: JSON is slower and bigger on the wire than protobuf; swapping in
// generated protobuf messages is a self-contained change to this package.
package grpcx

import (
	"context"
	"encoding/json"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"

	"raftkv/kv"
	"raftkv/raft"
)

const maxMsg = 256 << 20 // snapshots travel in one message

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (jsonCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
func (jsonCodec) Name() string                    { return "json" }

func init() { encoding.RegisterCodec(jsonCodec{}) }

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

// Service is what a RaftKV process exposes.
type Service interface {
	HandleRequestVote(*raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	HandleAppendEntries(*raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	HandleInstallSnapshot(*raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)
	Get(context.Context, *kv.GetArgs) (*kv.GetReply, error)
	Write(context.Context, *kv.WriteArgs) (*kv.WriteReply, error)
	AddServer(context.Context, *kv.AddServerArgs) (*kv.AdminReply, error)
	RemoveServer(context.Context, *kv.RemoveServerArgs) (*kv.AdminReply, error)
	Status(context.Context, *kv.StatusArgs) (*raft.Status, error)
}

// Adapter turns a *kv.Server into a Service.
type Adapter struct{ *kv.Server }

func (a Adapter) HandleRequestVote(x *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return a.Raft().HandleRequestVote(x)
}
func (a Adapter) HandleAppendEntries(x *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return a.Raft().HandleAppendEntries(x)
}
func (a Adapter) HandleInstallSnapshot(x *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return a.Raft().HandleInstallSnapshot(x)
}

func unary[Req any, Resp any](fn func(Service, context.Context, *Req) (*Resp, error)) func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		in := new(Req)
		if err := dec(in); err != nil {
			return nil, err
		}
		return fn(srv.(Service), ctx, in)
	}
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "raftkv.RaftKV",
	HandlerType: (*Service)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "RequestVote", Handler: unary(func(s Service, _ context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return s.HandleRequestVote(a)
		})},
		{MethodName: "AppendEntries", Handler: unary(func(s Service, _ context.Context, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return s.HandleAppendEntries(a)
		})},
		{MethodName: "InstallSnapshot", Handler: unary(func(s Service, _ context.Context, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
			return s.HandleInstallSnapshot(a)
		})},
		{MethodName: "Get", Handler: unary(func(s Service, ctx context.Context, a *kv.GetArgs) (*kv.GetReply, error) {
			return s.Get(ctx, a)
		})},
		{MethodName: "Write", Handler: unary(func(s Service, ctx context.Context, a *kv.WriteArgs) (*kv.WriteReply, error) {
			return s.Write(ctx, a)
		})},
		{MethodName: "AddServer", Handler: unary(func(s Service, ctx context.Context, a *kv.AddServerArgs) (*kv.AdminReply, error) {
			return s.AddServer(ctx, a)
		})},
		{MethodName: "RemoveServer", Handler: unary(func(s Service, ctx context.Context, a *kv.RemoveServerArgs) (*kv.AdminReply, error) {
			return s.RemoveServer(ctx, a)
		})},
		{MethodName: "Status", Handler: unary(func(s Service, ctx context.Context, a *kv.StatusArgs) (*raft.Status, error) {
			return s.Status(ctx, a)
		})},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "raftkv",
}

// NewServer returns a gRPC server exposing svc.
func NewServer(svc Service) *grpc.Server {
	s := grpc.NewServer(grpc.MaxRecvMsgSize(maxMsg), grpc.MaxSendMsgSize(maxMsg))
	s.RegisterService(&serviceDesc, svc)
	return s
}

// ---------------------------------------------------------------------------
// Client side: implements raft.Transport and kv.Caller.
// ---------------------------------------------------------------------------

type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewClient() *Client { return &Client{conns: map[string]*grpc.ClientConn{}} }

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc := c.conns[addr]; cc != nil {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.CallContentSubtype("json"),
			grpc.MaxCallRecvMsgSize(maxMsg),
			grpc.MaxCallSendMsgSize(maxMsg),
		))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = cc
	return cc, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.conns {
		cc.Close()
	}
	c.conns = map[string]*grpc.ClientConn{}
}

func call[Resp any](c *Client, ctx context.Context, addr, method string, args any) (*Resp, error) {
	cc, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	out := new(Resp)
	if err := cc.Invoke(ctx, "/raftkv.RaftKV/"+method, args, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) RequestVote(ctx context.Context, to raft.Member, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return call[raft.RequestVoteReply](c, ctx, to.Addr, "RequestVote", a)
}

func (c *Client) AppendEntries(ctx context.Context, to raft.Member, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return call[raft.AppendEntriesReply](c, ctx, to.Addr, "AppendEntries", a)
}

func (c *Client) InstallSnapshot(ctx context.Context, to raft.Member, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return call[raft.InstallSnapshotReply](c, ctx, to.Addr, "InstallSnapshot", a)
}

func (c *Client) Get(ctx context.Context, to raft.Member, a *kv.GetArgs) (*kv.GetReply, error) {
	return call[kv.GetReply](c, ctx, to.Addr, "Get", a)
}

func (c *Client) Write(ctx context.Context, to raft.Member, a *kv.WriteArgs) (*kv.WriteReply, error) {
	return call[kv.WriteReply](c, ctx, to.Addr, "Write", a)
}

func (c *Client) AddServer(ctx context.Context, addr string, a *kv.AddServerArgs) (*kv.AdminReply, error) {
	return call[kv.AdminReply](c, ctx, addr, "AddServer", a)
}

func (c *Client) RemoveServer(ctx context.Context, addr string, a *kv.RemoveServerArgs) (*kv.AdminReply, error) {
	return call[kv.AdminReply](c, ctx, addr, "RemoveServer", a)
}

func (c *Client) Status(ctx context.Context, addr string) (*raft.Status, error) {
	return call[raft.Status](c, ctx, addr, "Status", &kv.StatusArgs{})
}
