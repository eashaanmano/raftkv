# RaftKV

A fault-tolerant, linearizable, sharded key-value store built on a from-scratch
implementation of Raft in Go, with a Jepsen-style fault injector and a
linearizability checker.

```
client ──► Clerk ──consistent-hash ring──► replica group g1 (Raft, 5 nodes)
                                       └─► replica group g2 (Raft, 3 nodes) ...
```

## What's implemented

| Area | Where | Notes |
|---|---|---|
| Leader election | `raft/raft.go` | randomized timeouts, **PreVote** (§9.6) so a rejoining node can't bump terms and depose a healthy leader, **CheckQuorum** so a partitioned leader steps down |
| Log replication | `raft/raft.go` | one replicator goroutine per follower, batching up to 4096 entries/RPC, fast backtracking with `ConflictTerm`/`ConflictIndex` |
| Commit rule | `advanceCommit` | only entries from the current term commit by counting (the "Figure 8" rule) |
| Durability | `raft/storage.go` | CRC-framed write-ahead log, one fsync per batch, torn-tail recovery, atomic snapshot + WAL rewrite (tmp → fsync → rename → fsync dir) |
| Snapshots / compaction | `Node.Snapshot`, `HandleInstallSnapshot` | state machine triggers at a byte threshold; lagging followers get `InstallSnapshot` |
| Membership changes | `AddServer` / `RemoveServer` | single-server changes (dissertation §4.1), new servers catch up in rounds before becoming voters, leader may remove itself, waits for its term's no-op first (the known single-server-change bug fix) |
| Linearizable reads | `Node.ReadIndex`, `kv.Server.Get` | ReadIndex (§6.4): no log write, one heartbeat round confirms leadership; concurrent reads coalesce onto the same round |
| Exactly-once writes | `kv/server.go` | per-client sequence numbers; retried writes are recognised and not re-applied (state is part of snapshots) |
| Group commit | `kv.Server.proposer` | concurrent client writes are batched into one `StartBatch` → one fsync, one replication round |
| Sharding | `shard/ring.go` | consistent hashing with 128 virtual nodes per group; servers reject keys they don't own |
| Transport | `transport/grpcx` | gRPC over HTTP/2 with a JSON codec (no protoc step) |
| Fault injection | `jepsen/`, `transport/inmem` | partitions, leader isolation, crash/restart (incl. majority crashes), clock skew (rate 0.25–4×, ±2s jumps), 20% message loss + reordering, membership churn |
| Linearizability checker | `lincheck/` | Wing–Gong–Lowe search with memoization, per-key decomposition, timed-out writes modelled as "may have happened" |

## Repository layout

```
raft/              consensus: election, replication, snapshots, membership, ReadIndex, WAL
kv/                replicated state machine + client (Clerk)
shard/             consistent-hash ring
transport/grpcx/   gRPC transport (production)
transport/inmem/   simulated network with fault injection (tests)
cluster/           in-process sharded cluster used by tests/bench/jepsen
lincheck/          linearizability checker
jepsen/            fault-injection harness
bench/             closed-loop load generator
cmd/               raftkv-server, raftkv-cli, raftkv-bench, raftkv-jepsen
deploy/            cluster configs; Dockerfile + docker-compose.yml; scripts/nemesis.sh
```

## Quick start

```bash
go mod tidy                 # fetches gRPC, writes go.sum
go test -short ./...        # ~1.5 min
make jepsen                 # 10 × 60s fault-injection runs
make bench                  # 5 gRPC servers on loopback, fsynced WAL
make bench-failover         # kill the leader mid-run

# real processes
go build -o bin/ ./cmd/...
for i in 1 2 3; do bin/raftkv-server -config deploy/cluster-local.json -id n$i -data /tmp/rkv/n$i & done
bin/raftkv-cli -config deploy/cluster-local.json put hello world
bin/raftkv-cli -config deploy/cluster-local.json get hello
bin/raftkv-cli -config deploy/cluster-local.json status

# docker: 5-node cluster
docker compose up -d --build
docker compose run --rm bench            # terminal 1
scripts/nemesis.sh kill-leader           # terminal 2
```

Adding a server: start it with `-join -group g1 -listen :7004`, then
`raftkv-cli add-server g1 n4 host:7004`.

## Testing

* **Raft unit tests** (`raft/raft_test.go`): election, re-election, no
  leader without quorum, agreement with failed followers, no commit without a
  majority, crash-all recovery, snapshot install to a lagging node, an
  unreliable "Figure 8"-style test with random leader crashes, membership
  changes including removing the leader, ReadIndex refusing to serve from a
  minority, PreVote preventing term inflation, and WAL torn-write recovery.
  Every applied entry is cross-checked between nodes (State Machine Safety).
* **Fault-injection runs** (`jepsen/`): clients issue random get/put/append
  operations with unique append values; the nemesis cycles through faults; the
  full history is checked for linearizability. Keys rotate every 3s so per-key
  histories stay small enough to check exhaustively.
* **Negative control** (`TestCheckerCatchesStaleReads`): the same harness with
  ReadIndex disabled (reads served from any replica's local state). The checker
  must report a violation. This proves the harness can actually catch the class
  of bug it claims to rule out.

## Measured results

Measured on a **2-vCPU, 7 GB cloud VM with all replicas in one process**
(sharing CPU and one disk), simulated network with no added latency, fsynced
WAL. They are lower bounds for this environment, not hardware-independent
numbers. Re-measure on your own machine and report those.

**Fault injection** (`raftkv-jepsen`): 5 runs × 60s on a 5-node group plus
3 runs × 30s on 2 groups × 3 nodes, ~399K operations total, all faults
enabled: **0 linearizability violations**, cluster recovered after every
run. The negative control was flagged as non-linearizable.

**Throughput / latency** (`raftkv-bench -mode inmem`, 100 B values, write-only):

| Cluster | Clients | Writes/s | p50 | p99 | p99.9 |
|---|---|---|---|---|---|
| 5 nodes | 64 | 15.0K | 3.4 ms | 13.8 ms | 21.4 ms |
| 5 nodes | 256 | 32.4K | 6.4 ms | 31.5 ms | 74.8 ms |
| 3 nodes | 256 | 39.6K | 5.1 ms | 24.8 ms | 51.3 ms |

The profile is CPU-bound (5 replicas on 2 vCPUs): in-memory storage was
*slower* than the fsynced WAL because fsync waits create larger batches.

**Failover** (leader killed mid-load, time until the first write issued after
the kill succeeds, 6 runs each):

| Election timeout | Observed |
|---|---|
| 300 ms (default) | 380–480 ms |
| 150 ms | 185–340 ms |

This is detection (one randomized timeout in [T, 2T)) + PreVote round +
election + the new leader's no-op commit.

