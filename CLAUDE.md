# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Kage — a Kafka-inspired distributed message queue, built **KRaft-style**: Raft owns metadata/control
plane, ISR (in-sync replicas) owns the data plane, deliberately kept separate so consensus never sits
on the hot write path. The project is being built in three phases (see the "Project phase" section
below); the repo today is Phase 1 complete + Phase 2 in progress: the storage engine, a raw-TCP
data-plane server (`api/rawdata`) serving both `Fetch` (zero-copy `sendfile(2)`) and `Produce`, and a
`hashicorp/raft`-backed metadata control plane (package `raft`) that a multi-broker cluster actually
joins and replicates through — verified against two real broker processes, not just unit tests.
`data.proto` keeps only `GetMetadata`/`CommitOffset`/`GetOffset`/`ListOffsets` now that `Fetch`/`Produce`
moved to the raw path.

## Commands

Standard Go tooling, no Makefile/CI config in the repo:

```
go build ./...     # build everything
go vet ./...        # static checks
go test ./...        # run all tests
go test ./test/storage/... -v          # storage package tests (Index/Segment/Log)
go test ./test/storage/... -run TestName -v   # run a single test
go test ./test/storage/... -update     # regenerate golden fixtures after an intentional behavior change
go test ./test/broker/... -v           # broker package tests (Registry: log/flusher bookkeeping)
go test ./test/broker/... -update      # regenerate golden fixtures after an intentional behavior change
go test ./test/rawdata/... -v          # raw TCP data-plane integration tests (Fetch + Produce)
go test ./test/bench/... -bench=. -benchmem   # zero-copy vs legacy-decode Fetch benchmark
go test ./test/raft/... -v             # FSM unit tests + single-node Node bootstrap/propose tests
```

Regenerating gRPC code from `.proto` sources (`api/rpc/<plane>/*.pb.go` mirrors
`api/proto/<plane>/*.proto` under the `--go_out` dir, i.e. `paths=source_relative` — `--proto_path`
is rooted at `api/proto` itself so the generated tree lands flat under `api/rpc`, no doubled segment):

```
protoc --proto_path=api/proto \
  --go_out=api/rpc --go_opt=paths=source_relative \
  --go-grpc_out=api/rpc --go-grpc_opt=paths=source_relative \
  data/data.proto kadmin/kadmin.proto
```

## Architecture

Four packages, cleanly layered: `storage` (the log engine) → `broker` (per-partition local-disk
bookkeeping on top of the log engine — no cluster metadata anymore, see below) → `raft` (the metadata
control-plane consensus layer, sits beside `broker`, drives it) → `api` (two servers: a gRPC control
plane — generated code under `api/rpc/...` plus a hand-written server implementation in `api/kadmin`
that talks to `raft.Node`, not `broker.Registry` — and a hand-written raw-TCP data plane in
`api/rawdata` that still talks to `broker.Registry` directly, since message data never goes through
Raft). `cmd/kage/main.go` wires a `raft.Node` + `broker.Registry` + `raft.Reconciler` to both: a
`kadmin` gRPC server (control/admin plane) and a raw TCP server (data plane: `Fetch` and `Produce`).

### `storage`: the append-only log engine

Three-tier design, `Log` → `Segment` → `Index`:

- **Record format** (`segment.go`): each record on disk is `8-byte big-endian absolute offset` +
  `4-byte big-endian length` + payload. No checksum field currently.
- **`Segment`** owns one `<baseOffset>.log`/`<baseOffset>.index` file pair (filenames are the base
  offset zero-padded to 20 digits, e.g. `00000000000000000000.log`). `Append` writes the record, then
  adds a **sparse** index entry (offset relative to the segment's baseOffset → physical byte position)
  only for the segment's first record and thereafter whenever `bytesSinceIndex >= IndexIntervalBytes`
  (default 4 KB, matching Kafka's `log.index.interval.bytes`). `Read` binary-searches the index for the
  nearest entry at-or-before the target offset, then linear-scans forward through the log from that
  physical position until it hits the exact offset — this scan is what makes the index safe to leave
  sparse.
- **`Index`** (`index.go`) is a memory-mapped (`gommap`) fixed-width array of `(4-byte relative
  offset, 4-byte physical position)` entries, truncated up front to `MaxIndexSize` and mmap'd; `Close`
  truncates back down to actual used size before syncing. `Read` binary-searches by relative offset,
  returning the nearest entry ≤ the target when there's no exact match, or `io.EOF` if the target is
  smaller than every stored entry; `Read(-1)` is used as a "give me the last entry" query (relies on
  unsigned wraparound of the target search value). The binary search bounds (`low`/`high`) are signed
  (`int64`) specifically so `high` can go negative for that below-smallest-entry case instead of
  underflowing as a `uint64` and driving an out-of-range mmap access.
- **`Log`** (`log.go`) owns an ordered slice of segments plus the current `activeSegment`. `Append`
  rolls to a new segment (`newSegment(activeSegment.nextOffset)`) when the write would exceed
  `MaxSegmentSize`. `Read` binary-searches `segments` by `nextOffset` (`sort.Search`) to find which
  segment owns a given absolute offset, then delegates. On `setup()`, all `*.log` files in the
  directory are discovered, their base offsets parsed from the filename, sorted, and every segment is
  eagerly reopened/recovered.
- **Recovery** (`Segment.recover()`): because the index is sparse, its last entry is *not* necessarily
  the log's actual last record, so recovery can't just trust `Index.Read(-1)`. Instead it jumps to the
  last indexed position (or byte 0 if the index is empty) and scans forward record-by-record to the
  true end of the file, deriving both `nextOffset` and `bytesSinceIndex` from that scan. No separate
  WAL/checkpoint.
- `LogConfig`/`DefaultLogConfig()` centralizes `MaxSegmentSize`, `MaxIndexSize`, `IndexIntervalBytes`,
  `RetentionPeriod`, `FlushInterval`; retention/flushing are configured but not yet enforced anywhere
  in this package.
- **Zero-copy Fetch support**: `Segment.LocateRange(startOffset, maxBytes)` reuses the same sparse-index
  lookup as `Read`, then a shared `walkRecords` scan, to find the physical byte span of whole records
  (never partial) starting at `startOffset` — always at least one record even if it alone exceeds
  `maxBytes`. `Segment.OpenReader()` opens a brand-new, independent, read-only `*os.File` handle to the
  segment's `.log` file, so a fetch's sequential `Seek`+`Read` never disturbs the shared file offset
  `Append`/`Read` rely on. `Log.LocateRange`/`Log.HighWatermark` route to the owning segment and report
  `activeSegment.nextOffset` respectively. Consumed by `api/rawdata`'s Fetch path — see
  `docs/zero-copy-design.md`.
- **Tests**: `test/storage/` is an external (`storage_test`) test suite covering `Index`/`Segment`/`Log`
  — append/read round trips, sparse-index nearest-lower-entry lookups, the linear-scan fallback for
  offsets that fall between sparse index entries, segment rollover, and crash recovery. Recovery cases
  can't close and reopen a live `Segment`/`Log` (neither type exposes a `Close()`, and `Index`'s own
  `Close()`-driven truncation-to-used-size is required for a reopened index to compute its size
  correctly), so those tests instead hand-construct raw `.log`/`.index` files on disk to simulate data
  left behind by a crash. Expected values are stored as JSON `.golden` files under
  `test/storage/testdata/<index|segment|log>/`, one per test, rather than as inline assertions;
  regenerate them with `go test ./test/storage/... -update` after an intentional behavior change.

### `broker`: per-partition local-disk bookkeeping (no cluster metadata anymore)

- **`Registry`** (`registry.go`) maps `topic -> partition -> *storage.Log`, plus `topic -> partition ->
  *Flusher`. Partition directories on disk are named `<topic>-<partition>` (split on the *last* `-`, so
  topic names may contain dashes). `Startup()` eager-loads every existing partition directory under
  `baseDir`; `CreateLog` creates the partition directory and a new `Log` in it. `GetFlusher(topic,
  partition, batchSize, linger)` lazily creates and caches a `Flusher` per partition (double-checked
  locking); `ok=false` if the log doesn't exist, since raw Produce doesn't auto-create topics. This is
  purely local-disk bookkeeping now — **no topic config, no cluster membership, no leader election
  live here anymore**; that all moved to the `raft` package below. (`Registry.topics`/`TopicConfig` and
  the old `broker/topic.go`/`broker/cluster.go` files — topic-level ops and `GetClusterInfo`/`BrokerInfo`
  — were deleted once Phase 2's Raft FSM became the real source of truth for that state; `api/kadmin`
  no longer calls into `Registry` for metadata at all.)
- **`Flusher`** (`flusher.go`) is a per-partition async batching writer: producers call `Push(value)`
  and get back a `chan AppendResult` to block on; a background goroutine batches incoming
  `BatchItem`s and flushes to `storage.Log.Append` either when `batchSize` is reached or on a
  `lingerTime` ticker (classic group-commit). Each flushed item's result is delivered individually
  down its own channel so the many producers waiting on one batch each unblock independently. Consumed
  by `api/rawdata`'s Produce path via `Registry.GetFlusher`.
- **Tests**: `test/broker/` mirrors `test/storage/`'s convention — an external (`broker_test`)
  package, results asserted via `.golden` JSON files under `test/broker/testdata/registry/`, regenerate
  with `go test ./test/broker/... -update`.

### `raft`: metadata control-plane consensus (Phase 2)

Wraps `hashicorp/raft` (`raft-boltdb/v2` for the log/stable store, `raft.NewFileSnapshotStore` for
snapshots — durable from the start, not `NewInmemStore`). `raft` is allowed to import `broker`; `broker`
never imports `raft`, keeping storage/local-disk code independently testable.

- **`fsm.go`** — `State{Brokers map[int32]BrokerInfo, Topics map[string]TopicMeta}` is the entire
  metadata state machine. `Command{Type, Payload json.RawMessage}` is a single envelope (`Apply`
  type-switches once) for `RegisterBroker`/`CreateTopic`/`DeleteTopic`. Encoded as plain JSON (not
  gob/protobuf — these commands never cross their own network boundary, they ride inside
  `hashicorp/raft`'s own msgpack-encoded transport). `Snapshot`/`Restore` JSON-marshal/unmarshal the
  whole `State` (small enough that incremental snapshots aren't needed). **Known gap**:
  `applyCreateTopic` still leaves `TopicMeta.Partitions` empty — round-robin replica placement across
  brokers isn't implemented yet (`docs/raft-plan.md` Round B step 15), so `DescribeTopic` reports zero
  partitions today.
- **`node.go`** — `Node` wraps `*hraft.Raft`. `Propose(cmd, timeout)` checks leadership first; a
  non-leader gets a typed `*ErrNotLeader{LeaderID, LeaderAddr}` immediately — **redirect, don't
  proxy**, the caller (client or `api/kadmin`) is expected to cache the leader address and retry
  itself, same as a real Kafka client treats a controller redirect. `Bootstrap()` writes a
  single-member raft configuration so a fresh node can win its own election and become `Leader`;
  restarting the same bootstrap broker swallows `hraft.ErrCantBootstrap` instead of failing (repeat
  `-bootstrap` on the same node is normal, not an error). `Join(req)` is leader-only: `AddVoter(...)`
  then `Propose(CmdRegisterBroker)` — a follower asked to `Join` returns the same not-leader redirect,
  since a raft configuration change must go through consensus (a follower self-adding could split-brain
  against a concurrent add elsewhere).
- **`reconciler.go`** — a separate goroutine (woken by the FSM's post-apply signal or a ~5s fallback
  ticker), *not* a direct `Registry.CreateLog` call inside `FSM.Apply`. Diffs `State.Topics[...].
  Partitions[...].Replicas` (which partitions this broker ID should own) against `Registry.GetLog`
  existence and calls `Registry.CreateLog` for anything missing. Kept out of `Apply` because `Apply`
  replays during snapshot `Restore`, must stay a pure fast deterministic function, and runs on every
  node regardless of whether that node owns the partition — local disk I/O errors must never affect
  consensus.
- **Multi-broker join**: verified against two real `cmd/kage` processes on loopback — broker 2's
  `-join-addr` flow logs `raft: joined cluster via <addr>`, and `GetClusterInfo` queried against
  *either* broker returns both, confirming real replication (not a one-sided view). `CreateTopic`
  issued directly at the follower correctly redirects (`"not leader; leader is broker 1 at ..."`).
- **Not yet done** (`docs/raft-plan.md` Round B/C): round-robin partition placement (step 15), a
  3-node convergence test (`test/raft/cluster_test.go`, step 16), and redirect-to-leader plus
  reconciler verification across a real 3-process cluster (Round C, steps 17-21 — includes the
  follow-up to this very file once that round lands).
- **Tests**: `test/raft/fsm_test.go` (no network — `Apply` sequences, snapshot/restore round-trip,
  golden JSON state) and `test/raft/node_test.go` (single-node `Node` over a real loopback raft
  transport: bootstrap, wait for leadership, propose, assert FSM state; plus a not-leader `Propose`
  case).

### `api`: gRPC control plane + raw-TCP data plane

Two `.proto` files, each in its own subdirectory under `api/proto/` so they generate into distinct Go
packages, split by plane per the project's control/data-plane split, plus a third, hand-written
(no `.proto`) package for the raw-TCP data plane:

- **`data/data.proto`** (`option go_package = "kage/rpc/data"`) — data plane, client-facing:
  `GetMetadata` (partition→leader discovery, meant to be called once and cached, not on every
  message), `CommitOffset`/`GetOffset`, `ListOffsets`. `Fetch` and `Produce` (and their
  `FetchRequest`/`FetchResponse`/`FetchRecord`/`ProduceRequest`/`ProduceResponse`/`Record` messages)
  were both removed (`docs/zero-copy-plan.md`'s Round 1 and Round 2) in favor of `api/rawdata`'s
  zero-copy path — see `docs/zero-copy-design.md`. Generated code: `api/rpc/data/data.{pb,grpc.pb}.go`,
  package `data` (import path `github.com/nalgnaohel/kage/api/rpc/data`).
- **`kadmin/kadmin.proto`** (`option go_package = "kage/rpc/kadmin"`) — control/admin plane:
  `CreateTopic`/`DeleteTopic`/`ListTopics`/`DescribeTopic`, `GetClusterInfo`, `JoinCluster` (broker
  membership: `broker_id`/`host`/`port` — this broker's kadmin address — plus `raft_addr` — its raft
  transport address, used for `AddVoter`). `PartitionInfo`/`PartitionMetadata` carry `leader`/
  `replicas`/`isr` fields; `isr` is set equal to `replicas` for now (real ISR tracking is Phase 3).
  Generated code: `api/rpc/kadmin/kadmin.{pb,grpc.pb}.go`, package `kadmin` (import path
  `github.com/nalgnaohel/kage/api/rpc/kadmin`).
- **`api/kadmin`** (hand-written, package `kadmin` — a different import path than the generated
  `.../api/rpc/kadmin` package, which callers alias as `pb`) implements `kadmin.KafkaAdminServer` by
  wrapping a `*raft.Node`, **not** `*broker.Registry` — every RPC goes through Raft now.
  `CreateTopic`/`DeleteTopic`/`JoinCluster` call `node.Propose`/`node.Join`; a non-leader response is
  translated to `success=false, message="not leader; leader is broker <id> at <host>:<port>"` (the
  proto has no error-code field, so this free-text convention carries all domain errors, including
  "topic already exists"). `ListTopics`/`DescribeTopic`/`GetClusterInfo` read `node.FSM().State()`
  directly with no leader check (reads may be slightly stale on a lagging follower — accepted, matches
  how Kafka clients already treat metadata as eventually consistent). Registered in `cmd/kage/main.go`
  (flags: `-data-dir`, `-grpc-addr`, `-raw-addr`, `-host`, `-port`, `-broker-id`, `-cluster-id`,
  `-raft-addr`, `-raft-port`, `-bootstrap`, `-join-addr`) as the `kadmin` gRPC control-plane server.
- **`api/rawdata`** (hand-written, package `rawdata`, no `.proto` — a small hand-rolled binary framing,
  BigEndian, documented in `docs/zero-copy-design.md`) — the raw-TCP data plane, both `Fetch` and
  `Produce`. `protocol.go` defines the request/response frame layout and both `ApiFetch`/`ApiProduce`
  API keys; `server.go`'s `Server{registry *broker.Registry}` accepts connections and dispatches by API
  key; `fetch.go` implements `ApiFetch` end-to-end (`Registry.GetLog` → `Log.LocateRange`/
  `HighWatermark` → `Segment.OpenReader` → `io.CopyN` straight onto the `net.Conn`, hitting real
  `sendfile(2)`); `produce.go` implements `ApiProduce` (`Registry.GetFlusher` → `Flusher.Push` → block
  on the result channel → `EncodeProduceResponse` with the new `baseOffset`). Registered in
  `cmd/kage/main.go` via a second listener (`-raw-addr`, default `:9092`, Kafka's own client port)
  running alongside the `kadmin` gRPC listener.

## Project phase (context for design decisions)

The architecture intentionally mirrors modern Kafka (KRaft), not the simpler "Raft over everything"
approach: metadata (topic config, partition→leader assignment, broker membership) goes through a
`hashicorp/raft` controller quorum (package `raft`), while message data replicates leader→follower via
ISR with a high-watermark, deliberately kept off the consensus hot path (Phase 3, not started). Message
data must never be routed through Raft — nothing in `raft/fsm.go` or `raft/commands.go` carries record
bytes, only topic/broker metadata.

- **Phase 1 (storage + data plane): done.** Storage engine (`storage`), and a raw-TCP data-plane server
  (`api/rawdata`) serving both `Fetch` and `Produce`. `docs/zero-copy-plan.md`/`docs/zero-copy-design.md`
  document why: gRPC/HTTP2 framing + TLS both rule out kernel `sendfile(2)`, so `Fetch`/`Produce` moved
  off gRPC entirely; `data.proto` no longer carries either RPC.
- **Phase 2 (Raft metadata control plane): Round A + Round B mostly done, per `docs/raft-plan.md`.**
  Round A (single-node FSM, bootstrap, `CreateTopic`/`ListTopics` through Raft) and Round B (broker
  membership + multi-node `JoinCluster`, steps 11-14) are implemented and verified against two real
  `cmd/kage` processes replicating over loopback raft — not just unit tests. Still open: round-robin
  partition placement (step 15 — `TopicMeta.Partitions` is populated but always empty right now) and a
  3-node convergence test (step 16), then Round C (redirect-to-leader across a real 3-process cluster,
  reconciler verification, `test/kadmin/`).
- **Phase 3 (ISR data replication): not started.** Design-only for now — high-watermark and ISR flow are
  not implemented; `PartitionInfo`'s `isr` field is currently just a copy of `replicas`.
