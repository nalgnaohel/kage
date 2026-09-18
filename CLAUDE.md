# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Kage — a Kafka-inspired distributed message queue, built **KRaft-style**: Raft owns metadata/control
plane, ISR (in-sync replicas) owns the data plane, deliberately kept separate so consensus never sits
on the hot write path. The project is being built in three phases (see the "Project phase" section
below); the repo today is early/mid Phase 1 — the storage engine, a running single-broker `kadmin`
gRPC control-plane server (topic create/list/describe/delete, cluster info), and a raw-TCP data-plane
server (`api/rawdata`) serving both `Fetch` (zero-copy `sendfile(2)`) and `Produce`. `data.proto` keeps
only `GetMetadata`/`CommitOffset`/`GetOffset`/`ListOffsets` now that both moved to the raw path.

## Commands

Standard Go tooling, no Makefile/CI config in the repo:

```
go build ./...     # build everything
go vet ./...        # static checks
go test ./...        # run all tests
go test ./test/storage/... -v          # storage package tests (Index/Segment/Log)
go test ./test/storage/... -run TestName -v   # run a single test
go test ./test/storage/... -update     # regenerate golden fixtures after an intentional behavior change
go test ./test/broker/... -v           # broker package tests (topic/cluster management)
go test ./test/broker/... -update      # regenerate golden fixtures after an intentional behavior change
go test ./test/rawdata/... -v          # raw TCP data-plane integration tests (Fetch)
go test ./test/bench/... -bench=. -benchmem   # zero-copy vs legacy-decode Fetch benchmark
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

Three packages, cleanly layered: `storage` (the log engine) → `broker` (per-partition orchestration
plus topic/cluster bookkeeping on top of the log engine) → `api` (two servers: a gRPC control plane —
generated code under `api/rpc/...` plus a hand-written server implementation in `api/kadmin` — and a
hand-written raw-TCP data plane in `api/rawdata`). `main.go` wires `broker.Registry` to both: a
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

### `broker`: per-partition orchestration above the log engine

- **`Registry`** (`registry.go`) maps `topic -> partition -> *storage.Log`. Partition directories on
  disk are named `<topic>-<partition>` (split on the *last* `-`, so topic names may contain dashes).
  `Startup()` eager-loads every existing partition directory under `baseDir`; `CreateLog` creates the
  partition directory and a new `Log` in it. This is the layer the `kadmin` gRPC server (and, later, a
  data-plane server) look up logs through — there's no partition assignment/leader logic here, it's
  local-disk bookkeeping only (control-plane concerns like leader election belong to the not-yet-built
  Raft metadata layer).
- **`TopicConfig`/topic management** (`topic.go`) builds topic-level operations on top of `Registry`:
  `CreateTopic(topic, numPartitions, replicationFactor)` creates one `Log` per partition via
  `CreateLog` and records the config in `r.topics`; `ListTopics`/`DescribeTopic` read topic/partition
  *existence* from `r.logs`, not `r.topics`, so they stay correct across a restart — `r.topics` isn't
  persisted anywhere yet, so `Startup()` rebuilds a best-effort entry (`ReplicationFactor` defaults to
  `1`) for any topic missing one. `DeleteTopic` closes and permanently removes every partition's files
  (synchronous, irreversible — fine at single-broker scale).
- **`BrokerInfo`/`GetClusterInfo`** (`cluster.go`) reports this broker's own identity
  (`clusterID`/`brokerID`/`host`/`port`, set once at `NewRegistry` construction) as the cluster's only
  member. Real multi-broker membership is Raft/KRaft controller work (Phase 2, not started).
- **`Flusher`** (`flusher.go`) is a per-partition async batching writer: producers call `Push(value)`
  and get back a `chan AppendResult` to block on; a background goroutine batches incoming
  `BatchItem`s and flushes to `storage.Log.Append` either when `batchSize` is reached or on a
  `lingerTime` ticker (classic group-commit). Each flushed item's result is delivered individually
  down its own channel so the many producers waiting on one batch each unblock independently.
  `Registry.GetFlusher(topic, partition, batchSize, linger)` lazily creates and caches one per
  partition (double-checked locking); `ok=false` if the log doesn't exist, since raw Produce doesn't
  auto-create topics. Consumed by `api/rawdata`'s Produce path.
- **Tests**: `test/broker/` mirrors `test/storage/`'s convention — an external (`broker_test`)
  package, results asserted via `.golden` JSON files under `test/broker/testdata/<registry|topic|
  cluster>/`, regenerate with `go test ./test/broker/... -update`.

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
  `CreateTopic`/`DeleteTopic`/`ListTopics`/`DescribeTopic`, `GetClusterInfo`. `PartitionInfo`/
  `PartitionMetadata` already carry `leader`/`replicas`/`isr` fields anticipating the Raft + ISR phases
  even though nothing populates them yet. Generated code: `api/rpc/kadmin/kadmin.{pb,grpc.pb}.go`,
  package `kadmin` (import path `github.com/nalgnaohel/kage/api/rpc/kadmin`).
- **`api/kadmin`** (hand-written, package `kadmin` — a different import path than the generated
  `.../api/rpc/kadmin` package, which callers alias as `pb`) implements
  `kadmin.KafkaAdminServer` by wrapping a `*broker.Registry`: each RPC is a thin translation to the
  matching `Registry`/topic-management method. `CreateTopic`/`DeleteTopic` report domain errors via
  `success=false, message=...` (the proto has no error-code field); `DescribeTopic` returns a gRPC
  `NotFound` status for an unknown topic and fills every partition's `leader`/`replicas`/`isr` with
  this single broker's own ID (placeholder until Raft/ISR exist). Registered in `main.go` (flags:
  `-data-dir`, `-grpc-addr`, `-raw-addr`, `-host`, `-port`, `-broker-id`, `-cluster-id`) as the `kadmin`
  gRPC control-plane server.
- **`api/rawdata`** (hand-written, package `rawdata`, no `.proto` — a small hand-rolled binary framing,
  BigEndian, documented in `docs/zero-copy-design.md`) — the raw-TCP data plane, both `Fetch` and
  `Produce`. `protocol.go` defines the request/response frame layout and both `ApiFetch`/`ApiProduce`
  API keys; `server.go`'s `Server{registry *broker.Registry}` accepts connections and dispatches by API
  key; `fetch.go` implements `ApiFetch` end-to-end (`Registry.GetLog` → `Log.LocateRange`/
  `HighWatermark` → `Segment.OpenReader` → `io.CopyN` straight onto the `net.Conn`, hitting real
  `sendfile(2)`); `produce.go` implements `ApiProduce` (`Registry.GetFlusher` → `Flusher.Push` → block
  on the result channel → `EncodeProduceResponse` with the new `baseOffset`). Registered in `main.go`
  via a second listener (`-raw-addr`, default `:9092`, Kafka's own client port) running alongside the
  `kadmin` gRPC listener.

## Project phase (context for design decisions)

The architecture intentionally mirrors modern Kafka (KRaft), not the simpler "Raft over everything"
approach: metadata (topic config, partition→leader assignment, broker membership) is meant to go
through a `hashicorp/raft` controller quorum (Phase 2, not started), while message data is meant to
replicate leader→follower via ISR with a high-watermark, deliberately kept off the consensus hot path
(Phase 3, not started). Message data must never be routed through Raft. Today's code is Phase 1: a
working single-broker storage engine, a running `kadmin` gRPC control-plane server (topic/cluster
management), and a raw-TCP data-plane server (`api/rawdata`) serving both `Fetch` and `Produce`.
`docs/zero-copy-plan.md` documents this data plane's design: bypass gRPC entirely for `Produce`/`Fetch`
in favor of a raw TCP path, so `Fetch` can use kernel `sendfile(2)` (gRPC/HTTP2 framing and TLS both
rule that out) — see `docs/zero-copy-design.md` for the as-built version. Both Round 1 (`Fetch`) and
Round 2 (`Produce`) are done; `data.proto` no longer carries either RPC.
