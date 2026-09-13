# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Kage — a Kafka-inspired distributed message queue, built **KRaft-style**: Raft owns metadata/control
plane, ISR (in-sync replicas) owns the data plane, deliberately kept separate so consensus never sits
on the hot write path. The project is being built in three phases (see the "Project phase" section
below); the repo today is early/mid Phase 1 — the storage engine, not yet wired to a running server.

## Commands

Standard Go tooling, no Makefile/CI config in the repo:

```
go build ./...     # build everything
go vet ./...        # static checks
go test ./...        # run all tests
go test ./test/storage/... -v          # storage package tests (Index/Segment/Log)
go test ./test/storage/... -run TestName -v   # run a single test
go test ./test/storage/... -update     # regenerate golden fixtures after an intentional behavior change
```

Regenerating gRPC code from `.proto` sources (`api/rpc/api/proto/<plane>/*.pb.go` mirrors
`api/proto/<plane>/*.proto` under the `--go_out` dir, i.e. `paths=source_relative` — note the doubled
`api/proto` path segment, a side effect of `--go_out=api/rpc` with paths already rooted at `api/proto`):

```
protoc --proto_path=. \
  --go_out=api/rpc --go_opt=paths=source_relative \
  --go-grpc_out=api/rpc --go-grpc_opt=paths=source_relative \
  api/proto/data/data.proto api/proto/kadmin/kadmin.proto
```

## Architecture

Three packages, cleanly layered: `storage` (the log engine) → `broker` (per-partition orchestration
on top of the log engine) → `api` (gRPC surface, hand-written `.proto` + generated code). `main.go`
is still the unmodified GoLand template — nothing is wired to a running server yet.

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
  `Startup()` eager-loads every existing partition directory under `baseDir`; `CreateLog` creates a
  new one. This is the layer a future gRPC server looks up logs through — there's no partition
  assignment/leader logic here, it's local-disk bookkeeping only (control-plane concerns like leader
  election belong to the not-yet-built Raft metadata layer).
- **`Flusher`** (`flusher.go`) is a per-partition async batching writer: producers call `Push(value)`
  and get back a `chan AppendResult` to block on; a background goroutine batches incoming
  `BatchItem`s and flushes to `storage.Log.Append` either when `batchSize` is reached or on a
  `lingerTime` ticker (classic group-commit). Each flushed item's result is delivered individually
  down its own channel so the many producers waiting on one batch each unblock independently.

### `api`: gRPC surface

Two `.proto` files, each in its own subdirectory under `api/proto/` so they generate into distinct Go
packages, split by plane per the project's control/data-plane split:

- **`data/data.proto`** (`option go_package = "kage/rpc/data"`) — data plane, client-facing:
  `GetMetadata` (partition→leader discovery, meant to be called once and cached, not on every
  message), `Produce`, `Fetch` (long-poll style via `max_wait_ms`/`min_bytes`), `CommitOffset`/
  `GetOffset`, `ListOffsets`. Generated code: `api/rpc/api/proto/data/data.{pb,grpc.pb}.go`, package
  `data` (import path `github.com/nalgnaohel/kage/api/rpc/api/proto/data`).
- **`kadmin/kadmin.proto`** (`option go_package = "kage/rpc/kadmin"`) — control/admin plane:
  `CreateTopic`/`DeleteTopic`/`ListTopics`/`DescribeTopic`, `GetClusterInfo`. `PartitionInfo`/
  `PartitionMetadata` already carry `leader`/`replicas`/`isr` fields anticipating the Raft + ISR phases
  even though nothing populates them yet. Generated code: `api/rpc/api/proto/kadmin/kadmin.{pb,grpc.pb}.go`,
  package `kadmin` (import path `github.com/nalgnaohel/kage/api/rpc/api/proto/kadmin`).

## Project phase (context for design decisions)

The architecture intentionally mirrors modern Kafka (KRaft), not the simpler "Raft over everything"
approach: metadata (topic config, partition→leader assignment, broker membership) is meant to go
through a `hashicorp/raft` controller quorum (Phase 2, not started), while message data is meant to
replicate leader→follower via ISR with a high-watermark, deliberately kept off the consensus hot path
(Phase 3, not started). Message data must never be routed through Raft. Today's code is Phase 1: a
working single-broker storage engine, plus the `broker` package's registry/flusher scaffolding for
wiring that engine up to the `data.proto` gRPC service.
