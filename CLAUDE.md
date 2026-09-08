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
go test ./...        # run tests (none exist yet)
go test ./storage -run TestName -v   # run a single test once tests are added
```

Regenerating gRPC code from `.proto` sources (inferred from the existing generated-file layout —
`api/rpc/api/proto/*.pb.go` mirrors `api/proto/*.proto` under the `--go_out` dir, i.e.
`paths=source_relative`):

```
protoc --proto_path=. \
  --go_out=api/rpc --go_opt=paths=source_relative \
  --go-grpc_out=api/rpc --go-grpc_opt=paths=source_relative \
  api/proto/data.proto api/proto/kadmin.proto
```

`broker/flusher.go` currently fails to build: it uses `time.Duration`/`time.NewTicker` and
`storage.Log` with no `import` block, and references `item.ResultChan` although the `BatchItem`
struct field is the unexported `resultChan`. Fix the imports and the field name/casing before
building the `broker` package.

## Architecture

Three packages, cleanly layered: `storage` (the log engine) → `broker` (per-partition orchestration
on top of the log engine) → `api` (gRPC surface, hand-written `.proto` + generated code). `main.go`
is still the unmodified GoLand template — nothing is wired to a running server yet.

### `storage`: the append-only log engine

Three-tier design, `Log` → `Segment` → `Index`:

- **Record format** (`segment.go`): each record on disk is `8-byte big-endian absolute offset` +
  `4-byte big-endian length` + payload. No checksum field currently.
- **`Segment`** owns one `<baseOffset>.log`/`<baseOffset>.index` file pair (filenames are the base
  offset zero-padded to 20 digits, e.g. `00000000000000000000.log`). `Append` writes the record then
  appends an index entry keyed by the *offset relative to the segment's baseOffset* → physical byte
  position in the log file. `Read` binary-searches the index for the nearest entry, then linear-scans
  forward through the log from that physical position to the exact offset (index is sparse-capable
  even though writes today add an entry per record).
- **`Index`** (`index.go`) is a memory-mapped (`gommap`) fixed-width array of `(4-byte relative
  offset, 4-byte physical position)` entries, truncated up front to `MaxIndexSize` and mmap'd; `Close`
  truncates back down to actual used size before syncing. `Read` binary-searches by relative offset;
  `Read(-1)` is used as a "give me the last entry" query (relies on unsigned wraparound of the target
  search value) to recover `nextOffset` on startup.
- **`Log`** (`log.go`) owns an ordered slice of segments plus the current `activeSegment`. `Append`
  rolls to a new segment (`newSegment(activeSegment.nextOffset)`) when the write would exceed
  `MaxSegmentSize`. `Read` binary-searches `segments` by `nextOffset` (`sort.Search`) to find which
  segment owns a given absolute offset, then delegates. On `setup()`, all `*.log` files in the
  directory are discovered, their base offsets parsed from the filename, sorted, and every segment is
  eagerly reopened/recovered (recovery = re-derive `currentSize` from file size and `nextOffset` from
  the last index entry — no separate WAL/checkpoint).
- `Config`/`DefaultConfig()` centralizes `MaxSegmentSize`, `MaxIndexSize`, `RetentionPeriod`,
  `FlushInterval`; retention/flushing are configured but not yet enforced anywhere in this package.

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
  down its own channel so the many producers waiting on one batch each unblock independently. (See
  the build-breaking bug noted under Commands.)

### `api`: gRPC surface

Two `.proto` files under `api/proto/`, split by plane per the project's control/data-plane split:

- **`data.proto`** — data plane, client-facing: `GetMetadata` (partition→leader discovery, meant to
  be called once and cached, not on every message), `Produce`, `Fetch` (long-poll style via
  `max_wait_ms`/`min_bytes`), `CommitOffset`/`GetOffset`, `ListOffsets`.
- **`kadmin.proto`** — control/admin plane: `CreateTopic`/`DeleteTopic`/`ListTopics`/`DescribeTopic`,
  `GetClusterInfo`. `PartitionInfo`/`PartitionMetadata` already carry `leader`/`replicas`/`isr` fields
  anticipating the Raft + ISR phases even though nothing populates them yet.

Generated code lands in `api/rpc/api/proto/{data,kadmin}.{pb,grpc.pb}.go` — note the doubled
`api/proto` path segment, a side effect of `--go_out=api/rpc --go_opt=paths=source_relative`. Both
proto files currently declare `option go_package = "kage/rpc/kadmin"`, so both generate into the same
Go package `kadmin` (import path `github.com/nalgnaohel/kage/api/rpc/api/proto`) — worth splitting
into distinct packages before the data-plane and admin-plane servers grow much further.

## Project phase (context for design decisions)

The architecture intentionally mirrors modern Kafka (KRaft), not the simpler "Raft over everything"
approach: metadata (topic config, partition→leader assignment, broker membership) is meant to go
through a `hashicorp/raft` controller quorum (Phase 2, not started), while message data is meant to
replicate leader→follower via ISR with a high-watermark, deliberately kept off the consensus hot path
(Phase 3, not started). Message data must never be routed through Raft. Today's code is Phase 1: a
working single-broker storage engine, plus the `broker` package's registry/flusher scaffolding for
wiring that engine up to the `data.proto` gRPC service.
