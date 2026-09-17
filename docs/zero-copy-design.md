# Zero-copy Fetch: raw TCP data-plane design

This document describes the raw TCP data-plane path (`api/rawdata`) that carries `Fetch` in place of
gRPC, and the storage-layer support for it (`storage/segment.go`, `storage/log.go`). See
`docs/zero-copy-plan.md` for the implementation plan and rationale trail this was built from — this
doc is the as-built reference, matching `docs/storage-design.md`'s role for the storage engine.

Status: Round 1 (Fetch) is implemented and covered by automated tests/benchmarks. Produce still goes
through the `kadmin`-adjacent gRPC service (`api/rpc/data`) — see Known trade-offs.

TODO (manual verification, not yet done):
- `strace -f -e trace=sendfile -p <pid>` against a running broker during a real Fetch, to directly
  confirm the `sendfile(2)` syscall fires.
- End-to-end interop check: produce a record via the real `kadmin`-adjacent gRPC `Produce` RPC, then
  fetch it back via the raw TCP path, and confirm the bytes match. The current automated integration
  test (`test/rawdata/fetch_test.go`) writes via `storage.Log.Append` directly and doesn't exercise the
  gRPC server, so this hasn't been checked against a real running server yet.

## Why this shape

`data.proto`'s old `Fetch` RPC decoded every record into a `FetchRecord` message before the client
ever saw it. That shape can't reach zero-copy for two separate reasons: gRPC's HTTP/2 framing (and any
TLS layered on top) forces at least one user-space pass over the bytes regardless of what's inside
them, and building a `FetchRecord` message per record requires allocating/copying even before framing
gets involved. Real Kafka hit the same wall decades ago and answered it the same way this project
does: skip the RPC framework entirely for the client data path and use a small, plaintext, binary wire
format instead, so the record bytes can go straight from the log segment's page cache to the socket via
`sendfile(2)` with no user-space copy in between.

`GetMetadata`/`CommitOffset`/`GetOffset`/`ListOffsets` stay on gRPC — they're low-volume control calls,
not per-record hot path, so there's nothing to gain by moving them.

## Layout at a glance

Request frame:

```
+--------------+-----------+---------------+----------------+-----------------------+
| totalLength  | apiKey    | apiVersion    | correlationID  | body                  |
| 4B           | 2B        | 2B            | 4B             | (apiKey-specific)     |
+--------------+-----------+---------------+----------------+-----------------------+

FetchRequest body:
+--------------+-----------+---------------+----------------+---------------+
| topicLen     | topic     | partition     | fetchOffset    | maxBytes      |
| 2B           | topicLen  | 4B            | 8B             | 4B            |
+--------------+-----------+---------------+----------------+---------------+
```

Response frame — the payload is the raw on-disk record span, byte-for-byte, never decoded:

```
+--------------+----------------+-----------+----------------+----------------+---------------+-----------+
| totalLength  | correlationID  | errorCode | highWatermark  | nextOffset     | payloadLength | payload   |
| 4B           | 4B             | 2B        | 8B             | 8B             | 4B            | ...       |
+--------------+----------------+-----------+----------------+----------------+---------------+-----------+
```

`Log.Read`/`Segment.Read` decode-and-return: they walk to the exact offset and copy just that one
record's payload out. `LocateRange` does the opposite — it walks forward accumulating whole records
until `maxBytes` is reached (always including at least one record, even if it alone exceeds
`maxBytes`), then hands back a `(Pos, Length)` byte span into the `.log` file, untouched. Nothing in
that span is parsed; the caller transfers it as-is.

## Components

- `storage.Segment.LocateRange(startOffset, maxBytes)` — reuses the same sparse-index lookup as
  `Read`, then a shared `walkRecords` scan, to find the physical byte span of whole records starting
  at `startOffset`.
- `storage.Segment.OpenReader()` — opens a brand-new, independent, read-only `*os.File` handle to the
  segment's `.log` file. Every fetch gets its own handle so a sequential `Seek`+`Read` never disturbs
  the shared file offset that `Append`/`Read` rely on being untouched.
- `storage.Log.LocateRange`/`HighWatermark` — route to the owning segment and report
  `activeSegment.nextOffset` respectively; in this single-broker, no-replication phase the high
  watermark is just the log's own write position.
- `api/rawdata`'s Fetch path (`protocol.go`, `server.go`, `fetch.go`) — decodes the request, looks the
  log up via `broker.Registry.GetLog`, handles the caught-up (`fetchOffset == HighWatermark`) and
  out-of-range (`fetchOffset > HighWatermark`) cases, then `LocateRange` → `OpenReader` → `Seek` →
  `EncodeFetchResponseHeader` → `io.CopyN(conn, f, length)`. That last call is where the transfer
  dispatches to real `sendfile(2)`: `io.CopyN` type-asserts the destination's dynamic type as
  `io.ReaderFrom`, which `*net.TCPConn` satisfies, and its `ReadFrom` unwraps the source down to the
  underlying `*os.File` to call the syscall directly — provided nothing wraps the `net.Conn` (no TLS,
  no `bufio.Writer`) between here and the actual write.
- `main.go`'s raw TCP listener — a second `net.Listener` (`-raw-addr`, default `:9092`, Kafka's own
  client port) running `rawdata.Server.Serve` in its own goroutine, alongside the existing `kadmin`
  gRPC listener.

## Known trade-offs

- **Plaintext only.** Adding TLS here would force the same fallback real Kafka's SSL listeners hit —
  TLS terminates in user space, so `sendfile(2)` stops being reachable. Accepted, not overlooked, for
  this phase.
- **No long-poll blocking.** `min_bytes`/`max_wait_ms` semantics (Kafka's long-poll Fetch) are deferred;
  v1's Fetch always answers immediately with whatever is available.
- **A fetch never crosses a segment boundary.** `LocateRange` only ever looks inside the one segment
  that owns `startOffset` — matches Kafka's own per-chunk-per-file Fetch behavior; a consumer near a
  segment boundary just issues another Fetch for the next range.
- **Produce is still on gRPC.** `api/rawdata` defines `ApiProduce`'s wire format already but
  `handleConn` only dispatches `ApiFetch`; an `ApiProduce` request gets back a fixed `ErrInternal`
  response. Raw Produce, `broker.Registry.GetFlusher`, and removing `Produce` from `data.proto` are
  Round 2 of `docs/zero-copy-plan.md`, not done here.

## Benchmark

`test/bench/fetch_bench_test.go` compares the old decode-per-record approach (`seg.Read` per record →
copy into a mirror `legacyFetchRecord` → `json.Marshal` → length-prefixed write) against the zero-copy
path (`seg.LocateRange` → `seg.OpenReader` → `EncodeFetchResponseHeader` → `io.CopyN`), both over a
real loopback TCP connection, across two sub-cases: `Small_100Bx200recs` (200 records × 100 bytes) and
`Large_64KBx4recs` (4 records × 64 KB).

Representative output (`go test ./test/bench/... -bench=. -benchmem`):

```
BenchmarkFetch_LegacyDecode/Small_100Bx200recs-12    1142    1072095 ns/op   111475 B/op    412 allocs/op
BenchmarkFetch_LegacyDecode/Large_64KBx4recs-12      3110     373024 ns/op  1010882 B/op     16 allocs/op
BenchmarkFetch_ZeroCopy/Small_100Bx200recs-12      112506      10784 ns/op      344 B/op      9 allocs/op
BenchmarkFetch_ZeroCopy/Large_64KBx4recs-12         31195      33614 ns/op      344 B/op      9 allocs/op
```

The shape matches the prediction: `BenchmarkFetch_ZeroCopy`'s `B/op`/`allocs/op` stay constant
regardless of record count or payload size (just the fixed 30-byte header buffer plus file-handle
bookkeeping), while `BenchmarkFetch_LegacyDecode`'s allocations scale with record count (412 allocs at
200 small records) and its bytes scale with payload size (~1 MB copied for the 4×64 KB case) — the gap
between the two widens most at `Large_64KBx4recs`, since zero-copy's advantage tracks payload size, not
record count.

`-benchmem` can't observe the `sendfile(2)` syscall itself, only its absence of allocation. To confirm
it directly: run the broker, issue a real Fetch (e.g. a small throwaway client), and watch
`strace -f -e trace=sendfile -p <pid>` for a `sendfile` line — a manual, one-time check, not part of
the automated suite.
