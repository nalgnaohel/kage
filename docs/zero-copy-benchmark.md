# Zero-copy Fetch benchmark

How the zero-copy `Fetch` path (`api/rawdata/fetch.go`) is benchmarked against the old decode-per-record
approach, and the results. See `docs/zero-copy-design.md` for why the raw-TCP path exists at all; this
doc is just the measurement.

## What's being compared

Two code paths, both serving the same records over a real loopback TCP connection, in
`test/bench/fetch_bench_test.go`:

- **`BenchmarkFetch_LegacyDecode`** — simulates the pre-zero-copy shape `data.proto`'s old `Fetch` RPC
  had: `seg.Read` once per record → copy each value into a mirror `legacyFetchRecord` struct →
  `json.Marshal` the whole response → length-prefixed write to the socket. This is a stand-in for "decode
  every record into an RPC message before the client sees it," not a real gRPC server — see
  [Limitations](#limitations) below.
- **`BenchmarkFetch_ZeroCopy`** — the real production path: `seg.LocateRange` finds the byte span →
  `seg.OpenReader` opens an independent file handle → `EncodeFetchResponseHeader` writes the fixed
  header → `io.CopyN(conn, f, length)` streams the segment bytes straight to the socket, hitting
  `sendfile(2)`.

Three sub-cases, chosen to span from many-tiny-records to few-huge-records:

| Case | Records | Size each | Total payload |
|---|---|---|---|
| `Small_100Bx200recs` | 200 | 100 B | ~20 KB |
| `Large_64KBx4recs` | 4 | 64 KB | 256 KB |
| `Huge_1MBx8recs` | 8 | 1 MB | 8 MB |

`Huge_1MBx8recs` was added specifically to check whether the zero-copy advantage keeps widening at
payload sizes closer to a real large-message batch, or tapers off once fixed per-syscall overhead stops
dominating.

## Running it

```
go test ./test/bench/... -bench=. -benchmem
```

Add `-benchtime=200x` (or any fixed iteration count) for reproducible allocation counts across runs
instead of Go's default time-based iteration count, which can otherwise change `B/op` slightly between
runs on a noisy machine.

## Results

Run on an 11th Gen Intel i7-1185G7, `go test ./test/bench/... -bench=. -benchmem -benchtime=200x`:

```
BenchmarkFetch_LegacyDecode/Small_100Bx200recs-8      200    1357755 ns/op   112555 B/op    412 allocs/op
BenchmarkFetch_LegacyDecode/Large_64KBx4recs-8        200     371849 ns/op  1048565 B/op     16 allocs/op
BenchmarkFetch_LegacyDecode/Huge_1MBx8recs-8          200   16037938 ns/op 46733708 B/op     33 allocs/op
BenchmarkFetch_ZeroCopy/Small_100Bx200recs-8          200      10856 ns/op      349 B/op      9 allocs/op
BenchmarkFetch_ZeroCopy/Large_64KBx4recs-8            200      43372 ns/op      391 B/op      9 allocs/op
BenchmarkFetch_ZeroCopy/Huge_1MBx8recs-8              200    1907525 ns/op      344 B/op      9 allocs/op
```

| Case | Legacy | Zero-copy | Speedup | Legacy bytes | Zero-copy bytes |
|---|---|---|---|---|---|
| Small (200×100B) | 1.36 ms/op | 10.9 µs/op | ~125x | 112.6 KB/op | 349 B/op |
| Large (4×64KB) | 372 µs/op | 43.4 µs/op | ~8.6x | 1.05 MB/op | 391 B/op |
| Huge (8×1MB) | 16.0 ms/op | 1.91 ms/op | ~8.4x | 46.7 MB/op | 344 B/op |

### Reading the shape

- **`BenchmarkFetch_ZeroCopy`'s `B/op`/`allocs/op` stay constant** (~344-391 B, 9 allocs) regardless of
  record count or payload size — just the fixed response-header buffer plus file-handle bookkeeping.
  `io.CopyN` never allocates a buffer proportional to the payload; the data never enters a Go-managed
  buffer at all once it reaches `sendfile(2)`.
- **`BenchmarkFetch_LegacyDecode`'s allocations scale two different ways**: allocation *count* scales
  with record count (412 allocs at 200 small records, one alloc per record-copy), while bytes *copied*
  scale with payload size (~1 MB at 4×64KB; ~46.7 MB at 8×1MB — inflated ~5.8x over the raw 8 MB because
  `encoding/json` base64-encodes `[]byte` fields, and `Marshal` still holds the whole encoded response in
  memory before writing).
- **The time gap is latency-bound at small sizes, bandwidth-bound at large sizes.** ~125x at
  `Small_100Bx200recs` is dominated by fixed per-record/per-allocation overhead (412 small allocations
  cost more per byte than the copy itself). ~8.6x at `Large_64KBx4recs` and ~8.4x at `Huge_1MBx8recs`
  converge to roughly the same ratio — once payload size dominates over fixed overhead, the gap tracks
  the raw cost of copying bytes through user-space twice (legacy) versus zero times (sendfile), and
  holds steady rather than shrinking back down as messages get bigger. That's the actual evidence that
  zero-copy's advantage scales with payload size, not just record count.

## Manual `sendfile(2)` verification

`-benchmem` can only observe the *absence* of allocation — it can't observe the `sendfile(2)` syscall
itself. Confirming the syscall fires is a manual, one-time check, not part of the automated suite:

1. Run the broker: `go run ./cmd/kage`
2. Attach `strace` to its pid, watching only `sendfile`: `strace -f -e trace=sendfile -p <pid>`
3. Run the manual client, `go run ./cmd/manualverify` — it creates a topic over the `kadmin` gRPC port,
   produces one record over the raw port, and fetches it back, printing whether the round-tripped value
   matches.

**Run 2026-09-20**: the produce+fetch round trip matched end to end
(`Fetch: highWatermark=2 value="hello-zero-copy" match=true`), and `strace` showed the syscall firing
directly during the fetch:

```
sendfile(11, 12, NULL, 27) = 27
```

confirming the kernel copies record bytes straight from the segment file (fd 12) to the client socket
(fd 11) with no user-space buffer in between, exactly as designed.

## Limitations

Two gaps worth being explicit about before citing these numbers as "zero-copy beats gRPC":

- **`BenchmarkFetch_LegacyDecode` is not a real gRPC server round trip.** It reproduces the
  allocation/copy pattern the old `data.proto` `Fetch` RPC had (decode-per-record, marshal-the-whole-
  response), but doesn't go through an actual `grpc.Server`, HTTP/2 framing, or protobuf marshaling — so
  it's a lower bound on real gRPC overhead, not a measurement of it. A benchmark that spins up a real
  `grpc.Server` serving equivalent `FetchRecord` messages would be needed to compare against gRPC
  specifically.
- **8 MB is still short of "cực to."** The largest case here is 8 MB total; it wasn't pushed further
  (multi-hundred-MB or GB-scale single fetches) to keep the benchmark fast to run repeatedly. The
  bandwidth-bound ~8.4-8.6x ratio holding steady between the 256 KB and 8 MB cases is suggestive that it
  won't regress at larger sizes, but that hasn't been directly measured.
