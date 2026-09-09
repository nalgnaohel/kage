# Storage engine design: Log, Segment, Index

This document describes the on-disk layout and read/write paths of Kage's storage engine
(`storage/log.go`, `storage/segment.go`, `storage/index.go`) — the append-only log at the bottom of
the KRaft-style architecture described in `CLAUDE.md`. It intentionally mirrors Apache Kafka's own
log layer: a `Log` is an ordered sequence of `Segment`s, and each `Segment` pairs an append-only
`.log` file with a sparse `.index` file for fast lookups.

## Why this shape

Message data must never be routed through the (future) Raft consensus layer — consensus owns
metadata, not the hot write path. That means the storage engine has to stand on its own: fast
sequential appends, and reads that don't require scanning the whole file. A dense index (one entry
per record) would make lookups O(log n) exact, but at the cost of an index file as large as the log
itself. Kafka's answer — and this project's — is a **sparse** index: index only every few KB, and
fall back to a short linear scan from the nearest indexed position. That trade keeps the index small
and mmap-friendly while keeping lookups fast in practice.

## Layout at a glance

```
+--------------------------------------------------------------------+
| Log  (one directory per partition, e.g. data/orders-0/)            |
|                                                                    |
| segments[]  (ordered by baseOffset, ascending):                    |
|                                                                    |
|   +--------------+   +--------------+   +--------------+           |
|   | Segment #0   |   | Segment #1   |   | Segment #2   |           |
|   | base = 0     |   | base = 120   |   | base = 250   |           |
|   | next = 120   |   | next = 250   |   | next = 278   |           |
|   | [0, 120)     |   | [120, 250)   |   | [250, 278)   |           |
|   +--------------+   +--------------+   +--------------+           |
|                                                                    |
|   activeSegment = the last segment in the slice (Segment #2 above) |
|                                                                    |
| Read(offset):                                                      |
|   sort.Search(segments, seg.nextOffset > offset)                   |
|   -> pick the segment whose [baseOffset, nextOffset) range         |
|      contains offset, then delegate to it                          |
|                                                                    |
| Append(data):                                                      |
|   if activeSegment would exceed MaxSegmentSize                     |
|   -> newSegment(activeSegment.nextOffset), append there            |
+--------------------------------------------------------------------+

  each Segment = one file pair on disk:

    00000000000000000000.log   00000000000000000000.index
    00000000000000000120.log   00000000000000000120.index
    00000000000000000250.log   00000000000000000250.index
        (filename = baseOffset, zero-padded to 20 digits)

+------------------------------------------------------------------------------------------------+
| Segment #1 (base = 120)                                                                        |
|                                                                                                |
| .log  (append-only; each record = 12-byte header + payload)                                    |
|                                                                                                |
| +-------------+----------+-----------+  +-------------+----------+---------+                   |
| | offset (8B) | len (4B) | payload   |  | offset (8B) | len (4B) | payload |                   |
| +-------------+----------+-----------+  +-------------+----------+---------+                   |
| | = 120, BE   | = N, BE  | (N bytes) |  | = 121, BE   |          |         |                   |
| +-------------+----------+-----------+  +-------------+----------+---------+                   |
|   ...                                                                                          |
|                                                                                                |
|   the first record starts at pos = 0; each next record starts at                               |
|   pos + 12 + len(payload), and so on                                                           |
|                                                                                                |
| .index  (mmap via gommap; each entry = 8 bytes; SPARSE - not every record)                     |
|                                                                                                |
| +----------------+-------------+                                                               |
| | relOffset = 0  | physPos = 0 |   <- the segment's first record is always indexed (pos == 0), |
|                                         so a lookup at baseOffset never scans from byte 0      |
| +----------------+-------------+                                                               |
| | relOffset = 34 | physPos = ? |   <- next entry once bytesSinceIndex >= IndexIntervalBytes    |
|                                         (4 KB by default)                                      |
| +----------------+-------------+                                                               |
|                                                                                                |
|   (the file is truncated to MaxIndexSize up front for the mmap;                                |
|    Close() truncates it back down to the actual used size)                                     |
|                                                                                                |
| Read(offset = 125):                                                                            |
|   1. relOffset = 125 - 120 = 5                                                                 |
|   2. Index.Read(5) -> binary search -> nearest entry with relOffset <= 5                       |
|      (io.EOF, not a panic, if 5 is smaller than every stored entry)                            |
|   3. Seek the .log file to that entry's physPos, then LINEAR-SCAN                              |
|      forward record by record (reading each header) until                                      |
|      actualOffset == 125 (or actualOffset > 125 -> "not found")                                |
|      -> this scan is exactly what makes a sparse index safe                                    |
+------------------------------------------------------------------------------------------------+
```

## Components

### `Log`

An ordered slice of `Segment`s plus a pointer to the current `activeSegment`, all living under one
partition directory. `Log` doesn't know anything about the record format — its only job is routing:

- **Append** writes to `activeSegment`, rolling to a brand-new segment (starting at
  `activeSegment.nextOffset`) first if the write would exceed `MaxSegmentSize`.
- **Read** binary-searches `segments` (via `sort.Search` on `nextOffset`) to find which segment's
  `[baseOffset, nextOffset)` range owns a given absolute offset, then delegates to it.
- **setup()** (called from `NewLog`) discovers every `*.log` file already on disk, parses each
  filename's base offset, sorts them, and eagerly reopens/recovers each one — so restarting a broker
  picks up exactly where it left off.

### `Segment`

Owns one `<baseOffset>.log` / `<baseOffset>.index` file pair and the bookkeeping for it
(`baseOffset`, `nextOffset`, `currentSize`, `bytesSinceIndex`).

- **Append** writes the 8-byte offset + 4-byte length + payload record to the log file, then decides
  whether to add a sparse index entry: always for the segment's very first record (`pos == 0`),
  otherwise only once `bytesSinceIndex >= IndexIntervalBytes` since the last indexed entry.
- **Read** looks up the nearest index entry at-or-before the target offset, then linear-scans forward
  through the log from that physical position until it hits the exact offset (or overshoots it, which
  means the offset doesn't exist).
- **recover()** rebuilds `nextOffset` and `bytesSinceIndex` after (re)opening a segment. Because the
  index is sparse, its last entry is *not* necessarily the log's actual last record — so recovery
  jumps to the last indexed position (or byte 0 if the index is empty) and scans forward
  record-by-record to the file's true end, deriving both values from that scan rather than trusting
  the index alone.
- **Close** flushes the log file and truncates the index down to its real used size (see below) —
  required for a later reopen of the same segment to compute the index's size correctly.

### `Index`

A memory-mapped (`gommap`), fixed-width array of `(4-byte relative offset, 4-byte physical position)`
entries.

- The backing file is truncated up front to `MaxIndexSize` so the whole thing can be mapped once;
  **Close** truncates it back down to the actual bytes used before syncing, so a later `NewIndex` on
  the same file reads its real size instead of the padded one.
- **Read** binary-searches by relative offset. An exact match returns directly; otherwise it returns
  the nearest entry *at or before* the target — the property that makes leaving the index sparse safe,
  since `Segment.Read` can always resume a linear scan from there. If the target is smaller than every
  stored entry, it returns `io.EOF` rather than a match. `Read(-1)` is a special "give me the last
  entry" query, exploiting unsigned wraparound of the search target.

## Two recovery scenarios, two different tests

`test/storage/` exercises recovery two different ways, because they cover genuinely different
failures:

- **Graceful restart** (`Segment.Close()` / `Log.Close()` then reopen): the index has already been
  truncated to its real size, so a fresh `Index` reads it correctly.
- **Crash mid-write** (no `Close()` call at all — e.g. the process died): the index file may still be
  padded to `MaxIndexSize`, and any records written since the last index entry were never indexed.
  These tests hand-construct raw `.log`/`.index` files on disk to simulate exactly that state, and
  assert that `recover()`'s forward scan still finds the true end of the log.

## Known trade-offs

- Sparse indexing trades a small amount of read latency (a bounded linear scan, at most
  `IndexIntervalBytes` worth of records) for a much smaller index file — the same trade Kafka makes
  with `log.index.interval.bytes`.
- There's no checksum field in the record format yet, and retention/flush-interval config
  (`RetentionPeriod`, `FlushInterval`) is parsed but not enforced anywhere in `storage/` yet.
