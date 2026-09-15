package storage_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/nalgnaohel/kage/storage"
)

func segCfg(maxLogSize, maxIndexSize, indexIntervalBytes uint64) storage.SegmentConfig {
	return storage.SegmentConfig{
		MaxLogSize:         maxLogSize,
		MaxIndexSize:       maxIndexSize,
		IndexIntervalBytes: indexIntervalBytes,
	}
}

func TestNewSegment_FreshDirectory_StartsAtBaseOffset(t *testing.T) {
	// input: empty directory, baseOffset = 5
	// golden: offset returned by the first Append (nextOffset starts at
	// baseOffset)
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 5, segCfg(1024, 1024, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	off, err := seg.Append([]byte("x"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	assertGolden(t, "segment", struct{ Offset uint64 }{off})
}

func TestSegmentAppendRead_RoundTrip(t *testing.T) {
	// input: Append("hello")
	// golden: the offset returned and the payload read back
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1024, 1024, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	off, err := seg.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, err := seg.Read(off)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	assertGolden(t, "segment", struct {
		Offset  uint64
		Payload string
	}{off, string(got)})
}

func TestSegmentAppend_SequentialOffsets(t *testing.T) {
	// input: Append 3 records back to back, baseOffset = 10
	// golden: the sequence of offsets returned
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 10, segCfg(1024, 1024, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	offsets := make([]uint64, 3)
	for i := range offsets {
		off, err := seg.Append([]byte(fmt.Sprintf("rec-%d", i)))
		if err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
		offsets[i] = off
	}

	assertGolden(t, "segment", struct{ Offsets []uint64 }{offsets})
}

func TestSegmentRead_FirstRecordAlwaysIndexed(t *testing.T) {
	// input: IndexIntervalBytes set very high (1<<30), so the sparse-index
	// byte threshold would never be reached by one small record
	// golden: Read(baseOffset) still succeeds - Append always indexes a
	// segment's very first record regardless of the interval
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1<<20, 4096, 1<<30))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	off, err := seg.Append([]byte("first"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, readErr := seg.Read(off)

	assertGolden(t, "segment", struct {
		Offset  uint64
		Payload string
		Err     string
	}{off, string(got), errString(readErr)})
}

func TestSegmentRead_UnindexedRecordFoundByLinearScan(t *testing.T) {
	// input: IndexIntervalBytes set very high (1<<30) so only the first
	// record (offset 0) ever gets an index entry; append 3 more small
	// records that never reach the interval threshold
	// golden: Read on the last, never-indexed record - Segment.Read must
	// fall back to a linear scan forward from the nearest indexed entry
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1<<20, 4096, 1<<30))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	var last uint64
	for i := 0; i < 4; i++ {
		off, err := seg.Append([]byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
		last = off
	}
	got, err := seg.Read(last)

	assertGolden(t, "segment", struct {
		LastOffset uint64
		Payload    string
		Err        string
	}{last, string(got), errString(err)})
}

func TestSegmentRead_OffsetNotFound_ReturnsError(t *testing.T) {
	// input: Append a single record at offset 0, then Read(100)
	// golden: the error returned for a never-written offset
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1024, 1024, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	if _, err := seg.Append([]byte("only")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	_, readErr := seg.Read(100)
	assertGolden(t, "segment", struct{ Err string }{errString(readErr)})
}

func TestSegmentRecover_ScansPastLastIndexEntry(t *testing.T) {
	// input: a raw .log file pre-populated with 3 records (offsets 0,1,2)
	// but a raw .index file that only indexes record 0 - simulating a
	// crash that happened before the sparse index caught up with records
	// 1 and 2
	// golden: the offset returned by a subsequent Append (proves
	// nextOffset was derived by scanning past the last index entry to the
	// log's true end), and the payload of Read(2), a record that was
	// never indexed (proves the linear-scan fallback works after
	// recovery)
	dir := t.TempDir()
	records := []rawRecord{
		{offset: 0, payload: []byte("r0")},
		{offset: 1, payload: []byte("r1")},
		{offset: 2, payload: []byte("r2")},
	}
	writeSegmentFiles(t, dir, 0, records, []rawIndexEntry{{relOffset: 0, pos: 0}})

	seg, err := storage.NewSegment(dir, 0, segCfg(1<<20, 4096, 1<<30))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	off, err := seg.Append([]byte("r3"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, readErr := seg.Read(2)

	assertGolden(t, "segment", struct {
		NextAppendOffset      uint64
		RecoveredPayloadAtTwo string
		ReadErr               string
	}{off, string(got), errString(readErr)})
}

func TestSegmentClose_ThenReopen_PreservesDataAndNextOffset(t *testing.T) {
	// input: append 4 records with IndexIntervalBytes set very high (so
	// only the first record ever gets an index entry), Close the
	// segment, then NewSegment again on the same directory
	// golden: the offset of the next Append after reopening (proves
	// nextOffset survived a real close/reopen cycle), and the payload of
	// a record that was never individually indexed (proves Close()
	// truncated the index file down to its real used size - without
	// that, a reopened Index would misread the file's MaxIndexSize
	// padding as if it were real entries)
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1<<20, 4096, 1<<30))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := seg.Append([]byte(fmt.Sprintf("a%d", i))); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := storage.NewSegment(dir, 0, segCfg(1<<20, 4096, 1<<30))
	if err != nil {
		t.Fatalf("NewSegment (reopen): %v", err)
	}

	off, err := reopened.Append([]byte("a4"))
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	got, readErr := reopened.Read(2)

	assertGolden(t, "segment", struct {
		NextAppendOffset      uint64
		RecoveredPayloadAtTwo string
		ReadErr               string
	}{off, string(got), errString(readErr)})
}

func TestSegmentLocateRange_ReturnsWholeRecordsWithinMaxBytes(t *testing.T) {
	// input: 3 records of equal size (16 bytes on disk each: 12-byte header
	// + 4-byte payload), LocateRange(0, maxBytes=32) - exactly enough for
	// the first 2 records, not the third
	// golden: Pos/Length/NextOffset plus a hex dump of the bytes at
	// [Pos,Pos+Length) read independently via ReadAt - proves the span is
	// exactly the first 2 whole records, not a partial third
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1<<20, 1024, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := seg.Append([]byte(fmt.Sprintf("rec%d", i))); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}

	rng, err := seg.LocateRange(0, 32)
	if err != nil {
		t.Fatalf("LocateRange: %v", err)
	}
	raw := readRawLogBytes(t, dir, 0, rng.Pos, rng.Length)

	assertGolden(t, "segment", struct {
		Pos        int64
		Length     int64
		NextOffset uint64
		HexBytes   string
	}{rng.Pos, rng.Length, rng.NextOffset, hex.EncodeToString(raw)})
}

func TestSegmentLocateRange_AlwaysReturnsAtLeastOneRecordEvenOverMaxBytes(t *testing.T) {
	// input: 2 records of 16 bytes each on disk, LocateRange(0, maxBytes=1)
	// - smaller than even one whole record
	// golden: Pos/Length/NextOffset plus a hex dump - proves exactly one
	// whole record is returned (Length=16 > maxBytes=1), never zero records
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1<<20, 1024, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := seg.Append([]byte(fmt.Sprintf("rec%d", i))); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}

	rng, err := seg.LocateRange(0, 1)
	if err != nil {
		t.Fatalf("LocateRange: %v", err)
	}
	raw := readRawLogBytes(t, dir, 0, rng.Pos, rng.Length)

	assertGolden(t, "segment", struct {
		Pos        int64
		Length     int64
		NextOffset uint64
		HexBytes   string
	}{rng.Pos, rng.Length, rng.NextOffset, hex.EncodeToString(raw)})
}

func TestSegmentLocateRange_CapsAtSegmentEnd(t *testing.T) {
	// input: 3 records, LocateRange(0, maxBytes=1<<20) - far larger than
	// the segment's total committed data
	// golden: Pos/Length/NextOffset plus a hex dump - proves the scan stops
	// cleanly at the segment's actual end (NextOffset equals the count of
	// records written) instead of erroring or reading past currentSize
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1<<20, 1024, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := seg.Append([]byte(fmt.Sprintf("rec%d", i))); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}

	rng, err := seg.LocateRange(0, 1<<20)
	if err != nil {
		t.Fatalf("LocateRange: %v", err)
	}
	raw := readRawLogBytes(t, dir, 0, rng.Pos, rng.Length)

	assertGolden(t, "segment", struct {
		Pos        int64
		Length     int64
		NextOffset uint64
		HexBytes   string
	}{rng.Pos, rng.Length, rng.NextOffset, hex.EncodeToString(raw)})
}

func TestSegmentLocateRange_UnindexedOffset_FoundByLinearScan(t *testing.T) {
	// input: IndexIntervalBytes set very high (1<<30) so only the first
	// record (offset 0) ever gets an index entry; append 4 small records
	// and LocateRange starting at the last, never-indexed offset
	// golden: Pos/Length/NextOffset plus a hex dump - proves LocateRange
	// falls back to the same linear-scan-forward behavior as Read when the
	// requested offset isn't itself indexed
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1<<20, 4096, 1<<30))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	var last uint64
	for i := 0; i < 4; i++ {
		off, err := seg.Append([]byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
		last = off
	}

	rng, err := seg.LocateRange(last, 1<<20)
	if err != nil {
		t.Fatalf("LocateRange: %v", err)
	}
	raw := readRawLogBytes(t, dir, 0, rng.Pos, rng.Length)

	assertGolden(t, "segment", struct {
		Pos        int64
		Length     int64
		NextOffset uint64
		HexBytes   string
	}{rng.Pos, rng.Length, rng.NextOffset, hex.EncodeToString(raw)})
}

func TestSegmentLocateRange_OffsetNotFound_ReturnsError(t *testing.T) {
	// input: Append a single record at offset 0, then LocateRange(100, ...)
	// golden: the error returned for a never-written offset
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1024, 1024, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	if _, err := seg.Append([]byte("only")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	_, err = seg.LocateRange(100, 1024)
	assertGolden(t, "segment", struct{ Err string }{errString(err)})
}

func TestSegmentOpenReader_ConcurrentWithAppend_NoInterference(t *testing.T) {
	// input: one goroutine keeps Appending while another repeatedly opens a
	// brand-new reader via OpenReader, Seeks to a span already located
	// before the race starts, and reads it sequentially
	// assertion (not golden - run with -race): the bytes read back never
	// change across iterations, proving OpenReader's private handle is
	// unaffected by concurrent Append on the shared s.log handle
	dir := t.TempDir()
	seg, err := storage.NewSegment(dir, 0, segCfg(1<<20, 4096, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	seedOff, err := seg.Append([]byte("seed-payload"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	rng, err := seg.LocateRange(seedOff, 1<<20)
	if err != nil {
		t.Fatalf("LocateRange: %v", err)
	}
	want := readRawLogBytes(t, dir, 0, rng.Pos, rng.Length)

	const iterations = 200
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if _, err := seg.Append([]byte(fmt.Sprintf("filler-%d", i))); err != nil {
				t.Errorf("Append #%d: %v", i, err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			r, err := seg.OpenReader()
			if err != nil {
				t.Errorf("OpenReader #%d: %v", i, err)
				return
			}

			if _, err := r.Seek(rng.Pos, io.SeekStart); err != nil {
				t.Errorf("Seek #%d: %v", i, err)
				r.Close()
				return
			}
			got := make([]byte, rng.Length)
			if _, err := io.ReadFull(r, got); err != nil {
				t.Errorf("ReadFull #%d: %v", i, err)
				r.Close()
				return
			}
			r.Close()

			if !bytes.Equal(got, want) {
				t.Errorf("bytes at [%d,%d) changed on iteration %d: got %x want %x", rng.Pos, rng.Pos+rng.Length, i, got, want)
				return
			}
		}
	}()

	wg.Wait()
}

func TestSegmentRecover_EmptyLogFile_StartsAtBaseOffset(t *testing.T) {
	// input: an empty (zero-byte) pre-existing .log/.index pair at
	// baseOffset 7 - nothing to recover
	// golden: offset returned by the first Append (should equal
	// baseOffset)
	dir := t.TempDir()
	writeSegmentFiles(t, dir, 7, nil, nil)

	seg, err := storage.NewSegment(dir, 7, segCfg(1024, 1024, 4096))
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	off, err := seg.Append([]byte("x"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	assertGolden(t, "segment", struct{ Offset uint64 }{off})
}
