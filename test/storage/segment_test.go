package storage_test

import (
	"fmt"
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
