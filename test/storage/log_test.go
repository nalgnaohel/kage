package storage_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/nalgnaohel/kage/storage"
)

func logCfg(maxSegmentSize, maxIndexSize, indexIntervalBytes uint64) storage.LogConfig {
	return storage.LogConfig{
		MaxSegmentSize:     maxSegmentSize,
		MaxIndexSize:       maxIndexSize,
		IndexIntervalBytes: indexIntervalBytes,
	}
}

func TestNewLog_EmptyDirectory_CreatesInitialSegmentAtZero(t *testing.T) {
	// input: brand-new empty directory
	// golden: offset returned by the first Append (setup() should have
	// created one initial segment at baseOffset 0)
	dir := t.TempDir()
	log, err := storage.NewLog(dir, logCfg(1<<20, 4096, 4096))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	off, err := log.Append([]byte("x"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	assertGolden(t, "log", struct{ Offset int }{off})
}

func TestLogAppendRead_RoundTrip(t *testing.T) {
	// input: Append("payload")
	// golden: the offset returned and the payload read back
	dir := t.TempDir()
	log, err := storage.NewLog(dir, logCfg(1<<20, 4096, 4096))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	off, err := log.Append([]byte("payload"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, err := log.Read(uint64(off))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	assertGolden(t, "log", struct {
		Offset  int
		Payload string
	}{off, string(got)})
}

func TestLogAppend_RollsOverToNewSegmentWhenFull(t *testing.T) {
	// input: MaxSegmentSize set small (32 bytes) so a handful of small
	// records overflow the active segment; append 5 records
	// golden: the sequence of offsets (should be sequential with no gaps
	// across the rollover) and how many segment files exist on disk
	// afterward (should be more than 1)
	dir := t.TempDir()
	log, err := storage.NewLog(dir, logCfg(32, 4096, 4096))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	offsets := make([]int, 5)
	for i := range offsets {
		off, err := log.Append([]byte(fmt.Sprintf("rec%d", i)))
		if err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
		offsets[i] = off
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}

	assertGolden(t, "log", struct {
		Offsets          []int
		SegmentFileCount int
	}{offsets, len(matches)})
}

func TestLogRead_AfterRollover_ReadsFromOlderSegment(t *testing.T) {
	// input: force a rollover (MaxSegmentSize = 32) after writing an early
	// record, then keep appending filler records
	// golden: the early record's offset and its payload as read back
	// after the active segment has moved on
	dir := t.TempDir()
	log, err := storage.NewLog(dir, logCfg(32, 4096, 4096))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	firstOff, err := log.Append([]byte("first-record"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := log.Append([]byte(fmt.Sprintf("filler%d", i))); err != nil {
			t.Fatalf("Append filler #%d: %v", i, err)
		}
	}

	got, err := log.Read(uint64(firstOff))
	if err != nil {
		t.Fatalf("Read(%d): %v", firstOff, err)
	}

	assertGolden(t, "log", struct {
		FirstOffset int
		Payload     string
	}{firstOff, string(got)})
}

func TestLogRead_OutOfRange_ReturnsError(t *testing.T) {
	// input: Append exactly one record, then Read an offset far beyond
	// anything ever written
	// golden: the out-of-range error message
	dir := t.TempDir()
	log, err := storage.NewLog(dir, logCfg(1<<20, 4096, 4096))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	if _, err := log.Append([]byte("only")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	_, readErr := log.Read(9999)
	assertGolden(t, "log", struct{ Err string }{errString(readErr)})
}

func TestLogClose_ThenReopen_PreservesDataAcrossSegments(t *testing.T) {
	// input: force a rollover (MaxSegmentSize = 32) across 5 Appends so
	// the directory ends up with multiple segment files, Close the log,
	// then NewLog again on the same directory
	// golden: the payload of an offset from the pre-rollover (no longer
	// active) segment, and the offset of the next Append after
	// reopening - proves setup() picked the highest-baseOffset segment
	// as active and recovered its nextOffset correctly after a real
	// close/reopen cycle
	dir := t.TempDir()
	log, err := storage.NewLog(dir, logCfg(32, 4096, 4096))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := log.Append([]byte(fmt.Sprintf("rec%d", i))); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := storage.NewLog(dir, logCfg(32, 4096, 4096))
	if err != nil {
		t.Fatalf("NewLog (reopen): %v", err)
	}

	got, err := reopened.Read(0)
	if err != nil {
		t.Fatalf("Read(0): %v", err)
	}
	off, err := reopened.Append([]byte("rec5"))
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}

	assertGolden(t, "log", struct {
		PayloadAtZero    string
		NextAppendOffset int
	}{string(got), off})
}

func TestNewLog_DiscoversPreexistingSegmentsInSortedOrder(t *testing.T) {
	// input: a directory already containing two segments left behind by a
	// previous run - baseOffset 0 with records at offsets 0,1, and
	// baseOffset 2 with a record at offset 2, each fully indexed
	// golden: payloads read back at offsets 0,1,2 (0/1 resolve through
	// the first segment, 2 through the second), and the offset of a
	// subsequent Append (proves setup() picked the highest-baseOffset
	// segment as active and correctly recovered its nextOffset)
	dir := t.TempDir()
	writeSegmentFiles(t, dir, 0,
		[]rawRecord{{offset: 0, payload: []byte("a")}, {offset: 1, payload: []byte("b")}},
		[]rawIndexEntry{{relOffset: 0, pos: 0}},
	)
	writeSegmentFiles(t, dir, 2,
		[]rawRecord{{offset: 2, payload: []byte("c")}},
		[]rawIndexEntry{{relOffset: 0, pos: 0}},
	)

	log, err := storage.NewLog(dir, logCfg(1<<20, 4096, 1<<30))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	got0, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read(0): %v", err)
	}
	got1, err := log.Read(1)
	if err != nil {
		t.Fatalf("Read(1): %v", err)
	}
	got2, err := log.Read(2)
	if err != nil {
		t.Fatalf("Read(2): %v", err)
	}
	off, err := log.Append([]byte("d"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	assertGolden(t, "log", struct {
		PayloadAtZero    string
		PayloadAtOne     string
		PayloadAtTwo     string
		NextAppendOffset int
	}{string(got0), string(got1), string(got2), off})
}
