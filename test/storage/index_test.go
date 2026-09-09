package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nalgnaohel/kage/storage"
)

// readResult captures an Index.Read call's result for golden comparison.
type readResult struct {
	Offset uint32
	Pos    uint64
	Err    string
}

func newTestIndex(t *testing.T, maxIndexSize uint64) *storage.Index {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "test.index"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open index file: %v", err)
	}
	idx, err := storage.NewIndex(f, maxIndexSize)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	return idx
}

func TestNewIndex_EmptyFile(t *testing.T) {
	// input: brand-new empty file, maxIndexSize = 1024
	// golden: file size right after NewIndex (should be truncated up
	// front to maxIndexSize even though nothing has been written yet)
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open index file: %v", err)
	}
	if _, err := storage.NewIndex(f, 1024); err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	assertGolden(t, "index", struct{ FileSizeAfterNewIndex int64 }{fi.Size()})
}

func TestIndexWriteRead_ExactMatch(t *testing.T) {
	// input: entries (0,0), (5,40), (10,90); Read(5)
	// golden: the exact-match result of Read(5)
	idx := newTestIndex(t, 1024)
	for _, e := range []struct {
		off uint32
		pos uint64
	}{{0, 0}, {5, 40}, {10, 90}} {
		if err := idx.Write(e.off, e.pos); err != nil {
			t.Fatalf("Write(%d,%d): %v", e.off, e.pos, err)
		}
	}

	off, pos, err := idx.Read(5)
	assertGolden(t, "index", readResult{off, pos, errString(err)})
}

func TestIndexRead_NearestLowerEntry(t *testing.T) {
	// input: entries (0,0), (10,100), (20,200); Read(15) (no exact match)
	// golden: the nearest entry at-or-before the target, which is what
	// makes it safe to leave the index sparse
	idx := newTestIndex(t, 1024)
	for _, e := range []struct {
		off uint32
		pos uint64
	}{{0, 0}, {10, 100}, {20, 200}} {
		if err := idx.Write(e.off, e.pos); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	off, pos, err := idx.Read(15)
	assertGolden(t, "index", readResult{off, pos, errString(err)})
}

func TestIndexRead_NegativeOneReturnsLastEntry(t *testing.T) {
	// input: entries (0,0), (10,100), (20,200); Read(-1)
	// golden: the last entry - Read(-1) is the "give me the last entry"
	// query, relying on unsigned wraparound of the search target
	idx := newTestIndex(t, 1024)
	for _, e := range []struct {
		off uint32
		pos uint64
	}{{0, 0}, {10, 100}, {20, 200}} {
		if err := idx.Write(e.off, e.pos); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	off, pos, err := idx.Read(-1)
	assertGolden(t, "index", readResult{off, pos, errString(err)})
}

func TestIndexRead_EmptyIndexReturnsEOF(t *testing.T) {
	// input: no entries ever written; Read(0)
	// golden: io.EOF
	idx := newTestIndex(t, 1024)
	off, pos, err := idx.Read(0)
	assertGolden(t, "index", readResult{off, pos, errString(err)})
}

func TestIndexWrite_FullReturnsEOF(t *testing.T) {
	// input: maxIndexSize = 8 (room for exactly one 8-byte entry); write
	// twice
	// golden: the first Write succeeds ("") and the second returns "EOF"
	// because there is no more room in the mmap'd region
	idx := newTestIndex(t, 8)
	err1 := idx.Write(1, 10)
	err2 := idx.Write(2, 20)

	assertGolden(t, "index", struct {
		FirstWriteErr  string
		SecondWriteErr string
	}{errString(err1), errString(err2)})
}

func TestIndexClose_TruncatesToUsedSize(t *testing.T) {
	// input: maxIndexSize = 1024, write 2 entries (16 bytes of real data),
	// then Close
	// golden: the file size on disk after Close (should be 16, not the
	// padded 1024)
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open index file: %v", err)
	}
	idx, err := storage.NewIndex(f, 1024)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if err := idx.Write(0, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := idx.Write(1, 10); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	assertGolden(t, "index", struct{ FileSizeAfterClose int64 }{fi.Size()})
}

func TestIndexReopen_PreservesEntriesAfterClose(t *testing.T) {
	// input: write 2 entries, Close, then NewIndex again on the same file
	// golden: both entries as read back after the reopen (persistence
	// across a close/reopen cycle)
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	f1, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open index file: %v", err)
	}
	idx1, err := storage.NewIndex(f1, 1024)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if err := idx1.Write(0, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := idx1.Write(3, 30); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := idx1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f2, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("reopen index file: %v", err)
	}
	idx2, err := storage.NewIndex(f2, 1024)
	if err != nil {
		t.Fatalf("NewIndex on reopen: %v", err)
	}

	off0, pos0, err0 := idx2.Read(0)
	off3, pos3, err3 := idx2.Read(3)

	assertGolden(t, "index", struct {
		Entry0 readResult
		Entry3 readResult
	}{
		Entry0: readResult{off0, pos0, errString(err0)},
		Entry3: readResult{off3, pos3, errString(err3)},
	})
}

func TestIndexRead_TargetBelowSmallestEntry_ReturnsEOF(t *testing.T) {
	// Fixed bug: see storage/index.go - Read's binary search used to
	// narrow an unsigned `high` via `high = mid - 1`, which underflowed
	// to a huge value (instead of going negative) and panicked on an
	// out-of-range mmap access when the target was smaller than every
	// stored offset. `high` is now signed, so this falls through to a
	// normal "not found" result.
	// input: entries (10,100), (20,200), (30,300); Read(0), below all of
	// them
	// golden: io.EOF, not a panic
	idx := newTestIndex(t, 1024)
	for _, e := range []struct {
		off uint32
		pos uint64
	}{{10, 100}, {20, 200}, {30, 300}} {
		if err := idx.Write(e.off, e.pos); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	off, pos, err := idx.Read(0)
	assertGolden(t, "index", readResult{off, pos, errString(err)})
}
