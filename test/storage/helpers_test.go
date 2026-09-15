package storage_test

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var updateGolden = flag.Bool("update", false, "update .golden files with the current test output instead of comparing against them")

// assertGolden marshals got as indented JSON and compares it against the
// golden file at test/storage/testdata/<dir>/<test-name>.golden, keeping
// expected values out of the Go source so they're easier to read/diff.
// Run `go test ./test/storage/... -update` to (re)write golden files
// after an intentional behavior change.
func assertGolden(t *testing.T, dir string, got any) {
	t.Helper()

	gotJSON, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("marshal golden data: %v", err)
	}
	gotJSON = append(gotJSON, '\n')

	path := filepath.Join("testdata", dir, t.Name()+".golden")

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatalf("create golden dir: %v", err)
		}
		if err := os.WriteFile(path, gotJSON, 0644); err != nil {
			t.Fatalf("write golden file: %v", err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden file %s (run with -update to create it): %v", path, err)
	}
	if string(gotJSON) != string(want) {
		t.Errorf("result does not match golden file %s\n--- got ---\n%s--- want ---\n%s", path, gotJSON, want)
	}
}

// errString renders an error for golden output: "" for nil, else the
// error's message (e.g. io.EOF renders as "EOF").
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// rawRecord mirrors the on-disk record layout from segment.go:
// 8-byte big-endian absolute offset + 4-byte big-endian length + payload.
type rawRecord struct {
	offset  uint64
	payload []byte
}

func encodeLog(records []rawRecord) []byte {
	var buf []byte
	for _, r := range records {
		header := make([]byte, 12)
		binary.BigEndian.PutUint64(header[:8], r.offset)
		binary.BigEndian.PutUint32(header[8:], uint32(len(r.payload)))
		buf = append(buf, header...)
		buf = append(buf, r.payload...)
	}
	return buf
}

// rawIndexEntry mirrors storage.Index's on-disk entry layout: 4-byte
// big-endian relative offset + 4-byte big-endian physical position.
type rawIndexEntry struct {
	relOffset uint32
	pos       uint32
}

func encodeIndex(entries []rawIndexEntry) []byte {
	var buf []byte
	for _, e := range entries {
		entry := make([]byte, 8)
		binary.BigEndian.PutUint32(entry[:4], e.relOffset)
		binary.BigEndian.PutUint32(entry[4:], e.pos)
		buf = append(buf, entry...)
	}
	return buf
}

// readRawLogBytes independently reads length bytes at pos from the segment's
// on-disk <baseOffset>.log file, bypassing Segment/Log entirely — used to
// prove a LocateRange result is byte-exact against the real file, not just
// numerically plausible.
func readRawLogBytes(t *testing.T, dir string, baseOffset uint64, pos, length int64) []byte {
	t.Helper()
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	f, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("open raw log file: %v", err)
	}
	defer f.Close()

	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, pos); err != nil {
		t.Fatalf("ReadAt raw log file: %v", err)
	}
	return buf
}

// writeSegmentFiles plants a pre-existing <baseOffset>.log/.index pair on
// disk, simulating data left behind by a previous process run (e.g. right
// before a crash). This lets recovery be tested without needing a Close()
// method, which storage.Segment/Log don't currently expose.
func writeSegmentFiles(t *testing.T, dir string, baseOffset uint64, records []rawRecord, indexEntries []rawIndexEntry) {
	t.Helper()
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	if err := os.WriteFile(logPath, encodeLog(records), 0644); err != nil {
		t.Fatalf("write raw log file: %v", err)
	}
	if err := os.WriteFile(indexPath, encodeIndex(indexEntries), 0644); err != nil {
		t.Fatalf("write raw index file: %v", err)
	}
}
