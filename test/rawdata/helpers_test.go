package rawdata_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nalgnaohel/kage/api/rawdata"
)

var updateGolden = flag.Bool("update", false, "update .golden files with the current test output instead of comparing against them")

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

func errorCodeString(code rawdata.ErrorCode) string {
	switch code {
	case rawdata.ErrNone:
		return "ErrNone"
	case rawdata.ErrUnknownTopicOrPartition:
		return "ErrUnknownTopicOrPartition"
	case rawdata.ErrOffsetOutOfRange:
		return "ErrOffsetOutOfRange"
	case rawdata.ErrInternal:
		return "ErrInternal"
	default:
		return fmt.Sprintf("ErrorCode(%d)", code)
	}
}
