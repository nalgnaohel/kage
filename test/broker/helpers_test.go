package broker_test

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/storage"
)

var updateGolden = flag.Bool("update", false, "update .golden files with the current test output instead of comparing against them")

// assertGolden mirrors test/storage's helper of the same name: marshals got
// as indented JSON and compares it against testdata/<dir>/<test-name>.golden.
// Run `go test ./test/broker/... -update` to (re)write golden files after an
// intentional behavior change.
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
// error's message.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func newTestRegistry(t *testing.T) *broker.Registry {
	t.Helper()

	dir := t.TempDir()
	reg := broker.NewRegistry(dir, storage.DefaultLogConfig(), "test-cluster", 0, "localhost", 9093)
	if err := reg.Startup(); err != nil {
		t.Fatalf("Startup() failed: %v", err)
	}
	return reg
}
