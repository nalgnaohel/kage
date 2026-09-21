package raft_test

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	hraft "github.com/hashicorp/raft"
	"github.com/nalgnaohel/kage/raft"
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

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func respErrString(resp any) string {
	err, ok := resp.(error)
	if !ok || err == nil {
		return ""
	}
	return err.Error()
}

func apply(t *testing.T, fsm *raft.FSM, cmd raft.Command) any {
	t.Helper()

	data, err := raft.Encode(cmd)
	if err != nil {
		t.Fatalf("Encode() failed: %v", err)
	}
	return fsm.Apply(&hraft.Log{Data: data})
}
