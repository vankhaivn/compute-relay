package localstorage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInventoryLogicalTotalsCacheAndSymlinkBoundary(t *testing.T) {
	root := t.TempDir()
	for path, size := range map[string]int{"inputs/a": 13, "results/b": 17, "state/runtime.db": 19, "state/runtime.db-wal": 23, "state/marker": 29} {
		target := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, make([]byte, size), 0600); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), "private")
	if err := os.WriteFile(outside, make([]byte, 1000), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "outside")); err != nil {
		t.Fatal(err)
	}
	reader := New(root)
	out, err := reader.Measure(t.Context(), false)
	if err != nil || out.InputsBytes != 13 || out.ResultsBytes != 17 || out.DatabaseBytes != 42 || out.OtherBytes != 29 || out.TotalBytes != 101 || out.MeasuredAt.IsZero() {
		t.Fatal(out, err)
	}
	if err := os.Remove(filepath.Join(root, "inputs/a")); err != nil {
		t.Fatal(err)
	}
	cached, err := reader.Measure(t.Context(), false)
	if err != nil || cached != out {
		t.Fatal("cache rewalked", cached, err)
	}
	refreshed, err := reader.Measure(t.Context(), true)
	if err != nil || refreshed.TotalBytes != 88 || refreshed.InputsBytes != 0 {
		t.Fatal("refresh retained stale bytes", refreshed, err)
	}
}
