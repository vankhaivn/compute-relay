package statefs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRootLockAndNoUnlink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(path, "runtime.lock")
	before, err := os.Stat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := Open(path); !errors.Is(err, ErrLocked) {
		if second != nil {
			second.Close()
		}
		t.Fatal("second lock succeeded", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal("non-idempotent close", err)
	}
	r, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	after, err := os.Stat(lockPath)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("lock inode replaced", err)
	}
}
func TestRejectSymlinksAndNonregularFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(path, "runtime.lock")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(path, "runtime.lock")); err != nil {
		t.Skipf("symlink creation not available: %v", err)
	}
	if r, err := Open(path); !errors.Is(err, ErrUnsafe) {
		if r != nil {
			r.Close()
		}
		t.Fatal("symlink followed", err)
	}
	data, _ := os.ReadFile(outside)
	if string(data) != "keep" {
		t.Fatal("outside file modified")
	}
}
