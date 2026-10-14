package statefs

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
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

func TestChildCapabilitiesPinOwnerLockAndRejectReopenAfterOwnerClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime")
	owner, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	child, err := owner.Child("state")
	if err != nil {
		t.Fatal(err)
	}
	retained, err := child.Retain()
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(path, "runtime.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := child.Retain(); !errors.Is(err, ErrClosed) {
		t.Fatal("closed child reopened", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	for _, root := range []*Root{owner, retained} {
		if _, err := root.Child("new"); !errors.Is(err, ErrClosed) {
			t.Fatal("closed owner allowed child", err)
		}
		if _, err := root.Retain(); !errors.Is(err, ErrClosed) {
			t.Fatal("closed owner allowed retain", err)
		}
	}
	if _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatal("retained child lost owner lock", err)
	}
	if err := retained.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := os.Stat(filepath.Join(path, "runtime.lock"))
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("lock inode changed", err)
	}
	if _, err := os.Stat(filepath.Join(path, "state", "runtime.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("child acquired its own lock", err)
	}
}

func TestChildRejectsUnsafeComponentsAndPaths(t *testing.T) {
	owner, err := Open(filepath.Join(t.TempDir(), "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	for _, name := range []string{"", ".", "..", "../escape", "a/b", `a\b`, "/absolute"} {
		if child, err := owner.Child(name); !errors.Is(err, ErrUnsafe) {
			if child != nil {
				child.Close()
			}
			t.Fatal("unsafe child accepted", name, err)
		}
	}
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(owner.Path, "linked")); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Child("linked"); !errors.Is(err, ErrUnsafe) {
		t.Fatal("symlink child accepted", err)
	}
	if err := os.Mkdir(filepath.Join(owner.Path, "public"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Child("public"); !errors.Is(err, ErrUnsafe) {
		t.Fatal("public child accepted", err)
	}
}

func TestConcurrentRetainsCannotReleasePinnedRootDuringOwnerClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime")
	owner, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	child, err := owner.Child("state")
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			pin, err := child.Retain()
			if err == nil {
				if err := pin.Close(); err != nil {
					t.Error(err)
				}
			} else if !errors.Is(err, ErrClosed) {
				t.Error(err)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	close(start)
	wg.Wait()
	if _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatal("concurrent closes released live child's lock", err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
}
