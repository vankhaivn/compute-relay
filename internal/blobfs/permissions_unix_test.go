//go:build linux || darwin

package blobfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRejectNonPrivateOrSymlinkedRootWithoutDeletingData(t *testing.T) {
	root := filepath.Join(t.TempDir(), "public")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(root, "keep")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if s, err := New(root, DefaultLimits()); !errors.Is(err, ErrCorrupt) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("nonprivate root: %v", err)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatal("unrelated file removed")
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if s, err := New(link, DefaultLimits()); !errors.Is(err, ErrCorrupt) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("symlink root: %v", err)
	}
}
