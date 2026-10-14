package blobfs

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestLockedRootStorePinsHostAndPreservesBytesAndIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host")
	owner, err := statefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	child, err := owner.Child("inputs")
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewRoot(child, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id, err := store.RetentionIdentity(background)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.Put(background, declaration("hosted"), bytes.NewBufferString("preserved"))
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := statefs.Open(path); !errors.Is(err, statefs.ErrLocked) {
		t.Fatal("live blob released host lock", err)
	}
	if _, err := NewRoot(child, DefaultLimits()); !errors.Is(err, ErrClosed) {
		t.Fatal("closed capability reused", err)
	}
	if got := readObject(t, store, "a", meta.ID); string(got) != "preserved" {
		t.Fatal("bytes changed")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err = statefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	child, err = owner.Child("inputs")
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	store, err = NewRoot(child, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	after, err := store.RetentionIdentity(background)
	if err != nil || after != id || string(readObject(t, store, "a", meta.ID)) != "preserved" {
		t.Fatal("reopen lost bytes or identity", after, err)
	}
	for _, lock := range []string{".lock", "runtime.lock"} {
		if _, err := os.Stat(filepath.Join(child.Path, lock)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("hosted blobs acquired extra lock", err)
		}
	}
}
