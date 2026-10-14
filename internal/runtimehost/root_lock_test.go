package runtimehost

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/statefs"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

func TestHostedLayoutCreatesOneLockAndKeepsItForLiveChildren(t *testing.T) {
	path := initialized(t)
	for _, name := range []string{"state/runtime.lock", "inputs/.lock", "results/.lock", "state/.installation"} {
		if _, err := os.Lstat(filepath.Join(path, filepath.FromSlash(name))); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("redundant lock or identity created", name, err)
		}
	}
	host, err := Open(context.Background(), path)
	requireNoError(t, err)
	defer host.Close()
	before, err := os.Stat(filepath.Join(path, "runtime.lock"))
	requireNoError(t, err)
	requireNoError(t, host.root.Close())
	if _, err := host.root.Child("extra"); !errors.Is(err, statefs.ErrClosed) {
		t.Fatal("closed host issued child", err)
	}
	if _, err := statefs.Open(path); !errors.Is(err, statefs.ErrLocked) {
		t.Fatal("children lost host lock", err)
	}
	if _, err := host.Status(context.Background()); err != nil {
		t.Fatal("early parent close broke live child", err)
	}
	requireNoError(t, host.results.Close())
	requireNoError(t, host.inputs.Close())
	if _, err := statefs.Open(path); !errors.Is(err, statefs.ErrLocked) {
		t.Fatal("database did not pin host lock", err)
	}
	requireNoError(t, host.Close())
	reopened, err := Open(context.Background(), path)
	requireNoError(t, err)
	defer reopened.Close()
	after, err := os.Stat(filepath.Join(path, "runtime.lock"))
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("host lock inode replaced", err)
	}
}

func TestHostedChildrenRefuseStandaloneConstructors(t *testing.T) {
	path := initialized(t)
	for _, malformed := range []bool{false, true} {
		if malformed {
			requireNoError(t, os.WriteFile(filepath.Join(path, markerName), []byte("invalid"), 0600))
		}
		if store, err := sqlite.Open(context.Background(), filepath.Join(path, "state"), sqlite.DefaultOptions()); err == nil {
			store.Close()
			t.Fatal("standalone state bypassed host capability")
		}
		for _, name := range []string{"inputs", "results", "missing"} {
			if blobs, err := blobfs.New(filepath.Join(path, name), blobfs.DefaultLimits()); err == nil {
				blobs.Close()
				t.Fatal("standalone blobs bypassed host capability", name)
			}
		}
	}
	for _, name := range []string{"state/runtime.lock", "state/.installation", "inputs/.lock", "results/.lock", "missing"} {
		if _, err := os.Lstat(filepath.Join(path, filepath.FromSlash(name))); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("refused constructor wrote hosted state", name, err)
		}
	}
}

func TestHostedLegacyMarkerRemovedOnlyAfterCompleteValidation(t *testing.T) {
	path := initialized(t)
	host, err := Open(context.Background(), path)
	requireNoError(t, err)
	id := host.identity.InstallationID
	requireNoError(t, host.Close())
	marker := filepath.Join(path, "state", ".installation")
	requireNoError(t, os.WriteFile(marker, []byte(id), 0600))
	locks := map[string]os.FileInfo{}
	for _, name := range []string{"state/runtime.lock", "inputs/.lock", "results/.lock"} {
		file := filepath.Join(path, filepath.FromSlash(name))
		requireNoError(t, os.WriteFile(file, nil, 0600))
		locks[file], err = os.Stat(file)
		requireNoError(t, err)
	}
	resultsID := filepath.Join(path, "results", ".retention-id")
	original, err := os.ReadFile(resultsID)
	requireNoError(t, err)
	inputID, err := os.ReadFile(filepath.Join(path, "inputs", ".retention-id"))
	requireNoError(t, err)
	requireNoError(t, os.WriteFile(resultsID, inputID, 0600))
	if failed, err := Open(context.Background(), path); err == nil {
		failed.Close()
		t.Fatal("swapped result identity accepted")
	}
	retained, err := os.ReadFile(marker)
	if err != nil || string(retained) != id {
		t.Fatal("failed startup removed legacy marker", err)
	}
	requireNoError(t, os.WriteFile(resultsID, original, 0600))
	host, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal("failed startup leaked a lock", err)
	}
	defer host.Close()
	if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("validated hosted legacy marker retained", err)
	}
	for file, before := range locks {
		after, err := os.Stat(file)
		if err != nil || !os.SameFile(before, after) {
			t.Fatal("legacy lock inode replaced", file, err)
		}
	}
}

func TestHostedEmptyAndReplacedDatabaseFailBeforeMutation(t *testing.T) {
	other := initialized(t)
	replacement, err := os.ReadFile(filepath.Join(other, "state", "runtime.db"))
	requireNoError(t, err)
	for _, content := range [][]byte{nil, replacement} {
		path := initialized(t)
		database := filepath.Join(path, "state", "runtime.db")
		requireNoError(t, os.WriteFile(database, content, 0600))
		if host, err := Open(context.Background(), path); err == nil {
			host.Close()
			t.Fatal("empty or foreign database adopted")
		}
		after, err := os.ReadFile(database)
		if err != nil || !bytes.Equal(content, after) {
			t.Fatal("failed open modified database", err)
		}
		if _, err := os.Lstat(filepath.Join(path, "state", ".installation")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("failed open created child marker", err)
		}
	}
}
