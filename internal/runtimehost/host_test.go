package runtimehost

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func initialized(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime")
	status, err := Initialize(context.Background(), path)
	if err != nil || status.InstallationID == "" || status.SchemaVersion < 9 || status.DispatchEnabled || status.Mode != "local-admission-only" {
		t.Fatal(status, err)
	}
	return path
}
func TestLocalRuntimeInitializeReopenAndExclusiveOwnership(t *testing.T) {
	ctx := context.Background()
	path := initialized(t)
	if _, err := Initialize(ctx, path); err == nil {
		t.Fatal("initialization overwrote existing state")
	}
	h, err := Open(ctx, path)
	requireNoError(t, err)
	before, err := h.Status(ctx)
	requireNoError(t, err)
	if other, err := Open(ctx, path); err == nil {
		_ = other.Close()
		t.Fatal("second host acquired state")
	}
	requireNoError(t, h.Close())
	h, err = Open(ctx, path)
	requireNoError(t, err)
	defer h.Close()
	after, err := h.Status(ctx)
	if err != nil || before != after {
		t.Fatal("restart changed identity", err)
	}
}
func TestLocalRuntimeMissingEvidenceNeverReinitializes(t *testing.T) {
	for _, name := range []string{markerName, "state/runtime.db", "state/.compute-relay-state", "inputs/.retention-id", "results/.retention-id"} {
		t.Run(name, func(t *testing.T) {
			path := initialized(t)
			missing := filepath.Join(path, filepath.FromSlash(name))
			requireNoError(t, os.Remove(missing))
			if h, err := Open(context.Background(), path); err == nil {
				h.Close()
				t.Fatal("missing evidence recreated")
			}
			if _, err := os.Lstat(missing); !os.IsNotExist(err) {
				t.Fatal("open wrote replacement evidence", err)
			}
		})
	}
	absent := filepath.Join(t.TempDir(), "absent")
	if _, err := Open(context.Background(), absent); err == nil {
		t.Fatal("missing runtime accepted")
	}
	if _, err := os.Lstat(absent); !os.IsNotExist(err) {
		t.Fatal("non-init created a directory")
	}
}
func TestLocalRuntimeCorruptMarkerAndSwappedStoresFailClosed(t *testing.T) {
	path := initialized(t)
	marker := filepath.Join(path, markerName)
	raw, err := os.ReadFile(marker)
	requireNoError(t, err)
	for _, bad := range [][]byte{nil, []byte("{}"), append(append([]byte{}, raw...), '\n'), []byte(`{"version":1,"version":1}`)} {
		requireNoError(t, os.WriteFile(marker, bad, 0600))
		if h, err := Open(context.Background(), path); err == nil {
			h.Close()
			t.Fatal("invalid marker accepted")
		}
	}
	requireNoError(t, os.WriteFile(marker, raw, 0600))
	requireNoError(t, os.Rename(filepath.Join(path, "inputs"), filepath.Join(path, "swap")))
	requireNoError(t, os.Rename(filepath.Join(path, "results"), filepath.Join(path, "inputs")))
	requireNoError(t, os.Rename(filepath.Join(path, "swap"), filepath.Join(path, "results")))
	if h, err := Open(context.Background(), path); err == nil {
		h.Close()
		t.Fatal("swapped blob roots adopted")
	}
}
