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
	if err != nil {
		t.Fatal(err)
	}
	before, err := h.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(ctx, path); err == nil {
		_ = other.Close()
		t.Fatal("second host acquired state")
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	h, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
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
			if err := os.Remove(missing); err != nil {
				t.Fatal(err)
			}
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
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{nil, []byte("{}"), append(append([]byte{}, raw...), '\n'), []byte(`{"version":1,"version":1}`)} {
		if err := os.WriteFile(marker, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if h, err := Open(context.Background(), path); err == nil {
			h.Close()
			t.Fatal("invalid marker accepted")
		}
	}
	if err := os.WriteFile(marker, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(path, "inputs"), filepath.Join(path, "swap")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(path, "results"), filepath.Join(path, "inputs")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(path, "swap"), filepath.Join(path, "results")); err != nil {
		t.Fatal(err)
	}
	if h, err := Open(context.Background(), path); err == nil {
		h.Close()
		t.Fatal("swapped blob roots adopted")
	}
}
