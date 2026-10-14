package sqlite

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestHostedStorePinsLockAndKeepsIdentityWithoutChildMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host")
	owner, err := statefs.Open(path)
	requireNoError(t, err)
	child, err := owner.Child("state")
	requireNoError(t, err)
	store, err := OpenRoot(testctx, child, DefaultOptions(), "")
	requireNoError(t, err)
	defer store.Close()
	workspace(t, store, "a")
	before, err := store.Info(testctx)
	requireNoError(t, err)
	requireNoError(t, child.Close())
	requireNoError(t, owner.Close())
	if _, err := statefs.Open(path); !errors.Is(err, statefs.ErrLocked) {
		t.Fatal("live database released host lock", err)
	}
	if _, err := OpenRoot(testctx, child, DefaultOptions(), before.InstallationID); !errors.Is(err, statefs.ErrClosed) {
		t.Fatal("closed capability reopened", err)
	}
	if _, err := store.LookupWorkspace(testctx, "a"); err != nil {
		t.Fatal("owner close invalidated live store", err)
	}
	requireNoError(t, store.Close())
	owner, err = statefs.Open(path)
	requireNoError(t, err)
	defer owner.Close()
	child, err = owner.Child("state")
	requireNoError(t, err)
	defer child.Close()
	store, err = OpenRoot(testctx, child, DefaultOptions(), before.InstallationID)
	requireNoError(t, err)
	defer store.Close()
	after, err := store.Info(testctx)
	if err != nil || after != before {
		t.Fatal("hosted reopen changed identity", after, err)
	}
	for _, name := range []string{".installation", "runtime.lock"} {
		if _, err := os.Lstat(filepath.Join(child.Path, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("hosted child created redundant evidence", name, err)
		}
	}
}

func TestHostedExpectedIdentityRejectsMissingAndEmptyDatabaseBeforeBootstrap(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "empty"}[exists], func(t *testing.T) {
			owner, err := statefs.Open(filepath.Join(t.TempDir(), "host"))
			requireNoError(t, err)
			defer owner.Close()
			child, err := owner.Child("state")
			requireNoError(t, err)
			defer child.Close()
			database := filepath.Join(child.Path, databaseName)
			if exists {
				requireNoError(t, statefs.WriteNew(database, nil))
			}
			if _, err := OpenRoot(testctx, child, DefaultOptions(), "rt_expected"); !errors.Is(err, ErrCorrupt) {
				t.Fatal("missing database initialized", err)
			}
			entries, err := os.ReadDir(child.Path)
			want := 0
			if exists {
				want = 1
			}
			if err != nil || len(entries) != want {
				t.Fatal("failed open created bootstrap files", entries, err)
			}
			if exists {
				info, err := os.Stat(database)
				if err != nil || info.Size() != 0 {
					t.Fatal("failed open wrote database", err)
				}
			}
		})
	}
}

func TestHostedHistoricalIdentityCheckedBeforeMigration(t *testing.T) {
	for _, fixture := range []string{baselineDir, "testdata/baseline-b8dc5b4"} {
		t.Run(filepath.Base(fixture), func(t *testing.T) {
			script, err := os.ReadFile(filepath.Join(fixture, "runtime.sql"))
			requireNoError(t, err)
			existing := loadStateScript(t, string(script))
			original, err := connect(filepath.Join(existing, databaseName))
			requireNoError(t, err)
			var id string
			var version int
			requireNoError(t, original.QueryRow("SELECT installation_id FROM runtime_installation").Scan(&id))
			requireNoError(t, original.QueryRow("PRAGMA user_version").Scan(&version))
			requireNoError(t, original.Close())
			owner, err := statefs.Open(filepath.Join(t.TempDir(), "host"))
			requireNoError(t, err)
			defer owner.Close()
			requireNoError(t, os.Rename(existing, filepath.Join(owner.Path, "state")))
			child, err := owner.Child("state")
			requireNoError(t, err)
			defer child.Close()
			requireNoError(t, writeInstallation(child.Path, id))
			if _, err := OpenRoot(testctx, child, DefaultOptions(), "rt_different"); !errors.Is(err, ErrCorrupt) {
				t.Fatal("foreign database adopted", err)
			}
			raw, err := os.ReadFile(filepath.Join(child.Path, ".installation"))
			if err != nil || string(raw) != id {
				t.Fatal("failed startup removed legacy marker", err)
			}
			original, err = connect(filepath.Join(child.Path, databaseName))
			requireNoError(t, err)
			var unchanged int
			if err := original.QueryRow("PRAGMA user_version").Scan(&unchanged); err != nil || unchanged != version {
				t.Fatal("identity mismatch migrated database", unchanged, err)
			}
			requireNoError(t, original.Close())
			store, err := OpenRoot(testctx, child, DefaultOptions(), id)
			requireNoError(t, err)
			defer store.Close()
			info, err := store.Info(testctx)
			if err != nil || info.InstallationID != id || info.SchemaVersion != len(migrations) {
				t.Fatal("historical hosted upgrade changed identity", info, err)
			}
			raw, err = os.ReadFile(filepath.Join(child.Path, ".installation"))
			if err != nil || !bytes.Equal(raw, []byte(id)) {
				t.Fatal("constructor removed marker before full host validation", err)
			}
			requireNoError(t, store.RemoveLegacyInstallation(testctx, id))
		})
	}
}
