package sqlite

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestUpgradeFromFirstMigrationPreservesIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	root, err := statefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := initializeRoot(root.Path); err != nil {
		t.Fatal(err)
	}
	if err := statefs.WriteNew(filepath.Join(path, databaseName), nil); err != nil {
		t.Fatal(err)
	}
	db, err := connect(filepath.Join(path, databaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrate(testctx, db, migrations[:1]); err != nil {
		t.Fatal(err)
	}
	var before string
	if err := db.QueryRow("SELECT installation_id FROM runtime_installation").Scan(&before); err != nil {
		t.Fatal(err)
	}
	db.Close()
	root.Close()
	s, err := Open(testctx, path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	info, _ := s.Info(testctx)
	if info.InstallationID != before || info.SchemaVersion != len(migrations) {
		t.Fatal("bad upgrade", info)
	}
	workspace(t, s, "upgraded")
	if err := migrate(testctx, s.db, migrations); err != nil {
		t.Fatal("repeat migration", err)
	}
}

func TestFailedMigrationIsAtomic(t *testing.T) {
	s, _ := newStore(t)
	sql := "CREATE TABLE partial_migration (v INTEGER); INSERT INTO nonexistent_table VALUES(1);"
	sum := sha256.Sum256([]byte(sql))
	set := append(append([]migration{}, migrations...), migration{len(migrations) + 1, "injected.sql", sql, hex.EncodeToString(sum[:])})
	if err := migrate(testctx, s.db, set); err == nil {
		t.Fatal("invalid migration succeeded")
	}
	var count, version int
	if err := s.db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name='partial_migration'").Scan(&count); err != nil || count != 0 {
		t.Fatal("partial DDL persisted", err)
	}
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != len(migrations) {
		t.Fatal("failed migration advanced version")
	}
	if err := s.Ready(testctx); err != nil {
		t.Fatal(err)
	}
}
func TestRejectNewerTamperedAndForeignDatabases(t *testing.T) {
	for name, sql := range map[string]string{"newer": "PRAGMA user_version=999", "checksum": "UPDATE schema_migrations SET sha256='" + strings.Repeat("0", 64) + "' WHERE version=1", "gap": "DELETE FROM schema_migrations WHERE version=1", "foreign": "PRAGMA application_id=7", "missing-identity": "DELETE FROM runtime_installation"} {
		t.Run(name, func(t *testing.T) {
			s, path := newStore(t)
			if _, err := s.db.Exec(sql); err != nil {
				t.Fatal(err)
			}
			s.Close()
			other, err := Open(testctx, path, DefaultOptions())
			if other != nil {
				other.Close()
			}
			if !errors.Is(err, ErrSchema) {
				t.Fatalf("bad database accepted: %v", err)
			}
		})
	}
}
func TestMissingDatabaseAndInterruptedRestoreDoNotCreateFreshIdentity(t *testing.T) {
	for _, name := range []string{"missing", "empty", "restoring"} {
		t.Run(name, func(t *testing.T) {
			s, path := newStore(t)
			s.Close()
			db := filepath.Join(path, databaseName)
			switch name {
			case "missing":
				if err := os.Remove(db); err != nil {
					t.Fatal(err)
				}
			case "empty":
				if err := os.Truncate(db, 0); err != nil {
					t.Fatal(err)
				}
			case "restoring":
				if err := statefs.WriteNew(db+".restore", []byte("unfinished")); err != nil {
					t.Fatal(err)
				}
			}
			other, err := Open(testctx, path, DefaultOptions())
			if other != nil {
				other.Close()
			}
			if err == nil {
				t.Fatal("damaged state silently reinitialized")
			}
		})
	}
}
func TestUnrelatedDirectoryIsNotClaimed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(path, "keep.txt")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(testctx, path, DefaultOptions())
	if s != nil {
		s.Close()
	}
	if !errors.Is(err, ErrSchema) {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(sentinel); string(data) != "keep" {
		t.Fatal("unrelated data changed")
	}
}
