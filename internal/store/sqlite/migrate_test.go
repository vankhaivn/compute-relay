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
	db, err := connect(filepath.Join(path, databaseName))
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

// Existing installations record these digests and refuse to open when a shipped migration
// changes, so every released file is frozen byte for byte. Append new migrations instead.
func TestReleasedMigrationsAreFrozen(t *testing.T) {
	released := []string{
		"0001_identity.sql 20d73957c03ded0f24ac743e23ed6975246f2a2aa50de8f2cd5706e999c56a1d",
		"0002_workspace_objects.sql 23fa96b850befd5ca1fb8db6d9e8697a0322624476d66e65a13c29fbec7d44ae",
		"0003_admission.sql e20173c79349c7ced7b79c357e9cf3abedc78c295f0e340c48a44b719a59031d",
		"0004_scheduler.sql 0f700afbb305c80df33c425651ffc5189651af7738233129cb3aa4637d4b1575",
		"0005_dispatch.sql 6151f59b2b362e1b73543fffc0a8a1653d41284177cdf0f9b01b8093dfd37d54",
		"0006_operations.sql d2f23ab35dd7dde59e49aa2e60b42d524ac35f0f9fc4d68db9107c801a5876b7",
		"0007_collection.sql 7035b159fe7947a6ee55056e48a2fb1c80b2e0bba54c97515d48bae261a533b1",
		"0008_retention.sql 3d2c2a6678dca327a87d6779d35efd30db1275f68c060cbbb79fdcf8c995b6ad",
		"0009_retention_roots.sql 4ea9f136b568646613467716ca0f67662e2f4f4bfc891ed3154c49c4b4b81e78",
		"0010_managed_connections.sql ebc2183edb541d214823ad2caff163c2fdfebbac6c931e5664cefd181f13dbc5",
		"0011_execution_authorization.sql 56c7114ae536dd74a58fcf4f9238183c90ee8725c5aa70603327feb6133deed6",
		"0012_collection_progress.sql 1856d9995c682f374c040307df13fbe2b911cda27d6dde40c5349b27772400df",
		"0013_connection_configuration.sql 521a2da75f70fd51e47d3c17640c70261a35dcc81c2dc098a69cde618dd123e0",
		"0014_preparation_progress.sql cefc2b8c6f80a12c1e0104b90b0f1dd7d2b50b38d209ec0df1a81db16e415fe6",
		"0015_remote_cleanup.sql f49f069e0e9090f68d9f715046ed7d9f373c841ec4b2d35c6417ed52a5ede8b8",
		"0016_host_evidence.sql d7e6de95216edde30e285f65519b03929975f4d404c596b22b2423a970c9da11",
		"0017_execution_limits.sql 55c1e35f710ae013377fb8593e11ae7acb88576f5229b660c20c7f60a00ba67c",
		"0018_image_installation.sql 96399646976406dd8afb6a77ead738f41402d952c79b6554fe22d1ea3aa8f015",
		"0019_execution_gates.sql c4ef339a684c860a2f950c068227723279424e5d934acc00bd7b2abe988862cb",
		"0020_workload_secrets.sql e4eb39f7cbd07af139d136eb8f81af26cf970844fe46bcc767545732885b148a",
		"0021_cleanup_resources.sql 18d1a698cf1617f23b01ecef1f26b7081ee7a2561eed71443e1f6de7d0595d6b",
	}
	if len(migrations) < len(released) {
		t.Fatalf("released migrations removed: %d < %d", len(migrations), len(released))
	}
	for i, want := range released {
		if got := migrations[i].name + " " + migrations[i].digest; got != want {
			t.Fatalf("released migration %d changed: %s", i+1, got)
		}
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
func TestMissingDatabaseDoesNotCreateFreshIdentity(t *testing.T) {
	for _, name := range []string{"missing", "empty"} {
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
