package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

// copyFixtureAtSchema builds the actual released schema in a NEW test root, then
// copies compatible fixture rows. It never downgrades a production database or
// pretends a current schema became old by deleting migration ledger entries.
func copyFixtureAtSchema(t *testing.T, source *Store, version int) string {
	t.Helper()
	if version < 1 || version >= len(migrations) {
		t.Fatal("invalid fixture schema version")
	}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "old-state")
	root, err := statefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := initializeRoot(path); err != nil {
		t.Fatal(err)
	}
	if err := statefs.WriteNew(filepath.Join(path, databaseName), nil); err != nil {
		t.Fatal(err)
	}
	db, err := connect(filepath.Join(path, databaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(ctx, db, migrations[:version]); err != nil {
		t.Fatal(err)
	}
	type trigger struct{ name, sql string }
	triggers := []trigger{}
	tables := []string{}
	rows, err := db.Query("SELECT type,name,sql FROM sqlite_schema WHERE type IN ('table','trigger') AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var kind, name, definition string
		if err := rows.Scan(&kind, &name, &definition); err != nil {
			t.Fatal(err)
		}
		if kind == "trigger" {
			triggers = append(triggers, trigger{name, definition})
		} else if name != "schema_migrations" {
			tables = append(tables, name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if _, err := db.Exec("ATTACH DATABASE ? AS fixture_source", filepath.Join(source.root.Path, databaseName)); err != nil {
		t.Fatal(err)
	}
	quote := func(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
	err = withTx(ctx, db, func(tx *sql.Tx) error {
		if _, err := tx.Exec("PRAGMA defer_foreign_keys=ON"); err != nil {
			return err
		}
		// No enqueue or state side effects while copying already committed fixtures.
		// Restore the exact released triggers before commit and verify foreign keys.
		for _, tr := range triggers {
			if _, err := tx.Exec("DROP TRIGGER " + quote(tr.name)); err != nil {
				return err
			}
		}
		for _, name := range tables {
			if _, err := tx.Exec("DELETE FROM " + quote(name)); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO " + quote(name) + " SELECT * FROM fixture_source." + quote(name)); err != nil {
				return err
			}
		}
		for _, tr := range triggers {
			if _, err := tx.Exec(tr.sql); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal("copy released fixture", err)
	}
	if _, err := db.Exec("DETACH DATABASE fixture_source"); err != nil {
		t.Fatal(err)
	}
	if err := recordInstallation(ctx, db, path); err != nil {
		t.Fatal(err)
	}
	if err := integrity(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectSchema(ctx, db, migrations[:version], false); err != nil {
		t.Fatal(err)
	}
	return path
}
