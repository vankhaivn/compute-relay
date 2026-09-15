package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

// copyFixtureAtSchema builds a NEW, unpublished test database from the actual
// released migrations. Compatible synthetic rows are copied without triggers or
// insertion-order constraints, then ALL foreign keys and integrity are verified.
// No production database or production connection changes its enforcement policy.
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
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "ATTACH DATABASE ? AS fixture_source", filepath.Join(source.root.Path, databaseName)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	quote := func(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
	for _, tr := range triggers {
		if _, err := tx.Exec("DROP TRIGGER " + quote(tr.name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range tables {
		if _, err := tx.Exec("DELETE FROM " + quote(name)); err != nil {
			t.Fatal("clear fixture table", name, err)
		}
		if _, err := tx.Exec("INSERT INTO " + quote(name) + " SELECT * FROM fixture_source." + quote(name)); err != nil {
			t.Fatal("copy fixture table", name, err)
		}
	}
	for _, tr := range triggers {
		if _, err := tx.Exec(tr.sql); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal("fixture copy commit", err)
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		t.Fatal(err)
	}
	var enabled bool
	if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&enabled); err != nil || !enabled {
		t.Fatal("fixture foreign-key enforcement not restored", err)
	}
	if _, err := conn.ExecContext(ctx, "DETACH DATABASE fixture_source"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := recordInstallation(ctx, db, path); err != nil {
		t.Fatal(err)
	}
	if err := integrity(ctx, db); err != nil {
		t.Fatal("released fixture integrity/foreign keys", err)
	}
	if _, err := inspectSchema(ctx, db, migrations[:version], false); err != nil {
		t.Fatal(err)
	}
	return path
}
