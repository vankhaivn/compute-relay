package sqlite

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"strconv"
	"time"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	version           int
	name, sql, digest string
}

var migrations = loadMigrations()

func loadMigrations() []migration {
	names := []string{"0001_identity.sql", "0002_workspace_objects.sql", "0003_admission.sql", "0004_scheduler.sql", "0005_dispatch.sql", "0006_operations.sql"}
	result := make([]migration, 0, len(names))
	for i, name := range names {
		data, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			panic("missing embedded state migration")
		}
		digest := sha256.Sum256(data)
		result = append(result, migration{i + 1, name, string(data), hex.EncodeToString(digest[:])})
	}
	return result
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func inspectSchema(ctx context.Context, q queryer, set []migration, allowEmpty bool) (int, error) {
	var app, version, count int
	if err := q.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return 0, dbError(err)
	}
	if err := q.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, dbError(err)
	}
	if app == 0 && version == 0 && allowEmpty {
		if err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").Scan(&count); err != nil {
			return 0, dbError(err)
		}
		if count == 0 {
			return 0, nil
		}
	}
	if app != applicationID || version < 1 || version > len(set) {
		return 0, ErrSchema
	}
	rows, err := q.QueryContext(ctx, "SELECT version,name,sha256 FROM schema_migrations ORDER BY version")
	if err != nil {
		return 0, ErrSchema
	}
	i := 0
	for rows.Next() {
		var n int
		var name, digest string
		if err := rows.Scan(&n, &name, &digest); err != nil {
			rows.Close()
			return 0, ErrSchema
		}
		if i >= version || n != i+1 || name != set[i].name || digest != set[i].digest {
			rows.Close()
			return 0, ErrSchema
		}
		i++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, dbError(err)
	}
	if i != version {
		return 0, ErrSchema
	}
	var id, created string
	if err := q.QueryRowContext(ctx, "SELECT installation_id,created_at FROM runtime_installation WHERE singleton=1").Scan(&id, &created); err != nil {
		return 0, ErrSchema
	}
	if !validID(id) || !validTime(created) {
		return 0, ErrSchema
	}
	return version, nil
}

func migrate(ctx context.Context, db *sql.DB, set []migration) error {
	return withTx(ctx, db, func(tx *sql.Tx) error {
		version, err := inspectSchema(ctx, tx, set, true)
		if err != nil {
			return err
		}
		if version == 0 {
			if _, err := tx.ExecContext(ctx, `CREATE TABLE schema_migrations (
     version INTEGER PRIMARY KEY CHECK(version>0), name TEXT NOT NULL UNIQUE,
     sha256 TEXT NOT NULL CHECK(length(sha256)=64), applied_at TEXT NOT NULL
   ) STRICT`); err != nil {
				return dbError(err)
			}
			if _, err := tx.ExecContext(ctx, "PRAGMA application_id="+strconv.Itoa(applicationID)); err != nil {
				return dbError(err)
			}
		}
		for _, m := range set[version:] {
			if _, err := tx.ExecContext(ctx, m.sql); err != nil {
				return dbError(err)
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations(version,name,sha256,applied_at) VALUES(?,?,?,?)", m.version, m.name, m.digest, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return dbError(err)
			}
			if _, err := tx.ExecContext(ctx, pragmaVersion(m.version)); err != nil {
				return dbError(err)
			}
		}
		if version == 0 {
			var id [16]byte
			if _, err := rand.Read(id[:]); err != nil {
				return ErrUnavailable
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO runtime_installation VALUES(1,?,?)", "rt_"+hex.EncodeToString(id[:]), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return dbError(err)
			}
		}
		return nil
	})
}

func integrity(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return dbError(err)
	}
	count := 0
	for rows.Next() {
		var result string
		if rows.Scan(&result) != nil || result != "ok" {
			rows.Close()
			return ErrCorrupt
		}
		count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return dbError(err)
	}
	if count != 1 {
		return ErrCorrupt
	}
	rows, err = db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return dbError(err)
	}
	defer rows.Close()
	if rows.Next() {
		return ErrCorrupt
	}
	return dbError(rows.Err())
}

func validTime(value string) bool {
	t, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && !t.IsZero()
}
