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
	requireNoError(t, err)
	defer root.Close()
	requireNoError(t, initializeRoot(path))
	requireNoError(t, statefs.WriteNew(filepath.Join(path, databaseName), nil))
	db, err := connect(filepath.Join(path, databaseName))
	requireNoError(t, err)
	defer db.Close()
	requireNoError(t, migrate(ctx, db, migrations[:version]))
	type trigger struct{ name, sql string }
	triggers := []trigger{}
	tables := []string{}
	rows, err := db.Query("SELECT type,name,sql FROM sqlite_schema WHERE type IN ('table','trigger') AND name NOT LIKE 'sqlite_%' ORDER BY name")
	requireNoError(t, err)
	for rows.Next() {
		var kind, name, definition string
		requireNoError(t, rows.Scan(&kind, &name, &definition))
		if kind == "trigger" {
			triggers = append(triggers, trigger{name, definition})
		} else if name != "schema_migrations" {
			tables = append(tables, name)
		}
	}
	requireNoError(t, rows.Err())
	rows.Close()
	conn, err := db.Conn(ctx)
	requireNoError(t, err)
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "ATTACH DATABASE ? AS fixture_source", filepath.Join(source.root.Path, databaseName)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	requireNoError(t, err)
	defer tx.Rollback()
	quote := func(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
	for _, tr := range triggers {
		if _, err := tx.Exec("DROP TRIGGER " + quote(tr.name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range tables {
		// These unused tables were retired at schema 23; older fixtures need empty tables.
		if name == "retention_holds" || name == "retention_audit" || name == "cleanup_previews" {
			continue
		}
		if _, err := tx.Exec("DELETE FROM " + quote(name)); err != nil {
			t.Fatal("clear fixture table", name, err)
		}
		columnRows, err := tx.Query("PRAGMA table_xinfo(" + quote(name) + ")")
		requireNoError(t, err)
		var writable []string
		for columnRows.Next() {
			var ordinal, notNull, primaryKey, hidden int
			var column, kind string
			var defaultValue any
			requireNoError(t, columnRows.Scan(&ordinal, &column, &kind, &notNull, &defaultValue, &primaryKey, &hidden))
			if hidden == 0 {
				writable = append(writable, column)
			}
		}
		requireNoError(t, columnRows.Err())
		columnRows.Close()
		targetColumns := strings.Join(writable, ",")
		columns := targetColumns
		if name == "managed_connection_operations" && version < 18 {
			var installed int
			if err := tx.QueryRow("SELECT count(*) FROM fixture_source.managed_connection_operations WHERE action='install_image' OR length(installation)>0").Scan(&installed); err != nil || installed != 0 {
				t.Fatal("legacy fixture cannot represent image installation", err)
			}
			columns = "operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,fingerprint,stage,status,credential_key,receipt,problem,created_at,updated_at"
		}
		if name == "managed_connections" && version < 17 {
			var limited int
			if err := tx.QueryRow("SELECT count(*) FROM fixture_source.managed_connections WHERE max_active_attempts!=0 OR max_processes!=0").Scan(&limited); err != nil || limited != 0 {
				t.Fatal("legacy fixture cannot represent execution limit overrides", err)
			}
			columns = "connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,current_profile,pending_operation,updated_at,max_remote_wall_seconds,host_evidence"
		}
		if name == "managed_connections" && version < 16 {
			var evidenced int
			if err := tx.QueryRow("SELECT count(*) FROM fixture_source.managed_connections WHERE length(host_evidence)>0").Scan(&evidenced); err != nil || evidenced != 0 {
				t.Fatal("legacy fixture cannot represent host evidence", err)
			}
			columns = "connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,current_profile,pending_operation,updated_at,max_remote_wall_seconds"
		}
		if name == "managed_connections" && version < 13 {
			var configured int
			if err := tx.QueryRow("SELECT count(*) FROM fixture_source.managed_connections WHERE max_remote_wall_seconds IS NOT NULL").Scan(&configured); err != nil || configured != 0 {
				t.Fatal("legacy fixture cannot represent configured wall-time overrides", err)
			}
			columns = "connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,current_profile,pending_operation,updated_at"
		}
		sourceTable := "fixture_source." + quote(name)
		// Reconstruct historical receipt/grant layouts without rewriting their JSON.
		switch name {
		case "operations":
			if version < 28 {
				sourceTable += " WHERE kind!='cleanup'"
			}
		case "cleanup_operations":
			sourceTable = "(SELECT operation_seq AS cleanup_seq,workspace_id,operation_id,job_id,attempt_id,resource_id,status,revision,created_at,request_sha256,request_json,record FROM fixture_source.operations WHERE kind='cleanup')"
		case "idempotency":
			sourceTable = "fixture_source.receipts WHERE namespace='admission'"
			columns = "workspace_id,scope,key_sha256,request_sha256,canonical_version,job_id,attempt_id,receipt"
		case "operation_idempotency":
			sourceTable = "fixture_source.receipts WHERE namespace='operation' AND scope!='cleanup'"
			columns = "workspace_id,scope,key_sha256,request_sha256,canonical_version,subject_id,receipt"
		case "cleanup_idempotency":
			sourceTable = "fixture_source.receipts WHERE namespace='operation' AND scope='cleanup'"
			columns = "workspace_id,key_sha256,request_sha256,canonical_version,subject_id,receipt"
		case "managed_connection_operations":
			if version >= 27 {
				break
			}
			sourceTable = "(SELECT o.*,r.request_sha256 AS fingerprint,r.receipt FROM fixture_source.managed_connection_operations o JOIN fixture_source.receipts r ON r.workspace_id=o.workspace_id AND r.namespace='connection' AND r.scope='' AND r.key_sha256=o.key_sha256)"
			columns = "operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,fingerprint,stage,status,credential_key,receipt,problem,created_at,updated_at"
			if version >= 18 {
				columns += ",installation"
			}
		case "execution_authorizations", "preparation_authorizations", "start_authorizations", "workload_secret_authorizations":
			kind := map[string]string{"execution_authorizations": "execution", "preparation_authorizations": "prepare", "start_authorizations": "start", "workload_secret_authorizations": "secret"}[name]
			sourceTable = "fixture_source.attempt_grants g JOIN fixture_source.receipts r ON r.workspace_id=g.workspace_id AND r.namespace='grant' AND r.scope=g.kind AND r.key_sha256=g.key_sha256"
			columns = "g.workspace_id,g.authorization_id,g.job_id,g.attempt_id,g.key_sha256,r.request_sha256,"
			switch kind {
			case "execution":
				columns += "g.binding,g.account_scope,g.inputs_sha256,g.attempt_nonce,g.max_remote_wall_seconds,g.granting_token_id,g.created_at,r.receipt,g.status,g.consumed_at"
			case "prepare":
				columns += "g.request,r.receipt,g.binding,g.account_scope,g.inputs_sha256,g.attempt_nonce,g.reserved_bytes,g.ready_until_ms,g.consumed_at,g.ready_revision,g.ready_at,g.storage_released"
			case "start":
				columns += "g.request,r.receipt,g.consumed_at"
			case "secret":
				columns += "g.metadata,r.receipt,g.expires_at_ms,g.consumed_at,g.revoked_at,g.revocation_key_sha256,v.receipt"
				sourceTable += " LEFT JOIN fixture_source.receipts v ON v.workspace_id=g.workspace_id AND v.namespace='revoke' AND v.scope=g.authorization_id AND v.key_sha256=g.revocation_key_sha256"
			}
			sourceTable += " WHERE g.kind='" + kind + "'"
		}
		// Reconstruct the released lease layout when creating an older synthetic fixture.
		switch name {
		case "work_leases":
			columns = "kind,key,generation,owner,fence,until_ms,held,operation_id,attempt_revision"
		case "scheduler_leases":
			sourceTable = "fixture_source.work_leases WHERE kind='dispatch'"
			columns = "queue_seq,generation,CASE held WHEN 0 THEN '' ELSE owner END,CASE held WHEN 0 THEN '' ELSE fence END,CASE held WHEN 0 THEN 0 ELSE until_ms END,held"
		case "collection_leases":
			sourceTable = "fixture_source.work_leases WHERE kind='collection'"
			columns = "workspace_id,job_id,attempt_id,operation_id,generation,fence,until_ms,attempt_revision,held"
		case "cleanup_leases":
			sourceTable = "fixture_source.work_leases WHERE kind='cleanup'"
			columns = "workspace_id,cleanup_operation_id,generation,fence,until_ms"
		case "collection_progress":
			columns = "workspace_id,job_id,attempt_id,operation_id,generation,fence,stage,progress"
		}
		if _, err := tx.Exec("INSERT INTO " + quote(name) + "(" + targetColumns + ") SELECT " + columns + " FROM " + sourceTable); err != nil {
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
	requireNoError(t, conn.Close())
	requireNoError(t, recordInstallation(ctx, db, path))
	if err := integrity(ctx, db); err != nil {
		t.Fatal("released fixture integrity/foreign keys", err)
	}
	if _, err := inspectSchema(ctx, db, migrations[:version], false); err != nil {
		t.Fatal(err)
	}
	return path
}
