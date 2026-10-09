package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestManagedConfigurationMigrationPreservesOperationsAndConstraints(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-managed")
	root, err := statefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err = initializeRoot(path); err != nil {
		t.Fatal(err)
	}
	if err = statefs.WriteNew(filepath.Join(path, databaseName), nil); err != nil {
		t.Fatal(err)
	}
	db, err := connect(filepath.Join(path, databaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = migrate(ctx, db, migrations[:12]); err != nil {
		t.Fatal(err)
	}
	legacy := &Store{db: db, root: root, options: DefaultOptions()}
	workspace(t, legacy, "a")
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	stamp := now.Format(time.RFC3339Nano)
	if _, err = db.Exec(`INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,pending_operation,updated_at) VALUES('con_old','a','fixture','Retained connection',1,'pending','enabled','op_old',?)`, stamp); err != nil {
		t.Fatal(err)
	}
	receipt, _ := json.Marshal(connections.Operation{ID: "op_old", Workspace: "a", ConnectionID: "con_old", Action: "create", Status: "accepted", ConnectionRevision: 1, CreatedAt: now, UpdatedAt: now})
	if _, err = db.Exec(`INSERT INTO managed_connection_operations VALUES('op_old','a','con_old','create',1,'retained_token',?,?,'waiting_secret','accepted','op_old',?,NULL,?,?)`, strings.Repeat("a", 64), strings.Repeat("b", 64), string(receipt), stamp, stamp); err != nil {
		t.Fatal(err)
	}
	var installation string
	if err = db.QueryRow("SELECT installation_id FROM runtime_installation").Scan(&installation); err != nil {
		t.Fatal(err)
	}
	if err = migrate(ctx, db, migrations); err != nil {
		t.Fatal(err)
	}
	var afterID, afterReceipt, pending, stage, key, fingerprint string
	var wall sql.NullInt64
	if err = db.QueryRow("SELECT installation_id FROM runtime_installation").Scan(&afterID); err != nil || afterID != installation {
		t.Fatal("migration changed installation identity", err)
	}
	if err = db.QueryRow(`SELECT receipt,stage,key_sha256,fingerprint FROM managed_connection_operations WHERE operation_id='op_old'`).Scan(&afterReceipt, &stage, &key, &fingerprint); err != nil || afterReceipt != string(receipt) || stage != "waiting_secret" || key != strings.Repeat("a", 64) || fingerprint != strings.Repeat("b", 64) {
		t.Fatal("migration changed original intent or receipt", err)
	}
	if err = db.QueryRow(`SELECT pending_operation,max_remote_wall_seconds FROM managed_connections WHERE connection_id='con_old'`).Scan(&pending, &wall); err != nil || pending != "op_old" || wall.Valid {
		t.Fatal("migration changed pending operation or invented an override", err)
	}
	if _, err = db.Exec(`UPDATE managed_connection_operations SET receipt='{}' WHERE operation_id='op_old'`); err == nil {
		t.Fatal("migration removed operation immutability")
	}
	if _, err = db.Exec(`UPDATE managed_connections SET max_remote_wall_seconds=0 WHERE connection_id='con_old'`); err == nil {
		t.Fatal("invalid wall-time stored")
	}
	if err = integrity(ctx, db); err != nil {
		t.Fatal(err)
	}
}
