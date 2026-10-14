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
	requireNoError(t, err)
	defer root.Close()
	err = initializeRoot(path)
	requireNoError(t, err)
	err = statefs.WriteNew(filepath.Join(path, databaseName), nil)
	requireNoError(t, err)
	db, err := connect(filepath.Join(path, databaseName))
	requireNoError(t, err)
	defer db.Close()
	err = migrate(ctx, db, migrations[:12])
	requireNoError(t, err)
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
	err = db.QueryRow("SELECT installation_id FROM runtime_installation").Scan(&installation)
	requireNoError(t, err)
	err = migrate(ctx, db, migrations)
	requireNoError(t, err)
	var afterID, afterReceipt, pending, stage, key, fingerprint string
	var wall sql.NullInt64
	if err = db.QueryRow("SELECT installation_id FROM runtime_installation").Scan(&afterID); err != nil || afterID != installation {
		t.Fatal("migration changed installation identity", err)
	}
	if err = db.QueryRow(`SELECT r.receipt,o.stage,o.key_sha256,r.request_sha256 FROM managed_connection_operations o JOIN receipts r ON r.workspace_id=o.workspace_id AND r.namespace='connection' AND r.scope='' AND r.key_sha256=o.key_sha256 WHERE o.operation_id='op_old'`).Scan(&afterReceipt, &stage, &key, &fingerprint); err != nil || afterReceipt != string(receipt) || stage != "waiting_secret" || key != strings.Repeat("a", 64) || fingerprint != strings.Repeat("b", 64) {
		t.Fatal("migration changed original intent or receipt", err)
	}
	if err = db.QueryRow(`SELECT pending_operation,max_remote_wall_seconds FROM managed_connections WHERE connection_id='con_old'`).Scan(&pending, &wall); err != nil || pending != "op_old" || wall.Valid {
		t.Fatal("migration changed pending operation or invented an override", err)
	}
	if _, err = db.Exec(`UPDATE receipts SET receipt='{}' WHERE namespace='connection' AND subject_id='op_old'`); err == nil {
		t.Fatal("migration removed operation immutability")
	}
	if _, err = db.Exec(`UPDATE managed_connections SET max_remote_wall_seconds=0 WHERE connection_id='con_old'`); err == nil {
		t.Fatal("invalid wall-time stored")
	}
	var imagePlan, hostEvidence []byte
	var active, processes int
	if err = db.QueryRow(`SELECT installation FROM managed_connection_operations WHERE operation_id='op_old'`).Scan(&imagePlan); err != nil || len(imagePlan) != 0 {
		t.Fatal("migration invented an image installation", err)
	}
	if err = db.QueryRow(`SELECT host_evidence,max_active_attempts,max_processes FROM managed_connections WHERE connection_id='con_old'`).Scan(&hostEvidence, &active, &processes); err != nil || len(hostEvidence) != 0 || active != 0 || processes != 0 {
		t.Fatal("migration invented host evidence or limits", err)
	}
	if _, err = db.Exec(`UPDATE managed_connection_operations SET installation=X'01' WHERE operation_id='op_old'`); err == nil {
		t.Fatal("image installation plan is mutable")
	}
	err = integrity(ctx, db)
	requireNoError(t, err)
}
