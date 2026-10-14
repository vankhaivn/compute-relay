package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

// These are synthetic released-schema fixtures, including old cleanup records
// without the newer control fields or per-resource deletion intents.
func TestCleanupOperationMigrationPreservesOriginalBytesAndFences(t *testing.T) {
	for _, state := range []string{"accepted", "running", "succeeded", "failed", "legacy_running", "legacy_unknown"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			f, id := collectedFixture(t)
			service, actor := cleanupService(t, f, "a", auth.Read, auth.Operate)
			body := []byte(`{"attempt_id":"` + string(id.AttemptID) + `","accept_name_based_delete":true}`)
			receipt, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCleanup, "legacy-cleanup", body)
			if err != nil {
				t.Fatal(err)
			}
			if state == "running" || strings.HasPrefix(state, "legacy_") {
				w, err := f.s.ClaimCleanup(ctx, time.Second)
				if err != nil || w == nil {
					t.Fatal(w, err)
				}
				if err = f.s.BeginCleanupResource(ctx, *w, w.Resources[0]); err != nil {
					t.Fatal(err)
				}
			}
			if state == "succeeded" || state == "failed" {
				worker, probe := cleanupWorker(t, f)
				if state == "failed" {
					probe.err = errors.New("offline unknown deletion")
				}
				runCleanup(t, worker)
			}
			root := copyFixtureAtSchema(t, f.s, 27)
			legacy, err := connect(filepath.Join(root, databaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			// Preserve the exact original legacy JSON layout and insignificant whitespace.
			var recordRaw string
			if err = legacy.QueryRow(`SELECT record FROM cleanup_operations WHERE operation_id=?`, receipt.Operation.ID).Scan(&recordRaw); err != nil {
				t.Fatal(err)
			}
			current, err := decodeOperation(recordRaw, 8192)
			if err != nil {
				t.Fatal(err)
			}
			if state == "legacy_unknown" {
				p, _ := domain.NewProblem(domain.CodeProviderStateUnknown, "The earlier deletion outcome is unknown.", domain.FailureStageObservation)
				current.Operation, err = current.Operation.Transition(domain.OperationFailed, &p, current.Operation.UpdatedAt)
				if err != nil {
					t.Fatal(err)
				}
				current.Effect = operations.Failed
			}
			oldRecord := func(r operations.Record) string {
				raw, err := json.Marshal(struct {
					Operation domain.Operation
					Effect    operations.Effect
					Replay    bool
				}{r.Operation, r.Effect, false})
				if err != nil {
					t.Fatal(err)
				}
				return " \n" + string(raw) + "\t"
			}
			triggers := []string{"cleanup_identity", "cleanup_revision", "cleanup_terminal", "immutable_receipt"}
			if strings.HasPrefix(state, "legacy_") {
				triggers = append(triggers, "cleanup_resource_snapshot_keep")
			}
			var restore []string
			for _, name := range triggers {
				var raw string
				if err = legacy.QueryRow(`SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?`, name).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				restore = append(restore, raw)
				if _, err = legacy.Exec("DROP TRIGGER " + name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = legacy.Exec(`UPDATE cleanup_operations SET status=?,revision=?,record=?,request_json=' '||request_json||char(10) WHERE operation_id=?`, current.Operation.Status, current.Operation.Revision, oldRecord(current), receipt.Operation.ID); err != nil {
				t.Fatal(err)
			}
			if _, err = legacy.Exec(`UPDATE receipts SET receipt=? WHERE namespace='operation' AND scope='cleanup'`, oldRecord(receipt)); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(state, "legacy_") {
				if _, err = legacy.Exec(`DELETE FROM cleanup_resources`); err != nil {
					t.Fatal(err)
				}
			}
			for _, raw := range restore {
				if _, err = legacy.Exec(raw); err != nil {
					t.Fatal(err)
				}
			}
			queries := []string{
				`SELECT operation_seq,workspace_id,operation_id,job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record,new_attempt_id,cancel_started FROM operations ORDER BY operation_seq`,
				`SELECT workspace_id,operation_id,job_id,attempt_id,resource_id,status,revision,created_at,request_sha256,request_json,record FROM cleanup_operations ORDER BY cleanup_seq`,
				`SELECT workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt FROM receipts ORDER BY workspace_id,namespace,scope,key_sha256`,
				`SELECT * FROM cleanup_resources ORDER BY workspace_id,operation_id,ordinal`,
				`SELECT * FROM work_leases ORDER BY kind,key`,
				`SELECT * FROM collection_progress ORDER BY workspace_id,job_id,attempt_id`,
				`SELECT * FROM attempt_grants ORDER BY workspace_id,kind,authorization_id`,
				`SELECT * FROM managed_connection_operations ORDER BY operation_id`,
				`SELECT * FROM collection_publications ORDER BY workspace_id,job_id,attempt_id`,
				`SELECT * FROM artifacts ORDER BY workspace_id,artifact_id`,
				`SELECT * FROM retention_expirations ORDER BY workspace_id,job_id,attempt_id`,
			}
			before := make([][][]any, len(queries))
			for i, q := range queries {
				before[i] = upgradeRows(t, legacy, q)
			}
			if err = legacy.Close(); err != nil {
				t.Fatal(err)
			}
			calls := f.backend.Stats()
			f.root = root
			f.restart(t)
			for i, q := range queries {
				if i == 0 {
					q = strings.Replace(q, "FROM operations ORDER", "FROM operations WHERE kind!='cleanup' ORDER", 1)
				}
				if i == 1 {
					q = strings.Replace(q, "FROM cleanup_operations ORDER BY cleanup_seq", "FROM operations WHERE kind='cleanup' ORDER BY operation_seq", 1)
				}
				if after := upgradeRows(t, f.s.db, q); !reflect.DeepEqual(before[i], after) {
					t.Fatalf("migration rewrote original evidence %s", q)
				}
			}
			if f.backend.Stats() != calls {
				t.Fatal("migration invoked provider")
			}
			got, err := f.s.ReadOperation(ctx, "a", actor.TokenID(), receipt.Operation.ID)
			if err != nil || !reflect.DeepEqual(got.Operation, current.Operation) || got.Effect != current.Effect {
				t.Fatal(got, err)
			}
			if strings.HasPrefix(state, "legacy_") && len(got.Resources) != 0 {
				t.Fatal("migration manufactured resource outcomes", got.Resources)
			}
			hash, _ := (operations.Request{AttemptID: id.AttemptID, AcceptNameBasedDelete: true}).Digest(id.JobID, domain.OperationCleanup)
			key, _ := admission.KeyDigest("legacy-cleanup")
			replay, err := f.s.ReplayOperation(ctx, "a", actor.TokenID(), domain.OperationCleanup, key, hash)
			if err != nil || replay == nil || !replay.Replay || replay.Operation != receipt.Operation || len(replay.Resources) != 0 {
				t.Fatal("original receipt changed", replay, err)
			}
			falseHash, _ := (operations.Request{AttemptID: id.AttemptID}).Digest(id.JobID, domain.OperationCleanup)
			if _, err = f.s.ReplayOperation(ctx, "a", actor.TokenID(), domain.OperationCleanup, key, falseHash); !errors.Is(err, operations.ErrConflict) {
				t.Fatal("legacy true acceptance was normalized", err)
			}
			if err = integrity(ctx, f.s.db); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCleanupOperationMigrationKeepsFIFOAndSequenceHighWater(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	_, _, receipt := admitCleanup(t, f, id, "fifo-cleanup", false)
	root := copyFixtureAtSchema(t, f.s, 27)
	old, err := connect(filepath.Join(root, databaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	var revisionGuard, terminalGuard string
	if err = old.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='operation_revision'`).Scan(&revisionGuard); err != nil {
		t.Fatal(err)
	}
	if err = old.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='operation_terminal'`).Scan(&terminalGuard); err != nil {
		t.Fatal(err)
	}
	if _, err = old.Exec(`DROP TRIGGER operation_revision; DROP TRIGGER operation_terminal; UPDATE operations SET operation_seq=17;` + revisionGuard + `;` + terminalGuard + `; UPDATE sqlite_sequence SET seq=40 WHERE name='operations'`); err != nil {
		t.Fatal(err)
	}
	legacy := receipt
	legacy.Operation.ID = "op_legacy_failed"
	p, _ := domain.NewProblem(domain.CodeProviderStateUnknown, "The original deletion outcome is unknown.", domain.FailureStageObservation)
	legacy.Operation, err = legacy.Operation.Transition(domain.OperationFailed, &p, legacy.Operation.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	legacy.Effect = operations.Failed
	raw, _ := json.Marshal(legacy)
	if _, err = old.Exec(`INSERT INTO cleanup_operations(cleanup_seq,workspace_id,operation_id,job_id,attempt_id,resource_id,status,revision,created_at,request_sha256,request_json,record)
 SELECT 100,workspace_id,?,job_id,attempt_id,resource_id,'failed',?,created_at,request_sha256,request_json,? FROM cleanup_operations`, legacy.Operation.ID, legacy.Operation.Revision, string(raw)); err != nil {
		t.Fatal(err)
	}
	expected := upgradeRows(t, old, `SELECT operation_seq,operation_id FROM operations ORDER BY operation_seq`)
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	f.root = root
	f.restart(t)
	if got := upgradeRows(t, f.s.db, `SELECT operation_seq,operation_id FROM operations WHERE kind!='cleanup' ORDER BY operation_seq`); !reflect.DeepEqual(got, expected) {
		t.Fatal("original FIFO gap changed", got, expected)
	}
	rows := upgradeRows(t, f.s.db, `SELECT operation_seq,operation_id FROM operations WHERE kind='cleanup' ORDER BY operation_seq`)
	if !reflect.DeepEqual(rows, [][]any{{int64(41), string(receipt.Operation.ID)}, {int64(42), string(legacy.Operation.ID)}}) {
		t.Fatal("cleanup FIFO did not append deterministically", rows)
	}
	var seq int64
	if err = f.s.db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='operations'`).Scan(&seq); err != nil || seq != 42 {
		t.Fatal(seq, err)
	}
	if err = integrity(ctx, f.s.db); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupOperationMigrationEmptyHighWaterAndOverflowRollback(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty_high_water", true: "overflow_rollback"}[overflow], func(t *testing.T) {
			ctx := context.Background()
			var source *Store
			if overflow {
				f, id := collectedFixture(t)
				admitCleanup(t, f, id, "overflow-cleanup", false)
				source = f.s
			} else {
				source, _ = newStore(t)
			}
			root := copyFixtureAtSchema(t, source, 27)
			old, err := connect(filepath.Join(root, databaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			high := int64(9000)
			if overflow {
				high = math.MaxInt64
			}
			if _, err = old.Exec(`DELETE FROM sqlite_sequence WHERE name='operations'`); err != nil {
				t.Fatal(err)
			}
			if _, err = old.Exec(`INSERT INTO sqlite_sequence(name,seq) VALUES('operations',?)`, high); err != nil {
				t.Fatal(err)
			}
			original := upgradeRows(t, old, `SELECT * FROM cleanup_operations ORDER BY cleanup_seq`)
			if err = old.Close(); err != nil {
				t.Fatal(err)
			}
			upgraded, err := Open(ctx, root, DefaultOptions())
			if !overflow {
				if err != nil {
					t.Fatal(err)
				}
				defer upgraded.Close()
				var got int64
				if err = upgraded.db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='operations'`).Scan(&got); err != nil || got != high {
					t.Fatal("empty high-water mark lost", got, err)
				}
				return
			}
			if err == nil {
				upgraded.Close()
				t.Fatal("overflow silently reassigned operation order")
			}
			old, err = connect(filepath.Join(root, databaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			var version int
			if err = old.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 27 {
				t.Fatal("failed migration changed schema version", version, err)
			}
			if got := upgradeRows(t, old, `SELECT * FROM cleanup_operations ORDER BY cleanup_seq`); !reflect.DeepEqual(got, original) {
				t.Fatal("failed migration lost original cleanup", got, original)
			}
			if err = integrity(ctx, old); err != nil {
				t.Fatal("rollback lost FK graph", err)
			}
		})
	}
}
