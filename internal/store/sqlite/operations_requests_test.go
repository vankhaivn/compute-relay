package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

func TestStoredCleanupRequestPreservesHistoricalHashes(t *testing.T) {
	const original = `{"version":"cleanup/v1","job_id":"job_1","kind":"cleanup","request":`
	for _, test := range []struct {
		raw, canonical string
		legacyTrue     bool
	}{
		{`{"attempt_id":"att_1"}`, `{"attempt_id":"att_1","discard_uncollected":false}`, false},
		{` {"reason":"","accept_name_based_delete":false,"attempt_id":"att_1"} `, `{"attempt_id":"att_1","discard_uncollected":false}`, false},
		{`{"attempt_id":"att_1","accept_name_based_delete":true}`, `{"attempt_id":"att_1","discard_uncollected":false,"accept_name_based_delete":true}`, true},
		{`{"reason":"keep identity","discard_uncollected":true,"attempt_id":"att_1","accept_name_based_delete":true}`, `{"attempt_id":"att_1","discard_uncollected":true,"accept_name_based_delete":true,"reason":"keep identity"}`, true},
	} {
		t.Run(test.raw, func(t *testing.T) {
			before := []byte(test.raw)
			request, hash, err := decodeStoredOperationRequest("job_1", domain.OperationCleanup, before)
			expected := sha256.Sum256([]byte(original + test.canonical + "}"))
			if err != nil || hash != hex.EncodeToString(expected[:]) || request.AttemptID != "att_1" || string(before) != test.raw {
				t.Fatal(request, hash, err)
			}
			current, err := request.Digest("job_1", domain.OperationCleanup)
			if err != nil || (hash != current) != test.legacyTrue {
				t.Fatal("retired true option was normalized into the current request", hash, current, err)
			}
			encoded, _ := json.Marshal(request)
			if strings.Contains(string(encoded), "accept_name_based_delete") {
				t.Fatal("legacy option escaped the stored decoder", string(encoded))
			}
		})
	}
	for _, kind := range []domain.OperationKind{domain.OperationCancel, domain.OperationRetryCompute, domain.OperationReconcile, domain.OperationCollect} {
		raw := []byte(`{"attempt_id":"att_1","reason":"explicit"}`)
		request, hash, err := decodeStoredOperationRequest("job_1", kind, raw)
		expected, _ := request.Digest("job_1", kind)
		if err != nil || hash != expected {
			t.Fatal("ordinary control hash changed", kind, err)
		}
		if _, _, err := decodeStoredOperationRequest("job_1", kind, []byte(`{"attempt_id":"att_1","reason":"explicit","accept_name_based_delete":false}`)); !errors.Is(err, ErrCorrupt) {
			t.Fatal("legacy cleanup field entered another kind", kind, err)
		}
	}
}

func TestStoredCleanupRequestRejectsCorruptData(t *testing.T) {
	for _, raw := range []string{
		"", `null`, `[]`, `{}`, `{"attempt_id":null}`, `{"attempt_id":""}`, `{"attempt_id":"att_1","attempt_id":"att_1"}`,
		`{"attempt_id":"att_1","accept_name_based_delete":null}`, `{"attempt_id":"att_1","accept_name_based_delete":"true"}`, `{"attempt_id":"att_1","accept_name_based_delete":1}`,
		`{"attempt_id":"att_1","accept_name_based_delete":true,"accept_name_based_delete":false}`, `{"attempt_id":"att_1","Accept_Name_Based_Delete":true}`,
		`{"attempt_id":"att_1","discard_uncollected":null}`, `{"attempt_id":"att_1","discard_uncollected":"true"}`, `{"attempt_id":"att_1","reason":null}`,
		`{"attempt_id":"att_1","reason":"line\nbreak"}`, `{"attempt_id":"att_1","reason":"` + strings.Repeat("x", 513) + `"}`,
		`{"attempt_id":"att_1","extra":true}`, `{"attempt_id":"att_1"}{}`, `{"attempt_id":"att_1","reason":"\ud800"}`, "{\"attempt_id\":\"att_1\",\"reason\":\"\xff\"}",
		strings.Repeat(" ", operations.MaxRequestBytes) + `{"attempt_id":"att_1"}`,
	} {
		if _, _, err := decodeStoredOperationRequest("job_1", domain.OperationCleanup, []byte(raw)); !errors.Is(err, ErrCorrupt) {
			t.Fatal("corrupt stored request accepted", raw, err)
		}
	}
	if _, _, err := decodeStoredOperationRequest("", domain.OperationCleanup, []byte(`{"attempt_id":"att_1"}`)); !errors.Is(err, ErrCorrupt) {
		t.Fatal("invalid stored job accepted", err)
	}
}

func TestStoredCleanupReceiptsRemainReadableWithoutPublicOption(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			ctx := context.Background()
			f, id := collectedFixture(t)
			service, actor, original := admitCleanup(t, f, id, "historical-cleanup", false)
			root := copyFixtureAtSchema(t, f.s, 27)
			old, err := connect(filepath.Join(root, databaseName))
			if err != nil {
				t.Fatal(err)
			}
			hash := rewriteSyntheticCleanupRequest(t, old, original.Operation.ID, accepted)
			if err = old.Close(); err != nil {
				t.Fatal(err)
			}
			f.root = root
			f.restart(t)
			service, actor = cleanupService(t, f, "a", auth.Read, auth.Operate)
			query := `SELECT o.request_sha256,o.request_json,o.record,r.request_sha256,r.canonical_version,r.receipt FROM operations o JOIN receipts r ON r.workspace_id=o.workspace_id AND r.subject_id=o.operation_id WHERE o.kind='cleanup' AND r.namespace='operation' AND r.scope='cleanup'`
			before := upgradeRows(t, f.s.db, query)
			read, err := service.Get(ctx, actor, "a", original.Operation.ID)
			if err != nil || !reflect.DeepEqual(read.Operation, original.Operation) || len(read.Resources) != 2 {
				t.Fatal(read, err)
			}
			for _, flag := range []string{"true", "false"} {
				if _, err = service.Submit(ctx, actor, "a", id.JobID, domain.OperationCleanup, "historical-cleanup", []byte(fmt.Sprintf(`{"attempt_id":%q,"accept_name_based_delete":%s}`, id.AttemptID, flag))); !errors.Is(err, operations.ErrRequest) {
					t.Fatal("public retired option accepted", flag, err)
				}
			}
			receipt, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCleanup, "historical-cleanup", cleanupBody(id.AttemptID, false, ""))
			if accepted {
				if !errors.Is(err, operations.ErrConflict) {
					t.Fatal("new request reused a legacy true key", receipt, err)
				}
			} else if err != nil || !receipt.Replay || receipt.Operation != original.Operation || len(receipt.Resources) != 0 {
				t.Fatal("old false receipt not replayed", receipt, err)
			}
			key, _ := admission.KeyDigest("historical-cleanup")
			saved, err := f.s.ReplayOperation(ctx, "a", actor.TokenID(), domain.OperationCleanup, key, hash)
			if err != nil || saved == nil || !saved.Replay || saved.Operation != original.Operation || len(saved.Resources) != 0 {
				t.Fatal("stored original receipt lost", saved, err)
			}
			if after := upgradeRows(t, f.s.db, query); !reflect.DeepEqual(before, after) {
				t.Fatal("read/replay rewrote historical bytes", before, after)
			}
			worker, probe := cleanupWorker(t, f)
			runCleanup(t, worker)
			if len(probe.requests) != 2 {
				t.Fatal("historical option changed the exact-target cleanup path", len(probe.requests))
			}
			for i, request := range probe.requests {
				if request.LedgerID != read.Resources[i].ResourceID {
					t.Fatal("historical target changed", request)
				}
			}
		})
	}
}

// Rewrite only unpublished released-schema copies, never a current store or an
// original fixture. This models requests produced by the removed public option.
func rewriteSyntheticCleanupRequest(t *testing.T, db *sql.DB, id domain.OperationID, accepted bool) string {
	t.Helper()
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 26 && version != 27 {
		t.Fatal("expected a synthetic released cleanup schema", version, err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var job domain.JobID
	var requestRaw string
	var attemptID domain.AttemptID
	if err = tx.QueryRow(`SELECT job_id,attempt_id,request_json FROM cleanup_operations WHERE operation_id=?`, id).Scan(&job, &attemptID, &requestRaw); err != nil {
		t.Fatal(err)
	}
	current, err := operations.Parse(domain.OperationCleanup, []byte(requestRaw))
	if err != nil || current.Reason != "" || current.DiscardUncollected {
		t.Fatal("unsupported synthetic legacy request", current, err)
	}
	flag := "false"
	optional := ""
	if accepted {
		flag = "true"
		optional = `,"accept_name_based_delete":true`
	}
	request := fmt.Sprintf(`{"attempt_id":%q,"discard_uncollected":false,"accept_name_based_delete":%s}`, attemptID, flag)
	canonical := fmt.Sprintf(`{"version":"cleanup/v1","job_id":%q,"kind":"cleanup","request":{"attempt_id":%q,"discard_uncollected":false%s}}`, job, attemptID, optional)
	sum := sha256.Sum256([]byte(canonical))
	hash := hex.EncodeToString(sum[:])
	receiptTable, immutable := "receipts", "immutable_receipt"
	if version == 26 {
		receiptTable, immutable = "cleanup_idempotency", "immutable_cleanup_receipt"
	}
	var restore []string
	for _, name := range []string{"cleanup_identity", "cleanup_terminal", "cleanup_revision", immutable} {
		var raw string
		if err = tx.QueryRow(`SELECT sql FROM sqlite_schema WHERE name=? AND type='trigger'`, name).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		restore = append(restore, raw)
		if _, err = tx.Exec("DROP TRIGGER " + name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec(`UPDATE cleanup_operations SET request_json=?,request_sha256=? WHERE operation_id=?`, request, hash, id); err != nil {
		t.Fatal(err)
	}
	query := "UPDATE " + receiptTable + " SET request_sha256=? WHERE operation_id=?"
	if version == 27 {
		query = "UPDATE receipts SET request_sha256=? WHERE namespace='operation' AND scope='cleanup' AND subject_id=?"
	}
	if _, err = tx.Exec(query, hash, id); err != nil {
		t.Fatal(err)
	}
	for _, raw := range restore {
		if _, err = tx.Exec(raw); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return hash
}
