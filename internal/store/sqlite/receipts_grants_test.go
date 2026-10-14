package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

// Exercise actual released tables, including states not present in the v14/v22
// public fixtures. Whitespace in historical JSON must survive without reencoding.
func TestReceiptGrantMigrationPreservesEveryStateAndOriginalBytes(t *testing.T) {
	ctx := context.Background()
	f, collected := collectedFixture(t)
	cleanupService, principal := cleanupService(t, f, "a", auth.Operate)
	legacyCleanup := operations.Request{AttemptID: collected.AttemptID, AcceptNameBasedDelete: true}
	legacyCleanupRaw, _ := json.Marshal(legacyCleanup)
	if _, err := cleanupService.Submit(ctx, principal, "a", collected.JobID, domain.OperationCleanup, "migration-cleanup", legacyCleanupRaw); err != nil {
		t.Fatal(err)
	}
	f.authorizeSeed = false
	ids := make([]scheduler.Identity, 5)
	for i := range ids {
		ids[i] = f.seedRaw(t, "a", i+20, false, dispatchJSON)
	}
	root := copyFixtureAtSchema(t, f.s, 26)
	legacy, err := connect(filepath.Join(root, databaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	now := f.clock.Now()
	stamp := now.Format(time.RFC3339Nano)
	binding, _ := json.Marshal(f.profile)
	digest := strings.Repeat("c", 64)
	var connection string
	if err = legacy.QueryRow("SELECT connection_id FROM managed_connections LIMIT 1").Scan(&connection); err != nil {
		t.Fatal(err)
	}
	connectionReceipt, _ := json.Marshal(connections.Operation{ID: "op_migration_connection", Workspace: "a", ConnectionID: connection, Action: "check", Status: "accepted", ConnectionRevision: 1, CreatedAt: now, UpdatedAt: now})
	if _, err = legacy.Exec(`INSERT INTO managed_connection_operations(operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,fingerprint,stage,status,credential_key,receipt,created_at,updated_at) VALUES('op_migration_connection','a',?,'check',1,'token_fixture',?,?,'ready','accepted','fixture',?,?,?)`, connection, digest, strings.Repeat("e", 64), " \n"+string(connectionReceipt), stamp, stamp); err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		key := fmt.Sprintf("%064x", i+100)
		grantID := domain.OperationID(fmt.Sprintf("grant_migration_execution_%d", i))
		execution := executionauth.Receipt{AuthorizationID: grantID, WorkspaceID: "a", JobID: id.JobID, AttemptID: id.AttemptID, Status: executionauth.Granted, MaxRemoteWallSeconds: 10, CreatedAt: now}
		raw, _ := json.Marshal(execution)
		status := "granted"
		var consumed any
		if i == 1 || i == 4 {
			status = "consumed"
			consumed = stamp
		}
		if i == 2 {
			status = "revoked"
		}
		if _, err = legacy.Exec(`INSERT INTO execution_authorizations VALUES('a',?,?,?,?,?,?,?,?,?,10,'token_fixture',?,?,?,?)`, grantID, id.JobID, id.AttemptID, key, digest, string(binding), f.profile.AccountScope, digest, digest, stamp, " \n"+string(raw), status, consumed); err != nil {
			t.Fatal("execution", err)
		}
		prepID := domain.OperationID(fmt.Sprintf("grant_migration_prepare_%d", i))
		prep := executionauth.GateReceipt{AuthorizationID: prepID, WorkspaceID: "a", JobID: id.JobID, AttemptID: id.AttemptID, Kind: "prepare", Status: executionauth.Granted, CreatedAt: now}
		raw, _ = json.Marshal(prep)
		request, _ := json.Marshal(executionauth.PrepareRequest{AttemptID: id.AttemptID, MaxPreparationSeconds: 60, ReadyUntil: now.Add(time.Hour).Format(time.RFC3339), MaxStagedBytes: 1024, AuthorizePrivateStaging: true})
		var ready, readyAt any
		consumed = nil
		released := 0
		if i > 0 {
			consumed = stamp
		}
		if i > 1 {
			ready = digest
			readyAt = stamp
		}
		if i == 3 {
			released = 1
		}
		if _, err = legacy.Exec(`INSERT INTO preparation_authorizations VALUES('a',?,?,?,?,?,?,?,?,?,?,?,1024,?,?,?,?,?)`, prepID, id.JobID, id.AttemptID, key, digest, " \n"+string(request), " \n"+string(raw), string(binding), f.profile.AccountScope, digest, digest, now.Add(time.Hour).UnixMilli(), consumed, ready, readyAt, released); err != nil {
			t.Fatal("prepare", err)
		}
		startID := domain.OperationID(fmt.Sprintf("grant_migration_start_%d", i))
		start := executionauth.GateReceipt{AuthorizationID: startID, WorkspaceID: "a", JobID: id.JobID, AttemptID: id.AttemptID, Kind: "start", Status: executionauth.Granted, CreatedAt: now}
		raw, _ = json.Marshal(start)
		request, _ = json.Marshal(executionauth.StartRequest{AttemptID: id.AttemptID, ReadinessRevision: domain.SHA256Digest(digest), MaxRemoteWallSeconds: 10, ExecutionDeadline: now.Add(time.Minute).Format(time.RFC3339), AuthorizeCompute: true})
		consumed = nil
		if i == 1 {
			consumed = stamp
		}
		if _, err = legacy.Exec(`INSERT INTO start_authorizations VALUES('a',?,?,?,?,?,?,?,?)`, startID, id.JobID, id.AttemptID, key, digest, " \n"+string(request), " \n"+string(raw), consumed); err != nil {
			t.Fatal("start", err)
		}
		secretID := domain.OperationID(fmt.Sprintf("secret_migration_%d", i))
		created, until := now, now.Add(time.Minute)
		if i == 3 {
			created = now.Add(-2 * time.Minute)
			until = now.Add(-time.Minute)
		}
		metadata := workloadsecret.Metadata{AttemptID: id.AttemptID, ReadinessRevision: domain.SHA256Digest(digest), ExpiresAt: until.Format(time.RFC3339), Names: []string{"ingest"}}
		secret := workloadsecret.Receipt{AuthorizationID: secretID, WorkspaceID: "a", JobID: id.JobID, Metadata: metadata, Status: "granted", CreatedAt: created}
		raw, _ = json.Marshal(secret)
		meta, _ := json.Marshal(metadata)
		var revoked, revokeKey, revokeReceipt any
		consumed = nil
		if i == 1 {
			consumed = stamp
		}
		if i == 2 || i == 4 {
			revoked = stamp
			revokeKey = digest
			secret.Status = "revoked"
			secret.RevokedAt = &now
			revokedRaw, _ := json.Marshal(secret)
			revokeReceipt = string(revokedRaw)
		}
		if _, err = legacy.Exec(`INSERT INTO workload_secret_authorizations VALUES('a',?,?,?,?,?,?,?,?,?,?,?,?)`, secretID, id.JobID, id.AttemptID, key, digest, string(meta), string(raw), until.UnixMilli(), consumed, revoked, revokeKey, revokeReceipt); err != nil {
			t.Fatal("secret", err)
		}
	}
	receiptBefore := upgradeRows(t, legacy, `SELECT workspace_id,'admission',operation,key_sha256,request_sha256,canonical_version,job_id,job_id,attempt_id,receipt FROM idempotency
 UNION ALL SELECT r.workspace_id,'operation',r.kind,r.key_sha256,r.request_sha256,r.canonical_version,r.operation_id,o.job_id,o.attempt_id,r.receipt FROM operation_idempotency r JOIN operations o USING(workspace_id,operation_id)
 UNION ALL SELECT r.workspace_id,'operation','cleanup',r.key_sha256,r.request_sha256,r.canonical_version,r.operation_id,o.job_id,o.attempt_id,r.receipt FROM cleanup_idempotency r JOIN cleanup_operations o USING(workspace_id,operation_id)
 UNION ALL SELECT workspace_id,'connection','',key_sha256,fingerprint,'',operation_id,NULL,NULL,receipt FROM managed_connection_operations
 UNION ALL SELECT workspace_id,'grant','execution',key_sha256,request_sha256,'',authorization_id,job_id,attempt_id,original_receipt FROM execution_authorizations
 UNION ALL SELECT workspace_id,'grant','prepare',key_sha256,request_sha256,'',authorization_id,job_id,attempt_id,original_receipt FROM preparation_authorizations
 UNION ALL SELECT workspace_id,'grant','start',key_sha256,request_sha256,'',authorization_id,job_id,attempt_id,original_receipt FROM start_authorizations
 UNION ALL SELECT workspace_id,'grant','secret',key_sha256,request_hmac,'',authorization_id,job_id,attempt_id,original_receipt FROM workload_secret_authorizations
 UNION ALL SELECT workspace_id,'revoke',authorization_id,revocation_key_sha256,key_sha256,'',authorization_id,job_id,attempt_id,revocation_receipt FROM workload_secret_authorizations WHERE revoked_at IS NOT NULL
 ORDER BY 1,2,3,4`)
	type preservation struct {
		before, after string
		rows          [][]any
	}
	queries := []preservation{
		{before: "SELECT * FROM execution_authorizations ORDER BY authorization_id", after: `SELECT g.workspace_id,g.authorization_id,g.job_id,g.attempt_id,g.key_sha256,r.request_sha256,g.binding,g.account_scope,g.inputs_sha256,g.attempt_nonce,g.max_remote_wall_seconds,g.granting_token_id,g.created_at,r.receipt,g.status,g.consumed_at FROM attempt_grants g JOIN receipts r ON r.workspace_id=g.workspace_id AND r.namespace='grant' AND r.scope=g.kind AND r.key_sha256=g.key_sha256 WHERE g.kind='execution' ORDER BY g.authorization_id`},
		{before: "SELECT * FROM preparation_authorizations ORDER BY authorization_id", after: `SELECT g.workspace_id,g.authorization_id,g.job_id,g.attempt_id,g.key_sha256,r.request_sha256,g.request,r.receipt,g.binding,g.account_scope,g.inputs_sha256,g.attempt_nonce,g.reserved_bytes,g.ready_until_ms,g.consumed_at,g.ready_revision,g.ready_at,g.storage_released FROM attempt_grants g JOIN receipts r ON r.workspace_id=g.workspace_id AND r.namespace='grant' AND r.scope=g.kind AND r.key_sha256=g.key_sha256 WHERE g.kind='prepare' ORDER BY g.authorization_id`},
		{before: "SELECT * FROM start_authorizations ORDER BY authorization_id", after: `SELECT g.workspace_id,g.authorization_id,g.job_id,g.attempt_id,g.key_sha256,r.request_sha256,g.request,r.receipt,g.consumed_at FROM attempt_grants g JOIN receipts r ON r.workspace_id=g.workspace_id AND r.namespace='grant' AND r.scope=g.kind AND r.key_sha256=g.key_sha256 WHERE g.kind='start' ORDER BY g.authorization_id`},
		{before: "SELECT * FROM workload_secret_authorizations ORDER BY authorization_id", after: `SELECT g.workspace_id,g.authorization_id,g.job_id,g.attempt_id,g.key_sha256,r.request_sha256,g.metadata,r.receipt,g.expires_at_ms,g.consumed_at,g.revoked_at,g.revocation_key_sha256,v.receipt FROM attempt_grants g JOIN receipts r ON r.workspace_id=g.workspace_id AND r.namespace='grant' AND r.scope=g.kind AND r.key_sha256=g.key_sha256 LEFT JOIN receipts v ON v.workspace_id=g.workspace_id AND v.namespace='revoke' AND v.scope=g.authorization_id AND v.key_sha256=g.revocation_key_sha256 WHERE g.kind='secret' ORDER BY g.authorization_id`},
	}
	for i := range queries {
		queries[i].rows = upgradeRows(t, legacy, queries[i].before)
	}
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.Clock = f.clock.Now
	upgraded, err := Open(ctx, root, opts)
	if err != nil {
		t.Fatal("migration", err)
	}
	defer upgraded.Close()
	if got := upgradeRows(t, upgraded.db, `SELECT workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt FROM receipts ORDER BY 1,2,3,4`); !reflect.DeepEqual(got, receiptBefore) {
		t.Fatal("receipt bytes, hash, version, key scope or identity changed")
	}
	for _, q := range queries {
		if got := upgradeRows(t, upgraded.db, q.after); !reflect.DeepEqual(got, q.rows) {
			t.Fatal("grant evidence changed", q.before)
		}
	}
	if err = integrity(ctx, upgraded.db); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"execution", "prepare", "start", "secret"} {
		err = upgraded.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
			key := receiptKey{"a", "grant", kind, fmt.Sprintf("%064x", 100), digest, "", operations.ErrConflict}
			prior, replay, e := replayOrCreate(ctx, tx, key, nil)
			if e != nil || !replay || prior.job != ids[0].JobID {
				t.Fatal("migrated replay", kind, e)
			}
			original := prior.raw
			changed, again, e := replayOrCreate(ctx, tx, key, &savedReceipt{prior.subject, prior.job, prior.attempt, "{}"})
			if e != nil || !again || changed.raw != original {
				t.Fatal("create overwrote original receipt", e)
			}
			key.hash = strings.Repeat("f", 64)
			if _, _, e = replayOrCreate(ctx, tx, key, nil); !errors.Is(e, operations.ErrConflict) {
				t.Fatal("changed request accepted", e)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	cleanupKey, _ := admission.KeyDigest("migration-cleanup")
	cleanupHash, _ := legacyCleanup.Digest(collected.JobID, domain.OperationCleanup)
	if replay, e := upgraded.ReplayOperation(ctx, "a", principal.TokenID(), domain.OperationCleanup, cleanupKey, cleanupHash); e != nil || replay == nil || !replay.Replay {
		t.Fatal("legacy true cleanup replay lost", e)
	}
	legacyCleanup.AcceptNameBasedDelete = false
	changedHash, _ := legacyCleanup.Digest(collected.JobID, domain.OperationCleanup)
	if _, e := upgraded.ReplayOperation(ctx, "a", principal.TokenID(), domain.OperationCleanup, cleanupKey, changedHash); !errors.Is(e, operations.ErrConflict) {
		t.Fatal("changed historical cleanup request accepted", e)
	}
	testSharedGrantConsumption(t, upgraded, ids, now)
	if err = upgraded.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if n := controlCount(t, reopened, `SELECT count(*) FROM attempt_grants WHERE consumed_at IS NOT NULL AND attempt_id='`+string(ids[0].AttemptID)+`'`); n != 4 {
		t.Fatal("restart lost consumption", n)
	}
	if n := controlCount(t, reopened, `SELECT count(*) FROM receipts WHERE namespace='revoke'`); n != 2 {
		t.Fatal("per-authorization revoke keys collided", n)
	}
}

func testSharedGrantConsumption(t *testing.T, s *Store, ids []scheduler.Identity, now time.Time) {
	t.Helper()
	ctx := context.Background()
	for _, kind := range []string{"execution", "prepare", "start", "secret"} {
		t.Run(kind, func(t *testing.T) {
			rollback := errors.New("rollback consumed grant")
			call := func(tx *sql.Tx, at time.Time) error {
				return consumeGrant(ctx, tx, "a", ids[0].JobID, ids[0].AttemptID, kind, at, executionauth.ErrState)
			}
			err := s.write(ctx, func(_ context.Context, tx *sql.Tx, at time.Time) error {
				if e := call(tx, at); e != nil {
					return e
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
			if err = s.write(ctx, func(_ context.Context, tx *sql.Tx, at time.Time) error { return call(tx, at) }); err != nil {
				t.Fatal("rollback rearmed no grant", err)
			}
			if err = s.write(ctx, func(_ context.Context, tx *sql.Tx, at time.Time) error { return call(tx, at) }); !errors.Is(err, executionauth.ErrState) {
				t.Fatal("grant consumed twice", err)
			}
		})
	}
	for _, index := range []int{2, 3, 4} {
		err := s.write(ctx, func(_ context.Context, tx *sql.Tx, _ time.Time) error {
			return consumeGrant(ctx, tx, "a", ids[index].JobID, ids[index].AttemptID, "secret", now, workloadsecret.ErrUnavailable)
		})
		if !errors.Is(err, workloadsecret.ErrUnavailable) {
			t.Fatal("revoked/expired secret consumed", index, err)
		}
	}
	for _, query := range []string{
		`UPDATE receipts SET receipt=receipt`,
		`DELETE FROM receipts`,
		`UPDATE attempt_grants SET consumed_at=NULL WHERE consumed_at IS NOT NULL`,
		`UPDATE attempt_grants SET storage_released=0 WHERE kind='prepare' AND storage_released=1`,
		`UPDATE attempt_grants SET ready_revision=NULL,ready_at=NULL WHERE ready_revision IS NOT NULL`,
		`UPDATE attempt_grants SET status='revoked',revoked_at='2026-01-01T00:00:00Z',revocation_key_sha256='` + strings.Repeat("a", 64) + `' WHERE kind='secret' AND consumed_at IS NOT NULL`,
	} {
		if _, err := s.db.Exec(query); err == nil {
			t.Fatal("one-shot evidence changed", query)
		}
	}
	// A receipt cannot point to a grant but borrow another valid attempt's identity.
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		_, _, e := replayOrCreate(ctx, tx, receiptKey{"a", "grant", "execution", strings.Repeat("f", 64), strings.Repeat("f", 64), "", operations.ErrConflict}, &savedReceipt{"grant_migration_execution_0", ids[1].JobID, ids[1].AttemptID, "{}"})
		return e
	})
	if err == nil {
		t.Fatal("receipt accepted another attempt's grant")
	}
}

func TestReceiptGrantMigrationPreservesPreparedStartAndSecretReplay(t *testing.T) {
	for _, state := range []string{"pending", "consumed", "revoked"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			f, id, gate, p, ready, secrets, pool := secretFixture(t, fake.AcceptLoseResponse)
			var prepareRaw string
			if err := f.s.db.QueryRow(`SELECT request FROM attempt_grants WHERE kind='prepare' AND workspace_id='a' AND job_id=?`, id.JobID).Scan(&prepareRaw); err != nil {
				t.Fatal(err)
			}
			secretRaw := secretRequest(f, id, ready)
			secret, err := secrets.Supply(ctx, p, "a", id.JobID, "migration-secret", secretRaw)
			if err != nil {
				t.Fatal(err)
			}
			startRaw := secretStartRequest(f, id, ready, secret.AuthorizationID)
			start, err := gate.Start(ctx, p, "a", id.JobID, "migration-start", startRaw)
			if err != nil {
				t.Fatal(err)
			}
			revokeRaw, _ := json.Marshal(map[string]any{"attempt_id": id.AttemptID})
			var revoked workloadsecret.Receipt
			if state == "revoked" {
				revoked, err = secrets.Revoke(ctx, p, "a", id.JobID, secret.AuthorizationID, "migration-revoke", revokeRaw)
				if err != nil {
					t.Fatal(err)
				}
			}
			if state == "consumed" {
				if _, err = gateStep(t, f); err != nil {
					t.Fatal(err)
				}
			}
			priorCalls := f.backend.Stats().SubmitCalls
			originalReceipts := upgradeRows(t, f.s.db, `SELECT namespace,scope,key_sha256,request_sha256,canonical_version,receipt FROM receipts ORDER BY namespace,scope,key_sha256`)
			f.root = copyFixtureAtSchema(t, f.s, 26)
			pool.Close()
			f.restart(t)
			attachGate(t, f)
			gate, p = authorizationService(t, f, "a", auth.Read, auth.Execute)
			secrets, pool = attachSecrets(t, f, gate)
			if got := upgradeRows(t, f.s.db, `SELECT namespace,scope,key_sha256,request_sha256,canonical_version,receipt FROM receipts ORDER BY namespace,scope,key_sha256`); !reflect.DeepEqual(got, originalReceipts) {
				t.Fatal("migration rewrote actual workflow receipts")
			}
			if replay, e := gate.Prepare(ctx, p, "a", id.JobID, "prepare-key-"+string(id.JobID), []byte(prepareRaw)); e != nil || !replay.Replay || replay.Status != executionauth.Granted {
				t.Fatal("preparation replay changed", e)
			}
			if replay, e := gate.Start(ctx, p, "a", id.JobID, "migration-start", startRaw); e != nil || !replay.Replay || replay.AuthorizationID != start.AuthorizationID || replay.Status != executionauth.Granted {
				t.Fatal("start replay changed", e)
			}
			replay, e := secrets.Supply(ctx, p, "a", id.JobID, "migration-secret", secretRaw)
			if e != nil || !replay.Replay || replay.AuthorizationID != secret.AuthorizationID || replay.Status != "granted" {
				t.Fatal("secret original replay changed", e)
			}
			current, e := secrets.Get(ctx, p, "a", id.JobID, secret.AuthorizationID)
			expected := map[string]string{"pending": "granted", "consumed": "consumed", "revoked": "revoked"}[state]
			if e != nil || current.Status != expected {
				t.Fatal("secret current state lost", current.Status, e)
			}
			if state == "revoked" {
				again, e := secrets.Revoke(ctx, p, "a", id.JobID, secret.AuthorizationID, "migration-revoke", revokeRaw)
				if e != nil || !again.Replay || !again.RevokedAt.Equal(*revoked.RevokedAt) {
					t.Fatal("original revocation lost", e)
				}
			}
			if state != "pending" && pool.Available(secret.Key(), secret.AuthorizationID, f.clock.Now()) {
				t.Fatal("migration replay rearmed material")
			}
			if _, e = gateStep(t, f); e != nil {
				t.Fatal(e)
			}
			expectedCalls := priorCalls
			if state == "pending" {
				expectedCalls++
			}
			if f.backend.Stats().SubmitCalls != expectedCalls {
				t.Fatal("migration repeated or rearmed start")
			}
			assertNoWorkloadValues(t, f.s)
		})
	}
}
