package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestExplicitRetirementPreservesEvidenceAndNeverClaimsProviderWork(t *testing.T) {
	for _, phase := range []string{"queued", "prepared", "submission_unknown", "collecting", "succeeded", "cleanup_running"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			var f *dispatchFixture
			var id scheduler.Identity
			if phase == "collecting" || phase == "succeeded" || phase == "cleanup_running" {
				var p *collectionProvider
				f, id, p, results := collectionFixture(t)
				if phase == "succeeded" || phase == "cleanup_running" {
					if worked, err := collectionEngine(t, f, p, results, f.s).RunOnce(ctx); err != nil || !worked {
						t.Fatal("collect fixture", worked, err)
					}
					service, actor := cleanupService(t, f, "a", auth.Read, auth.Operate)
					if _, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCleanup, "legacy-pending-cleanup", cleanupBody(id.AttemptID, false, "")); err != nil {
						t.Fatal("source cleanup fixture", err)
					}
					if phase == "cleanup_running" {
						work, err := f.s.ClaimCleanup(ctx, time.Minute)
						if err != nil || work == nil || len(work.Resources) == 0 {
							t.Fatal("claim legacy cleanup", work, err)
						}
						if err = f.s.BeginCleanupResource(ctx, *work, work.Resources[0]); err != nil {
							t.Fatal("legacy deletion intent", err)
						}
					}
				} else {
					if work, err := f.s.ClaimCollection(ctx, time.Minute, 1); err != nil || work == nil {
						t.Fatal("pending collection fixture", work, err)
					}
				}
				testExplicitRetirement(t, f, id)
				return
			}
			scenario := fake.DefaultScenario()
			if phase == "submission_unknown" {
				scenario.Mode = fake.AcceptLoseResponse
			}
			f = newDispatchFixture(t, scenario)
			id = f.seed(t, "a", 1, false)
			if phase != "queued" {
				if err := f.step(t); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "submission_unknown" {
				if err := f.step(t); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "submission_unknown" {
				controls, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
				if _, err := controls.Submit(ctx, actor, "a", id.JobID, domain.OperationReconcile, "legacy-reconcile", controlRequest(id.AttemptID)); err != nil {
					t.Fatal("legacy pending reconcile", err)
				}
			}
			testExplicitRetirement(t, f, id)
		})
	}
}

func testExplicitRetirement(t *testing.T, f *dispatchFixture, id scheduler.Identity) {
	t.Helper()
	ctx := context.Background()
	root := copyFixtureAtSchema(t, f.s, 23)
	legacy, err := connect(filepath.Join(root, databaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct a pre-retirement explicit revision without altering a released
	// fixture or migration. Explicit mode had no durable per-attempt grant.
	var trigger string
	if err = legacy.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='immutable_profile_revision'`).Scan(&trigger); err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.Exec(`DROP TRIGGER immutable_profile_revision`); err != nil {
		t.Fatal(err)
	}
	profile := f.profile
	profile.CredentialRef = "env:LEGACY_FIXTURE"
	raw, _ := json.Marshal(profile)
	if _, err = legacy.Exec(`UPDATE profile_revisions SET snapshot=? WHERE profile=? AND revision=?`, string(raw), profile.Binding.Profile, profile.Binding.ConfigurationRevision); err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.Exec(trigger); err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.Exec(`DELETE FROM execution_authorizations`); err != nil {
		t.Fatal(err)
	}
	var priorBarrier bool
	if err = legacy.QueryRow(`SELECT dispatch_barrier FROM scheduler_queue WHERE workspace_id=? AND job_id=? AND attempt_id=?`, id.WorkspaceID, id.JobID, id.AttemptID).Scan(&priorBarrier); err != nil {
		t.Fatal(err)
	}
	prior, nonce, err := loadAttempt(ctx, legacy, id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	queries := []string{
		"SELECT * FROM jobs ORDER BY job_id",
		"SELECT * FROM profile_revisions ORDER BY profile,revision",
		"SELECT * FROM idempotency ORDER BY workspace_id,operation,key_sha256",
		"SELECT * FROM job_objects ORDER BY workspace_id,job_id,role",
		"SELECT * FROM dispatch_journals ORDER BY queue_seq",
		"SELECT * FROM submission_intents ORDER BY queue_seq",
		"SELECT * FROM provider_resources ORDER BY resource_id",
		"SELECT operation_seq,workspace_id,operation_id,job_id,attempt_id,kind,created_at,request_sha256,request_json,new_attempt_id,cancel_started FROM operations WHERE kind!='cleanup' ORDER BY operation_seq",
		"SELECT * FROM operation_idempotency ORDER BY workspace_id,kind,key_sha256",
		"SELECT * FROM collection_snapshots ORDER BY workspace_id,job_id,attempt_id",
		"SELECT * FROM collection_publications ORDER BY workspace_id,job_id,attempt_id",
		"SELECT * FROM artifacts ORDER BY workspace_id,job_id,attempt_id,artifact_id",
		"SELECT workspace_id,operation_id,job_id,attempt_id,resource_id,created_at,request_sha256,request_json FROM cleanup_operations ORDER BY cleanup_seq",
		"SELECT * FROM cleanup_idempotency ORDER BY workspace_id,key_sha256",
		"SELECT workspace_id,operation_id,resource_id,ordinal,target,target_sha256 FROM cleanup_resources ORDER BY workspace_id,operation_id,ordinal",
	}
	resourceStates := upgradeRows(t, legacy, `SELECT workspace_id,operation_id,resource_id,state FROM cleanup_resources ORDER BY ordinal`)
	before := make([][][]any, len(queries))
	for i, query := range queries {
		before[i] = upgradeRows(t, legacy, query)
	}
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := Open(ctx, root, DefaultOptions())
	if err != nil {
		t.Fatalf("open retired fixture: %v", err)
	}
	defer upgraded.Close()
	after, afterNonce, err := loadAttempt(ctx, upgraded.db, id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	expected := prior
	if !prior.State.Orchestration.Terminal() {
		expected.State.Orchestration = domain.OrchestrationNeedsAttention
		if prior.State.Execution == domain.ExecutionNotSubmitted && prior.State.RemoteActivity == domain.RemoteActivityNotStarted {
			if !priorBarrier {
				expected.State.Orchestration = domain.OrchestrationFailed
			}
		}
		expected.Revision++
	}
	if after != expected || afterNonce != nonce {
		t.Fatal("migration rewrote remote facts or attempt identity", after, expected)
	}
	for i, query := range queries {
		afterQuery := query
		switch {
		case strings.Contains(query, "FROM idempotency"):
			afterQuery = "SELECT workspace_id,scope,key_sha256,request_sha256,canonical_version,job_id,attempt_id,receipt FROM receipts WHERE namespace='admission' ORDER BY workspace_id,scope,key_sha256"
		case strings.Contains(query, "FROM operation_idempotency"):
			afterQuery = "SELECT workspace_id,scope,key_sha256,request_sha256,canonical_version,subject_id,receipt FROM receipts WHERE namespace='operation' AND scope!='cleanup' ORDER BY workspace_id,scope,key_sha256"
		case strings.Contains(query, "FROM cleanup_operations"):
			afterQuery = strings.ReplaceAll(query, "FROM cleanup_operations ORDER BY cleanup_seq", "FROM operations WHERE kind='cleanup' ORDER BY operation_seq")
		case strings.Contains(query, "FROM cleanup_idempotency"):
			afterQuery = "SELECT workspace_id,key_sha256,request_sha256,canonical_version,subject_id,receipt FROM receipts WHERE namespace='operation' AND scope='cleanup' ORDER BY workspace_id,key_sha256"
		}
		if got := upgradeRows(t, upgraded.db, afterQuery); !reflect.DeepEqual(got, before[i]) {
			t.Fatal("migration changed frozen evidence", query)
		}
	}
	for _, row := range resourceStates {
		var state, reason string
		if err = upgraded.db.QueryRow(`SELECT state,reason FROM cleanup_resources WHERE workspace_id=? AND operation_id=? AND resource_id=?`, row[0], row[1], row[2]).Scan(&state, &reason); err != nil {
			t.Fatal(err)
		}
		expected := row[3].(string)
		if expected == "pending" {
			expected = "retained"
		}
		if expected == "running" {
			expected = "unknown"
			if reason != "prior_delete_outcome_unknown" {
				t.Fatal("started deletion lost uncertainty", reason)
			}
		}
		if state != expected {
			t.Fatal("retired deletion manufactured an outcome", state, expected)
		}
	}
	f.clock.advance(time.Hour)
	if err = upgraded.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
		t.Fatal(err)
	}
	if claim, err := upgraded.ClaimNext(ctx, "upgrade-worker"); err != nil || claim.Claim != nil {
		t.Fatal("retired dispatch claimed", claim, err)
	}
	if claim, err := upgraded.ClaimRecovery(ctx, "upgrade-worker"); err != nil || claim != nil {
		t.Fatal("retired recovery claimed", claim, err)
	}
	if claim, err := upgraded.ClaimCollection(ctx, time.Minute, 1); err != nil || claim != nil {
		t.Fatal("retired collection claimed", claim, err)
	}
	if claim, err := upgraded.ClaimCleanup(ctx, time.Minute); err != nil || claim != nil {
		t.Fatal("retired cleanup claimed", claim, err)
	}
	for _, table := range []string{"operations"} {
		var pending int
		if err = upgraded.db.QueryRow("SELECT count(*) FROM " + table + " WHERE status IN ('accepted','running')").Scan(&pending); err != nil || pending != 0 {
			t.Fatal("retired operations remain pending", table, pending, err)
		}
		rows, err := upgraded.db.Query("SELECT record FROM " + table + " WHERE status='failed'")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var raw string
			if err = rows.Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var record struct{ Operation domain.Operation }
			if json.Unmarshal([]byte(raw), &record) != nil || record.Operation.Validate() != nil || record.Operation.Failure == nil || record.Operation.Failure.Details[domain.ReasonDetail] != "explicit_mode_retired" {
				t.Fatal("invalid retired operation outcome")
			}
		}
		rows.Close()
	}
	var enabled bool
	if err = upgraded.db.QueryRow(`SELECT enabled FROM profiles WHERE profile=?`, profile.Binding.Profile).Scan(&enabled); err != nil || enabled {
		t.Fatal("explicit alias remains eligible", enabled, err)
	}
	access, err := auth.New(upgraded, upgraded)
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := access.Issue(ctx, id.WorkspaceID, []auth.Scope{auth.Read, auth.Operate}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	actor, err := access.Authenticate(ctx, token.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	controls, err := operations.New(access, upgraded, f.blobs, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []domain.OperationKind{domain.OperationCancel, domain.OperationReconcile, domain.OperationCollect} {
		if _, err = controls.Submit(ctx, actor, id.WorkspaceID, id.JobID, kind, "retired-new-"+string(kind), controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrState) {
			t.Fatal("retired control admitted", kind, err)
		}
	}
	cleanups, err := operations.New(access, upgraded, nil, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cleanups.Submit(ctx, actor, id.WorkspaceID, id.JobID, domain.OperationCleanup, "retired-new-cleanup", cleanupBody(id.AttemptID, false, "")); !errors.Is(err, operations.ErrState) {
		t.Fatal("retired cleanup admitted", err)
	}
	if record, err := upgraded.ReadJob(ctx, id.WorkspaceID, actor.TokenID(), id.JobID); err != nil || record.Attempt != after {
		t.Fatal("legacy record is not readable", err)
	}
}
