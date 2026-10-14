package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestCleanupPartialOutcomeRowsAreImmutable(t *testing.T) {
	f, id := uncollectedFixture(t)
	admitCleanup(t, f, id, "partial-request", true)
	worker, probe := cleanupWorker(t, f)
	probe.resourceHook = func(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		if q.Purpose == "staging" {
			return provider.CleanupOutcome{}, errors.New("synthetic staging refusal")
		}
		return probe.probeProvider.CleanupResource(ctx, q)
	}
	runCleanup(t, worker)
	if controlCount(t, f.s, "SELECT count(*) FROM cleanup_resources WHERE state='deleted'") != 1 || controlCount(t, f.s, "SELECT count(*) FROM cleanup_resources WHERE state='unknown'") != 1 {
		t.Fatal("partial SQL outcome fixture lost distinct evidence")
	}
	for _, sql := range []string{"DELETE FROM cleanup_resources", "UPDATE cleanup_resources SET target=target", "UPDATE cleanup_resources SET state='absent',reason='forged'"} {
		if _, err := f.s.db.Exec(sql); err == nil {
			t.Fatal("cleanup history was mutable", sql)
		}
	}
}

func TestCleanupSharedReferencesAcrossBindingAliasesAreRetained(t *testing.T) {
	ctx := context.Background()
	for _, purpose := range []string{"staging", "execution"} {
		t.Run(purpose, func(t *testing.T) {
			f, id := collectedFixture(t)
			other := f.seed(t, "b", 2, false)
			// A second persisted owner references the same underlying resource through
			// another binding alias. The attempt being cleaned must preserve it.
			_, err := f.s.db.Exec(`INSERT INTO provider_resources
 SELECT 'res_shared','b',?,?,'other_alias',account_scope,purpose,'op_shared',resource_key,identity,plan_sha256,resource_ref,readiness,cleanup_state,created_at
 FROM provider_resources WHERE workspace_id='a' AND purpose=?`, string(other.JobID), string(other.AttemptID), purpose)
			if err != nil {
				t.Fatal(err)
			}
			service, principal := cleanupService(t, f, "a", auth.Operate)
			preview, err := cleanupPreviewService(t, f).WithPreviewResolver(cleanupResolver{f.adapter}).Preview(ctx, principal, "a", id.JobID, id.AttemptID, false)
			if err != nil || preview.Eligible || preview.Reason != "shared_resource" || len(preview.Resources) != 2 {
				t.Fatal("shared physical resource was eligible", preview, err)
			}
			for _, resource := range preview.Resources {
				if resource.Target == nil || resource.Target.ID != f.journal(t, id).Prepared.Resource || resource.Eligible {
					t.Fatal("excluded target was hidden or eligible", resource)
				}
			}
			if _, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCleanup, "shared-cleanup", cleanupBody(id.AttemptID, false, "")); !errors.Is(err, operations.ErrUnresolved) {
				t.Fatal("shared target admitted", err)
			}
			if f.backend.Stats().CleanupCalls != 0 {
				t.Fatal("shared target reached provider")
			}
		})
	}
}

func TestCleanupResourceFencesAndAggregateEvidence(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	admitCleanup(t, f, id, "resource-fence", false)
	first, err := f.s.ClaimCleanup(ctx, time.Second)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	if err := f.s.BeginCleanupResource(ctx, *first, first.Resources[1]); !errors.Is(err, operations.ErrUnresolved) {
		t.Fatal("staging deletion preceded unresolved execution", err)
	}
	if err := f.s.CompleteCleanup(ctx, *first, cleanup.Outcome{Effect: operations.RemoteDeleted}); !errors.Is(err, ErrInvalid) {
		t.Fatal("aggregate succeeded without resource outcomes", err)
	}
	result := first.Resources[0].Result
	result.State, result.Reason = operations.ResourceDeleted, "deletion_confirmed"
	if err := f.s.CompleteCleanupResource(ctx, *first, result); !errors.Is(err, operations.ErrChanged) {
		t.Fatal("deletion confirmed without resource intent", err)
	}
	if err := f.s.BeginCleanupResource(ctx, *first, first.Resources[0]); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(2 * time.Second)
	second, err := f.s.ClaimCleanup(ctx, time.Minute)
	if err != nil || second == nil {
		t.Fatal(err)
	}
	if err := f.s.BeginCleanupResource(ctx, *first, first.Resources[0]); !errors.Is(err, cleanup.ErrLeaseLost) {
		t.Fatal("stale worker obtained mutation intent", err)
	}
	if err := f.s.CompleteCleanupResource(ctx, *first, result); !errors.Is(err, cleanup.ErrLeaseLost) {
		t.Fatal("stale worker recorded outcome", err)
	}
	finishCleanupResourceFixtures(t, f, *second)
	if err := f.s.CompleteCleanup(ctx, *second, cleanup.Outcome{Effect: operations.AlreadyAbsent}); !errors.Is(err, ErrInvalid) {
		t.Fatal("aggregate disagreed with resource evidence", err)
	}
	if err := f.s.CompleteCleanup(ctx, *second, cleanup.Outcome{Effect: operations.RemoteDeleted}); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupSchemaTwentyUpgradePinsOlderPendingTargets(t *testing.T) {
	ctx := context.Background()
	for _, running := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "running"}[running], func(t *testing.T) {
			f, id := collectedFixture(t)
			_, principal, receipt := admitCleanup(t, f, id, "before-upgrade", false)
			if running {
				if work, err := f.s.ClaimCleanup(ctx, time.Second); err != nil || work == nil {
					t.Fatal(err)
				}
			}
			before, err := f.s.ReadOperation(ctx, "a", principal.TokenID(), receipt.Operation.ID)
			if err != nil || len(before.Resources) != 2 {
				t.Fatal(before, err)
			}
			f.root = copyFixtureAtSchema(t, f.s, 20)
			f.restart(t)
			upgraded, err := f.s.ReadOperation(ctx, "a", principal.TokenID(), receipt.Operation.ID)
			if err != nil || upgraded.Operation != before.Operation || len(upgraded.Resources) != 0 || f.backend.Stats().CleanupCalls != 0 {
				t.Fatal("migration changed receipt or invoked provider", upgraded, err)
			}
			f.clock.advance(2 * time.Second)
			worker, _ := cleanupWorker(t, f)
			runCleanup(t, worker)
			after, err := f.s.ReadOperation(ctx, "a", principal.TokenID(), receipt.Operation.ID)
			if err != nil || len(after.Resources) != 2 {
				t.Fatal("older request lost resource outcomes", after, err)
			}
			if running {
				if after.Operation.Status != domain.OperationFailed || after.Operation.Failure == nil || after.Operation.Failure.Code != domain.CodeProviderStateUnknown {
					t.Fatal("old running deletion lost uncertainty", after)
				}
				for _, resource := range after.Resources {
					if resource.State != operations.ResourceUnknown {
						t.Fatal("old running deletion acquired invented proof", resource)
					}
				}
				plan, err := f.s.PlanCleanup(ctx, "a", principal.TokenID(), id.JobID, id.AttemptID, false)
				if err != nil {
					t.Fatal(err)
				}
				for _, resource := range plan.Resources {
					request := resource.Request
					request.Mode = provider.CleanupDryRun
					out, err := f.adapter.CleanupResource(ctx, request)
					if err != nil || !out.WouldDelete {
						t.Fatal("recovery deleted retained resource", out, err)
					}
				}
			} else if after.Operation.Status != domain.OperationSucceeded {
				t.Fatal("old accepted deletion did not finish", after)
			}
			for i := range after.Resources {
				if after.Resources[i].Fingerprint != before.Resources[i].Fingerprint || after.Resources[i].ResourceID != before.Resources[i].ResourceID {
					t.Fatal("upgrade changed original deletion targets", after.Resources, before.Resources)
				}
			}
		})
	}
}

func TestCleanupLegacyUnknownEffectsBlockCollectionAfterUpgrade(t *testing.T) {
	ctx := context.Background()
	for _, phase := range []string{"running_binding_refused", "already_failed_unknown"} {
		t.Run(phase, func(t *testing.T) {
			f, id := uncollectedFixture(t)
			_, principal, receipt := admitCleanup(t, f, id, "legacy-uncertainty", true)
			if phase == "already_failed_unknown" {
				worker, probe := cleanupWorker(t, f)
				probe.err = errors.New("lost legacy deletion acknowledgement")
				runCleanup(t, worker)
			} else if work, err := f.s.ClaimCleanup(ctx, time.Second); err != nil || work == nil {
				t.Fatal(err)
			}
			// Released schema 20 has only the attempt-wide running/failed receipt;
			// it cannot say which resources the old callback may already have deleted.
			f.root = copyFixtureAtSchema(t, f.s, 20)
			f.restart(t)
			f.clock.advance(2 * time.Second)
			worker, probe := cleanupWorker(t, f)
			probe.verifyErr = errors.New("legacy binding unavailable")
			if _, err := worker.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			current, err := f.s.ReadOperation(ctx, "a", principal.TokenID(), receipt.Operation.ID)
			if err != nil || current.Operation.Status != domain.OperationFailed || current.Operation.Failure == nil || current.Operation.Failure.Code != domain.CodeProviderStateUnknown {
				t.Fatal("upgrade turned an unacknowledged call into a known refusal", current, err)
			}
			if phase == "running_binding_refused" {
				if len(current.Resources) != 2 || current.Resources[0].State != operations.ResourceUnknown || current.Resources[1].State != operations.ResourceUnknown {
					t.Fatal("legacy attempt-wide call lost its possible effects", current.Resources)
				}
			}
			controls, cp, _ := controlService(t, f, "a", auth.Operate)
			if _, err := controls.Submit(ctx, cp, "a", id.JobID, domain.OperationCollect, "legacy-collect", controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrState) {
				t.Fatal("unresolved legacy deletion permitted collection", err)
			}
			if len(probe.requests) != 0 || f.backend.Stats().CleanupCalls != 0 {
				t.Fatal("upgrade changed remote data")
			}
		})
	}
}

func TestCleanupTargetEncodingPreservesReleasedFingerprint(t *testing.T) {
	f, id := collectedFixture(t)
	_, _, receipt := admitCleanup(t, f, id, "exact-cleanup", false)
	// The retired runtime flag was false/omitted in immutable snapshots. Its
	// removal must preserve the pre-removal request encoding and fingerprint.
	rows, err := f.s.db.Query(`SELECT target,target_sha256 FROM cleanup_resources WHERE operation_id=? ORDER BY ordinal`, receipt.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var target, hash string
		if err = rows.Scan(&target, &hash); err != nil {
			t.Fatal(err)
		}
		var resource cleanup.Resource
		if err = json.Unmarshal([]byte(target), &resource); err != nil {
			t.Fatal(err)
		}
		q := resource.Request
		legacy := struct {
			provider.CleanupRequest
			Purpose              string
			OwnershipOperationID domain.OperationID
			PlanSHA256           domain.SHA256Digest
			RetiredAcceptance    bool `json:"AcceptNameBasedDelete,omitempty"`
			RecoverOnly          bool `json:",omitempty"`
		}{q.CleanupRequest, q.Purpose, q.OwnershipOperationID, q.PlanSHA256, false, false}
		original, _ := json.Marshal(legacy)
		current, _ := json.Marshal(q)
		if string(original) != string(current) || string(provider.Digest(original)) != hash {
			t.Fatal("retired option changed the original target fingerprint", target)
		}
		count++
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if count != 2 {
		t.Fatal("missing original snapshots", count)
	}
}

func TestCleanupResourceIntentRejectsChangedPlan(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, principal := cleanupService(t, f, "a", auth.Read, auth.Operate)
	body, _ := json.Marshal(operations.Request{AttemptID: id.AttemptID})
	_, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCleanup, "name-recovery", body)
	if err != nil {
		t.Fatal(err)
	}
	first, err := f.s.ClaimCleanup(ctx, time.Second)
	if err != nil || first == nil {
		t.Fatal(first, err)
	}
	changed := first.Resources[0]
	changed.Resource.Request.PlanSHA256 = provider.Digest([]byte("changed original plan"))
	changed.Result.Fingerprint = changed.Resource.Fingerprint()
	if err := f.s.BeginCleanupResource(ctx, *first, changed); !errors.Is(err, operations.ErrChanged) {
		t.Fatal("worker changed durable target", err)
	}
}

func TestRevokedCleanupPreviewDoesNotCreateOperationRows(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	_, actor := cleanupService(t, f, "a", auth.Read, auth.Operate)
	probe := &cleanupProbe{probeProvider: f.adapter}
	revoked := false
	probe.resourceHook = func(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		if !revoked {
			revoked = true
			if err := f.s.RevokeToken(ctx, actor.TokenID()); err != nil {
				return provider.CleanupOutcome{}, err
			}
		}
		return probe.probeProvider.CleanupResource(ctx, q)
	}
	if _, err := cleanupPreviewService(t, f).WithPreviewResolver(cleanupResolver{probe}).Preview(ctx, actor, "a", id.JobID, id.AttemptID, false); err == nil || !revoked {
		t.Fatal("preview did not revalidate revoked authority", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup'") != 0 || controlCount(t, f.s, "SELECT count(*) FROM cleanup_resources") != 0 {
		t.Fatal("revoked preview created cleanup rows")
	}
}
