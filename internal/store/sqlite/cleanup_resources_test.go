package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestCleanupPreviewEnumeratesOriginalTargetsWithoutMutation(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, principal := cleanupService(t, f, "a", auth.Read, auth.Operate)
	probe := &cleanupProbe{probeProvider: f.adapter}
	previewService := cleanupPreviewService(t, f).WithPreviewResolver(cleanupResolver{probe})
	before := f.backend.Stats()
	preview, err := previewService.Preview(ctx, principal, "a", id.JobID, id.AttemptID, false)
	if err != nil || !preview.Eligible || len(preview.Resources) != 2 {
		t.Fatal(preview, err)
	}
	j := f.journal(t, id)
	for i, resource := range preview.Resources {
		purpose := []string{"execution", "staging"}[i]
		if resource.Purpose != purpose || resource.ResourceID != domain.ProviderResourceID("res_"+purpose+"_"+string(j.Plan.Job.Identity.IntentID)) ||
			!resource.Eligible || resource.Observation != "present" || resource.Target == nil || resource.Target.Bytes != nil ||
			resource.Target.ID != j.Prepared.Resource || !resource.Fingerprint.Valid() {
			t.Fatal("preview lost exact target or unknown size", resource)
		}
	}
	if len(probe.requests) != 0 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup'") != 0 ||
		f.backend.Stats().SubmitCalls != before.SubmitCalls || f.backend.Stats().Executions != before.Executions {
		t.Fatal("preview created intent or compute")
	}
	raw, _ := json.Marshal(preview)
	for _, private := range []string{"CredentialRef", "AccountScope", "Nonce", "CreationOperationID", "Remote", "Request", "provider_plan"} {
		if strings.Contains(string(raw), private) {
			t.Fatal("preview serialized private ownership evidence", private)
		}
	}
	// Dry-run leaves the same exact target available for an explicit request.
	_, _, receipt := admitCleanup(t, f, id, "after-preview", false)
	worker, _ := cleanupWorker(t, f)
	runCleanup(t, worker)
	current, err := service.Get(ctx, principal, "a", receipt.Operation.ID)
	if err != nil || current.Effect != operations.RemoteDeleted {
		t.Fatal("preview deleted the resource", current, err)
	}
	for i, outcome := range current.Resources {
		if outcome.Fingerprint != preview.Resources[i].Fingerprint || outcome.ResourceID != preview.Resources[i].ResourceID {
			t.Fatal("apply retargeted the preview inventory", outcome)
		}
	}
}

func TestCleanupPreviewRechecksAuthorityAfterReads(t *testing.T) {
	ctx := context.Background()
	for _, change := range []string{"revoke"} {
		t.Run(change, func(t *testing.T) {
			f, id := collectedFixture(t)
			_, principal := cleanupService(t, f, "a", auth.Read, auth.Operate)
			probe := &cleanupProbe{probeProvider: f.adapter}
			changed := false
			probe.resourceHook = func(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
				if request.Mode != provider.CleanupDryRun {
					t.Fatal("preview attempted deletion")
				}
				if !changed {
					changed = true
					access, _ := auth.New(f.s, f.s)
					if err := access.Revoke(ctx, principal.TokenID()); err != nil {
						t.Fatal(err)
					}
				}
				return probe.probeProvider.CleanupResource(ctx, request)
			}
			previewService := cleanupPreviewService(t, f).WithPreviewResolver(cleanupResolver{probe})
			_, err := previewService.Preview(ctx, principal, "a", id.JobID, id.AttemptID, false)
			if !changed || err == nil || change == "hold" && !errors.Is(err, operations.ErrChanged) {
				t.Fatal("stale preview escaped current authority/eligibility", err)
			}
			if len(probe.requests) != 0 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup'") != 0 {
				t.Fatal("preview mutated cleanup state")
			}
		})
	}
}

func TestCleanupLostAcknowledgementRecoversOriginalTargets(t *testing.T) {
	for _, recovery := range []string{"absence", "binding refusal"} {
		t.Run(recovery, func(t *testing.T) {
			f, id := collectedFixture(t)
			_, principal, receipt := admitCleanup(t, f, id, "lost-ack", false)
			worker, probe := cleanupWorker(t, f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			probe.resourceHook = func(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
				outcome, err := probe.probeProvider.CleanupResource(ctx, request)
				if err != nil || !outcome.Deleted {
					t.Fatal("fault must follow a real fake-provider deletion", outcome, err)
				}
				cancel()
				return provider.CleanupOutcome{}, ctx.Err()
			}
			if worked, err := worker.RunOnce(ctx); !worked || !errors.Is(err, context.Canceled) {
				t.Fatal("fault did not interrupt outcome commit", worked, err)
			}
			before, err := f.s.ReadOperation(context.Background(), "a", principal.TokenID(), receipt.Operation.ID)
			if err != nil || before.Operation.Status != domain.OperationRunning || len(before.Resources) != 2 || before.Resources[0].State != operations.ResourceRunning || before.Resources[1].State != operations.ResourcePending {
				t.Fatal("missing durable intent at fault", before, err)
			}
			f.restart(t)
			f.clock.advance(5*time.Minute + 31*time.Second)
			worker, probe = cleanupWorker(t, f)
			if recovery == "binding refusal" {
				probe.verifyErr = errors.New("unavailable binding")
			}
			worked, err := worker.RunOnce(context.Background())
			if err != nil || !worked {
				t.Fatal("restart did not settle cleanup", worked, err)
			}
			after, err := f.s.ReadOperation(context.Background(), "a", principal.TokenID(), receipt.Operation.ID)
			if err != nil || len(after.Resources) != 2 {
				t.Fatal(after, err)
			}
			for i, result := range after.Resources {
				if result.ResourceID != before.Resources[i].ResourceID || result.Fingerprint != before.Resources[i].Fingerprint {
					t.Fatal("recovery changed the original target", before.Resources, after.Resources)
				}
			}
			if recovery == "absence" {
				if after.Operation.Status != domain.OperationSucceeded || after.Effect != operations.AlreadyAbsent ||
					after.Resources[0].State != operations.ResourceAbsent || after.Resources[1].State != operations.ResourceAbsent || len(probe.requests) != 2 {
					t.Fatal("recovery did not prove original absence", after)
				}
			} else if after.Operation.Status != domain.OperationFailed || after.Resources[0].State != operations.ResourceUnknown || after.Resources[1].State != operations.ResourceRetained || len(probe.requests) != 0 {
				t.Fatal("recovery refusal concealed the missing acknowledgement", after, probe.requests)
			}
			assertNoNewCompute(t, f)
		})
	}
}

func TestCleanupPartialOutcomesRemainImmutableAcrossExplicitRecovery(t *testing.T) {
	ctx := context.Background()
	f, id := uncollectedFixture(t)
	service, principal, first := admitCleanup(t, f, id, "partial-request", true)
	worker, probe := cleanupWorker(t, f)
	probe.resourceHook = func(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		if request.Purpose == "staging" {
			return provider.CleanupOutcome{}, errors.New("private-response-canary")
		}
		return probe.probeProvider.CleanupResource(ctx, request)
	}
	runCleanup(t, worker)
	partial, err := service.Get(ctx, principal, "a", first.Operation.ID)
	if err != nil || partial.Operation.Status != domain.OperationFailed || len(partial.Resources) != 2 || partial.Resources[0].State != operations.ResourceDeleted || partial.Resources[1].State != operations.ResourceUnknown {
		t.Fatal("partial outcome collapsed into attempt success", partial, err)
	}
	controls, cp, _ := controlService(t, f, "a", auth.Operate)
	if _, err := controls.Submit(ctx, cp, "a", id.JobID, domain.OperationCollect, "collect-partial", controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrState) {
		t.Fatal("partial cleanup permitted collection", err)
	}
	for _, sql := range []string{"DELETE FROM cleanup_resources", "UPDATE cleanup_resources SET target=target", "UPDATE cleanup_resources SET state='absent',reason='forged'"} {
		if _, err := f.s.db.Exec(sql); err == nil {
			t.Fatal("cleanup history was mutable", sql)
		}
	}
	f.restart(t)
	service, principal = cleanupService(t, f, "a", auth.Read, auth.Operate)
	second, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCleanup, "recover-partial", cleanupBody(id.AttemptID, true, ""))
	if err != nil || second.Operation.ID == first.Operation.ID {
		t.Fatal("explicit recovery was not separately recorded", second, err)
	}
	worker, _ = cleanupWorker(t, f)
	runCleanup(t, worker)
	settled, err := service.Get(ctx, principal, "a", second.Operation.ID)
	if err != nil || settled.Operation.Status != domain.OperationSucceeded || settled.Effect != operations.AlreadyAbsent {
		t.Fatal("original targets did not recover", settled, err)
	}
	old, err := service.Get(ctx, principal, "a", first.Operation.ID)
	if err != nil || !reflect.DeepEqual(old, partial) {
		t.Fatal("recovery rewrote historical evidence", old, partial, err)
	}
	for i := range settled.Resources {
		if settled.Resources[i].Fingerprint != partial.Resources[i].Fingerprint {
			t.Fatal("explicit recovery retargeted", settled.Resources, partial.Resources)
		}
	}
	assertNoNewCompute(t, f)
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

func TestCleanupProvenRejectedSubmissionKeepsStagingCleanable(t *testing.T) {
	fakeScenario := fake.DefaultScenario()
	fakeScenario.Mode = fake.Reject
	f := newDispatchFixture(t, fakeScenario)
	id := f.seed(t, "a", 1, false)
	for i := 0; f.journal(t, id).Phase != dispatch.Rejected; i++ {
		if i >= 4 {
			t.Fatal("submission was not rejected")
		}
		if err := f.step(t); err != nil {
			t.Fatal(err)
		}
	}
	service, principal, receipt := admitCleanup(t, f, id, "rejected-staging", false)
	worker, probe := cleanupWorker(t, f)
	runCleanup(t, worker)
	current, err := service.Get(context.Background(), principal, "a", receipt.Operation.ID)
	if err != nil || current.Operation.Status != domain.OperationSucceeded || len(current.Resources) != 1 ||
		current.Resources[0].Purpose != "staging" || current.Resources[0].State != operations.ResourceDeleted || len(probe.requests) != 1 {
		t.Fatal("proven non-creation prevented exact staging cleanup", current, err)
	}
	if f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 0 {
		t.Fatal("cleanup retried a rejected submission")
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

func TestCleanupProviderTimeoutPersistsUnknownWithoutStoppingWorker(t *testing.T) {
	f, id := collectedFixture(t)
	service, principal, receipt := admitCleanup(t, f, id, "cleanup-timeout", false)
	probe := &cleanupProbe{probeProvider: f.adapter}
	probe.resourceHook = func(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		<-ctx.Done()
		return provider.CleanupOutcome{}, ctx.Err()
	}
	config := cleanup.DefaultConfig()
	config.Timeout = time.Second
	worker, err := cleanup.New(f.s, cleanupResolver{probe}, config)
	if err != nil {
		t.Fatal(err)
	}
	runCleanup(t, worker)
	current, err := service.Get(context.Background(), principal, "a", receipt.Operation.ID)
	if err != nil || current.Operation.Status != domain.OperationFailed || len(current.Resources) != 2 ||
		current.Resources[0].State != operations.ResourceUnknown || current.Resources[1].State != operations.ResourceRetained || len(probe.requests) != 1 {
		t.Fatal("provider timeout lost durable uncertainty", current, err)
	}
	if worked, err := worker.RunOnce(context.Background()); worked || err != nil {
		t.Fatal("timed-out cleanup repeated automatically", worked, err)
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

func TestCleanupPreviewAndApplyKeepFrozenTargets(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, principal := cleanupService(t, f, "a", auth.Read, auth.Operate)
	probe := &cleanupProbe{probeProvider: f.adapter}
	previewService := cleanupPreviewService(t, f).WithPreviewResolver(cleanupResolver{probe})
	preview, err := previewService.Preview(ctx, principal, "a", id.JobID, id.AttemptID, false)
	if err != nil || !preview.Eligible || len(preview.Resources) != 2 || preview.Resources[0].Observation != "present" {
		t.Fatal(preview, err)
	}
	raw, _ := json.Marshal(preview)
	for _, field := range []string{`"name_based_delete"`, `"warning"`} {
		if strings.Contains(string(raw), field) {
			t.Fatal("removed preview field remains", string(raw))
		}
	}
	receipt, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCleanup, "exact-cleanup", cleanupBody(id.AttemptID, false, ""))
	if err != nil {
		t.Fatal(err)
	}
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
	f.restart(t)
	probe = &cleanupProbe{probeProvider: f.adapter}
	calls := 0
	probe.resourceHook = func(ctx context.Context, r provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		if r.RecoverOnly || r.LedgerID != preview.Resources[calls].ResourceID || preview.Resources[calls].Purpose != r.Purpose {
			t.Fatal("cleanup changed its frozen target/order", r)
		}
		calls++
		return probe.probeProvider.CleanupResource(ctx, r)
	}
	worker, err := cleanup.New(f.s, cleanupResolver{probe}, cleanup.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	runCleanup(t, worker)
	current, err := f.s.ReadOperation(ctx, "a", principal.TokenID(), receipt.Operation.ID)
	if err != nil || calls != 2 || current.Effect != operations.RemoteDeleted {
		t.Fatal(current, calls, err)
	}
	for i, r := range current.Resources {
		if r.Fingerprint != preview.Resources[i].Fingerprint {
			t.Fatal("cleanup changed original fingerprint", r)
		}
	}
}

func TestCleanupRecoveryOnlyReadsOriginalIntent(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, principal := cleanupService(t, f, "a", auth.Read, auth.Operate)
	body, _ := json.Marshal(operations.Request{AttemptID: id.AttemptID})
	receipt, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCleanup, "name-recovery", body)
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
	if err := f.s.BeginCleanupResource(ctx, *first, first.Resources[0]); err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	f.clock.advance(2 * time.Second)
	calls := 0
	probe := &cleanupProbe{probeProvider: f.adapter}
	probe.resourceHook = func(_ context.Context, r provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		calls++
		if !r.RecoverOnly || r.LedgerID != first.Resources[0].Result.ResourceID {
			t.Fatal("recovery could reapply or retarget", r)
		}
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeProviderStateUnknown, domain.FailureStageObservation, "earlier deletion remains unknown")
	}
	worker, err := cleanup.New(f.s, cleanupResolver{probe}, cleanup.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	runCleanup(t, worker)
	current, err := f.s.ReadOperation(ctx, "a", principal.TokenID(), receipt.Operation.ID)
	if err != nil || calls != 1 || current.Resources[0].State != operations.ResourceUnknown || current.Resources[1].State != operations.ResourceRetained {
		t.Fatal(current, calls, err)
	}
	if worked, err := worker.RunOnce(ctx); worked || err != nil {
		t.Fatal("unknown deletion repeated", worked, err)
	}
}
