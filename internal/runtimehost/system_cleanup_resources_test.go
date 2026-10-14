package runtimehost

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func (f *managedTestFixture) cleanupPreview(t *testing.T) *cleanup.Service {
	t.Helper()
	s, err := cleanup.NewService(f.host.access, f.host.store, f.fixtureClock())
	if err != nil {
		t.Fatal(err)
	}
	return s.WithPreviewResolver(f.services.resolver)
}

func TestCleanupPreviewEnumeratesOriginalTargetsWithoutMutation(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-preview")
	p := f.cleanupProbe(t, "")
	plan, err := f.host.store.PlanCleanup(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID, false)
	if err != nil {
		t.Fatal(err)
	}
	before := f.backend.Stats()
	preview, err := f.cleanupPreview(t).Preview(ctx, f.actor, "app", job.JobID, job.AttemptID, false)
	if err != nil || !preview.Eligible || len(preview.Resources) != 2 {
		t.Fatal(preview, err)
	}
	for i, r := range preview.Resources {
		q := plan.Resources[i].Request
		if r.Purpose != []string{"execution", "staging"}[i] || r.ResourceID != q.LedgerID || !r.Eligible || r.Observation != "present" || r.Target == nil || r.Target.Bytes != nil || r.Target.ID != q.Remote.Resource || !r.Fingerprint.Valid() {
			t.Fatal("preview changed target or unknown size", r)
		}
	}
	if len(p.requests) != 0 || f.backend.Stats().SubmitCalls != before.SubmitCalls || f.backend.Stats().Executions != before.Executions {
		t.Fatal("preview mutated provider")
	}
	raw := string(systemJSON(t, preview))
	for _, private := range []string{"CredentialRef", "AccountScope", "Nonce", "CreationOperationID", "Remote", "Request", "provider_plan"} {
		if strings.Contains(raw, private) {
			t.Fatal("private evidence exposed", private)
		}
	}
	r := f.admitCleanup(t, job, "after-preview", false)
	f.cleanupProbe(t, r.Operation.ID)
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	current := f.cleanupRecord(t, r.Operation.ID)
	if current.Effect != operations.RemoteDeleted {
		t.Fatal("preview deleted target", current)
	}
	for i, out := range current.Resources {
		if out.Fingerprint != preview.Resources[i].Fingerprint || out.ResourceID != preview.Resources[i].ResourceID {
			t.Fatal("apply retargeted preview", out)
		}
	}
	f.assertSingleCompute(t, job)
}

func TestCleanupPreviewRechecksAuthorityAfterReads(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "preview-revoke")
	p := f.cleanupProbe(t, "")
	changed := false
	p.resourceHook = func(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		if q.Mode != provider.CleanupDryRun {
			t.Fatal("preview attempted deletion")
		}
		if !changed {
			changed = true
			if err := f.host.access.Revoke(ctx, f.actor.TokenID()); err != nil {
				t.Fatal(err)
			}
		}
		return p.Provider.CleanupResource(ctx, q)
	}
	if _, err := f.cleanupPreview(t).Preview(ctx, f.actor, "app", job.JobID, job.AttemptID, false); !changed || err == nil {
		t.Fatal("stale authority returned preview", err)
	}
	if len(p.requests) != 0 {
		t.Fatal("preview applied mutation")
	}
}

func TestCleanupLostAcknowledgementRecoversOriginalTargets(t *testing.T) {
	for _, recovery := range []string{"absence", "binding refusal"} {
		t.Run(recovery, func(t *testing.T) {
			f := newSystemFixture(t, fake.DefaultScenario())
			job := f.collectedForCleanup(t, "cleanup-lost-ack")
			r := f.admitCleanup(t, job, "lost-ack", false)
			p := f.cleanupProbe(t, r.Operation.ID)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p.resourceHook = func(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
				out, err := p.Provider.CleanupResource(ctx, q)
				if err != nil || !out.Deleted {
					t.Fatal("fault must follow real deletion", out, err)
				}
				cancel()
				return provider.CleanupOutcome{}, ctx.Err()
			}
			if worked, err := f.cleanupEngine(t, nil).RunOnce(ctx); !worked || !errors.Is(err, context.Canceled) {
				t.Fatal("outcome commit not interrupted", worked, err)
			}
			before := f.cleanupRecord(t, r.Operation.ID)
			if before.Operation.Status != domain.OperationRunning || len(before.Resources) != 2 || before.Resources[0].State != operations.ResourceRunning || before.Resources[1].State != operations.ResourcePending {
				t.Fatal("durable intent missing", before)
			}
			f.reopen(t)
			f.advance(t, 5*time.Minute+31*time.Second)
			p = f.cleanupProbe(t, r.Operation.ID)
			if recovery == "binding refusal" {
				p.verifyErr = errors.New("unavailable binding")
			}
			runSystemCleanup(t, f.cleanupEngine(t, nil))
			after := f.cleanupRecord(t, r.Operation.ID)
			for i, result := range after.Resources {
				if result.ResourceID != before.Resources[i].ResourceID || result.Fingerprint != before.Resources[i].Fingerprint {
					t.Fatal("recovery retargeted", before, after)
				}
			}
			if recovery == "absence" {
				if after.Operation.Status != domain.OperationSucceeded || after.Effect != operations.AlreadyAbsent || after.Resources[0].State != operations.ResourceAbsent || after.Resources[1].State != operations.ResourceAbsent || len(p.requests) != 2 {
					t.Fatal(after, p.requests)
				}
			} else if after.Operation.Status != domain.OperationFailed || after.Resources[0].State != operations.ResourceUnknown || after.Resources[1].State != operations.ResourceRetained || len(p.requests) != 0 {
				t.Fatal("refusal concealed lost ACK", after, p.requests)
			}
			f.assertSingleCompute(t, job)
		})
	}
}

func TestCleanupPartialOutcomesRemainImmutableAcrossExplicitRecovery(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.uncollectedForCleanup(t, "partial-cleanup")
	first := f.admitCleanup(t, job, "partial-first", true)
	p := f.cleanupProbe(t, first.Operation.ID)
	p.resourceHook = func(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		if q.Purpose == "staging" {
			return provider.CleanupOutcome{}, errors.New("synthetic staging refusal")
		}
		return p.Provider.CleanupResource(ctx, q)
	}
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	partial := f.cleanupRecord(t, first.Operation.ID)
	if partial.Operation.Status != domain.OperationFailed || partial.Resources[0].State != operations.ResourceDeleted || partial.Resources[1].State != operations.ResourceUnknown {
		t.Fatal(partial)
	}
	if _, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "collect-partial", systemControlBody(t, job.AttemptID)); !errors.Is(err, operations.ErrState) {
		t.Fatal("unknown deletion allowed collection", err)
	}
	f.reopen(t)
	second := f.admitCleanup(t, job, "partial-explicit-recovery", true)
	if second.Operation.ID == first.Operation.ID {
		t.Fatal("new request reused failed receipt")
	}
	f.cleanupProbe(t, second.Operation.ID)
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	settled := f.cleanupRecord(t, second.Operation.ID)
	if settled.Operation.Status != domain.OperationSucceeded || settled.Effect != operations.AlreadyAbsent {
		t.Fatal(settled)
	}
	if old := f.cleanupRecord(t, first.Operation.ID); !reflect.DeepEqual(old, partial) {
		t.Fatal("recovery rewrote historical evidence", old, partial)
	}
	for i, r := range settled.Resources {
		if r.Fingerprint != partial.Resources[i].Fingerprint {
			t.Fatal("recovery retargeted", settled.Resources, partial.Resources)
		}
	}
	f.assertSingleCompute(t, job)
}

func TestCleanupProvenRejectedSubmissionKeepsStagingCleanable(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Reject
	f := newSystemFixture(t, scenario)
	connection := f.connection(t, "rejected-cleanup-connection", "SYNTHETIC_FIRST")
	job := f.submitJob(t, "rejected-cleanup", f.jobSpec(t, connection))
	f.authorizeJob(t, job)
	for i := 0; i < 20; i++ {
		r, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if r.Attempt.State.Orchestration.Terminal() {
			break
		}
		if i == 19 {
			t.Fatal("submission did not reject")
		}
		f.dispatchOnce(t)
	}
	r := f.admitCleanup(t, job, "rejected-staging", false)
	p := f.cleanupProbe(t, r.Operation.ID)
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	current := f.cleanupRecord(t, r.Operation.ID)
	if current.Effect != operations.RemoteDeleted || len(current.Resources) != 1 || current.Resources[0].Purpose != "staging" || len(p.requests) != 1 {
		t.Fatal(current, p.requests)
	}
	if stats := f.backend.Stats(); stats.SubmitCalls != 1 || stats.Executions != 0 {
		t.Fatal("cleanup reran rejected compute", stats)
	}
}

func TestCleanupProviderTimeoutPersistsUnknownWithoutStoppingWorker(t *testing.T) {
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-timeout")
	r := f.admitCleanup(t, job, "provider-timeout", false)
	p := f.cleanupProbe(t, r.Operation.ID)
	p.resourceHook = func(ctx context.Context, _ provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		<-ctx.Done()
		return provider.CleanupOutcome{}, ctx.Err()
	}
	cfg := cleanup.DefaultConfig()
	cfg.Timeout = time.Second
	e, err := cleanup.New(f.host.store, f.services.resolver, cfg)
	if err != nil {
		t.Fatal(err)
	}
	runSystemCleanup(t, e)
	current := f.cleanupRecord(t, r.Operation.ID)
	if current.Operation.Status != domain.OperationFailed || current.Resources[0].State != operations.ResourceUnknown || current.Resources[1].State != operations.ResourceRetained || len(p.requests) != 1 {
		t.Fatal(current, p.requests)
	}
	if worked, err := e.RunOnce(t.Context()); worked || err != nil {
		t.Fatal("timeout automatically repeated", worked, err)
	}
	f.assertSingleCompute(t, job)
}

func TestCleanupPreviewAndApplyKeepFrozenTargets(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-frozen")
	f.cleanupProbe(t, "")
	preview, err := f.cleanupPreview(t).Preview(ctx, f.actor, "app", job.JobID, job.AttemptID, false)
	if err != nil || !preview.Eligible || len(preview.Resources) != 2 {
		t.Fatal(preview, err)
	}
	for _, r := range preview.Resources {
		if !r.Eligible || r.Observation != "present" {
			t.Fatal(r)
		}
	}
	raw := string(systemJSON(t, preview))
	if strings.Contains(raw, "name_based_delete") || strings.Contains(raw, "warning") {
		t.Fatal("retired preview fields exposed", raw)
	}
	r := f.admitCleanup(t, job, "frozen-targets", false)
	f.reopen(t)
	p := f.cleanupProbe(t, r.Operation.ID)
	calls := 0
	p.resourceHook = func(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		if calls >= len(preview.Resources) || q.RecoverOnly || q.LedgerID != preview.Resources[calls].ResourceID || q.Purpose != preview.Resources[calls].Purpose {
			t.Fatal("target/order changed", q)
		}
		calls++
		return p.Provider.CleanupResource(ctx, q)
	}
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	current := f.cleanupRecord(t, r.Operation.ID)
	if calls != 2 || current.Effect != operations.RemoteDeleted {
		t.Fatal(current, calls)
	}
	for i, r := range current.Resources {
		if r.Fingerprint != preview.Resources[i].Fingerprint {
			t.Fatal("frozen fingerprint changed", r)
		}
	}
	f.assertSingleCompute(t, job)
}

func TestCleanupRecoveryOnlyReadsOriginalIntent(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "recovery-only")
	r := f.admitCleanup(t, job, "recovery-only-original", false)
	first, err := f.host.store.ClaimCleanup(ctx, time.Second)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	if err = f.host.store.BeginCleanupResource(ctx, *first, first.Resources[0]); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	f.advance(t, 2*time.Second)
	p := f.cleanupProbe(t, r.Operation.ID)
	calls := 0
	p.resourceHook = func(_ context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		calls++
		if !q.RecoverOnly || q.LedgerID != first.Resources[0].Result.ResourceID {
			t.Fatal("recovery could reapply/retarget", q)
		}
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeProviderStateUnknown, domain.FailureStageObservation, "earlier deletion remains unknown")
	}
	e := f.cleanupEngine(t, nil)
	runSystemCleanup(t, e)
	current := f.cleanupRecord(t, r.Operation.ID)
	if calls != 1 || current.Resources[0].State != operations.ResourceUnknown || current.Resources[1].State != operations.ResourceRetained {
		t.Fatal(current, calls)
	}
	if worked, err := e.RunOnce(ctx); worked || err != nil {
		t.Fatal("unknown deletion repeated", worked, err)
	}
	f.assertSingleCompute(t, job)
}

func TestCleanupRefusesUnresolvedBusyAndIneligibleAttempts(t *testing.T) {
	ctx := t.Context()
	t.Run("queued, claimed, unknown and cancelled before preparation", func(t *testing.T) {
		f := newSystemFixture(t, fake.DefaultScenario())
		c := f.connection(t, "cleanup-eligibility-connection", "SYNTHETIC_FIRST")
		job := f.submitJob(t, "queued-cleanup", f.jobSpec(t, c))
		request := func(key string, id domain.AttemptID) error {
			_, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "eligibility-"+key, systemCleanupBody(t, id, true, ""))
			return err
		}
		if err := request("queued", job.AttemptID); !errors.Is(err, operations.ErrState) {
			t.Fatal("queued admitted", err)
		}
		if err := request("unknown", "att_unknown"); err == nil {
			t.Fatal("unknown admitted")
		}
		f.authorizeJob(t, job)
		w, err := f.host.store.ClaimNext(ctx, "eligibility")
		if err != nil || w.Claim == nil {
			t.Fatal(err)
		}
		if err = request("held", job.AttemptID); !errors.Is(err, operations.ErrBusy) {
			t.Fatal("held dispatch admitted", err)
		}
		f.advance(t, 11*time.Minute)
		if _, err = f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCancel, "cancel-before-prepare", systemControlBody(t, job.AttemptID)); err != nil {
			t.Fatal(err)
		}
		if err = request("cancelled-unprepared", job.AttemptID); !errors.Is(err, operations.ErrState) {
			t.Fatal("unprepared admitted", err)
		}
	})
	t.Run("active remote execution", func(t *testing.T) {
		f := newSystemFixture(t, fake.DefaultScenario())
		c := f.connection(t, "active-cleanup-connection", "SYNTHETIC_FIRST")
		job := f.submitJob(t, "active-cleanup", f.jobSpec(t, c))
		f.authorizeJob(t, job)
		for i := 0; f.backend.Stats().SubmitCalls == 0; i++ {
			if i == 12 {
				t.Fatal("not submitted")
			}
			f.dispatchOnce(t)
		}
		if _, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "cleanup-active", systemCleanupBody(t, job.AttemptID, true, "")); !errors.Is(err, operations.ErrUnresolved) {
			t.Fatal("active admitted", err)
		}
	})
	t.Run("collection pending and transfer held", func(t *testing.T) {
		f := newSystemFixture(t, fake.DefaultScenario())
		job := f.collectibleJob(t, "collecting-cleanup", "automatic")
		source := f.collectionSource(t)
		request := func(key string) error {
			_, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "eligibility-"+key, systemCleanupBody(t, job.AttemptID, true, ""))
			return err
		}
		if err := request("collecting"); !errors.Is(err, operations.ErrState) {
			t.Fatal("pending collection admitted", err)
		}
		var refused error
		source.hook = func() error { refused = request("transfer-held"); return nil }
		f.collectOnce(t)
		if !errors.Is(refused, operations.ErrBusy) {
			t.Fatal("held collection admitted", refused)
		}
	})
	t.Run("discard confirmation and exclusive controls", func(t *testing.T) {
		f := newSystemFixture(t, fake.DefaultScenario())
		job := f.uncollectedForCleanup(t, "discard-cleanup")
		if _, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "no-discard", systemCleanupBody(t, job.AttemptID, false, "")); !errors.Is(err, operations.ErrUncollected) {
			t.Fatal("unconfirmed discard admitted", err)
		}
		if _, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "collect-again", systemControlBody(t, job.AttemptID)); err != nil {
			t.Fatal(err)
		}
		if _, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "pending-collect", systemCleanupBody(t, job.AttemptID, true, "")); !errors.Is(err, operations.ErrBusy) {
			t.Fatal("cleanup beside collect", err)
		}
	})
	t.Run("explicit discard blocks later collection", func(t *testing.T) {
		f := newSystemFixture(t, fake.DefaultScenario())
		job := f.uncollectedForCleanup(t, "confirmed-discard")
		f.admitCleanup(t, job, "cleanup-discard", true)
		if _, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "after-cleanup", systemControlBody(t, job.AttemptID)); !errors.Is(err, operations.ErrBusy) {
			t.Fatal("collect beside cleanup", err)
		}
	})
	t.Run("prepared staging is eligible", func(t *testing.T) {
		f := newSystemFixture(t, fake.DefaultScenario())
		job := f.preparedForCleanup(t, "eligible-prepared")
		r := f.admitCleanup(t, job, "prepared", false)
		if r.Operation.Status != domain.OperationAccepted {
			t.Fatal(r)
		}
		plan, err := f.host.store.PlanCleanup(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID, false)
		if err != nil || len(plan.Resources) != 1 || plan.Resources[0].Request.Purpose != "staging" {
			t.Fatal(plan, err)
		}
	})
}

func TestCleanupSharesOperationsWithoutChangingControlCapacityOrEvents(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-capacity")
	c := f.connection(t, "other-control-connection", "SYNTHETIC_FIRST")
	other := f.submitJob(t, "other-control-job", f.jobSpec(t, c))
	f.authorizeJob(t, other)
	for i := 0; f.backend.Stats().SubmitCalls < 2; i++ {
		if i == 12 {
			t.Fatal("other job not submitted")
		}
		f.dispatchOnce(t)
	}
	service, err := operations.New(f.host.access, f.host.store, f.host.inputs, admission.Limits{MaxOutstandingTotal: 1, MaxOutstandingWorkspace: 1})
	if err != nil {
		t.Fatal(err)
	}
	cancellation, err := service.Submit(ctx, f.actor, "app", other.JobID, domain.OperationCancel, "pending-control", systemControlBody(t, other.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	r, err := service.Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "cleanup-at-cap", systemCleanupBody(t, job.AttemptID, false, ""))
	if err != nil {
		t.Fatal("cleanup acquired control admission limit", err)
	}
	for i := 0; i < 12; i++ {
		current, err := service.Get(ctx, f.actor, "app", cancellation.Operation.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Operation.Status.Terminal() {
			break
		}
		if i == 11 {
			t.Fatal("cancel did not settle")
		}
		f.dispatchOnce(t)
	}
	if _, err = service.Submit(ctx, f.actor, "app", other.JobID, domain.OperationReconcile, "control-with-cleanup", systemControlBody(t, other.AttemptID)); err != nil {
		t.Fatal("cleanup consumed control capacity", err)
	}
	if current := f.cleanupRecord(t, r.Operation.ID); current.Operation != r.Operation {
		t.Fatal("dispatch claimed cleanup", current)
	}
	f.cleanupProbe(t, r.Operation.ID)
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	if current := f.cleanupRecord(t, r.Operation.ID); current.Effect != operations.RemoteDeleted {
		t.Fatal("own cleanup blocked eligibility", current)
	}
}
