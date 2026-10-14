package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

type systemCleanupProvider struct {
	provider.Provider
	requests     []provider.CleanupResourceRequest
	verifyErr    error
	result       *provider.CleanupOutcome
	err          error
	resourceHook func(context.Context, provider.CleanupResourceRequest) (provider.CleanupOutcome, error)
	beforeApply  func(context.Context, provider.CleanupResourceRequest) error
}

func (p *systemCleanupProvider) Verify(ctx context.Context, b provider.BindingSnapshot) error {
	if p.verifyErr != nil {
		return p.verifyErr
	}
	return p.Provider.Verify(ctx, b)
}
func (p *systemCleanupProvider) CleanupResource(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	if q.Mode == provider.CleanupApply {
		if p.beforeApply != nil {
			if err := p.beforeApply(ctx, q); err != nil {
				return provider.CleanupOutcome{}, err
			}
		}
		p.requests = append(p.requests, q)
	}
	if p.resourceHook != nil {
		return p.resourceHook(ctx, q)
	}
	if q.Mode == provider.CleanupApply {
		if p.result != nil {
			return *p.result, p.err
		}
		if p.err != nil {
			return provider.CleanupOutcome{}, p.err
		}
	}
	return p.Provider.CleanupResource(ctx, q)
}

func (f *managedTestFixture) collectedForCleanup(t *testing.T, key string) admission.Receipt {
	t.Helper()
	job := f.collectibleJob(t, key, "automatic")
	f.collectOnce(t)
	return job
}
func (f *managedTestFixture) uncollectedForCleanup(t *testing.T, key string) admission.Receipt {
	t.Helper()
	job := f.collectibleJob(t, key, "automatic")
	source := f.collectionSource(t)
	source.mode = "corrupt"
	var failure collection.Failure
	if worked, err := f.collectionEngine(t, nil).RunOnce(t.Context()); !worked || !errors.As(err, &failure) {
		t.Fatal("missing verification failure", worked, err)
	}
	return job
}
func (f *managedTestFixture) preparedForCleanup(t *testing.T, key string) admission.Receipt {
	t.Helper()
	connection := f.connection(t, key+"-connection", "SYNTHETIC_FIRST")
	job := f.submitJob(t, key, f.jobSpec(t, connection))
	f.authorizeJob(t, job)
	for range 12 {
		if f.backend.Stats().PrepareCalls != 0 {
			break
		}
		f.dispatchOnce(t)
	}
	if f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("fixture did not stop after preparation", f.backend.Stats())
	}
	r, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCancel, key+"-prevent", systemControlBody(t, job.AttemptID))
	if err != nil || r.Effect != operations.DispatchPrevented {
		t.Fatal("submission not prevented", r, err)
	}
	return job
}
func systemCleanupBody(t *testing.T, attempt domain.AttemptID, discard bool, reason string) []byte {
	t.Helper()
	return systemJSON(t, operations.Request{AttemptID: attempt, DiscardUncollected: discard, Reason: reason})
}
func (f *managedTestFixture) admitCleanup(t *testing.T, job admission.Receipt, key string, discard bool) operations.Record {
	t.Helper()
	r, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCleanup, key, systemCleanupBody(t, job.AttemptID, discard, ""))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (f *managedTestFixture) cleanupProbe(t *testing.T, op domain.OperationID) *systemCleanupProvider {
	t.Helper()
	probe := &systemCleanupProvider{}
	f.wrapProvider = func(base provider.Provider) provider.Provider { probe.Provider = base; return probe }
	if op != "" {
		probe.beforeApply = func(ctx context.Context, q provider.CleanupResourceRequest) error {
			// This public read must finish before the effect. It proves the running
			// resource intent committed and the callback is outside the transaction.
			call, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			r, err := f.host.store.ReadOperation(call, "app", f.actor.TokenID(), op)
			if err != nil {
				return err
			}
			for _, resource := range r.Resources {
				if resource.ResourceID == q.LedgerID && resource.State == operations.ResourceRunning {
					return nil
				}
			}
			return errors.New("provider effect without committed resource intent")
		}
	}
	return probe
}
func runSystemCleanup(t *testing.T, e *cleanup.Engine) {
	t.Helper()
	if worked, err := e.RunOnce(t.Context()); !worked || err != nil {
		t.Fatal("cleanup", worked, err)
	}
}
func (f *managedTestFixture) cleanupRecord(t *testing.T, id domain.OperationID) operations.Record {
	t.Helper()
	r, err := f.controls(t).Get(t.Context(), f.actor, "app", id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCleanupWorkerDeletesOnceAndKeepsReceipts(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-once")
	plan, err := f.host.store.PlanCleanup(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID, false)
	if err != nil {
		t.Fatal(err)
	}
	r := f.admitCleanup(t, job, "cleanup-original", false)
	probe := f.cleanupProbe(t, r.Operation.ID)
	e := f.cleanupEngine(t, nil)
	runSystemCleanup(t, e)
	current := f.cleanupRecord(t, r.Operation.ID)
	if current.Operation.Status != domain.OperationSucceeded || current.Effect != operations.RemoteDeleted || current.Operation.Revision != 3 || current.Operation.Failure != nil || len(probe.requests) != 2 || f.backend.Stats().CleanupCalls != 2 {
		t.Fatal(current, probe.requests)
	}
	for i, q := range probe.requests {
		if q != plan.Resources[i].Request || !q.ResultsCollected {
			t.Fatal("cleanup changed original target", q, plan.Resources[i])
		}
	}
	if worked, err := e.RunOnce(ctx); worked || err != nil {
		t.Fatal("terminal cleanup repeated", worked, err)
	}
	replay, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "cleanup-original", systemCleanupBody(t, job.AttemptID, false, ""))
	if err != nil || !replay.Replay || replay.Operation != r.Operation || replay.Effect != operations.Pending {
		t.Fatal("original receipt changed", replay, err)
	}
	coalesced := f.admitCleanup(t, job, "cleanup-other-key", false)
	if !coalesced.Replay || coalesced.Operation.ID != r.Operation.ID || coalesced.Effect != operations.RemoteDeleted {
		t.Fatal(coalesced)
	}
	state, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	if err != nil || state.Attempt.State.Result != domain.ResultAvailable || state.Attempt.State.Orchestration != domain.OrchestrationSucceeded {
		t.Fatal("remote deletion changed local result", err)
	}
	f.assertSingleCompute(t, job)
}

func TestCleanupWorkerReportsAlreadyAbsent(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-absence")
	plan, err := f.host.store.PlanCleanup(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID, false)
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.services.resolver.Resolve(plan.Binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range plan.Resources {
		q := target.Request
		q.Mode = provider.CleanupApply
		out, err := p.CleanupResource(ctx, q)
		if err != nil || !out.Deleted && !out.AlreadyAbsent {
			t.Fatal(out, err)
		}
	}
	r := f.admitCleanup(t, job, "cleanup-absent", false)
	f.cleanupProbe(t, r.Operation.ID)
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	current := f.cleanupRecord(t, r.Operation.ID)
	if current.Operation.Status != domain.OperationSucceeded || current.Effect != operations.AlreadyAbsent {
		t.Fatal(current)
	}
	f.assertSingleCompute(t, job)
}

func TestCleanupWorkerFailuresAreExplicit(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(*systemCleanupProvider)
		code  domain.ErrorCode
	}{
		{"adapter outcome unknown", func(p *systemCleanupProvider) { p.err = errors.New("provider-secret-canary") }, domain.CodeProviderStateUnknown},
		{"binding verification fails", func(p *systemCleanupProvider) { p.verifyErr = errors.New("binding-secret-canary") }, domain.CodeRemoteIdentityMismatch},
		{"adapter unsupported", func(p *systemCleanupProvider) {
			p.err = fmt.Errorf("adapter-secret-canary: %w", provider.ErrCleanupUnsupported)
		}, domain.CodeUnsupportedCapability},
		{"adapter returns no evidence", func(p *systemCleanupProvider) { p.result = &provider.CleanupOutcome{WouldDelete: true} }, domain.CodeProviderStateUnknown},
		{"contradictory deletion and refusal", func(p *systemCleanupProvider) {
			p.result = &provider.CleanupOutcome{Deleted: true}
			p.err = provider.Problem(domain.CodeProviderAccessDenied, domain.FailureStageAuthentication, "private-access-canary")
		}, domain.CodeProviderStateUnknown},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newSystemFixture(t, fake.DefaultScenario())
			job := f.uncollectedForCleanup(t, "cleanup-failure")
			r := f.admitCleanup(t, job, "cleanup-failed", true)
			p := f.cleanupProbe(t, r.Operation.ID)
			c.setup(p)
			runSystemCleanup(t, f.cleanupEngine(t, nil))
			current := f.cleanupRecord(t, r.Operation.ID)
			problem := current.Operation.Failure
			if current.Operation.Status != domain.OperationFailed || current.Effect != operations.Failed || problem == nil || problem.Code != c.code || problem.RecommendedAction != domain.RecommendedActionInspectProvider || problem.Validate() != nil || len(problem.Details) != 0 {
				t.Fatal(current)
			}
			if strings.Contains(problem.Message, "canary") || f.backend.Stats().CleanupCalls != 0 {
				t.Fatal("provider detail leaked or deletion performed", problem)
			}
			_, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCollect, "collect-after-failure", systemControlBody(t, job.AttemptID))
			if c.code == domain.CodeProviderStateUnknown {
				if !errors.Is(err, operations.ErrState) {
					t.Fatal("unknown mutation allowed collection", err)
				}
			} else if err != nil {
				t.Fatal("known refusal blocked collection", err)
			}
			f.assertSingleCompute(t, job)
		})
	}
	t.Run("new request after failure", func(t *testing.T) {
		f := newSystemFixture(t, fake.DefaultScenario())
		job := f.uncollectedForCleanup(t, "cleanup-retry")
		first := f.admitCleanup(t, job, "cleanup-first", true)
		p := f.cleanupProbe(t, first.Operation.ID)
		p.err = errors.New("transient")
		e := f.cleanupEngine(t, nil)
		runSystemCleanup(t, e)
		second := f.admitCleanup(t, job, "cleanup-second", true)
		if second.Operation.ID == first.Operation.ID || second.Replay {
			t.Fatal("failure absorbed explicit request", second)
		}
		p = f.cleanupProbe(t, second.Operation.ID)
		p.result = &provider.CleanupOutcome{Deleted: true}
		runSystemCleanup(t, e)
		if current := f.cleanupRecord(t, second.Operation.ID); current.Effect != operations.RemoteDeleted {
			t.Fatal(current)
		}
		if _, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCollect, "collect-after-delete", systemControlBody(t, job.AttemptID)); !errors.Is(err, operations.ErrState) {
			t.Fatal("collection after deletion", err)
		}
		f.assertSingleCompute(t, job)
	})
}

func TestCleanupWorkerTargetsPreparedResource(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.preparedForCleanup(t, "cleanup-prepared")
	plan, err := f.host.store.PlanCleanup(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID, false)
	if err != nil || len(plan.Resources) != 1 {
		t.Fatal(plan, err)
	}
	r := f.admitCleanup(t, job, "cleanup-prepared-request", false)
	p := f.cleanupProbe(t, r.Operation.ID)
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	if len(p.requests) != 1 || p.requests[0] != plan.Resources[0].Request || p.requests[0].ResultsCollected || p.requests[0].Remote.Version != "prepared" || p.requests[0].Purpose != "staging" {
		t.Fatal("prepared target changed", p.requests)
	}
	if current := f.cleanupRecord(t, r.Operation.ID); current.Effect != operations.RemoteDeleted {
		t.Fatal(current)
	}
	if stats := f.backend.Stats(); stats.SubmitCalls != 0 || stats.Executions != 0 {
		t.Fatal("cleanup started compute", stats)
	}
}

func TestCleanupAdmissionReplayConflictAndCoalescing(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-admission")
	service := f.controls(t)
	body := systemCleanupBody(t, job.AttemptID, false, "explicit operator request")
	original, err := service.Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "cleanup-original", body)
	if err != nil {
		t.Fatal(err)
	}
	op := original.Operation
	if op.Kind != domain.OperationCleanup || op.Status != domain.OperationAccepted || original.Effect != operations.Pending || original.Replay || op.AttemptID != job.AttemptID || op.Revision != 1 {
		t.Fatal(original)
	}
	replay, err := service.Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "cleanup-original", body)
	expected := original
	expected.Replay = true
	if err != nil || !reflect.DeepEqual(replay, expected) {
		t.Fatal(replay, err)
	}
	if _, err = service.Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "cleanup-original", systemCleanupBody(t, job.AttemptID, true, "explicit operator request")); !errors.Is(err, operations.ErrConflict) {
		t.Fatal("payload reused key", err)
	}
	if _, err = service.Submit(ctx, f.actor, "app", "job_other", domain.OperationCleanup, "cleanup-original", body); !errors.Is(err, operations.ErrConflict) {
		t.Fatal("job reused key", err)
	}
	coalesced := f.admitCleanup(t, job, "cleanup-second-key", false)
	if coalesced.Operation.ID != op.ID || !coalesced.Replay {
		t.Fatal(coalesced)
	}
	if got := f.cleanupRecord(t, op.ID); got.Operation != op || got.Replay {
		t.Fatal(got)
	}
	if _, err = service.Submit(ctx, f.actor, "app", job.JobID, domain.OperationReconcile, "reconcile-pending-cleanup", systemControlBody(t, job.AttemptID)); !errors.Is(err, operations.ErrBusy) {
		t.Fatal("control beside cleanup", err)
	}
	reader, _ := f.principal(t, "app", auth.Read)
	if _, err = service.Submit(ctx, reader, "app", job.JobID, domain.OperationCleanup, "read-only-cleanup", body); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope admitted cleanup", err)
	}
	other, _ := f.principal(t, "other", auth.Read, auth.Operate)
	if _, err = service.Get(ctx, other, "other", op.ID); !errors.Is(err, operations.ErrNotFound) {
		t.Fatal("cross-workspace receipt", err)
	}
	for _, raw := range []string{`{}`, `{"attempt_id":null}`, `{"attempt_id":"` + string(job.AttemptID) + `","discard_uncollected":"true"}`, `{"attempt_id":"` + string(job.AttemptID) + `","extra":1}`, `{"attempt_id":"` + string(job.AttemptID) + `"} {}`} {
		if _, err = service.Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "cleanup-invalid", []byte(raw)); !errors.Is(err, operations.ErrRequest) {
			t.Fatal("invalid request", raw, err)
		}
	}
	if f.backend.Stats().CleanupCalls != 0 {
		t.Fatal("admission called provider")
	}
	f.assertSingleCompute(t, job)
}
