package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

// The trace records values returned across the repository port, including a
// deliberately withheld acknowledgement. It never loads private journal rows.
type systemDispatchTrace struct {
	mu   sync.Mutex
	work map[domain.AttemptID]dispatch.Work
	lose dispatch.Kind
	lost atomic.Bool
}

func (r *systemDispatchTrace) record(w dispatch.Work) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.work[w.Job.Attempt.ID] = w
}
func (r *systemDispatchTrace) journal(t *testing.T, id scheduler.Identity) dispatch.Journal {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.work[id.AttemptID]; ok {
		return w.Journal
	}
	return dispatch.Journal{Phase: dispatch.Local}
}

type systemDispatchProbe struct {
	*sqlite.Store
	trace *systemDispatchTrace
}

func (r *systemDispatchProbe) LoadDispatch(ctx context.Context, c scheduler.Claim) (dispatch.Work, error) {
	w, err := r.Store.LoadDispatch(ctx, c)
	if err == nil {
		r.trace.record(w)
	}
	return w, err
}
func (r *systemDispatchProbe) ConsumeAuthorization(ctx context.Context, h dispatch.Handle) (dispatch.Work, error) {
	w, err := r.Store.ConsumeAuthorization(ctx, h)
	if err == nil {
		r.trace.record(w)
	}
	return w, err
}
func (r *systemDispatchProbe) CommitDispatch(ctx context.Context, h dispatch.Handle, a dispatch.Action) (dispatch.Work, error) {
	w, err := r.Store.CommitDispatch(ctx, h, a)
	if err != nil {
		return w, err
	}
	r.trace.record(w)
	if a.Kind == r.trace.lose && r.trace.lost.CompareAndSwap(false, true) {
		return dispatch.Work{}, sqlite.ErrUnavailable
	}
	return w, nil
}

type systemDispatchEffects struct {
	losePrepare, panicSubmit, public, rejectQuota bool
	prepareEntered, prepareRelease                chan struct{}
	afterPreparationRead                          func()
	prepare                                       func(context.Context, provider.Plan, domain.OperationID)
	reconcilePrepare                              func(context.Context, provider.Plan, domain.OperationID) error
	verify                                        func(context.Context, provider.BindingSnapshot) error
	observe                                       func(context.Context, provider.RemoteReference, provider.Observation) (provider.Observation, error)
	reconcile                                     func(context.Context, provider.Identity) error
	start                                         func(context.Context, provider.Prepared, *provider.StartAuthorization, *workloadsecret.Material, provider.SubmissionOutcome) provider.SubmissionOutcome
	deadlines                                     *systemStagingDeadlines
	cancel                                        *systemCancellation
}
type systemDispatchProvider struct {
	provider.Provider
	effects *systemDispatchEffects
}

func (p *systemDispatchProvider) Describe() provider.Descriptor {
	d := p.Provider.Describe()
	if p.effects.cancel != nil {
		d.Capabilities.RemoteCancellation = p.effects.cancel.supported
	}
	return d
}
func (p *systemDispatchProvider) Verify(ctx context.Context, b provider.BindingSnapshot) error {
	if d := p.effects.deadlines; d != nil {
		d.binding, _ = ctx.Deadline()
	}
	if p.effects.verify != nil {
		if err := p.effects.verify(ctx, b); err != nil {
			return err
		}
	}
	return p.Provider.Verify(ctx, b)
}
func (p *systemDispatchProvider) Prepare(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.Prepared, error) {
	e := p.effects
	if d := e.deadlines; d != nil {
		d.prepare, _ = ctx.Deadline()
	}
	if e.prepareEntered != nil {
		close(e.prepareEntered)
		select {
		case <-e.prepareRelease:
		case <-ctx.Done():
			return provider.Prepared{}, ctx.Err()
		}
	}
	if e.prepare != nil {
		e.prepare(ctx, plan, op)
	}
	prepared, err := p.Provider.Prepare(ctx, plan, op)
	if e.public {
		prepared.Private = false
	}
	if e.losePrepare {
		return provider.Prepared{}, errors.New("private-response-canary")
	}
	return prepared, err
}
func (p *systemDispatchProvider) ReconcilePrepare(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.PreparationObservation, error) {
	e := p.effects
	if d := e.deadlines; d != nil {
		d.staging, _ = ctx.Deadline()
	}
	if e.reconcilePrepare != nil {
		if err := e.reconcilePrepare(ctx, plan, op); err != nil {
			return provider.PreparationObservation{}, err
		}
	}
	seen, err := p.Provider.ReconcilePrepare(ctx, plan, op)
	if e.afterPreparationRead != nil {
		e.afterPreparationRead()
	}
	return seen, err
}
func (p *systemDispatchProvider) Start(ctx context.Context, prepared provider.Prepared, a *provider.StartAuthorization, m *workloadsecret.Material) provider.SubmissionOutcome {
	e := p.effects
	if d := e.deadlines; d != nil {
		d.start, _ = ctx.Deadline()
	}
	outcome := p.Provider.Start(ctx, prepared, a, m)
	if e.rejectQuota && outcome.Status == provider.SubmissionRejected {
		problem := provider.Problem(domain.CodeQuotaExhausted, domain.FailureStageSubmission, "fixture exhausted")
		outcome.Problem = &problem
	}
	if e.panicSubmit {
		panic("provider-secret-canary")
	}
	if e.start != nil {
		return e.start(ctx, prepared, a, m, outcome)
	}
	return outcome
}
func (p *systemDispatchProvider) Reconcile(ctx context.Context, id provider.Identity) (provider.Reconciliation, error) {
	if p.effects.reconcile != nil {
		if err := p.effects.reconcile(ctx, id); err != nil {
			return provider.Reconciliation{}, err
		}
	}
	return p.Provider.Reconcile(ctx, id)
}
func (p *systemDispatchProvider) Observe(ctx context.Context, r provider.RemoteReference) (provider.Observation, error) {
	if c := p.effects.cancel; c != nil {
		return c.observation(r), nil
	}
	seen, err := p.Provider.Observe(ctx, r)
	if err == nil && p.effects.observe != nil {
		return p.effects.observe(ctx, r, seen)
	}
	return seen, err
}
func (p *systemDispatchProvider) Cancel(ctx context.Context, r provider.RemoteReference, id domain.OperationID) (provider.CancellationOutcome, error) {
	if c := p.effects.cancel; c != nil {
		c.calls.Add(1)
		if c.lost {
			return provider.CancellationOutcome{}, errors.New("private-provider-response-canary")
		}
		return c.outcome, nil
	}
	return p.Provider.Cancel(ctx, r, id)
}

type systemStagingDeadlines struct{ binding, prepare, staging, start time.Time }
type systemCancellation struct {
	supported, lost bool
	outcome         provider.CancellationOutcome
	execution       domain.ExecutionState
	calls           atomic.Int32
	f               *managedTestFixture
}

func (c *systemCancellation) observation(r provider.RemoteReference) provider.Observation {
	activity, release := domain.RemoteActivityActive, domain.ReleaseEvidenceUnknown
	if c.execution.Terminal() {
		activity, release = domain.RemoteActivityInactive, domain.ReleaseEvidenceNotObservable
	}
	return provider.Observation{Remote: r, Execution: c.execution, RemoteActivity: activity, ReleaseEvidence: release, ObservedAt: c.f.fixtureClock().Now(), RawState: string(c.execution)}
}

func newSystemDispatch(t *testing.T, scenario fake.Scenario) (*managedTestFixture, *systemDispatchEffects, *systemDispatchTrace) {
	t.Helper()
	f := newSystemFixture(t, scenario)
	e := &systemDispatchEffects{}
	trace := &systemDispatchTrace{work: map[domain.AttemptID]dispatch.Work{}}
	f.wrapProvider = func(p provider.Provider) provider.Provider { return &systemDispatchProvider{Provider: p, effects: e} }
	f.wrapDispatch = func(s *sqlite.Store) dispatch.Repository { return &systemDispatchProbe{Store: s, trace: trace} }
	f.clockedServices(t)
	return f, e, trace
}
func (f *managedTestFixture) dispatchJob(t *testing.T, workspace domain.WorkspaceID, number int) scheduler.Identity {
	t.Helper()
	return f.dispatchJobForAccount(t, workspace, number, "SYNTHETIC_FIRST")
}
func (f *managedTestFixture) dispatchJobForAccount(t *testing.T, workspace domain.WorkspaceID, number int, token string) scheduler.Identity {
	t.Helper()
	actor := f.actor
	if workspace != "app" {
		actor, _ = f.principal(t, workspace, auth.Read, auth.Write, auth.Manage, auth.Execute, auth.Operate)
	}
	connection := f.connectionFor(t, actor, workspace, fmt.Sprintf("dispatch-connection-%d", number), token)
	spec := f.jobSpecFor(t, actor, workspace, connection)
	inputs, err := objects.New(f.host.access, f.host.inputs, f.host.store)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("frozen-input")
	object, err := inputs.Upload(t.Context(), actor, workspace, int64(len(data)), provider.Digest(data), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	spec.Inputs = []admission.Input{{Name: "data", Source: admission.Source{Kind: "object", ObjectID: object.ID}, Target: "data.txt"}}
	jobs, err := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	job, err := jobs.Submit(t.Context(), actor, workspace, fmt.Sprintf("dispatch-job-%d", number), systemJSON(t, spec))
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.services.authorizations.Authorize(t.Context(), actor, workspace, job.JobID, "dispatch-authorize-"+string(job.JobID), systemJSON(t, map[string]any{"attempt_id": job.AttemptID, "max_remote_wall_seconds": 10, "authorize_private_staging": true, "authorize_compute": true}))
	if err != nil {
		t.Fatal(err)
	}
	return scheduler.Identity{WorkspaceID: workspace, JobID: job.JobID, AttemptID: job.AttemptID}
}
func (f *managedTestFixture) dispatchStep(t *testing.T) error {
	t.Helper()
	f.advance(t, 10*time.Minute)
	worked, err := f.services.dispatcher.RunOnce(t.Context(), "dispatch-system-worker")
	if !worked && err == nil {
		t.Fatal("expected one phase of work")
	}
	return err
}
func (f *managedTestFixture) dispatchRecord(t *testing.T, id scheduler.Identity) admission.Record {
	t.Helper()
	r, err := f.host.store.ReadJob(t.Context(), id.WorkspaceID, f.actor.TokenID(), id.JobID)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (f *managedTestFixture) dispatchState(t *testing.T, id scheduler.Identity) domain.AttemptState {
	t.Helper()
	return f.dispatchRecord(t, id).Attempt.State
}
func (e *systemDispatchEffects) advance(t *testing.T, f *managedTestFixture, r provider.RemoteReference) error {
	t.Helper()
	a, err := f.host.store.ReadLogAttempt(t.Context(), r.Identity.WorkspaceID, f.actor.TokenID(), r.Identity.JobID, r.Identity.AttemptID)
	if err != nil {
		return err
	}
	p, err := fake.NewBound(f.backend, f.fixtureClock(), a.Binding)
	if err != nil {
		return err
	}
	return p.Advance(r)
}
func (e *systemDispatchEffects) markReady(t *testing.T, f *managedTestFixture, id provider.Identity) error {
	t.Helper()
	a, err := f.host.store.ReadLogAttempt(t.Context(), id.WorkspaceID, f.actor.TokenID(), id.JobID, id.AttemptID)
	if err != nil {
		return err
	}
	p, err := fake.NewBound(f.backend, f.fixtureClock(), a.Binding)
	if err != nil {
		return err
	}
	return p.MarkReady(id)
}
func systemAuthorizationRaw(id domain.AttemptID, wall int64) []byte {
	raw, _ := json.Marshal(map[string]any{"attempt_id": id, "max_remote_wall_seconds": wall, "authorize_private_staging": true, "authorize_compute": true})
	return raw
}
