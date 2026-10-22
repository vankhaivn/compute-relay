package runtimehost

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
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
	prepareReceiptError                           error
	prepareEntered, prepareRelease                chan struct{}
	afterPreparationRead                          func()
	prepare                                       func(context.Context, provider.Plan, domain.OperationID)
	reconcilePrepare                              func(context.Context, provider.Plan, domain.OperationID) error
	preparationObservation                        func(context.Context, provider.Plan, domain.OperationID) (provider.PreparationObservation, error)
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
		if e.prepareReceiptError != nil {
			return provider.Prepared{}, e.prepareReceiptError
		}
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
	if e.preparationObservation != nil {
		return e.preparationObservation(ctx, plan, op)
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
	requireNoError(t, err)
	data := []byte("frozen-input")
	object, err := inputs.Upload(t.Context(), actor, workspace, int64(len(data)), provider.Digest(data), bytes.NewReader(data))
	requireNoError(t, err)
	spec.Inputs = []admission.Input{{Name: "data", Source: admission.Source{Kind: "object", ObjectID: object.ID}, Target: "data.txt"}}
	jobs, err := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	requireNoError(t, err)
	job, err := jobs.Submit(t.Context(), actor, workspace, fmt.Sprintf("dispatch-job-%d", number), systemJSON(t, spec))
	requireNoError(t, err)
	_, err = f.services.authorizations.Authorize(t.Context(), actor, workspace, job.JobID, "dispatch-authorize-"+string(job.JobID), systemJSON(t, map[string]any{"attempt_id": job.AttemptID, "max_remote_wall_seconds": 10, "authorize_private_staging": true, "authorize_compute": true}))
	requireNoError(t, err)
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
	requireNoError(t, err)
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

func systemSubmitted(t *testing.T, f *managedTestFixture, trace *systemDispatchTrace, id scheduler.Identity) {
	t.Helper()
	for range 8 {
		if trace.journal(t, id).Phase == dispatch.Submitted {
			return
		}
		requireNoError(t, f.dispatchStep(t))
	}
	t.Fatal("execution did not become submitted")
}
func installSystemCancelProbe(t *testing.T, f *managedTestFixture, e *systemDispatchEffects, supported bool, out provider.CancellationOutcome) *systemCancellation {
	t.Helper()
	c := &systemCancellation{supported: supported, outcome: out, execution: domain.ExecutionSucceeded, f: f}
	e.cancel = c
	return c
}
func systemRemapProfile(t *testing.T, f *managedTestFixture, id scheduler.Identity) {
	t.Helper()
	old := f.dispatchRecord(t, id).Profile
	connectionID := strings.TrimPrefix(old.CredentialRef, "vault:")
	c, err := f.services.connections.Get(t.Context(), f.actor, "app", connectionID)
	requireNoError(t, err)
	_, err = f.services.connections.Submit(t.Context(), f.actor, "app", c.ID, "remap-future", systemJSON(t, map[string]any{"action": "configure", "expected_revision": c.Revision, "configuration": connections.Configuration{MaxRemoteWallSeconds: 200}}))
	requireNoError(t, err)
	current, err := f.services.connections.Get(t.Context(), f.actor, "app", c.ID)
	if err != nil || current.Selection.ConfigurationRevision == old.Binding.ConfigurationRevision {
		t.Fatal("future selection did not change", err)
	}
}

type systemBlockedInputs struct {
	operations.BlobReader
	entered, release chan struct{}
	once             sync.Once
}

func (b *systemBlockedInputs) Open(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	b.once.Do(func() {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	})
	return b.BlobReader.Open(ctx, w, id)
}

func newSystemGate(t *testing.T, mode fake.SubmitMode, maxActive ...int) (*managedTestFixture, *systemDispatchEffects, *systemDispatchTrace) {
	t.Helper()
	scenario := fake.DefaultScenario()
	scenario.SeparateStart = true
	scenario.Mode = mode
	f, e, trace := newSystemDispatch(t, scenario)
	if len(maxActive) > 0 {
		f.adapter.profileHook = func(p admission.Profile) admission.Profile { p.MaxActiveAttempts = maxActive[0]; return p }
	}
	return f, e, trace
}
func systemGateSeed(t *testing.T, f *managedTestFixture, n int) scheduler.Identity {
	t.Helper()
	connection := f.connection(t, "gate-connection-"+fmt.Sprint(n), "SYNTHETIC_FIRST")
	if connection.Selection.MaxActiveAttempts > 1 {
		profile, _, err := f.host.store.ReadProfile(t.Context(), connection.Selection.Profile)
		requireNoError(t, err)
		if err = f.host.store.ConfigureAccount(t.Context(), profile.AccountScope, scheduler.AccountPolicy{MaxActive: connection.Selection.MaxActiveAttempts}); err != nil {
			t.Fatal(err)
		}
	}
	spec := f.jobSpec(t, connection)
	spec.ExecutionControl = "prepare_then_start"
	job := f.submitJob(t, "gate-job-"+fmt.Sprint(n), spec)
	return scheduler.Identity{WorkspaceID: "app", JobID: job.JobID, AttemptID: job.AttemptID}
}
func systemGateStep(t *testing.T, f *managedTestFixture) (bool, error) {
	t.Helper()
	f.advance(t, 10*time.Millisecond)
	return f.services.dispatcher.RunOnce(t.Context(), "gate-system-worker")
}
func systemAuthorizationService(t *testing.T, f *managedTestFixture, w domain.WorkspaceID, scopes ...auth.Scope) (*executionauth.Service, auth.Principal) {
	t.Helper()
	actor, _ := f.principal(t, w, scopes...)
	return f.services.authorizations, actor
}
func systemPrepareGrant(t *testing.T, f *managedTestFixture, id scheduler.Identity) (*executionauth.Service, auth.Principal, executionauth.GateReceipt, []byte) {
	t.Helper()
	service, p := systemAuthorizationService(t, f, "app", auth.Execute, auth.Read)
	raw := systemJSON(t, executionauth.PrepareRequest{AttemptID: id.AttemptID, MaxPreparationSeconds: 60, ReadyUntil: f.fixtureClock().Now().Add(30 * time.Minute).UTC().Format(time.RFC3339), MaxStagedBytes: 1 << 20, AuthorizePrivateStaging: true})
	receipt, err := service.Prepare(t.Context(), p, "app", id.JobID, "prepare-key-"+string(id.JobID), raw)
	requireNoError(t, err)
	return service, p, receipt, raw
}
func systemReadyGate(t *testing.T, f *managedTestFixture, id scheduler.Identity) (*executionauth.Service, auth.Principal, executionauth.Readiness) {
	t.Helper()
	s, p, _, _ := systemPrepareGrant(t, f, id)
	if worked, err := systemGateStep(t, f); err != nil || !worked {
		t.Fatal("prepare phase", worked, err)
	}
	ready, err := s.Readiness(t.Context(), p, "app", id.JobID)
	if err != nil || ready.State != "ready" || !ready.Revision.Valid() || ready.StagedBytes <= 0 {
		t.Fatal("readiness", ready, err)
	}
	return s, p, ready
}
func systemStartGrantRaw(f *managedTestFixture, id scheduler.Identity, ready executionauth.Readiness) []byte {
	raw, _ := json.Marshal(executionauth.StartRequest{AttemptID: id.AttemptID, ReadinessRevision: ready.Revision, MaxRemoteWallSeconds: 10, ExecutionDeadline: f.fixtureClock().Now().Add(9 * time.Second).UTC().Format(time.RFC3339), AuthorizeCompute: true})
	return raw
}
func systemClaim(t *testing.T, f *managedTestFixture) dispatch.Work {
	t.Helper()
	r, err := f.host.store.ClaimNext(context.Background(), "system-boundary")
	if err != nil || r.Claim == nil {
		t.Fatal("claim", err)
	}
	w, err := f.host.store.LoadDispatch(t.Context(), *r.Claim)
	requireNoError(t, err)
	return w
}
func systemControlService(t *testing.T, f *managedTestFixture, w domain.WorkspaceID, scopes ...auth.Scope) (*operations.Service, auth.Principal, string) {
	t.Helper()
	p, secret := f.principal(t, w, scopes...)
	return f.controls(t), p, secret
}
func systemCancelControl(t *testing.T, f *managedTestFixture, s *operations.Service, p auth.Principal, id scheduler.Identity) operations.Record {
	t.Helper()
	r, err := s.Submit(t.Context(), p, id.WorkspaceID, id.JobID, domain.OperationCancel, "cancel-original-key", systemControlBody(t, id.AttemptID))
	requireNoError(t, err)
	return r
}

const systemWorkloadCanary = "synthetic-workload-canary/with+url?characters=only"

func systemSecretFixture(t *testing.T, mode fake.SubmitMode) (*managedTestFixture, *systemDispatchEffects, *systemDispatchTrace, scheduler.Identity, *executionauth.Service, auth.Principal, executionauth.Readiness, *workloadsecret.Service, *workloadsecret.Pool) {
	t.Helper()
	scenario := fake.DefaultScenario()
	scenario.SeparateStart = true
	scenario.EphemeralSecrets = true
	scenario.Mode = mode
	f, e, trace := newSystemDispatch(t, scenario)
	connection := f.connection(t, "secret-connection", "SYNTHETIC_FIRST")
	spec := f.jobSpec(t, connection)
	spec.ExecutionControl = "prepare_then_start"
	spec.WorkloadSecrets = []workloadsecret.Slot{{Name: "ingest", Exposure: "file"}}
	job := f.submitJob(t, "secret-job", spec)
	id := scheduler.Identity{WorkspaceID: "app", JobID: job.JobID, AttemptID: job.AttemptID}
	gate, p, ready := systemReadyGate(t, f, id)
	e.start = func(_ context.Context, _ provider.Prepared, _ *provider.StartAuthorization, m *workloadsecret.Material, out provider.SubmissionOutcome) provider.SubmissionOutcome {
		if m == nil || string(m.Values["ingest"]) != systemWorkloadCanary {
			t.Error("wrong synthetic material")
		}
		return out
	}
	return f, e, trace, id, gate, p, ready, f.services.secrets, f.host.secretPool
}
func systemSecretRequest(f *managedTestFixture, id scheduler.Identity, ready executionauth.Readiness) []byte {
	raw, _ := json.Marshal(workloadsecret.Supply{AttemptID: id.AttemptID, ReadinessRevision: ready.Revision, ExpiresAt: f.fixtureClock().Now().Add(time.Minute).UTC().Format(time.RFC3339), Values: map[string][]byte{"ingest": []byte(systemWorkloadCanary)}})
	return raw
}
func systemSecretStartRequest(f *managedTestFixture, id scheduler.Identity, ready executionauth.Readiness, secret domain.OperationID) []byte {
	var request executionauth.StartRequest
	_ = json.Unmarshal(systemStartGrantRaw(f, id, ready), &request)
	request.SecretAuthorizationID = secret
	raw, _ := json.Marshal(request)
	return raw
}
func assertSystemSecretSanitized(t *testing.T, f *managedTestFixture, id scheduler.Identity) {
	t.Helper()
	record := f.dispatchRecord(t, id)
	ready, err := f.services.authorizations.Readiness(t.Context(), f.actor, "app", id.JobID)
	requireNoError(t, err)
	for _, value := range []any{record.Problem, record.Preparation, record.Attempt.State, ready} {
		raw := systemJSON(t, value)
		if bytes.Contains(raw, []byte(systemWorkloadCanary)) || bytes.Contains(raw, []byte(base64.StdEncoding.EncodeToString([]byte(systemWorkloadCanary)))) {
			t.Fatal("public status leaked workload material")
		}
	}
}

type systemMatrixProbe struct {
	mode         string
	wrongAccount bool
	childExit    int
}

func installSystemMatrixProvider(t *testing.T, f *managedTestFixture, e *systemDispatchEffects, mode string) *systemMatrixProbe {
	t.Helper()
	p := &systemMatrixProbe{mode: mode}
	e.verify = func(context.Context, provider.BindingSnapshot) error {
		if p.wrongAccount {
			return provider.Problem(domain.CodeProviderAuthFailed, domain.FailureStageAuthentication, "Fixture credentials do not match the frozen account.").WithCause(errors.New("private-account-canary"))
		}
		return nil
	}
	e.observe = func(_ context.Context, _ provider.RemoteReference, obs provider.Observation) (provider.Observation, error) {
		switch p.mode {
		case "unknown-format":
			obs.Execution = domain.ExecutionUnknown
			obs.RemoteActivity = domain.RemoteActivityUnknown
			obs.ReleaseEvidence = domain.ReleaseEvidenceUnknown
			obs.RawState = "new-provider-format-private-canary"
		case "invalid-state":
			obs.Execution = "new-unmapped-state"
		case "modified-resource":
			obs.Remote.Version = "manually-replaced-version"
		}
		return obs, nil
	}
	e.reconcile = func(ctx context.Context, _ provider.Identity) error {
		if p.mode == "deadline" {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	e.start = func(ctx context.Context, _ provider.Prepared, _ *provider.StartAuthorization, _ *workloadsecret.Material, out provider.SubmissionOutcome) provider.SubmissionOutcome {
		if p.mode != "cli-exit" {
			return out
		}
		// A real nonzero child exit loses the response after synthetic acceptance.
		executable, err := os.Executable()
		if err != nil {
			t.Error(err)
			return provider.SubmissionOutcome{}
		}
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestSystemFaultMatrixCLIChild$", "--", "relay-cli-fault")
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		cmd.WaitDelay = time.Second
		var exit *exec.ExitError
		if err := cmd.Run(); errors.As(err, &exit) {
			p.childExit = exit.ExitCode()
		}
		return provider.SubmissionOutcome{}
	}
	if mode == "deadline" {
		cfg := dispatch.DefaultConfig()
		cfg.ControlTimeout = 30 * time.Millisecond
		var err error
		f.services.dispatcher, err = dispatch.New(f.wrapDispatch(f.host.store), f.services.resolver, f.host.inputs, f.fixtureClock(), cfg)
		requireNoError(t, err)
	}
	return p
}
