package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type guardHealthSampler struct {
	mu    sync.Mutex
	mode  string
	calls int
	hook  func(context.Context)
	clock *fixedClock
}

func guardMetric[T any](v T) hosthealth.Metric[T] {
	return hosthealth.Metric[T]{Status: hosthealth.Available, Value: &v}
}

func (s *guardHealthSampler) Sample(ctx context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
	s.mu.Lock()
	s.calls++
	mode, hook := s.mode, s.hook
	s.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	if mode == "failed" {
		return hosthealth.Snapshot{}, errors.New("synthetic bounded observation failure")
	}
	platform := "Darwin x86_64"
	if mode == "unsupported" {
		platform = "Linux x86_64"
	}
	snapshot := hosthealth.Parse(target.ResourceID, s.clock.Now(), map[hosthealth.Source]hosthealth.Sample{
		hosthealth.Platform:           {Status: hosthealth.Available, Data: []byte(platform)},
		hosthealth.NativeArchitecture: {Status: hosthealth.Available, Data: []byte("7\n")},
	})
	if mode != "unsupported" {
		snapshot.ThermalLevel = guardMetric(0)
		snapshot.CPUSpeedLimitPercent = guardMetric(float64(100))
		snapshot.PowerSource = guardMetric("ac")
		switch mode {
		case "warning":
			snapshot.ThermalLevel = guardMetric(1)
		case "critical":
			snapshot.ThermalLevel = guardMetric(2)
		case "battery":
			snapshot.PowerSource, snapshot.BatteryChargePercent = guardMetric("battery"), guardMetric(float64(5))
		case "unknown":
			snapshot.ThermalLevel = hosthealth.Metric[int]{Status: hosthealth.Unavailable, Reason: "source_failed"}
		}
	}
	return snapshot, nil
}

func (s *guardHealthSampler) set(mode string) { s.mu.Lock(); s.mode = mode; s.mu.Unlock() }
func (s *guardHealthSampler) count() int      { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

type hostGuardFixture struct {
	f          *managedTestFixture
	connection connections.Connection
	sampler    *guardHealthSampler
	guard      *managedHostHealthGuard
	jobs       *admission.Service
}

func newHostGuardFixture(t *testing.T, mode string) *hostGuardFixture {
	t.Helper()
	clock := &fixedClock{now: time.Now().UTC().Truncate(time.Second)}
	f := newManagedTestFixtureAt(t, true, clock, &managedSSHAdapter{verify: sshFixtureVerify, config: ManagedServeConfig{MaxRemoteWallSeconds: 600}})
	_, connection := f.sshConnection(t, "guard-connection", "SYNTHETIC_SSH")
	scenario := fake.DefaultScenario()
	scenario.States, scenario.SeparateStart = []domain.ExecutionState{domain.ExecutionRunning}, true
	f.useFake(t, scenario)
	g := &hostGuardFixture{f: f, connection: connection, sampler: &guardHealthSampler{mode: mode, clock: clock}}
	g.bind(t)
	return g
}

func (g *hostGuardFixture) bind(t *testing.T) {
	t.Helper()
	var err error
	health, err := hosthealth.NewService(g.f.host.access, g.f.host.store, g.sampler, g.f.fixtureClock())
	requireNoError(t, err)
	g.guard, err = g.f.host.newHostHealthGuard(health, g.f.fixtureClock())
	requireNoError(t, err)
	g.f.services.health, g.f.services.guard = health, g.guard
	g.f.services.dispatcher.WithNewWorkGuard(g.guard)
	g.jobs, err = admission.New(g.f.host.access, g.f.host.store, admission.DefaultLimits())
	requireNoError(t, err)
	g.jobs.WithPreflight(g.guard)
}

func (g *hostGuardFixture) spec(t *testing.T) admission.Spec {
	t.Helper()
	spec := g.f.jobSpec(t, g.connection)
	spec.Timeouts.RemoteWallSeconds = 300
	return spec
}

func (g *hostGuardFixture) admit(t *testing.T, key string, spec admission.Spec) admission.Receipt {
	t.Helper()
	receipt, err := g.jobs.Submit(t.Context(), g.f.actor, "app", key, systemJSON(t, spec))
	if err != nil {
		t.Fatal("guarded admission", err)
	}
	return receipt
}

func (g *hostGuardFixture) read(t *testing.T, job admission.Receipt) admission.Record {
	t.Helper()
	r, err := g.f.host.store.ReadJob(t.Context(), "app", g.f.actor.TokenID(), job.JobID)
	requireNoError(t, err)
	return r
}

func (g *hostGuardFixture) step(t *testing.T) error {
	t.Helper()
	g.f.advance(t, 100*time.Millisecond)
	worked, err := g.f.services.dispatcher.RunOnce(t.Context(), "health-guard-worker")
	if !worked {
		t.Fatal("guard dispatcher did not claim work", err)
	}
	return err
}

func (g *hostGuardFixture) running(t *testing.T) admission.Receipt {
	t.Helper()
	job := g.admit(t, "guard-running-job", g.spec(t))
	g.f.authorizeJob(t, job)
	for range 2 {
		requireNoError(t, g.step(t))
	}
	g.f.advance(t, 20*time.Second)
	requireNoError(t, g.step(t))
	if r := g.read(t, job); r.Attempt.State.Execution != domain.ExecutionRunning {
		t.Fatal("fixture did not reach running", r.Attempt.State)
	}
	return job
}

func TestHostHealthAdmissionDemandAuthReplayAndUnsupported(t *testing.T) {
	for _, mode := range []string{"ok", "warning", "critical", "unknown", "failed", "unsupported"} {
		t.Run(mode, func(t *testing.T) {
			g := newHostGuardFixture(t, mode)
			raw := systemJSON(t, g.spec(t))
			if _, err := g.jobs.Submit(t.Context(), g.f.actor, "other", "guard-demand", raw); !errors.Is(err, auth.ErrForbidden) || g.sampler.count() != 0 {
				t.Fatal("unauthorized request probed health", err)
			}
			g.sampler.hook = func(ctx context.Context) {
				if _, err := g.f.host.store.Info(ctx); err != nil {
					t.Fatal("remote sample retained SQLite transaction", err)
				}
			}
			first, err := g.jobs.Submit(t.Context(), g.f.actor, "app", "guard-demand", raw)
			allowed := mode == "ok" || mode == "unsupported"
			if allowed != (err == nil) || !allowed && !errors.Is(err, admission.ErrHostUnhealthy) || g.sampler.count() != 1 {
				t.Fatal("admission did not honor measured health", first, err, g.sampler.count())
			}
			if !allowed {
				if stats := g.f.backend.Stats(); stats.PrepareCalls != 0 || stats.SubmitCalls != 0 {
					t.Fatal("health refusal caused remote mutation", stats)
				}
				return
			}
			g.sampler.set("failed")
			g.f.advance(t, 2*time.Hour)
			g.f.reopen(t)
			g.bind(t)
			replay, err := g.jobs.Submit(t.Context(), g.f.actor, "app", "guard-demand", raw)
			first.Replay = true
			if err != nil || replay != first || g.sampler.count() != 1 {
				t.Fatal("exact admission replay depended on later health or restart", replay, err)
			}
		})
	}
}

func TestHostHealthAdmissionFinalTransactionRejectsChangedEvidenceAndBinding(t *testing.T) {
	for _, race := range []string{"new_snapshot", "failed_probe", "binding"} {
		t.Run(race, func(t *testing.T) {
			g := newHostGuardFixture(t, "ok")
			req, err := admission.Parse(systemJSON(t, g.spec(t)))
			requireNoError(t, err)
			key, err := admission.KeyDigest("guard-final-race")
			requireNoError(t, err)
			ctx, prior, err := g.guard.PreflightAdmission(t.Context(), "app", g.f.actor.TokenID(), key, req)
			if err != nil || prior != nil {
				t.Fatal(prior, err)
			}
			if race == "binding" {
				body := []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":600,"host_health":{"warning_thermal_level":0}}}`, g.connection.Revision))
				_, err = g.f.services.connections.Submit(t.Context(), g.f.actor, "app", g.connection.ID, "guard-change-binding", body)
				requireNoError(t, err)
			} else {
				g.sampler.set("warning")
				if race == "failed_probe" {
					g.sampler.set("failed")
				}
				g.f.advance(t, hosthealth.DemandCache)
				target, err := g.f.host.store.ResolveHostHealthTarget(t.Context(), "app", g.f.actor.TokenID(), g.connection.ID)
				requireNoError(t, err)
				_, err = g.guard.health.Refresh(t.Context(), target)
				requireNoError(t, err)
			}
			_, err = g.f.host.store.AdmitJob(ctx, "app", g.f.actor.TokenID(), key, req, admission.DefaultLimits())
			if !errors.Is(err, admission.ErrHostUnhealthy) && !(race == "binding" && errors.Is(err, admission.ErrSelection)) {
				t.Fatal("final admission accepted stale process or binding proof", err)
			}
		})
	}
}

func TestHostHealthAdmissionConcurrentReplayWinsOverFailedDemand(t *testing.T) {
	g := newHostGuardFixture(t, "failed")
	raw := systemJSON(t, g.spec(t))
	req, err := admission.Parse(raw)
	requireNoError(t, err)
	key, err := admission.KeyDigest("guard-concurrent-replay")
	requireNoError(t, err)
	var first admission.Receipt
	g.sampler.hook = func(ctx context.Context) {
		var err error
		first, err = g.f.host.store.AdmitJob(ctx, "app", g.f.actor.TokenID(), key, req, admission.DefaultLimits())
		requireNoError(t, err)
	}
	replay, err := g.jobs.Submit(t.Context(), g.f.actor, "app", "guard-concurrent-replay", raw)
	first.Replay = true
	if err != nil || replay != first || g.sampler.count() != 1 {
		t.Fatal("concurrent successful receipt lost to later failed health", replay, err)
	}
}

func TestHostHealthRefusesPreparationAndPreservesPreparedStartAuthority(t *testing.T) {
	for _, stage := range []string{"preparation", "start"} {
		t.Run(stage, func(t *testing.T) {
			g := newHostGuardFixture(t, "ok")
			spec := g.spec(t)
			if stage == "start" {
				spec.ExecutionControl = "prepare_then_start"
			}
			job := g.admit(t, "guard-before-"+stage, spec)
			var grant executionauth.GateReceipt
			if stage == "preparation" {
				g.f.authorizeJob(t, job)
			} else {
				id := scheduler.Identity{WorkspaceID: "app", JobID: job.JobID, AttemptID: job.AttemptID}
				service, actor, ready := systemReadyGate(t, g.f, id)
				raw := systemJSON(t, executionauth.StartRequest{AttemptID: job.AttemptID, ReadinessRevision: ready.Revision, MaxRemoteWallSeconds: 300, ExecutionDeadline: g.f.fixtureClock().Now().Add(299 * time.Second).UTC().Format(time.RFC3339), AuthorizeCompute: true})
				var err error
				grant, err = service.Start(t.Context(), actor, "app", job.JobID, "guard-start-grant", raw)
				requireNoError(t, err)
			}
			g.sampler.set("warning")
			g.f.advance(t, hosthealth.DemandCache)
			if err := g.step(t); !errors.Is(err, admission.ErrHostUnhealthy) {
				t.Fatal("unhealthy machine mutated", err)
			}
			r := g.read(t, job)
			if r.Problem == nil || r.Problem.Details[domain.ReasonDetail] != "host_unhealthy" || r.Problem.ComputeMayHaveStarted || r.Attempt.State.Execution != domain.ExecutionNotSubmitted || g.f.backend.Stats().SubmitCalls != 0 {
				t.Fatal("refusal lost stable pre-submit evidence", r.Attempt.State, r.Problem)
			}
			if stage == "preparation" {
				if g.f.backend.Stats().PrepareCalls != 0 || r.Attempt.State.Orchestration != domain.OrchestrationFailed {
					t.Fatal("preparation refusal staged or retained automatic work")
				}
				return
			}
			current, err := g.f.services.authorizations.GateAuthorization(t.Context(), g.f.actor, "app", job.JobID, grant.AuthorizationID)
			if err != nil || current.Status != executionauth.Granted || g.f.backend.Stats().PrepareCalls != 1 || r.Attempt.State.Orchestration != domain.OrchestrationPreparing {
				t.Fatal("refusal consumed start grant or discarded stopped preparation", current, err)
			}
			g.sampler.set("ok")
			g.f.advance(t, 40*time.Second)
			requireNoError(t, g.step(t))
			if g.f.backend.Stats().SubmitCalls != 1 || g.read(t, job).Attempt.ID != job.AttemptID {
				t.Fatal("healthy start did not use the original unused grant")
			}
		})
	}
}

type healthCancelProbe struct {
	mu        sync.Mutex
	calls     int
	lost      bool
	confirmed bool
}

type healthCancelProvider struct {
	provider.Provider
	probe *healthCancelProbe
}

func (p *healthCancelProvider) Describe() provider.Descriptor {
	descriptor := p.Provider.Describe()
	descriptor.Capabilities.RemoteCancellation = true
	return descriptor
}

func (p *healthCancelProvider) Cancel(_ context.Context, _ provider.RemoteReference, _ domain.OperationID) (provider.CancellationOutcome, error) {
	p.probe.mu.Lock()
	defer p.probe.mu.Unlock()
	p.probe.calls++
	if p.probe.lost {
		return provider.CancellationOutcome{}, errors.New("synthetic lost cancellation acknowledgement")
	}
	return provider.CancellationOutcome{Status: domain.CancellationConfirmed, TerminationConfirmed: true}, nil
}

func (p *healthCancelProvider) Observe(ctx context.Context, remote provider.RemoteReference) (provider.Observation, error) {
	seen, err := p.Provider.Observe(ctx, remote)
	p.probe.mu.Lock()
	confirmed := p.probe.confirmed
	p.probe.mu.Unlock()
	if err == nil && confirmed {
		seen.Execution, seen.RemoteActivity, seen.ReleaseEvidence = domain.ExecutionCancelled, domain.RemoteActivityInactive, domain.ReleaseEvidenceProviderReported
	}
	return seen, err
}

func (g *hostGuardFixture) cancelProbe(t *testing.T, lost bool) *healthCancelProbe {
	t.Helper()
	probe := &healthCancelProbe{lost: lost}
	g.f.wrapProvider = func(p provider.Provider) provider.Provider { return &healthCancelProvider{p, probe} }
	g.f.bindFake(t)
	return probe
}

func TestHostHealthRunningWarningUnknownContinueAndCriticalStopsOnce(t *testing.T) {
	for _, danger := range []string{"critical", "battery"} {
		t.Run(danger, func(t *testing.T) {
			g := newHostGuardFixture(t, "ok")
			probe := g.cancelProbe(t, false)
			job := g.running(t)
			for _, mode := range []string{"warning", "unknown", "failed"} {
				g.sampler.set(mode)
				g.f.advance(t, hosthealth.SampleCadence)
				requireNoError(t, g.guard.RunOnce(t.Context()))
				requireNoError(t, g.step(t))
				if r := g.read(t, job); r.Attempt.State.Execution != domain.ExecutionRunning || r.Attempt.State.Cancellation != domain.CancellationNotRequested || probe.calls != 0 {
					t.Fatal("warning or unknown stopped running work", mode, r.Attempt.State)
				}
			}
			g.sampler.set(danger)
			for i := range 3 {
				g.f.advance(t, hosthealth.SampleCadence)
				requireNoError(t, g.guard.RunOnce(t.Context()))
				r := g.read(t, job)
				if danger == "critical" && i < 2 && r.Attempt.State.Cancellation != domain.CancellationNotRequested {
					t.Fatal("unmeasured elapsed time was treated as sustained danger")
				}
				if danger == "battery" {
					break
				}
			}
			r := g.read(t, job)
			if r.Attempt.State.Cancellation != domain.CancellationRequested || r.Problem != nil && r.Problem.Details[domain.ReasonDetail] == "host_critical_stopped" || probe.calls != 0 {
				t.Fatal("stop request fabricated confirmed termination", r.Attempt.State, r.Problem)
			}
			requireNoError(t, g.step(t))
			r = g.read(t, job)
			if r.Attempt.State.Execution != domain.ExecutionCancelled || r.Problem == nil || r.Problem.Details[domain.ReasonDetail] != "host_critical_stopped" || probe.calls != 1 || r.Attempt.ID != job.AttemptID || r.Attempt.Number != 1 {
				t.Fatal("confirmed fenced cancellation lost origin or repeated", r.Attempt.State, r.Problem, probe.calls)
			}
			g.f.reopen(t)
			g.bind(t)
			requireNoError(t, g.guard.RunOnce(t.Context()))
			if r := g.read(t, job); r.Problem == nil || r.Problem.Details[domain.ReasonDetail] != "host_critical_stopped" || probe.calls != 1 || g.f.backend.Stats().SubmitCalls != 1 || g.f.backend.Stats().PrepareCalls != 1 {
				t.Fatal("restart repeated mutation or lost stable stop reason")
			}
		})
	}
}

func TestHostHealthRestartResetsSustainedWindowAndLostCancelNeverRepeats(t *testing.T) {
	g := newHostGuardFixture(t, "ok")
	probe := g.cancelProbe(t, true)
	job := g.running(t)
	g.sampler.set("critical")
	for range 2 {
		g.f.advance(t, hosthealth.SampleCadence)
		requireNoError(t, g.guard.RunOnce(t.Context()))
	}
	g.f.reopen(t)
	g.bind(t)
	for i := range 3 {
		g.f.advance(t, hosthealth.SampleCadence)
		requireNoError(t, g.guard.RunOnce(t.Context()))
		if i < 2 && g.read(t, job).Attempt.State.Cancellation != domain.CancellationNotRequested {
			t.Fatal("restart counted critical history from the old process")
		}
	}
	requireNoError(t, g.step(t))
	if r := g.read(t, job); probe.calls != 1 || r.Problem != nil && r.Problem.Details[domain.ReasonDetail] == "host_critical_stopped" || r.Attempt.State.Execution.Terminal() || r.Attempt.State.Cancellation != domain.CancellationManual {
		t.Fatal("lost acknowledgement invented stop", r.Attempt.State, r.Problem)
	}
	g.f.reopen(t)
	g.bind(t)
	g.f.advance(t, time.Minute)
	_, _ = g.f.services.dispatcher.RunOnce(t.Context(), "health-restart-observer")
	if probe.calls != 1 || g.f.backend.Stats().SubmitCalls != 1 || g.read(t, job).Attempt.ID != job.AttemptID {
		t.Fatal("restart repeated a stop or compute mutation", probe.calls, g.f.backend.Stats())
	}
	probe.mu.Lock()
	probe.confirmed = true
	probe.mu.Unlock()
	_, err := g.f.controls(t).Submit(t.Context(), g.f.actor, "app", job.JobID, domain.OperationReconcile, "health-stop-reconcile", systemControlBody(t, job.AttemptID))
	requireNoError(t, err)
	requireNoError(t, g.step(t))
	if r := g.read(t, job); r.Attempt.State.Execution != domain.ExecutionCancelled || r.Problem == nil || r.Problem.Details[domain.ReasonDetail] != "host_critical_stopped" || probe.calls != 1 {
		t.Fatal("confirmed observation lost system-stop origin", r.Attempt.State, r.Problem)
	}
}

func TestHostHealthUserCancellationCannotForgeCriticalStopReason(t *testing.T) {
	g := newHostGuardFixture(t, "ok")
	probe := g.cancelProbe(t, false)
	job := g.running(t)
	_, err := g.f.controls(t).Submit(t.Context(), g.f.actor, "app", job.JobID, domain.OperationCancel, "user-health-looking-reason", systemJSON(t, operations.Request{AttemptID: job.AttemptID, Reason: "host_critical_stopped"}))
	requireNoError(t, err)
	requireNoError(t, g.step(t))
	r := g.read(t, job)
	if r.Attempt.State.Execution != domain.ExecutionCancelled || probe.calls != 1 || r.Problem != nil && r.Problem.Details[domain.ReasonDetail] == "host_critical_stopped" {
		t.Fatal("user cancellation was projected as a system critical stop", r.Problem)
	}
}

type hostHealthMutationRace struct {
	*sqlite.Store
	beforeConsume func(context.Context)
	beforeCommit  func(context.Context, dispatch.Action)
}

func (r *hostHealthMutationRace) ConsumeAuthorization(ctx context.Context, handle dispatch.Handle) (dispatch.Work, error) {
	if r.beforeConsume != nil {
		r.beforeConsume(ctx)
	}
	return r.Store.ConsumeAuthorization(ctx, handle)
}

func (r *hostHealthMutationRace) CommitDispatch(ctx context.Context, handle dispatch.Handle, action dispatch.Action) (dispatch.Work, error) {
	if r.beforeCommit != nil {
		r.beforeCommit(ctx, action)
	}
	return r.Store.CommitDispatch(ctx, handle, action)
}

func TestHostHealthFinalMutationTransactionsRejectNewerWarning(t *testing.T) {
	for _, boundary := range []string{"authorization", "preparation", "start"} {
		t.Run(boundary, func(t *testing.T) {
			g := newHostGuardFixture(t, "ok")
			spec := g.spec(t)
			if boundary == "start" {
				spec.ExecutionControl = "prepare_then_start"
			}
			job := g.admit(t, "guard-race-"+boundary, spec)
			var grant executionauth.GateReceipt
			if boundary == "start" {
				id := scheduler.Identity{WorkspaceID: "app", JobID: job.JobID, AttemptID: job.AttemptID}
				service, actor, ready := systemReadyGate(t, g.f, id)
				var err error
				grant, err = service.Start(t.Context(), actor, "app", job.JobID, "guard-race-start-grant", systemJSON(t, executionauth.StartRequest{AttemptID: job.AttemptID, ReadinessRevision: ready.Revision, MaxRemoteWallSeconds: 300, ExecutionDeadline: g.f.fixtureClock().Now().Add(299 * time.Second).UTC().Format(time.RFC3339), AuthorizeCompute: true}))
				requireNoError(t, err)
			} else {
				g.f.authorizeJob(t, job)
			}
			inject := func(ctx context.Context) {
				g.sampler.set("warning")
				target, err := g.f.host.store.ResolveHostHealthTarget(ctx, "app", g.f.actor.TokenID(), g.connection.ID)
				requireNoError(t, err)
				snapshot, err := g.sampler.Sample(ctx, target)
				requireNoError(t, err)
				requireNoError(t, g.f.host.store.RecordHostHealth(ctx, snapshot))
			}
			repository := &hostHealthMutationRace{Store: g.f.host.store}
			if boundary == "authorization" {
				repository.beforeConsume = inject
			} else {
				repository.beforeCommit = func(ctx context.Context, action dispatch.Action) {
					if boundary == "preparation" && action.Kind == dispatch.BeginPreparation || boundary == "start" && action.Kind == dispatch.BeginSubmission {
						inject(ctx)
					}
				}
			}
			g.f.wrapDispatch = func(*sqlite.Store) dispatch.Repository { return repository }
			g.f.clockedServices(t)
			g.f.services.dispatcher.WithNewWorkGuard(g.guard)
			if err := g.step(t); !errors.Is(err, admission.ErrHostUnhealthy) {
				t.Fatal("final transaction accepted older healthy proof", err)
			}
			r := g.read(t, job)
			if r.Problem == nil || r.Problem.Details[domain.ReasonDetail] != "host_unhealthy" || g.f.backend.Stats().SubmitCalls != 0 || boundary != "start" && g.f.backend.Stats().PrepareCalls != 0 {
				t.Fatal("race caused mutation or lost refusal", r.Problem, g.f.backend.Stats())
			}
			if boundary == "start" {
				current, err := g.f.services.authorizations.GateAuthorization(t.Context(), g.f.actor, "app", job.JobID, grant.AuthorizationID)
				if err != nil || current.Status != executionauth.Granted {
					t.Fatal("raced refusal consumed start grant", current, err)
				}
			}
		})
	}
}

func TestHostHealthRunningStopUsesFrozenPolicyAfterConfigureAndDisable(t *testing.T) {
	g := newHostGuardFixture(t, "ok")
	probe := g.cancelProbe(t, false)
	job := g.running(t)
	original := g.read(t, job).Profile
	body := systemJSON(t, map[string]any{"action": "configure", "expected_revision": g.connection.Revision,
		"configuration": map[string]any{"max_remote_wall_seconds": 600, "host_health": map[string]any{"warning_thermal_level": 100, "critical_thermal_level": 200, "low_battery_percent": 0}}})
	_, err := g.f.services.connections.Submit(t.Context(), g.f.actor, "app", g.connection.ID, "guard-permissive-current-policy", body)
	requireNoError(t, err)
	current, err := g.f.services.connections.Get(t.Context(), g.f.actor, "app", g.connection.ID)
	requireNoError(t, err)
	_, err = g.f.services.connections.Submit(t.Context(), g.f.actor, "app", current.ID, "guard-disable-running", systemJSON(t, map[string]any{"action": "disable", "expected_revision": current.Revision}))
	requireNoError(t, err)
	worked, err := g.f.services.connections.RunOnce(t.Context())
	if !worked || err != nil {
		t.Fatal("disable running fixture", worked, err)
	}
	g.sampler.set("critical")
	for range 3 {
		g.f.advance(t, hosthealth.SampleCadence)
		requireNoError(t, g.guard.RunOnce(t.Context()))
	}
	if r := g.read(t, job); r.Profile != original || r.Attempt.State.Cancellation != domain.CancellationRequested || probe.calls != 0 {
		t.Fatal("current permissive/disabled connection changed frozen running protection", r.Attempt.State)
	}
	requireNoError(t, g.step(t))
	if r := g.read(t, job); r.Problem == nil || r.Problem.Details[domain.ReasonDetail] != "host_critical_stopped" || probe.calls != 1 {
		t.Fatal("frozen policy stop did not reach ordinary cancellation")
	}
}

func TestHostHealthPossibleSubmissionStopsAfterIdentityRecoveryWithoutResubmit(t *testing.T) {
	g := newHostGuardFixture(t, "ok")
	scenario := fake.DefaultScenario()
	scenario.Mode, scenario.States = fake.AcceptLoseResponse, []domain.ExecutionState{domain.ExecutionRunning}
	g.f.useFake(t, scenario)
	g.bind(t)
	probe := g.cancelProbe(t, false)
	job := g.admit(t, "guard-ambiguous-submission", g.spec(t))
	g.f.authorizeJob(t, job)
	requireNoError(t, g.step(t))
	requireNoError(t, g.step(t))
	if r := g.read(t, job); r.Attempt.State.RemoteActivity != domain.RemoteActivityPossible {
		t.Fatal("fixture did not retain ambiguous submission")
	}
	g.sampler.set("battery")
	g.f.advance(t, hosthealth.SampleCadence)
	requireNoError(t, g.guard.RunOnce(t.Context()))
	if g.read(t, job).Attempt.State.Cancellation != domain.CancellationRequested || probe.calls != 0 {
		t.Fatal("possible execution was not protected or cancellation preceded identity recovery")
	}
	requireNoError(t, g.step(t))
	g.f.advance(t, 20*time.Second)
	requireNoError(t, g.step(t))
	if r := g.read(t, job); r.Attempt.State.Execution != domain.ExecutionCancelled || r.Problem == nil || r.Problem.Details[domain.ReasonDetail] != "host_critical_stopped" || probe.calls != 1 || g.f.backend.Stats().SubmitCalls != 1 || g.f.backend.Stats().PrepareCalls != 1 || r.Attempt.ID != job.AttemptID {
		t.Fatal("ambiguous recovery repeated mutation or lost confirmed stop", r.Attempt.State, r.Problem, probe.calls, g.f.backend.Stats())
	}
}

type hostHealthWorkerRepository struct {
	*sqlite.Store
	failure error
	read    chan struct{}
}

func (r *hostHealthWorkerRepository) ActiveHostHealthTargets(ctx context.Context) ([]hosthealth.Target, error) {
	if r.read != nil {
		select {
		case r.read <- struct{}{}:
		default:
		}
	}
	if r.failure != nil {
		return nil, r.failure
	}
	return r.Store.ActiveHostHealthTargets(ctx)
}

func TestHostHealthWorkerTransientFailurePreservesIndependentStopAndRun(t *testing.T) {
	g := newHostGuardFixture(t, "ok")
	job := g.running(t)
	repository := &hostHealthWorkerRepository{Store: g.f.host.store}
	health, err := hosthealth.NewService(g.f.host.access, repository, g.sampler, g.f.fixtureClock())
	requireNoError(t, err)
	g.guard.health = health
	target, err := g.f.host.store.HostHealthTargetForAttempt(t.Context(), g.read(t, job))
	requireNoError(t, err)
	g.sampler.set("critical")
	for range 3 {
		g.f.advance(t, hosthealth.SampleCadence)
		_, err := health.Refresh(t.Context(), *target)
		requireNoError(t, err)
	}
	repository.failure = sqlite.ErrBusy
	if err := g.guard.RunOnce(t.Context()); !errors.Is(err, sqlite.ErrBusy) || g.read(t, job).Attempt.State.Cancellation != domain.CancellationRequested {
		t.Fatal("transient sampling failure skipped independently proven danger", err)
	}
	repository.read = make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- g.guard.Run(ctx) }()
	<-repository.read
	select {
	case err := <-done:
		t.Fatal("transient state failure stopped runtime guard", err)
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("guard did not join cancellation", err)
	}
	repository.failure = sqlite.ErrCorrupt
	if err := g.guard.Run(t.Context()); !errors.Is(err, sqlite.ErrCorrupt) {
		t.Fatal("corrupt local state did not stop runtime guard", err)
	}
}

var _ dispatch.NewWorkGuard = (*managedHostHealthGuard)(nil)
