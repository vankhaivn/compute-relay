package runtimehost

import (
	"context"
	"errors"
	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"testing"
	"time"
)

func TestSystemOrderlyShutdownDrainsOnlyIntendedPrepare(t *testing.T) {
	f, e, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	e.prepareEntered = make(chan struct{})
	e.prepareRelease = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() { _, err := f.services.dispatcher.RunOnce(ctx, "draining-prepare"); returned <- err }()
	select {
	case <-e.prepareEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("prepare did not enter")
	}
	cancel()
	select {
	case err := <-returned:
		t.Fatal("shutdown interrupted intended Prepare", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(e.prepareRelease)
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prepare did not drain")
	}
	j := trace.journal(t, id)
	if j.Phase != dispatch.Ready || j.SubmitStarted || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("drain changed authority", j, f.backend.Stats())
	}
	if worked, err := f.services.dispatcher.RunOnce(ctx, "cancelled-dispatch"); worked || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled runtime acquired new work", worked, err)
	}
	f.reopen(t)
	if f.backend.Stats().PrepareCalls != 1 {
		t.Fatal("restart repeated Prepare")
	}
}
func TestSystemObservationPreservesTypedProviderFault(t *testing.T) {
	for _, code := range []domain.ErrorCode{domain.CodeRemoteIdentityMismatch, domain.CodeRemoteExecutionUnresolved} {
		t.Run(string(code), func(t *testing.T) {
			f, e, trace := newSystemDispatch(t, fake.DefaultScenario())
			id := f.dispatchJob(t, "app", 1)
			requireNoError(t, f.dispatchStep(t))
			requireNoError(t, f.dispatchStep(t))
			stage := domain.FailureStageObservation
			if code == domain.CodeRemoteExecutionUnresolved {
				stage = domain.FailureStageOperation
			}
			e.observe = func(context.Context, provider.RemoteReference, provider.Observation) (provider.Observation, error) {
				return provider.Observation{}, provider.Problem(code, stage, "synthetic bounded observation failure")
			}
			for n := 0; n < dispatch.MaxFailures && trace.journal(t, id).Phase != dispatch.Attention; n++ {
				if f.dispatchStep(t) == nil {
					t.Fatal("fault hidden")
				}
			}
			j := trace.journal(t, id)
			state := f.dispatchState(t, id)
			if j.Phase != dispatch.Attention || j.Problem.Code != code || !scheduler.HoldsAccount(state, true) || f.backend.Stats().SubmitCalls != 1 {
				t.Fatal("provider identity fault masked or authority released", j, state)
			}
		})
	}
}

func TestSystemExactAbsenceFreesCapacityWithoutInventingOutcome(t *testing.T) {
	f, e, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	requireNoError(t, f.dispatchStep(t))
	requireNoError(t, f.dispatchStep(t))
	requireNoError(t, e.advance(t, f, *trace.journal(t, id).Remote))
	requireNoError(t, f.dispatchStep(t))
	original := trace.journal(t, id)
	if f.dispatchState(t, id).Execution != domain.ExecutionRunning {
		t.Fatal("fixture was not running")
	}
	e.observe = func(_ context.Context, remote provider.RemoteReference, _ provider.Observation) (provider.Observation, error) {
		return provider.Observation{}, provider.RemoteAbsence{Remote: remote, ObservedAt: f.fixtureClock().Now()}
	}
	requireNoError(t, f.dispatchStep(t))
	j, state := trace.journal(t, id), f.dispatchState(t, id)
	if j.Phase != dispatch.Attention || j.RemoteAbsence() == nil || j.RemoteAbsence().Remote != *original.Remote || state.Execution != domain.ExecutionUnknown || state.RemoteActivity != domain.RemoteActivityInactive || state.ReleaseEvidence != domain.ReleaseEvidenceProviderReported || scheduler.HoldsAccount(state, true) {
		t.Fatal("exact absence did not separate release and outcome", j, state)
	}
	if j.Problem.Code != domain.CodeRemoteExecutionUnresolved || j.Problem.PublicDetails()[domain.ReasonDetail] != "remote_not_found" || j.Problem.SafeOperationRetry || !j.Problem.ComputeMayHaveStarted {
		t.Fatal("absence problem invented outcome", j.Problem)
	}
	f.dispatchJob(t, "app", 2)
	requireNoError(t, f.dispatchStep(t))
	if f.backend.Stats().PrepareCalls != 2 || f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("next attempt blocked or old attempt replayed", f.backend.Stats())
	}
}

func TestHostHealthUnknownDefersSamePreparation(t *testing.T) {
	g := newHostGuardFixture(t, "ok")
	job := g.admit(t, "unknown-health-preparation", g.spec(t))
	g.f.authorizeJob(t, job)
	g.sampler.set("unknown")
	g.f.advance(t, hosthealth.DemandCache)
	if err := g.step(t); !errors.Is(err, admission.ErrHostUnhealthy) {
		t.Fatal("unknown health did not refuse safely", err)
	}
	if state := g.read(t, job).Attempt.State; state.Orchestration != domain.OrchestrationQueued || state.Execution != domain.ExecutionNotSubmitted || g.f.backend.Stats().PrepareCalls != 0 {
		t.Fatal("unknown health terminalized the attempt", state)
	}
	g.sampler.set("ok")
	g.f.advance(t, 40*time.Second)
	requireNoError(t, g.step(t))
	if g.f.backend.Stats().PrepareCalls != 1 || g.read(t, job).Attempt.ID != job.AttemptID {
		t.Fatal("health recovery replaced attempt")
	}
}

func TestSystemExhaustedPreparationEndsWithoutComputeAndUnblocksNextAttempt(t *testing.T) {
	f, e, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	e.losePrepare = true
	if f.dispatchStep(t) == nil {
		t.Fatal("preparation fault not injected")
	}
	if trace.journal(t, id).Phase != dispatch.Staging {
		t.Fatal("failed Prepare did not reconcile original intent")
	}
	e.verify = func(context.Context, provider.BindingSnapshot) error {
		return provider.Problem(domain.CodeProviderUnreachable, domain.FailureStageObservation, "synthetic transient observation outage")
	}
	f.advance(t, dispatch.PreparationObservationGrace+time.Second)
	if f.dispatchStep(t) == nil {
		t.Fatal("unanswered observation lost failure")
	}
	if trace.journal(t, id).Phase != dispatch.Staging || f.dispatchState(t, id).Orchestration.Terminal() {
		t.Fatal("an outage ended observation at the grace")
	}
	f.advance(t, dispatch.DefaultConfig().PreparationTimeout)
	if f.dispatchStep(t) == nil {
		t.Fatal("exhausted observation lost failure")
	}
	state := f.dispatchState(t, id)
	j := trace.journal(t, id)
	if state.Orchestration != domain.OrchestrationFailed || state.Execution != domain.ExecutionNotSubmitted || state.RemoteActivity != domain.RemoteActivityNotStarted || state.ReleaseEvidence != domain.ReleaseEvidenceNotApplicable || scheduler.HoldsAccount(state, true) || j.Problem.PublicDetails()[domain.ReasonDetail] != "staging_unconfirmed" {
		t.Fatal("exhausted staging has no actionable terminal exit", j, state)
	}
	e.verify = nil
	e.losePrepare = false
	f.dispatchJob(t, "app", 2)
	requireNoError(t, f.dispatchStep(t))
	if f.backend.Stats().PrepareCalls != 2 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("old intent replayed or next authority stranded", f.backend.Stats())
	}
}
