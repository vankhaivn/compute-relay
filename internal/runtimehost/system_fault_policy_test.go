package runtimehost

import (
	"context"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestSystemFaultMatrixCancellationWithoutRemoteIdentityStaysUnresolved(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.Scenario{
		Mode: fake.Unresolved, States: fake.DefaultScenario().States,
		PreparationReady: true, GPU: domain.CapabilitySupportUnsupported,
	})
	id := f.dispatchJob(t, "app", 1)
	for i := 0; i < 2; i++ {
		if err := f.dispatchStep(t); err != nil {
			t.Fatal(err)
		}
	}
	service, actor, _ := systemControlService(t, f, "app", auth.Read, auth.Operate)
	op := systemCancelControl(t, f, service, actor, id)
	probe := installSystemCancelProbe(t, f, effects, true, provider.CancellationOutcome{Status: domain.CancellationAccepted})
	for i := 0; i < dispatch.MaxFailures; i++ {
		if err := f.dispatchStep(t); err == nil {
			t.Fatal("missing remote identity was silently resolved")
		}
	}
	current, err := service.Get(context.Background(), actor, "app", op.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	state := f.dispatchState(t, id)
	if current.TerminationConfirmed || state.Cancellation != domain.CancellationManual || state.Execution != domain.ExecutionUnknown || state.RemoteActivity != domain.RemoteActivityPossible || probe.calls.Load() != 0 || trace.journal(t, id).Remote != nil {
		t.Fatal("missing session identity authorized or confirmed cancellation")
	}
	if f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("cancellation redispatched unknown compute")
	}
}

func TestSystemFaultMatrixQuotaConsumedAfterPositiveObservation(t *testing.T) {
	ctx := context.Background()
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Reject
	f, effects, trace := newSystemDispatch(t, scenario)
	id := f.dispatchJob(t, "app", 1)
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	f.advance(t, time.Minute)
	limit, used, remaining := 3600.0, 0.0, 3600.0
	q := provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "cpu", Unit: "seconds", Limit: &limit, Used: &used, Remaining: &remaining, ObservedAt: f.fixtureClock().Now(), Source: "synthetic previous allowance", Precision: "exact"}
	if err := f.host.store.RecordQuota(ctx, f.dispatchRecord(t, id).Profile.AccountScope, "cpu", q); err != nil {
		t.Fatal(err)
	}
	// External consumption is simulated by a provider-proven rejection after a
	// positive read, not by querying or consuming a real account's allowance.
	effects.rejectQuota = true
	worked, err := f.services.dispatcher.RunOnce(ctx, "quota-change")
	if err != nil || !worked {
		t.Fatal("fixture did not reach the rejection", err)
	}
	j := trace.journal(t, id)
	if j.Problem == nil || j.Problem.Code != domain.CodeQuotaExhausted || j.Problem.ComputeMayHaveStarted || f.dispatchState(t, id).Execution != domain.ExecutionNotSubmitted {
		t.Fatal("fresh quota read overrode proven non-acceptance")
	}
	f.dispatchJob(t, "other", 2)
	f.advance(t, time.Minute)
	worked, err = f.services.dispatcher.RunOnce(ctx, "must-stay-blocked")
	if err != nil || worked || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 0 {
		t.Fatal("quota race retried or overcommitted compute", err)
	}
}
