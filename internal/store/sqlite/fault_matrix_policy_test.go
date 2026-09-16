package sqlite

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

func TestFaultMatrixCancellationWithoutRemoteIdentityStaysUnresolved(t *testing.T) {
	f := newDispatchFixture(t, fake.Scenario{
		Mode: fake.Unresolved, States: fake.DefaultScenario().States,
		PreparationReady: true, GPU: domain.CapabilitySupportUnsupported,
	})
	id := f.seed(t, "a", 1, false)
	for i := 0; i < 2; i++ {
		if err := f.step(t); err != nil {
			t.Fatal(err)
		}
	}
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	op := cancelControl(t, f, service, actor, id)
	probe := installCancelProbe(t, f, true, provider.CancellationOutcome{Status: domain.CancellationAccepted})
	for i := 0; i < dispatch.MaxFailures; i++ {
		if err := f.step(t); err == nil {
			t.Fatal("missing remote identity was silently resolved")
		}
	}
	current, err := service.Get(context.Background(), actor, "a", op.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	state := f.state(t, id)
	if current.TerminationConfirmed || state.Cancellation != domain.CancellationManual || state.Execution != domain.ExecutionUnknown || state.RemoteActivity != domain.RemoteActivityPossible || probe.calls.Load() != 0 || f.journal(t, id).Remote != nil {
		t.Fatal("missing session identity authorized or confirmed cancellation")
	}
	if f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("cancellation redispatched unknown compute")
	}
}

func TestFaultMatrixQuotaConsumedAfterPositiveObservation(t *testing.T) {
	ctx := context.Background()
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Reject
	f := newDispatchFixture(t, scenario)
	id := f.seed(t, "a", 1, false)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(time.Minute)
	limit, used, remaining := 3600.0, 0.0, 3600.0
	q := provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "cpu", Unit: "seconds", Limit: &limit, Used: &used, Remaining: &remaining, ObservedAt: f.clock.Now(), Source: "synthetic previous allowance", Precision: "exact"}
	if err := f.s.RecordQuota(ctx, f.profile.AccountScope, "cpu", q, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	// External consumption is simulated by a provider-proven rejection after a
	// positive read, not by querying or consuming a real account's allowance.
	f.adapter.rejectQuota = true
	worked, err := f.engine.RunOnce(ctx, "quota-change")
	if err != nil || !worked {
		t.Fatal("fixture did not reach the rejection", err)
	}
	j := f.journal(t, id)
	if j.Problem == nil || j.Problem.Code != domain.CodeQuotaExhausted || j.Problem.ComputeMayHaveStarted || f.state(t, id).Execution != domain.ExecutionNotSubmitted {
		t.Fatal("fresh quota read overrode proven non-acceptance")
	}
	f.seed(t, "b", 2, false)
	f.clock.advance(time.Minute)
	worked, err = f.engine.RunOnce(ctx, "must-stay-blocked")
	if err != nil || worked || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 0 {
		t.Fatal("quota race retried or overcommitted compute", err)
	}
}
