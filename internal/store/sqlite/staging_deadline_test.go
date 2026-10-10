package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

type stagingDeadlineProbe struct {
	*probeProvider
	bindingDeadline time.Time
	prepareDeadline time.Time
	stagingDeadline time.Time
	submitDeadline  time.Time
}

func (p *stagingDeadlineProbe) VerifyBinding(ctx context.Context, binding provider.BindingSnapshot) error {
	p.bindingDeadline, _ = ctx.Deadline()
	return p.probeProvider.VerifyBinding(ctx, binding)
}

func (p *stagingDeadlineProbe) Prepare(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.Prepared, error) {
	p.prepareDeadline, _ = ctx.Deadline()
	return p.probeProvider.Prepare(ctx, plan, id)
}

func (p *stagingDeadlineProbe) ReconcilePreparation(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.PreparationObservation, error) {
	p.stagingDeadline, _ = ctx.Deadline()
	return p.probeProvider.ReconcilePreparation(ctx, plan, id)
}

func (p *stagingDeadlineProbe) Submit(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
	p.submitDeadline, _ = ctx.Deadline()
	return p.probeProvider.Submit(ctx, prepared)
}

func TestStagingObservationAndSubmissionKeepPreparationBudgetAndParentDeadline(t *testing.T) {
	for _, parentBudget := range []time.Duration{10 * time.Minute, 2 * time.Minute} {
		t.Run(parentBudget.String(), func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			f.adapter.losePrepare = true
			if err := f.step(t); err == nil {
				t.Fatal("fixture did not lose the original preparation acknowledgement")
			}
			if f.journal(t, id).Phase != dispatch.Staging {
				t.Fatal("fixture did not retain original staging intent")
			}
			probe := &stagingDeadlineProbe{probeProvider: f.adapter}
			registry := provider.NewSnapshotRegistry()
			if err := registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe); err != nil {
				t.Fatal(err)
			}
			config := dispatch.DefaultConfig()
			engine, err := dispatch.New(f.s, registry, f.blobs, nil, f.clock, config)
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []dispatch.Phase{dispatch.Staging, dispatch.Ready} {
				if f.journal(t, id).Phase != phase {
					t.Fatalf("expected %s before callback", phase)
				}
				f.clock.advance(10 * time.Minute)
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), parentBudget)
				parentDeadline, _ := ctx.Deadline()
				worked, err := engine.RunOnce(ctx, "deadline-worker")
				cancel()
				if err != nil || !worked {
					t.Fatal("finite fixture callback failed", err)
				}
				deadline := probe.stagingDeadline
				if phase == dispatch.Ready {
					deadline = probe.submitDeadline
				}
				expected := min(config.PreparationTimeout, parentBudget)
				remaining := deadline.Sub(start)
				if remaining < expected-5*time.Second || remaining > expected+5*time.Second || deadline.After(parentDeadline) {
					t.Fatalf("%s deadline budget=%s want=%s bounded by parent=%s", phase, remaining, expected, parentBudget)
				}
				if parentBudget < config.PreparationTimeout && !deadline.Equal(parentDeadline) {
					t.Fatalf("%s did not retain the shorter parent deadline", phase)
				}
				bindingBudget := probe.bindingDeadline.Sub(start)
				if bindingBudget < config.ControlTimeout-5*time.Second || bindingBudget > config.ControlTimeout+5*time.Second {
					t.Fatalf("ordinary binding verification lost its control budget: %s", bindingBudget)
				}
			}
			stats := f.backend.Stats()
			if stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 {
				t.Fatalf("deadline handling repeated original preparation/submission: %+v", stats)
			}
		})
	}
}

func TestOriginalPreparationOutlastsTheFormerFiveMinuteBudget(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	f.seed(t, "a", 1, false)
	probe := &stagingDeadlineProbe{probeProvider: f.adapter}
	registry := provider.NewSnapshotRegistry()
	if err := registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe); err != nil {
		t.Fatal(err)
	}
	engine, err := dispatch.New(f.s, registry, f.blobs, nil, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if worked, err := engine.RunOnce(context.Background(), "upload-worker"); err != nil || !worked {
		t.Fatal("original preparation failed", err)
	}
	if budget := probe.prepareDeadline.Sub(start); budget < 55*time.Minute || budget > time.Hour+5*time.Second {
		t.Fatalf("original upload/creation budget is %s", budget)
	}
	if stats := f.backend.Stats(); stats.PrepareCalls != 1 {
		t.Fatalf("preparation repeated: %+v", stats)
	}
}
