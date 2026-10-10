package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type preparationProgressProbe struct {
	*probeProvider
	during func(context.Context)
}

func (p *preparationProgressProbe) Prepare(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.Prepared, error) {
	p.during(ctx)
	return p.probeProvider.Prepare(ctx, plan, id)
}

func TestPreparationProgressIsFencedMonotonicAndShownOnlyWhilePreparing(t *testing.T) {
	ctx := context.Background()
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	access, err := auth.New(f.s, f.s, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := access.Issue(ctx, "a", []auth.Scope{auth.Read}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	read := func() *domain.PreparationStatus {
		t.Helper()
		record, err := f.s.ReadJob(ctx, "a", token.ID, id.JobID)
		if err != nil {
			t.Fatal(err)
		}
		return record.Preparation
	}
	if read() != nil {
		t.Fatal("queued job reported preparation progress")
	}
	var seen []int64
	probe := &preparationProgressProbe{probeProvider: f.adapter, during: func(c context.Context) {
		// 30 regresses and is rejected; 60 arrives within the sampling interval after 50 and is skipped.
		for _, step := range []struct {
			completed int64
			wait      time.Duration
		}{{0, 2 * time.Second}, {40, 2 * time.Second}, {30, 2 * time.Second}, {50, 0}, {60, 2 * time.Second}, {100, 0}} {
			provider.ReportPreparationProgress(c, step.completed, 100)
			f.clock.advance(step.wait)
			status := read()
			if status == nil || status.Progress.BytesTotal != 100 || status.Progress.Scope != "staged_input_bytes" {
				t.Fatalf("preparing status lacks progress: %+v", status)
			}
			seen = append(seen, status.Progress.BytesCompleted)
		}
	}}
	registry := provider.NewSnapshotRegistry()
	if err = registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe); err != nil {
		t.Fatal(err)
	}
	engine, err := dispatch.New(f.s, registry, f.blobs, nil, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	before := f.backend.Stats()
	if worked, err := engine.RunOnce(ctx, "progress-worker"); err != nil || !worked {
		t.Fatal("preparation failed", err)
	}
	if want := []int64{0, 40, 40, 50, 50, 100}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("recorded samples %v, want %v", seen, want)
	}
	if status := read(); f.journal(t, id).Phase != dispatch.Ready || status == nil || status.Progress.BytesCompleted != 100 {
		t.Fatal("final sample missing before submission")
	}
	if after := f.backend.Stats(); after.PrepareCalls != before.PrepareCalls+1 || after.SubmitCalls != before.SubmitCalls {
		t.Fatalf("progress changed provider effects: %+v", after)
	}

	f.clock.advance(time.Minute)
	claim, err := f.s.ClaimRecovery(ctx, "manual", f.clock.Now())
	if err != nil || claim == nil {
		t.Fatal("ready attempt was not claimable", err)
	}
	work, err := f.s.LoadDispatch(ctx, *claim, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	sample := domain.PreparationProgress{Scope: "staged_input_bytes", Generation: work.Handle.Claim.Generation, BytesTotal: 100, ObservedAt: f.clock.Now()}
	if err = f.s.RecordPreparationProgress(ctx, work.Handle, sample, f.clock.Now()); !errors.Is(err, dispatch.ErrConflict) {
		t.Fatalf("progress accepted outside staging: %v", err)
	}
	stale := work.Handle
	stale.Claim.Generation, sample.Generation = stale.Claim.Generation+1, stale.Claim.Generation+1
	if err = f.s.RecordPreparationProgress(ctx, stale, sample, f.clock.Now()); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatalf("stale lease recorded progress: %v", err)
	}
	if err = f.s.YieldDispatch(ctx, work.Handle, f.clock.Now(), 0); err != nil {
		t.Fatal(err)
	}
	if err = f.step(t); err != nil {
		t.Fatal(err)
	}
	if f.journal(t, id).Phase != dispatch.Submitted || read() != nil {
		t.Fatal("progress outlived preparation")
	}
}
