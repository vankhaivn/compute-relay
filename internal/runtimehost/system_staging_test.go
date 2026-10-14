package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestSystemStagingObservationAndSubmissionKeepPreparationBudgetAndParentDeadline(t *testing.T) {
	for _, parentBudget := range []time.Duration{10 * time.Minute, 2 * time.Minute} {
		t.Run(parentBudget.String(), func(t *testing.T) {
			f, e, trace := newSystemDispatch(t, fake.DefaultScenario())
			id := f.dispatchJob(t, "app", 1)
			e.losePrepare = true
			if err := f.dispatchStep(t); err == nil {
				t.Fatal("original preparation ACK not lost")
			}
			if trace.journal(t, id).Phase != dispatch.Staging {
				t.Fatal("original staging intent lost")
			}
			e.deadlines = &systemStagingDeadlines{}
			config := dispatch.DefaultConfig()
			for _, phase := range []dispatch.Phase{dispatch.Staging, dispatch.Ready} {
				if trace.journal(t, id).Phase != phase {
					t.Fatal("wrong phase before callback", phase)
				}
				f.advance(t, 10*time.Minute)
				start := time.Now()
				ctx, cancel := context.WithTimeout(t.Context(), parentBudget)
				parentDeadline, _ := ctx.Deadline()
				worked, err := f.services.dispatcher.RunOnce(ctx, "deadline-worker")
				cancel()
				if err != nil || !worked {
					t.Fatal("finite fixture callback failed", worked, err)
				}
				deadline := e.deadlines.staging
				if phase == dispatch.Ready {
					deadline = e.deadlines.start
				}
				expected := min(config.PreparationTimeout, parentBudget)
				remaining := deadline.Sub(start)
				if remaining < expected-5*time.Second || remaining > expected+5*time.Second || deadline.After(parentDeadline) {
					t.Fatal("incorrect bounded callback deadline", phase, remaining, expected)
				}
				if parentBudget < config.PreparationTimeout && !deadline.Equal(parentDeadline) {
					t.Fatal("shorter parent deadline lost", phase)
				}
				if b := e.deadlines.binding.Sub(start); b < config.ControlTimeout-5*time.Second || b > config.ControlTimeout+5*time.Second {
					t.Fatal("binding verification lost control budget", b)
				}
			}
			if stats := f.backend.Stats(); stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 {
				t.Fatal("callbacks repeated mutation", stats)
			}
		})
	}
}
func TestSystemOriginalPreparationOutlastsTheFormerFiveMinuteBudget(t *testing.T) {
	f, e, _ := newSystemDispatch(t, fake.DefaultScenario())
	f.dispatchJob(t, "app", 1)
	e.deadlines = &systemStagingDeadlines{}
	start := time.Now()
	if worked, err := f.services.dispatcher.RunOnce(t.Context(), "upload-worker"); err != nil || !worked {
		t.Fatal("original preparation failed", worked, err)
	}
	if b := e.deadlines.prepare.Sub(start); b < 55*time.Minute || b > time.Hour+5*time.Second {
		t.Fatal("wrong original preparation budget", b)
	}
	if stats := f.backend.Stats(); stats.PrepareCalls != 1 {
		t.Fatal(stats)
	}
}
func TestSystemReconcileExhaustedStagingPreservesConsumedPermitAndNeverPreparesAgain(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprint(ready), func(t *testing.T) {
			scenario := fake.DefaultScenario()
			scenario.PreparationReady = ready
			f, e, trace := newSystemDispatch(t, scenario)
			id := f.dispatchJob(t, "app", 1)
			grant, err := f.services.authorizations.Authorize(t.Context(), f.actor, "app", id.JobID, "dispatch-authorize-"+string(id.JobID), systemAuthorizationRaw(id.AttemptID, 10))
			if err != nil || !grant.Replay {
				t.Fatal("original authorization unavailable", err)
			}
			e.losePrepare = true
			if err := f.dispatchStep(t); err == nil {
				t.Fatal("lost preparation ACK hidden")
			}
			original := trace.journal(t, id)
			unavailable := true
			reads := 0
			e.reconcilePrepare = func(_ context.Context, plan provider.Plan, op domain.OperationID) error {
				reads++
				if plan.Digest() != original.Plan.Digest() || op != original.PreparationID {
					return errors.New("original identity changed")
				}
				if unavailable {
					return errors.New("synthetic staging observation failure")
				}
				return nil
			}
			for i := 1; i < dispatch.MaxFailures; i++ {
				if err := f.dispatchStep(t); err == nil {
					t.Fatal("failed observation cleared")
				}
			}
			before := trace.journal(t, id)
			if before.Phase != dispatch.Attention || before.Failures != dispatch.MaxFailures || before.SubmitStarted || before.Prepared != nil {
				t.Fatal("wrong exhausted staging state", before)
			}
			permit, err := f.services.authorizations.Get(t.Context(), f.actor, "app", id.JobID, grant.AuthorizationID)
			if err != nil || permit.Status != executionauth.Consumed {
				t.Fatal("permit not consumed", err)
			}
			service, actor, _ := systemControlService(t, f, "app", auth.Read, auth.Operate)
			accepted, err := service.Submit(t.Context(), actor, "app", id.JobID, domain.OperationReconcile, "staging-original-key", systemControlBody(t, id.AttemptID))
			if err != nil || accepted.Operation.Status != domain.OperationAccepted {
				t.Fatal("exhausted staging reconcile rejected", err)
			}
			for _, key := range []string{"staging-original-key", "staging-coalesced-key"} {
				r, err := service.Submit(t.Context(), actor, "app", id.JobID, domain.OperationReconcile, key, systemControlBody(t, id.AttemptID))
				if err != nil || r.Operation.ID != accepted.Operation.ID || !r.Replay {
					t.Fatal("reconcile replay multiplied work", err)
				}
			}
			unavailable = false
			if err := f.dispatchStep(t); err != nil {
				t.Fatal(err)
			}
			current := trace.journal(t, id)
			finished, err := service.Get(t.Context(), actor, "app", accepted.Operation.ID)
			if err != nil || finished.Operation.Status != domain.OperationSucceeded || finished.Effect != operations.ObservationRefreshed {
				t.Fatal("staging observation left reconcile pending", err)
			}
			phase := dispatch.Staging
			if ready {
				phase = dispatch.Ready
			}
			if current.Phase != phase || current.SubmitStarted || current.PreparationID != before.PreparationID || !reflect.DeepEqual(current.Plan, before.Plan) {
				t.Fatal("observation changed frozen preparation or started")
			}
			after, err := f.services.authorizations.Get(t.Context(), f.actor, "app", id.JobID, grant.AuthorizationID)
			if err != nil || !reflect.DeepEqual(after, permit) {
				t.Fatal("recovery changed original consumed permit", err)
			}
			if stats := f.backend.Stats(); stats.PrepareCalls != 1 || stats.SubmitCalls != 0 || stats.Executions != 0 || reads != dispatch.MaxFailures {
				t.Fatal("staging recovery repeated mutation or skipped observation", stats, reads)
			}
		})
	}
}
func TestSystemPreparationProgressIsMonotonicThrottledAndShownOnlyWhilePreparing(t *testing.T) {
	f, e, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	reader, _ := f.principal(t, "app", auth.Read)
	read := func() *domain.PreparationStatus {
		t.Helper()
		r, err := f.host.store.ReadJob(t.Context(), "app", reader.TokenID(), id.JobID)
		if err != nil {
			t.Fatal(err)
		}
		return r.Preparation
	}
	if read() != nil {
		t.Fatal("queued job reported remote preparation")
	}
	var seen []int64
	e.prepare = func(ctx context.Context, _ provider.Plan, _ domain.OperationID) {
		for _, step := range []struct {
			completed int64
			wait      time.Duration
		}{{0, 2 * time.Second}, {40, 2 * time.Second}, {30, 2 * time.Second}, {50, 0}, {60, 2 * time.Second}, {100, 0}} {
			provider.ReportPreparationProgress(ctx, step.completed, 100)
			f.advance(t, step.wait)
			status := read()
			if status == nil || status.Progress.BytesTotal != 100 || status.Progress.Scope != "staged_input_bytes" {
				t.Fatal("preparing status lacks progress", status)
			}
			seen = append(seen, status.Progress.BytesCompleted)
		}
	}
	before := f.backend.Stats()
	if worked, err := f.services.dispatcher.RunOnce(t.Context(), "progress-worker"); err != nil || !worked {
		t.Fatal("preparation failed", worked, err)
	}
	if want := []int64{0, 40, 40, 50, 50, 100}; !reflect.DeepEqual(seen, want) {
		t.Fatal("wrong progress samples", seen)
	}
	if status := read(); trace.journal(t, id).Phase != dispatch.Ready || status == nil || status.Progress.BytesCompleted != 100 {
		t.Fatal("final sample missing before start")
	}
	if after := f.backend.Stats(); after.PrepareCalls != before.PrepareCalls+1 || after.SubmitCalls != before.SubmitCalls {
		t.Fatal("progress changed provider effects", after)
	}
	f.advance(t, time.Minute)
	if worked, err := f.services.dispatcher.RunOnce(t.Context(), "start-worker"); err != nil || !worked {
		t.Fatal(worked, err)
	}
	if trace.journal(t, id).Phase != dispatch.Submitted || read() != nil {
		t.Fatal("progress outlived preparation")
	}
}
