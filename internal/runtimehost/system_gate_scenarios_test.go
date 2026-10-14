package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestSystemGateReadySurvivesRestartWithoutStarting(t *testing.T) {
	f, _, trace := newSystemGate(t, fake.Accept)
	id := systemGateSeed(t, f, 1)
	service, p, ready := systemReadyGate(t, f, id)
	before := f.backend.Stats()
	for range 3 {
		if worked, err := systemGateStep(t, f); err != nil || worked {
			t.Fatal("ready hold occupied worker", worked, err)
		}
	}
	if f.backend.Stats() != before {
		t.Fatal("ready polling changed provider")
	}
	f.reopen(t)
	service, p = systemAuthorizationService(t, f, "app", auth.Execute, auth.Read)
	again, err := service.Readiness(context.Background(), p, "app", id.JobID)
	if err != nil || again.Revision != ready.Revision {
		t.Fatal("readiness lost across restart", err)
	}
	if worked, err := systemGateStep(t, f); worked || err != nil {
		t.Fatal("restart inferred consent", worked, err)
	}
	if _, err = service.Authorize(context.Background(), p, "app", id.JobID, "forbidden-combined", systemAuthorizationRaw(id.AttemptID, 10)); !errors.Is(err, executionauth.ErrState) {
		t.Fatal("combined path bypass", err)
	}
	raw := systemStartGrantRaw(f, id, ready)
	original, err := service.Start(context.Background(), p, "app", id.JobID, "explicit-start", raw)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := systemGateStep(t, f); !worked || err != nil {
		t.Fatal("start phase", worked, err)
	}
	j := trace.journal(t, id)
	if j.Phase != dispatch.Submitted || j.StartAuthorization == nil || j.StartAuthorization.AuthorizationID != original.AuthorizationID || f.backend.Stats().Executions != 1 {
		t.Fatal("missing exact start", j.Phase, f.backend.Stats())
	}
	replay, err := service.Start(context.Background(), p, "app", id.JobID, "explicit-start", raw)
	if err != nil || !replay.Replay || replay.Status != executionauth.Granted {
		t.Fatal("original receipt changed", err)
	}
	current, err := service.GateAuthorization(context.Background(), p, "app", id.JobID, original.AuthorizationID)
	if err != nil || current.Status != executionauth.Consumed || current.ConsumedAt == nil {
		t.Fatal("consumption missing", err)
	}
}

func TestSystemGateConcurrentStartReplayConflictAndLostAck(t *testing.T) {
	f, _, trace := newSystemGate(t, fake.AcceptLoseResponse)
	id := systemGateSeed(t, f, 1)
	service, p, ready := systemReadyGate(t, f, id)
	raw := systemStartGrantRaw(f, id, ready)
	var wg sync.WaitGroup
	results := make(chan executionauth.GateReceipt, 8)
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := service.Start(context.Background(), p, "app", id.JobID, "concurrent-start", raw)
			results <- r
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first domain.OperationID
	originals := 0
	for r := range results {
		if first == "" {
			first = r.AuthorizationID
		}
		if r.AuthorizationID != first {
			t.Fatal("duplicate start grant")
		}
		if !r.Replay {
			originals++
		}
	}
	if originals != 1 {
		t.Fatal("multiple original receipts", originals)
	}
	if _, err := service.Start(context.Background(), p, "app", id.JobID, "different-key", raw); !errors.Is(err, executionauth.ErrConflict) {
		t.Fatal("second grant", err)
	}
	changed := strings.Replace(string(raw), `"max_remote_wall_seconds":10`, `"max_remote_wall_seconds":9`, 1)
	if _, err := service.Start(context.Background(), p, "app", id.JobID, "concurrent-start", []byte(changed)); !errors.Is(err, executionauth.ErrConflict) {
		t.Fatal("changed replay", err)
	}
	if _, err := systemGateStep(t, f); err != nil {
		t.Fatal(err)
	}
	if trace.journal(t, id).Phase != dispatch.Submitting {
		t.Fatal("lost start ack lost uncertainty")
	}
	f.reopen(t)
	if _, err := systemGateStep(t, f); err != nil {
		t.Fatal(err)
	}
	if trace.journal(t, id).Phase != dispatch.Submitted || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 1 {
		t.Fatal("recovery repeated original start", f.backend.Stats())
	}
}

func TestSystemGateExpiryReplayAndCancellationPreventStart(t *testing.T) {
	for _, kind := range []string{"queued_expiry", "ready_expiry", "cancel_after_grant", "cancel_before_grant"} {
		t.Run(kind, func(t *testing.T) {
			f, _, _ := newSystemGate(t, fake.Accept)
			id := systemGateSeed(t, f, 1)
			service, p, receipt, prepRaw := systemPrepareGrant(t, f, id)
			if kind != "queued_expiry" {
				if _, err := systemGateStep(t, f); err != nil {
					t.Fatal(err)
				}
			}
			ready, err := service.Readiness(context.Background(), p, "app", id.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "cancel_after_grant" {
				if _, err = service.Start(context.Background(), p, "app", id.JobID, "start-before-cancel", systemStartGrantRaw(f, id, ready)); err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasPrefix(kind, "cancel_") {
				control, op, _ := systemControlService(t, f, "app", auth.Operate)
				systemCancelControl(t, f, control, op, id)
			} else {
				f.advance(t, 31*time.Minute)
				if _, err = systemGateStep(t, f); err != nil {
					t.Fatal(err)
				}
			}
			state := f.dispatchState(t, id)
			if !state.Orchestration.Terminal() || state.Execution != domain.ExecutionNotSubmitted || f.backend.Stats().SubmitCalls != 0 {
				t.Fatal("gate prevention failed", state)
			}
			replay, err := service.Prepare(context.Background(), p, "app", id.JobID, "prepare-key-"+string(id.JobID), prepRaw)
			if err != nil || !replay.Replay || replay.AuthorizationID != receipt.AuthorizationID {
				t.Fatal("expired/cancelled receipt changed", err)
			}
			if _, err = service.Start(context.Background(), p, "app", id.JobID, "late-start", systemStartGrantRaw(f, id, ready)); err == nil {
				t.Fatal("prevented gate reopened")
			}
		})
	}
}

func TestSystemGateBoundsAndScopedReadiness(t *testing.T) {
	f, _, _ := newSystemGate(t, fake.Accept)
	const workers = 2
	settings := scheduler.DefaultSettings()
	settings.MaxWorkers = workers
	if err := f.host.store.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	service, p := systemAuthorizationService(t, f, "app", auth.Execute, auth.Read)
	var first scheduler.Identity
	for n := 1; n <= workers+1; n++ {
		id := systemGateSeed(t, f, n)
		if n == 1 {
			first = id
		}
		raw, _ := json.Marshal(executionauth.PrepareRequest{AttemptID: id.AttemptID, MaxPreparationSeconds: 60, ReadyUntil: f.fixtureClock().Now().Add(time.Minute).UTC().Format(time.RFC3339), MaxStagedBytes: 1 << 20, AuthorizePrivateStaging: true})
		_, err := service.Prepare(context.Background(), p, "app", id.JobID, "bounded-"+string(id.JobID), raw)
		if n <= workers && err != nil || n > workers && !errors.Is(err, admission.ErrLimit) {
			t.Fatal("unbounded holds", n, err)
		}
	}
	id := first
	readOnly, rp := systemAuthorizationService(t, f, "app", auth.Read)
	state, err := readOnly.Readiness(context.Background(), rp, "app", id.JobID)
	if err != nil || state.State != "queued" || state.StagedBytes != 0 {
		t.Fatal("uploaded bytes became remote readiness", state, err)
	}
	if _, err = readOnly.Start(context.Background(), rp, "app", id.JobID, "read-must-not-start", []byte(`{}`)); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope started work", err)
	}
	foreign, fp := systemAuthorizationService(t, f, "other", auth.Execute, auth.Read)
	if _, err = foreign.Readiness(context.Background(), fp, "other", id.JobID); !errors.Is(err, executionauth.ErrNotFound) {
		t.Fatal("cross-workspace readiness", err)
	}
	control, op, _ := systemControlService(t, f, "app", auth.Operate)
	systemCancelControl(t, f, control, op, id)

}

func TestSystemGateLostPreparationAckNeverRestagesOrStarts(t *testing.T) {
	f, effects, _ := newSystemGate(t, fake.Accept)
	id := systemGateSeed(t, f, 1)
	service, p, _, _ := systemPrepareGrant(t, f, id)
	effects.losePrepare = true
	if _, err := systemGateStep(t, f); err == nil {
		t.Fatal("lost prepare ack not surfaced")
	}
	f.reopen(t)
	f.advance(t, time.Minute)
	if _, err := systemGateStep(t, f); err != nil {
		t.Fatal(err)
	}
	ready, err := service.Readiness(context.Background(), p, "app", id.JobID)
	// The old service points to the closed store; a new composition is required.
	if err == nil {
		t.Fatal("closed store remained usable", ready)
	}
	service, p = systemAuthorizationService(t, f, "app", auth.Execute, auth.Read)
	ready, err = service.Readiness(context.Background(), p, "app", id.JobID)
	if err != nil || ready.State != "ready" || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("preparation recovery inferred start", ready, err, f.backend.Stats())
	}
}

func TestSystemGateCrashAtDurableBoundaries(t *testing.T) {
	ctx := context.Background()
	f, _, trace := newSystemGate(t, fake.Accept)
	id := systemGateSeed(t, f, 1)
	_, _, original, _ := systemPrepareGrant(t, f, id)
	work := systemClaim(t, f)
	if _, err := f.host.store.ConsumeAuthorization(ctx, work.Handle); err != nil {
		t.Fatal(err)
	}
	// Consumption survived but no preparation intent or provider mutation existed.
	f.reopen(t)
	f.advance(t, time.Minute)
	if worked, err := systemGateStep(t, f); err != nil || !worked {
		t.Fatal("consumed preparation recovery", worked, err)
	}
	service, p := systemAuthorizationService(t, f, "app", auth.Execute, auth.Read)
	consumed, err := service.GateAuthorization(ctx, p, "app", id.JobID, original.AuthorizationID)
	if err != nil || consumed.Status != executionauth.Consumed || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("preparation consent was lost or reused", consumed, err)
	}
	ready, err := service.Readiness(ctx, p, "app", id.JobID)
	if err != nil || ready.State != "ready" {
		t.Fatal(ready, err)
	}
	raw := systemStartGrantRaw(f, id, ready)
	start, err := service.Start(ctx, p, "app", id.JobID, "crash-start", raw)
	if err != nil {
		t.Fatal(err)
	}
	f.advance(t, 10*time.Millisecond)
	claim, err := f.host.store.ClaimRecovery(ctx, "crash_start_worker")
	if err != nil || claim == nil {
		t.Fatal("start claim", err)
	}
	work, err = f.host.store.LoadDispatch(ctx, *claim)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.host.store.CommitDispatch(ctx, work.Handle, dispatch.Action{Kind: dispatch.BeginSubmission}); err != nil {
		t.Fatal(err)
	}
	// The process died after committing intent and before it could call Start.
	f.reopen(t)
	f.advance(t, time.Minute)
	if _, err = systemGateStep(t, f); err == nil || !strings.HasPrefix(err.Error(), "PROVIDER_SUBMISSION_UNKNOWN:") {
		t.Fatal("missing original start lost uncertainty", err)
	}
	service, p = systemAuthorizationService(t, f, "app", auth.Execute, auth.Read)
	current, err := service.GateAuthorization(ctx, p, "app", id.JobID, start.AuthorizationID)
	if err != nil || current.Status != executionauth.Consumed || f.backend.Stats().SubmitCalls != 0 || !trace.journal(t, id).SubmitStarted || f.dispatchState(t, id).RemoteActivity != domain.RemoteActivityPossible {
		t.Fatal("crash rearmed start authority", current, err, f.dispatchState(t, id))
	}
	if replay, err := service.Start(ctx, p, "app", id.JobID, "crash-start", raw); err != nil || !replay.Replay || replay.Status != executionauth.Granted {
		t.Fatal("expired start replay changed original receipt", replay, err)
	}
}

func TestSystemGateReadyYieldsWorkerButPreservesAccountCapacity(t *testing.T) {
	f, _, _ := newSystemGate(t, fake.Accept, 2)
	settings := scheduler.DefaultSettings()
	settings.MaxWorkers, settings.MaxActivePerAccount = 3, 2
	if err := f.host.store.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= 2; n++ {
		id := systemGateSeed(t, f, n)
		systemReadyGate(t, f, id)

	}
	id := systemGateSeed(t, f, 3)
	systemPrepareGrant(t, f, id)
	// Previously granted holds survive a lower limit; readiness still yields the
	// sole worker while retaining the two occupied account slots.
	settings.MaxWorkers = 1
	if err := f.host.store.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	if worked, err := systemGateStep(t, f); err != nil || worked || f.backend.Stats().PrepareCalls != 2 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("ready hold lost worker or account bound", worked, err, f.backend.Stats())
	}
	// A combined authorization in another workspace and verified account can
	// use the sole worker while the first account keeps its two existing holds.
	// The blocked third gate remains its workspace's FIFO head.
	f.dispatchJobForAccount(t, "other", 4, "SYNTHETIC_OTHER_ACCOUNT")
	if worked, err := systemGateStep(t, f); err != nil || !worked || f.backend.Stats().PrepareCalls != 3 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("ready holds occupied sole worker", worked, err, f.backend.Stats())
	}

}

func TestSystemGateCancelDuringReadinessVerificationFencesStart(t *testing.T) {
	f, effects, _ := newSystemGate(t, fake.Accept)
	id := systemGateSeed(t, f, 1)
	service, p, ready := systemReadyGate(t, f, id)
	grant, err := service.Start(context.Background(), p, "app", id.JobID, "start-races-cancel", systemStartGrantRaw(f, id, ready))
	if err != nil {
		t.Fatal(err)
	}
	control, op, _ := systemControlService(t, f, "app", auth.Operate)
	effects.afterPreparationRead = func() { systemCancelControl(t, f, control, op, id) }
	if _, err = systemGateStep(t, f); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("cancel failed to fence the old readiness worker", err)
	}
	current, err := service.GateAuthorization(context.Background(), p, "app", id.JobID, grant.AuthorizationID)
	if err != nil || current.Status != executionauth.Granted || f.dispatchState(t, id).Orchestration != domain.OrchestrationCancelled || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("cancelled start consumed authority or executed", current, err, f.backend.Stats())
	}
}

func TestSystemGateUnsupportedProviderHasNoRemoteEffects(t *testing.T) {
	f, _, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := systemGateSeed(t, f, 1)
	systemPrepareGrant(t, f, id)
	if _, err := systemGateStep(t, f); err == nil {
		t.Fatal("unsupported preparation mode passed")
	}
	j := trace.journal(t, id)
	if j.Problem == nil || j.Problem.Code != domain.CodeUnsupportedCapability || f.backend.Stats().PrepareCalls != 0 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("unsupported provider mutated or retained a hold", j)
	}
}
