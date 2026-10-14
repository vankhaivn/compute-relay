package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
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
	requireNoError(t, err)
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
		requireNoError(t, err)
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
	requireNoError(t, err)
	f.advance(t, 10*time.Millisecond)
	claim, err := f.host.store.ClaimRecovery(ctx, "crash_start_worker")
	if err != nil || claim == nil {
		t.Fatal("start claim", err)
	}
	work, err = f.host.store.LoadDispatch(ctx, *claim)
	requireNoError(t, err)
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

func TestSystemGateCancelDuringReadinessVerificationFencesStart(t *testing.T) {
	f, effects, _ := newSystemGate(t, fake.Accept)
	id := systemGateSeed(t, f, 1)
	service, p, ready := systemReadyGate(t, f, id)
	grant, err := service.Start(context.Background(), p, "app", id.JobID, "start-races-cancel", systemStartGrantRaw(f, id, ready))
	requireNoError(t, err)
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

func TestSystemSecretRestartRequiresExplicitResupplyAndConsumesOnce(t *testing.T) {
	ctx := context.Background()
	f, _, trace, id, gate, p, ready, secrets, pool := systemSecretFixture(t, fake.AcceptLoseResponse)
	if ready.Secrets == nil || ready.Secrets.Status != "missing" {
		t.Fatal("missing slots not exposed")
	}
	if _, err := gate.Start(ctx, p, "app", id.JobID, "no-material-start", systemStartGrantRaw(f, id, ready)); !errors.Is(err, workloadsecret.ErrUnavailable) {
		t.Fatal("start without slots", err)
	}
	raw := systemSecretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw)
	requireNoError(t, err)
	startRaw := systemSecretStartRequest(f, id, ready, receipt.AuthorizationID)
	if _, err = gate.Start(ctx, p, "app", id.JobID, "start-original", startRaw); err != nil {
		t.Fatal(err)
	}
	assertSystemSecretSanitized(t, f, id)
	pool.Close()
	f.reopen(t)
	gate, p = systemAuthorizationService(t, f, "app", auth.Read, auth.Execute)
	secrets, pool = f.services.secrets, f.host.secretPool
	if _, err = systemGateStep(t, f); err != nil || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("memory loss started payload", err)
	}
	state, err := gate.Readiness(ctx, p, "app", id.JobID)
	if err != nil || state.Secrets.Status != "material_lost" {
		t.Fatal("material loss invisible", err)
	}
	replay, err := secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw)
	if err != nil || !replay.Replay || replay.AuthorizationID != receipt.AuthorizationID {
		t.Fatal("explicit resupply failed", err)
	}
	if _, err = systemGateStep(t, f); err != nil {
		t.Fatal(err)
	}
	if trace.journal(t, id).Phase != dispatch.Submitting || f.backend.Stats().Executions != 1 {
		t.Fatal("lost ack was not retained")
	}
	if pool.Available(workloadsecret.Key{WorkspaceID: "app", JobID: id.JobID, AttemptID: id.AttemptID}, receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("used material retained")
	}
	current, err := secrets.Get(ctx, p, "app", id.JobID, receipt.AuthorizationID)
	if err != nil || current.Status != "consumed" {
		t.Fatal("missing consumption", err)
	}
	if _, err = secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw); err != nil {
		t.Fatal(err)
	}
	if pool.Available(current.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("used replay restored material")
	}
	pool.Close()
	f.reopen(t)
	if _, err = systemGateStep(t, f); err != nil || f.backend.Stats().SubmitCalls != 1 || trace.journal(t, id).Phase != dispatch.Submitted {
		t.Fatal("recovery repeated delivery/start", err)
	}
	assertSystemSecretSanitized(t, f, id)
}

func TestSystemSecretRevokeDuringRemoteVerificationPreventsStart(t *testing.T) {
	ctx := context.Background()
	f, effects, trace, id, gate, p, ready, secrets, pool := systemSecretFixture(t, fake.Accept)
	raw := systemSecretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw)
	requireNoError(t, err)
	if _, err = gate.Start(ctx, p, "app", id.JobID, "start-original", systemSecretStartRequest(f, id, ready, receipt.AuthorizationID)); err != nil {
		t.Fatal(err)
	}
	revokeRaw, _ := json.Marshal(map[string]any{"attempt_id": id.AttemptID})
	effects.afterPreparationRead = func() {
		effects.afterPreparationRead = nil
		if _, err := secrets.Revoke(ctx, p, "app", id.JobID, receipt.AuthorizationID, "revoke-original", revokeRaw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = systemGateStep(t, f); err != nil || f.backend.Stats().SubmitCalls != 0 || trace.journal(t, id).SubmitStarted {
		t.Fatal("revocation lost start race", err)
	}
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("revoked bytes retained")
	}
	replay, err := secrets.Revoke(ctx, p, "app", id.JobID, receipt.AuthorizationID, "revoke-original", revokeRaw)
	if err != nil || !replay.Replay || replay.Status != "revoked" {
		t.Fatal("revocation replay", err)
	}
	if _, err = secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw); err != nil || pool.Available(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("revoked replay rearmed", err)
	}
	assertSystemSecretSanitized(t, f, id)
}

func TestSystemSecretCrashAfterConsumptionNeverRestoresOrSends(t *testing.T) {
	ctx := context.Background()
	f, _, trace, id, gate, p, ready, secrets, pool := systemSecretFixture(t, fake.Accept)
	raw := systemSecretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "app", id.JobID, "crash-secret-original", raw)
	requireNoError(t, err)
	if _, err = gate.Start(ctx, p, "app", id.JobID, "crash-start-original", systemSecretStartRequest(f, id, ready, receipt.AuthorizationID)); err != nil {
		t.Fatal(err)
	}
	f.advance(t, 10*time.Millisecond)
	claim, err := f.host.store.ClaimRecovery(ctx, "secret_crash_worker")
	if err != nil || claim == nil {
		t.Fatal("missing claim", err)
	}
	work, err := f.host.store.LoadDispatch(ctx, *claim)
	requireNoError(t, err)
	material, err := pool.Claim(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now(), func() error {
		_, err := f.host.store.CommitDispatch(ctx, work.Handle, dispatch.Action{Kind: dispatch.BeginSubmission, SecretsAvailable: true})
		return err
	})
	requireNoError(t, err)
	material.Clear()
	// Crash before the unique provider invocation leaves intent uncertain, not reusable.
	pool.Close()
	f.reopen(t)
	gate, p = systemAuthorizationService(t, f, "app", auth.Read, auth.Execute)
	secrets, pool = f.services.secrets, f.host.secretPool
	f.advance(t, time.Minute)
	if _, err = systemGateStep(t, f); err == nil || !strings.HasPrefix(err.Error(), "PROVIDER_SUBMISSION_UNKNOWN:") {
		t.Fatal("crash lost uncertainty", err)
	}
	current, err := secrets.Get(ctx, p, "app", id.JobID, receipt.AuthorizationID)
	if err != nil || current.Status != "consumed" {
		t.Fatal("consumption lost", err)
	}
	if replay, err := secrets.Supply(ctx, p, "app", id.JobID, "crash-secret-original", raw); err != nil || !replay.Replay {
		t.Fatal("original replay lost", err)
	}
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) || f.backend.Stats().SubmitCalls != 0 || !trace.journal(t, id).SubmitStarted || f.dispatchState(t, id).RemoteActivity != domain.RemoteActivityPossible {
		t.Fatal("crash rearmed material or execution")
	}
	assertSystemSecretSanitized(t, f, id)
}

func TestSystemControlsConcurrentRetryKeepsFrozenHistoryAndNewIdentity(t *testing.T) {
	f, _, _ := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	service, p, _ := systemControlService(t, f, "app", auth.Read, auth.Operate)
	systemCancelControl(t, f, service, p, id)
	old, err := f.host.store.ReadJob(context.Background(), "app", p.TokenID(), id.JobID)
	requireNoError(t, err)
	systemRemapProfile(t, f, id)
	var wg sync.WaitGroup
	results := make(chan operations.Record, 20)
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := service.Submit(context.Background(), p, "app", id.JobID, domain.OperationRetryCompute, "retry-concurrent-key", systemControlBody(t, id.AttemptID))
			if err != nil {
				failures <- err
			} else {
				results <- r
			}
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	var first operations.Record
	for r := range results {
		if first.NewAttemptID == "" {
			first = r
		}
		if r.Operation.ID != first.Operation.ID || r.NewAttemptID != first.NewAttemptID || r.Operation.AttemptID != id.AttemptID {
			t.Fatal("duplicate retry identity")
		}
	}
	if first.Effect != operations.NewAttemptCreated || first.NewAttemptID == id.AttemptID || first.NewAttemptID == "" {
		t.Fatal("retry did not create a distinct attempt")
	}
	current, err := f.host.store.ReadJob(context.Background(), "app", p.TokenID(), id.JobID)
	requireNoError(t, err)
	if current.Attempt.Number != 2 || current.Attempt.ID != first.NewAttemptID || current.AttemptNonce == old.AttemptNonce || current.Profile != old.Profile || !reflect.DeepEqual(current.Objects, old.Objects) || string(current.Request.Canonical()) != string(old.Request.Canonical()) {
		t.Fatal("retry changed immutable inputs/binding or reused the nonce")
	}
	if _, err = service.Submit(context.Background(), p, "app", id.JobID, domain.OperationRetryCompute, "retry-different-key", systemControlBody(t, id.AttemptID)); !errors.Is(err, operations.ErrChanged) {
		t.Fatal("new key targeted superseded attempt", err)
	}
	if _, err = service.Submit(context.Background(), p, "app", id.JobID, domain.OperationRetryCompute, "retry-running-key", systemControlBody(t, first.NewAttemptID)); !errors.Is(err, operations.ErrUnresolved) {
		t.Fatal("queued attempt retried", err)
	}
}

func TestSystemControlsUnsupportedAcceptedAndConfirmedCancellation(t *testing.T) {
	for _, mode := range []string{"unsupported", "accepted", "confirmed", "lost_response"} {
		t.Run(mode, func(t *testing.T) {
			f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
			id := f.dispatchJob(t, "app", 1)
			systemSubmitted(t, f, trace, id)
			service, p, _ := systemControlService(t, f, "app", auth.Read, auth.Operate)
			outcome := provider.CancellationOutcome{Status: domain.CancellationAccepted}
			if mode == "confirmed" {
				outcome = provider.CancellationOutcome{Status: domain.CancellationConfirmed, TerminationConfirmed: true}
			}
			probe := installSystemCancelProbe(t, f, effects, mode != "unsupported", outcome)
			probe.lost = mode == "lost_response"
			accepted := systemCancelControl(t, f, service, p, id)
			if accepted.Operation.Status != domain.OperationAccepted || accepted.TerminationConfirmed {
				t.Fatal("HTTP response claimed termination")
			}
			requireNoError(t, f.dispatchStep(t))
			current, err := service.Get(context.Background(), p, "app", accepted.Operation.ID)
			requireNoError(t, err)
			switch mode {
			case "unsupported", "lost_response":
				if current.Operation.Status != domain.OperationManualRequired || current.TerminationConfirmed || f.dispatchState(t, id).RemoteActivity == domain.RemoteActivityInactive {
					t.Fatal("manual request invented termination")
				}
				raw, _ := json.Marshal(current)
				if strings.Contains(string(raw), "canary") {
					t.Fatal("upstream diagnostic leaked")
				}
			case "accepted":
				if current.Operation.Status != domain.OperationRunning || current.TerminationConfirmed || f.dispatchState(t, id).Cancellation != domain.CancellationAccepted {
					t.Fatal("acceptance reported completion")
				}
			case "confirmed":
				if current.Operation.Status != domain.OperationSucceeded || !current.TerminationConfirmed || f.dispatchState(t, id).Execution != domain.ExecutionCancelled {
					t.Fatal("terminal cancellation evidence was lost")
				}
			}
			if mode != "confirmed" {
				if err = f.dispatchStep(t); err != nil {
					t.Fatal(err)
				}
				if f.dispatchState(t, id).Execution != domain.ExecutionSucceeded || f.dispatchState(t, id).Cancellation != domain.CancellationTooLate {
					t.Fatal("completion did not win")
				}
				latest, err := service.Get(context.Background(), p, "app", accepted.Operation.ID)
				requireNoError(t, err)
				if mode == "accepted" && (latest.Effect != operations.TooLate || latest.TerminationConfirmed) {
					t.Fatal("completion was reported as cancellation")
				}
				if mode != "accepted" && !reflect.DeepEqual(latest, current) {
					t.Fatal("terminal manual operation history was rewritten")
				}
			}
			want := int32(1)
			if mode == "unsupported" {
				want = 0
			}
			if probe.calls.Load() != want {
				t.Fatal("cancel call count", probe.calls.Load())
			}
			replay, err := service.Submit(context.Background(), p, "app", id.JobID, domain.OperationCancel, "cancel-original-key", systemControlBody(t, id.AttemptID))
			if err != nil || replay.Operation.Status != domain.OperationAccepted || !replay.Replay {
				t.Fatal("original receipt was not stable", err)
			}
		})
	}
}

func TestSystemControlsCancelIntentRestartDoesNotRepeatMutation(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	systemSubmitted(t, f, trace, id)
	service, p, _ := systemControlService(t, f, "app", auth.Read, auth.Operate)
	accepted := systemCancelControl(t, f, service, p, id)
	f.advance(t, time.Minute)
	claim, err := f.host.store.ClaimRecovery(context.Background(), "intent-crash")
	if err != nil || claim == nil {
		t.Fatal("claim", err)
	}
	work, err := f.host.store.LoadDispatch(context.Background(), *claim)
	requireNoError(t, err)
	committed, err := f.host.store.CommitCancellation(context.Background(), work.Handle, accepted.Operation.ID, dispatch.CancellationAction{Begin: true})
	requireNoError(t, err)
	// Lose the successful acknowledgement and restart before sending Cancel.
	if _, err = f.host.store.CommitCancellation(context.Background(), work.Handle, accepted.Operation.ID, dispatch.CancellationAction{Begin: true}); err == nil {
		t.Fatal("lost acknowledgement rearmed cancel")
	}
	if committed.Cancellation == nil || !committed.Cancellation.Started {
		t.Fatal("gate not persisted")
	}
	f.reopen(t)
	probe := installSystemCancelProbe(t, f, effects, true, provider.CancellationOutcome{Status: domain.CancellationAccepted})
	if err = f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if probe.calls.Load() != 0 || f.dispatchState(t, id).Cancellation != domain.CancellationTooLate {
		t.Fatal("restart repeated cancel instead of observing")
	}
}

func TestSystemControlsCancellationDuringStagingFencesWorker(t *testing.T) {
	f, effects, _ := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	service, p, _ := systemControlService(t, f, "app", auth.Read, auth.Operate)
	effects.prepareEntered = make(chan struct{})
	effects.prepareRelease = make(chan struct{})
	returned := make(chan error, 1)
	go func() { _, err := f.services.dispatcher.RunOnce(context.Background(), "staging-race"); returned <- err }()
	select {
	case <-effects.prepareEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("staging was not entered")
	}
	systemCancelControl(t, f, service, p, id)
	close(effects.prepareRelease)
	if err := <-returned; !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("stale preparation committed", err)
	}
	if f.dispatchState(t, id).Orchestration != domain.OrchestrationCancelled || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("cancel permitted submit")
	}

}
