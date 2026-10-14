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

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestSystemControlsPreventDispatchAndReplayAfterRestart(t *testing.T) {
	f, _, _ := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	service, p, secret := systemControlService(t, f, "app", auth.Read, auth.Operate)
	accepted := systemCancelControl(t, f, service, p, id)
	if accepted.Effect != operations.DispatchPrevented || accepted.TerminationConfirmed || accepted.Operation.Status != domain.OperationSucceeded {
		t.Fatal("prevention is not remote termination", accepted)
	}
	if f.dispatchState(t, id).Orchestration != domain.OrchestrationCancelled || f.backend.Stats().PrepareCalls != 0 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("dispatch not prevented")
	}
	f.reopen(t)
	access, _ := auth.New(f.host.store, f.host.store)
	p, err := access.Authenticate(context.Background(), secret)
	if err != nil {
		t.Fatal(err)
	}
	service, err = operations.New(access, f.host.store, f.host.inputs, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.Submit(context.Background(), p, "app", id.JobID, domain.OperationCancel, "cancel-original-key", systemControlBody(t, id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	expected := accepted
	expected.Replay = true
	if !reflect.DeepEqual(replay, expected) {
		t.Fatal("restart/replay changed the original receipt")
	}
	if _, err = service.Submit(context.Background(), p, "app", id.JobID, domain.OperationCancel, "cancel-original-key", []byte(`{"attempt_id":"different"}`)); !errors.Is(err, operations.ErrConflict) {
		t.Fatal("conflicting key accepted", err)
	}
}

func TestSystemControlsConcurrentRetryKeepsFrozenHistoryAndNewIdentity(t *testing.T) {
	f, _, _ := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	service, p, _ := systemControlService(t, f, "app", auth.Read, auth.Operate)
	systemCancelControl(t, f, service, p, id)
	old, err := f.host.store.ReadJob(context.Background(), "app", p.TokenID(), id.JobID)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
			if err := f.dispatchStep(t); err != nil {
				t.Fatal(err)
			}
			current, err := service.Get(context.Background(), p, "app", accepted.Operation.ID)
			if err != nil {
				t.Fatal(err)
			}
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
				if err != nil {
					t.Fatal(err)
				}
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
	if err != nil {
		t.Fatal(err)
	}
	committed, err := f.host.store.CommitCancellation(context.Background(), work.Handle, accepted.Operation.ID, dispatch.CancellationAction{Begin: true})
	if err != nil {
		t.Fatal(err)
	}
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

func TestSystemControlsCollectIsTransferOnlyAndReconcileNeverSubmits(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	systemSubmitted(t, f, trace, id)
	service, p, _ := systemControlService(t, f, "app", auth.Read, auth.Operate)
	if _, err := service.Submit(context.Background(), p, "app", id.JobID, domain.OperationRetryCompute, "unresolved-retry", systemControlBody(t, id.AttemptID)); !errors.Is(err, operations.ErrUnresolved) {
		t.Fatal("unresolved compute retried", err)
	}
	reconciliation, err := service.Submit(context.Background(), p, "app", id.JobID, domain.OperationReconcile, "reconcile-original", systemControlBody(t, id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	probe := installSystemCancelProbe(t, f, effects, false, provider.CancellationOutcome{})
	if err = f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	r, err := service.Get(context.Background(), p, "app", reconciliation.Operation.ID)
	if err != nil || r.Effect != operations.ObservationRefreshed {
		t.Fatal("reconciliation not acknowledged", err)
	}
	if _, err = service.Submit(context.Background(), p, "app", id.JobID, domain.OperationRetryCompute, "successful-retry", systemControlBody(t, id.AttemptID)); !errors.Is(err, operations.ErrCollect) {
		t.Fatal("successful compute rerun for missing artifacts", err)
	}
	collect, err := service.Submit(context.Background(), p, "app", id.JobID, domain.OperationCollect, "collect-original-key", systemControlBody(t, id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	again, err := service.Submit(context.Background(), p, "app", id.JobID, domain.OperationCollect, "collect-another-key", systemControlBody(t, id.AttemptID))
	if err != nil || again.Operation.ID != collect.Operation.ID {
		t.Fatal("parallel collection ticket duplicated", err)
	}
	if f.dispatchState(t, id).Result != domain.ResultNotAvailable || probe.calls.Load() != 0 || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 1 {
		t.Fatal("control replay started compute or invented artifacts")
	}
}

func TestSystemControlsAuthorizationRecheckedAfterRetryIO(t *testing.T) {
	f, _, _ := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	service, p, secret := systemControlService(t, f, "app", auth.Read, auth.Operate)
	systemCancelControl(t, f, service, p, id)
	access, _ := auth.New(f.host.store, f.host.store)
	blocked := &systemBlockedInputs{BlobReader: f.host.inputs, entered: make(chan struct{}), release: make(chan struct{})}
	service, err := operations.New(access, f.host.store, blocked, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := service.Submit(context.Background(), p, "app", id.JobID, domain.OperationRetryCompute, "revoked-during-read", systemControlBody(t, id.AttemptID))
		result <- err
	}()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("retry did not verify blobs")
	}
	// Test-only concurrent revocation, using the same persisted authority consulted at commit.
	if err = access.Revoke(context.Background(), p.TokenID()); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	if err = <-result; err == nil {
		t.Fatal("revoked authority committed retry")
	}
	if f.dispatchRecord(t, id).Attempt.ID != id.AttemptID {
		t.Fatal("revocation lost the commit race")
	}
	if _, err = access.Authenticate(context.Background(), secret); err == nil {
		t.Fatal("token was not revoked")
	}
	readOnly, reader, _ := systemControlService(t, f, "app", auth.Read)
	if _, err = readOnly.Submit(context.Background(), reader, "app", id.JobID, domain.OperationCancel, "read-only-operation", systemControlBody(t, id.AttemptID)); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope operated", err)
	}
}
