package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestSystemFaultMatrixUnknownAndModifiedObservations(t *testing.T) {
	for _, mode := range []string{"unknown-format", "invalid-state", "modified-resource"} {
		t.Run(mode, func(t *testing.T) {
			f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
			id := f.dispatchJob(t, "app", 1)
			systemSubmitted(t, f, trace, id)
			remote := *trace.journal(t, id).Remote
			if err := effects.advance(t, f, remote); err != nil {
				t.Fatal(err)
			}
			if err := f.dispatchStep(t); err != nil {
				t.Fatal(err)
			}
			before := f.dispatchState(t, id)
			installSystemMatrixProvider(t, f, effects, mode)
			err := f.dispatchStep(t)
			if mode != "unknown-format" && err == nil {
				t.Fatal("invalid identity/state was not rejected")
			}
			j, state := trace.journal(t, id), f.dispatchState(t, id)
			if j.Problem == nil || !j.Problem.ComputeMayHaveStarted || j.Problem.SafeOperationRetry || *j.Remote != remote ||
				state.Execution != before.Execution || state.RemoteActivity != before.RemoteActivity || state.ReleaseEvidence != before.ReleaseEvidence || state.Orchestration.Terminal() {
				t.Fatal("uncertain observation rewrote confirmed evidence")
			}
			raw, err := json.Marshal(j)
			if err != nil || bytes.Contains(raw, []byte("private-canary")) {
				t.Fatal("raw provider diagnostics escaped into journal")
			}
			f.reopen(t)
			effects.observe = nil
			effects.verify = nil
			effects.reconcile = nil
			effects.start = nil
			f.dispatchJob(t, "other", 2)
			claim, err := f.host.store.ClaimNext(context.Background(), "other")
			if err != nil || claim.Claim != nil {
				t.Fatal("uncertain remote activity released account capacity", err)
			}
			if f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 1 {
				t.Fatal("observation fault repeated compute")
			}
		})
	}
}

func TestSystemFaultMatrixRestartKeepsActiveAttempt(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	systemSubmitted(t, f, trace, id)
	remote := *trace.journal(t, id).Remote
	if err := effects.advance(t, f, remote); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	before := f.dispatchState(t, id)
	if before.Execution != domain.ExecutionRunning {
		t.Fatal("fixture did not reach active remote work")
	}
	stats := f.backend.Stats()
	f.reopen(t)
	effects.observe = nil
	effects.verify = nil
	effects.reconcile = nil
	effects.start = nil
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if f.dispatchState(t, id) != before || *trace.journal(t, id).Remote != remote || f.backend.Stats() != stats {
		t.Fatal("restart lost or recreated the active attempt")
	}
}

func TestSystemFaultMatrixLocalDeadlineKeepsUnknownExecution(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Unresolved
	f, effects, _ := newSystemDispatch(t, scenario)
	id := f.dispatchJob(t, "app", 1)
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	installSystemMatrixProvider(t, f, effects, "deadline")
	if err := f.dispatchStep(t); err == nil {
		t.Fatal("expired observation invocation reported success")
	}
	f.reopen(t)
	effects.observe = nil
	effects.verify = nil
	effects.reconcile = nil
	effects.start = nil
	state := f.dispatchState(t, id)
	if state.Execution != domain.ExecutionUnknown || state.RemoteActivity != domain.RemoteActivityPossible || state.Orchestration.Terminal() ||
		state.Cancellation != domain.CancellationNotRequested || state.ReleaseEvidence != domain.ReleaseEvidenceUnknown {
		t.Fatal("local invocation deadline fabricated remote termination", state)
	}
	f.dispatchJob(t, "other", 2)
	claim, err := f.host.store.ClaimNext(context.Background(), "other")
	if err != nil || claim.Claim != nil || f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("deadline/restart permitted replacement compute", err)
	}
}

func TestSystemFaultMatrixCredentialRotationKeepsFrozenAccount(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	systemSubmitted(t, f, trace, id)
	before := trace.journal(t, id)
	p := installSystemMatrixProvider(t, f, effects, "")
	p.wrongAccount = true
	if err := f.dispatchStep(t); err == nil {
		t.Fatal("mismatching effective account was accepted")
	}
	j := trace.journal(t, id)
	if j.Problem == nil || j.Problem.Code != domain.CodeProviderAuthFailed || !j.Problem.ComputeMayHaveStarted || *j.Remote != *before.Remote {
		t.Fatal("account failure changed remote identity")
	}
	// This models credentials rotated back to the SAME verified account, without
	// loading any real secret. A different account is never a fallback.
	p.wrongAccount = false
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	j = trace.journal(t, id)
	if j.Plan.Digest() != before.Plan.Digest() || *j.Remote != *before.Remote || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().PrepareCalls != 1 {
		t.Fatal("credential recovery remapped or resubmitted frozen work")
	}
}

func TestSystemFaultMatrixNonzeroCLIAfterAcceptanceDoesNotResubmit(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	p := installSystemMatrixProvider(t, f, effects, "cli-exit")
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	j := trace.journal(t, id)
	if p.childExit != 23 || j.Phase != dispatch.Submitting || j.Problem == nil || !j.Problem.ComputeMayHaveStarted || j.Problem.SafeOperationRetry || f.backend.Stats().Executions != 1 {
		t.Fatal("nonzero CLI exit was treated as proven non-acceptance")
	}
	identity := j.Plan.Job.Identity
	f.reopen(t)
	effects.observe = nil
	effects.verify = nil
	effects.reconcile = nil
	effects.start = nil
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	j = trace.journal(t, id)
	if j.Remote == nil || j.Remote.Identity != identity || j.Phase != dispatch.Submitted || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 1 {
		t.Fatal("CLI failure repeated the accepted execution")
	}
}

func TestSystemFaultMatrixCLIChild(t *testing.T) {
	if len(os.Args) == 0 || os.Args[len(os.Args)-1] != "relay-cli-fault" {
		t.Skip("helper process only; parent test verifies its actual exit status")
	}
	os.Exit(23)
}
