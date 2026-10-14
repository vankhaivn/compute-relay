package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

func TestSystemDispatchLostSubmitResponseRestartAndTerminalCollectionGate(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.AcceptLoseResponse
	f, effects, trace := newSystemDispatch(t, scenario)
	id := f.dispatchJob(t, "app", 1)
	requireNoError(t, f.dispatchStep(t))
	if trace.journal(t, id).Phase != dispatch.Ready || f.backend.Stats().Executions != 0 {
		t.Fatal("prepare allocated execution")
	}
	if _, err := os.Stat(filepath.Join(f.host.root.Path, "answer.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unexpected local workload output")
	}
	requireNoError(t, f.dispatchStep(t))
	j := trace.journal(t, id)
	if j.Phase != dispatch.Submitting || !j.SubmitStarted || !j.Problem.ComputeMayHaveStarted {
		t.Fatal("lost response not recorded")
	}
	f.reopen(t)
	requireNoError(t, f.dispatchStep(t))
	j = trace.journal(t, id)
	if j.Phase != dispatch.Submitted || j.Remote == nil {
		t.Fatal("reference not rediscovered")
	}
	requireNoError(t, effects.advance(t, f, *j.Remote))
	requireNoError(t, f.dispatchStep(t))
	if f.dispatchState(t, id).Orchestration != domain.OrchestrationRunning {
		t.Fatal("running evidence lost")
	}
	requireNoError(t, effects.advance(t, f, *j.Remote))
	requireNoError(t, f.dispatchStep(t))
	state := f.dispatchState(t, id)
	stats := f.backend.Stats()
	if state.Orchestration != domain.OrchestrationCollecting || state.Result != domain.ResultNotAvailable || state.RemoteActivity != domain.RemoteActivityInactive || stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 {
		t.Fatalf("unsafe terminal result: %+v %+v", state, stats)
	}
	if trace.journal(t, id).Phase != dispatch.Collectible {
		t.Fatal("collection gate absent")
	}
}

func TestSystemDispatchStagingLossAndDelayedReadinessNeverRepeatPrepare(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.PreparationReady = false
	f, effects, trace := newSystemDispatch(t, scenario)
	id := f.dispatchJob(t, "app", 1)
	effects.losePrepare = true
	if err := f.dispatchStep(t); err == nil {
		t.Fatal("lost staging response was hidden")
	}
	f.reopen(t)
	requireNoError(t, f.dispatchStep(t))
	j := trace.journal(t, id)
	if j.Phase != dispatch.Staging || j.Prepared == nil || j.Prepared.Ready || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("unready staging submitted")
	}
	requireNoError(t, effects.markReady(t, f, j.Plan.Job.Identity))
	requireNoError(t, f.dispatchStep(t))
	requireNoError(t, f.dispatchStep(t))
	if stats := f.backend.Stats(); stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 {
		t.Fatalf("duplicate staging/submit: %+v", stats)
	}
}

func TestSystemDispatchUnknownNotFoundStopsAndKeepsAccountCapacity(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Unresolved
	f, _, trace := newSystemDispatch(t, scenario)
	id := f.dispatchJob(t, "app", 1)
	f.dispatchJob(t, "other", 2)
	requireNoError(t, f.dispatchStep(t))
	requireNoError(t, f.dispatchStep(t))
	for i := 0; i < dispatch.MaxFailures; i++ {
		if err := f.dispatchStep(t); err == nil {
			t.Fatal("unresolved poll did not report a problem")
		}
	}
	j := trace.journal(t, id)
	if j.Phase != dispatch.Attention || !j.Problem.ComputeMayHaveStarted {
		t.Fatal("not-found was treated as non-acceptance")
	}
	f.advance(t, time.Hour)
	worked, err := f.services.dispatcher.RunOnce(context.Background(), "new-worker")
	if err != nil || worked || f.backend.Stats().SubmitCalls != 1 {
		t.Fatalf("ambiguous account slot was released: %v %v", worked, err)
	}
}

func TestSystemDispatchProviderPanicAfterAcceptanceIsReconciled(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	effects.panicSubmit = true
	requireNoError(t, f.dispatchStep(t))
	requireNoError(t, f.dispatchStep(t))
	j := trace.journal(t, id)
	raw, _ := json.Marshal(j)
	if j.Phase != dispatch.Submitting || bytes.Contains(raw, []byte("secret-canary")) {
		t.Fatal("panic escaped ambiguity/redaction")
	}
	f.reopen(t)
	requireNoError(t, f.dispatchStep(t))
	if f.backend.Stats().SubmitCalls != 1 || trace.journal(t, id).Remote == nil {
		t.Fatal("panic retried compute")
	}
}

func TestSystemDispatchCommitAcknowledgementLossDoesNotRepeatMutation(t *testing.T) {
	for _, kind := range []dispatch.Kind{dispatch.BeginPreparation, dispatch.BeginSubmission, dispatch.SubmissionSeen} {
		t.Run(string(kind), func(t *testing.T) {
			f, _, trace := newSystemDispatch(t, fake.DefaultScenario())
			id := f.dispatchJob(t, "app", 1)
			trace.lose = kind
			if kind != dispatch.BeginPreparation {
				requireNoError(t, f.dispatchStep(t))
			}
			if err := f.dispatchStep(t); err == nil {
				t.Fatal("lost ACK returned success")
			}
			f.reopen(t)
			if kind == dispatch.SubmissionSeen {
				requireNoError(t, f.dispatchStep(t))
			} else {
				_ = f.dispatchStep(t)
			}
			stats := f.backend.Stats()
			if kind == dispatch.BeginPreparation && (stats.PrepareCalls != 0 || stats.SubmitCalls != 0) {
				t.Fatal("recovery created unacknowledged preparation")
			}
			if kind == dispatch.BeginSubmission && stats.SubmitCalls != 0 {
				t.Fatal("recovery submitted unacknowledged intent")
			}
			if kind == dispatch.SubmissionSeen && (stats.SubmitCalls != 1 || stats.Executions != 1) {
				t.Fatal("accepted execution repeated")
			}
			if kind != dispatch.SubmissionSeen && trace.journal(t, id).Phase == dispatch.Rejected {
				t.Fatal("missing resource became proven rejection")
			}
		})
	}
}

func TestSystemDispatchFrozenProfileDoesNotFollowRemap(t *testing.T) {
	f, _, trace := newSystemDispatch(t, fake.DefaultScenario())
	connection := f.connection(t, "frozen-connection", "SYNTHETIC_FIRST")
	job := f.submitJob(t, "frozen-job", f.jobSpec(t, connection))
	f.authorizeJob(t, job)
	old := f.dispatchRecord(t, scheduler.Identity{WorkspaceID: "app", JobID: job.JobID, AttemptID: job.AttemptID}).Profile
	raw := systemJSON(t, map[string]any{"action": "configure", "expected_revision": connection.Revision, "configuration": map[string]any{"max_remote_wall_seconds": 200}})
	if _, err := f.services.connections.Submit(t.Context(), f.actor, "app", connection.ID, "change-future-profile", raw); err != nil {
		t.Fatal(err)
	}
	if worked, err := f.services.connections.RunOnce(t.Context()); err != nil {
		t.Fatal("configure", worked, err)
	}
	current, err := f.services.connections.Get(t.Context(), f.actor, "app", connection.ID)
	if err != nil || current.Selection.ConfigurationRevision == old.Binding.ConfigurationRevision {
		t.Fatal("fixture did not replace future selection", err)
	}
	if err = f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	id := scheduler.Identity{WorkspaceID: "app", JobID: job.JobID, AttemptID: job.AttemptID}
	if trace.journal(t, id).Plan.Job.Binding != old.Binding {
		t.Fatal("admitted binding silently followed remap")
	}
}

func TestSystemFaultMatrixCancellationWithoutRemoteIdentityStaysUnresolved(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.Scenario{
		Mode: fake.Unresolved, States: fake.DefaultScenario().States,
		PreparationReady: true, GPU: domain.CapabilitySupportUnsupported,
	})
	id := f.dispatchJob(t, "app", 1)
	for i := 0; i < 2; i++ {
		requireNoError(t, f.dispatchStep(t))
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
	requireNoError(t, err)
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
	requireNoError(t, f.dispatchStep(t))
	f.advance(t, time.Minute)
	limit, used, remaining := 3600.0, 0.0, 3600.0
	q := provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "cpu", Unit: "seconds", Limit: &limit, Used: &used, Remaining: &remaining, ObservedAt: f.fixtureClock().Now(), Source: "synthetic previous allowance", Precision: "exact"}
	requireNoError(t, f.host.store.RecordQuota(ctx, f.dispatchRecord(t, id).Profile.AccountScope, "cpu", q))
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

func TestSystemFaultMatrixUnknownAndModifiedObservations(t *testing.T) {
	for _, mode := range []string{"unknown-format", "invalid-state", "modified-resource"} {
		t.Run(mode, func(t *testing.T) {
			f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
			id := f.dispatchJob(t, "app", 1)
			systemSubmitted(t, f, trace, id)
			remote := *trace.journal(t, id).Remote
			requireNoError(t, effects.advance(t, f, remote))
			requireNoError(t, f.dispatchStep(t))
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
	requireNoError(t, effects.advance(t, f, remote))
	requireNoError(t, f.dispatchStep(t))
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
	requireNoError(t, f.dispatchStep(t))
	if f.dispatchState(t, id) != before || *trace.journal(t, id).Remote != remote || f.backend.Stats() != stats {
		t.Fatal("restart lost or recreated the active attempt")
	}
}

func TestSystemFaultMatrixLocalDeadlineKeepsUnknownExecution(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Unresolved
	f, effects, _ := newSystemDispatch(t, scenario)
	id := f.dispatchJob(t, "app", 1)
	requireNoError(t, f.dispatchStep(t))
	requireNoError(t, f.dispatchStep(t))
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
	requireNoError(t, f.dispatchStep(t))
	j = trace.journal(t, id)
	if j.Plan.Digest() != before.Plan.Digest() || *j.Remote != *before.Remote || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().PrepareCalls != 1 {
		t.Fatal("credential recovery remapped or resubmitted frozen work")
	}
}

func TestSystemFaultMatrixNonzeroCLIAfterAcceptanceDoesNotResubmit(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	p := installSystemMatrixProvider(t, f, effects, "cli-exit")
	requireNoError(t, f.dispatchStep(t))
	requireNoError(t, f.dispatchStep(t))
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
	requireNoError(t, f.dispatchStep(t))
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

func systemDeadlineJob(t *testing.T, f *managedTestFixture, deadline time.Time, authorize bool) scheduler.Identity {
	t.Helper()
	c := f.connection(t, "deadline-connection", "SYNTHETIC_FIRST")
	spec := f.jobSpec(t, c)
	spec.Timeouts.ExecutionDeadline = deadline.UTC().Format(time.RFC3339)
	job := f.submitJob(t, "deadline-job", spec)
	if authorize {
		f.authorizeJob(t, job)
	}
	return scheduler.Identity{WorkspaceID: "app", JobID: job.JobID, AttemptID: job.AttemptID}
}
func TestSystemPreparedExpiryAndLostStartKeepOneShotIdentity(t *testing.T) {
	for _, phase := range []string{"ready", "staging", "submitting"} {
		t.Run(phase, func(t *testing.T) {
			scenario := fake.DefaultScenario()
			scenario.AbsoluteDeadline = true
			scenario.Mode = fake.AcceptLoseResponse
			f, e, trace := newSystemDispatch(t, scenario)
			minutes := 15
			if phase == "submitting" {
				minutes = 25
			}
			id := systemDeadlineJob(t, f, f.fixtureClock().Now().Add(time.Duration(minutes)*time.Minute), true)
			e.losePrepare = phase == "staging"
			f.advance(t, 10*time.Minute)
			worked, err := f.services.dispatcher.RunOnce(t.Context(), "prepare-before-deadline")
			if !worked {
				t.Fatal("missing preparation", err)
			}
			if phase == "staging" {
				if err == nil || !strings.HasPrefix(err.Error(), "STAGING_FAILED:") {
					t.Fatal("lost preparation ACK hidden", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if phase == "submitting" {
				f.advance(t, 10*time.Minute)
				if worked, err := f.services.dispatcher.RunOnce(t.Context(), "start-before-deadline"); err != nil || !worked {
					t.Fatal(worked, err)
				}
				if trace.journal(t, id).Phase != dispatch.Submitting {
					t.Fatal("start response not lost")
				}
			}
			f.reopen(t)
			f.advance(t, 10*time.Minute)
			if worked, err := f.services.dispatcher.RunOnce(t.Context(), "recover-after-deadline"); err != nil || !worked {
				t.Fatal(worked, err)
			}
			j, state, stats := trace.journal(t, id), f.dispatchState(t, id), f.backend.Stats()
			if stats.PrepareCalls != 1 {
				t.Fatal("expiry repeated preparation", stats)
			}
			if phase == "submitting" {
				if j.Phase != dispatch.Submitted || stats.SubmitCalls != 1 || stats.Executions != 1 || state.Orchestration.Terminal() || state.RemoteActivity != domain.RemoteActivityPossible {
					t.Fatal("expiry lost possible original execution", j.Phase, state, stats)
				}
			} else if j.Phase != dispatch.Prevented || !state.DeadlineExceeded || stats.SubmitCalls != 0 {
				t.Fatal("expired preparation became executable", j.Phase, state, stats)
			}
		})
	}
}
func TestSystemGateExpiredLostPreparationRetainsExactCleanup(t *testing.T) {
	f, e, trace := newSystemGate(t, fake.Accept)
	id := systemGateSeed(t, f, 1)
	systemPrepareGrant(t, f, id)
	e.losePrepare = true
	if _, err := systemGateStep(t, f); err == nil {
		t.Fatal("lost preparation hidden")
	}
	f.reopen(t)
	f.advance(t, 31*time.Minute)
	for range 2 {
		if worked, err := systemGateStep(t, f); err != nil || !worked {
			t.Fatal("expired preparation recovery", worked, err)
		}
	}
	j := trace.journal(t, id)
	if j.Phase != dispatch.Prevented || j.Prepared == nil || j.Problem == nil || j.Problem.Details[domain.ReasonDetail] != "preparation_expired" || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("expiry discarded original resource or repeated work", j)
	}
	systemCleanupPrepared(t, f, id, j)
}
func systemCleanupPrepared(t *testing.T, f *managedTestFixture, id scheduler.Identity, j dispatch.Journal) {
	t.Helper()
	job := admission.Receipt{JobID: id.JobID, AttemptID: id.AttemptID}
	receipt := f.admitCleanup(t, job, "cleanup-expired-preparation", false)
	probe := f.cleanupProbe(t, receipt.Operation.ID)
	probe.result = &provider.CleanupOutcome{Deleted: true}
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	if len(probe.requests) != 1 || probe.requests[0].Remote.Resource != j.Prepared.Resource || probe.requests[0].Remote.Version != "prepared" || probe.requests[0].ResultsCollected {
		t.Fatal("expired cleanup lost exact never-started identity", probe.requests)
	}
	if current := f.cleanupRecord(t, receipt.Operation.ID); current.Effect != operations.RemoteDeleted {
		t.Fatal("cleanup not confirmed", current)
	}
	if stats := f.backend.Stats(); stats.SubmitCalls != 0 || stats.Executions != 0 {
		t.Fatal("expired cleanup started compute", stats)
	}
}

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
func TestSystemPreparationProgressIsMonotonicThrottledAndShownOnlyWhilePreparing(t *testing.T) {
	f, e, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	reader, _ := f.principal(t, "app", auth.Read)
	read := func() *domain.PreparationStatus {
		t.Helper()
		r, err := f.host.store.ReadJob(t.Context(), "app", reader.TokenID(), id.JobID)
		requireNoError(t, err)
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

// Cached expiry metadata can be shortened without changing the persisted claim authority.
type heartbeatDispatchRepository struct {
	*sqlite.Store
	now     func() time.Time
	failure error
}

func (r heartbeatDispatchRepository) ClaimNext(ctx context.Context, owner string) (scheduler.Result, error) {
	result, err := r.Store.ClaimNext(ctx, owner)
	if result.Claim != nil {
		result.Claim.ExpiresAt = r.now()
	}
	return result, err
}
func (r heartbeatDispatchRepository) RenewDispatch(context.Context, dispatch.Handle) (dispatch.Handle, error) {
	return dispatch.Handle{}, r.failure
}

type heartbeatCollectionRepository struct {
	*sqlite.Store
	now     func() time.Time
	failure error
}

func (r heartbeatCollectionRepository) ClaimCollection(ctx context.Context, ttl time.Duration, workers int) (*collection.Work, error) {
	work, err := r.Store.ClaimCollection(ctx, ttl, workers)
	if work != nil {
		work.Lease.Until = r.now()
	}
	return work, err
}
func (r heartbeatCollectionRepository) RenewCollection(context.Context, collection.Work, time.Duration) (time.Time, error) {
	return time.Time{}, r.failure
}

type heartbeatCleanupRepository struct {
	*sqlite.Store
	failure error
}

func (r heartbeatCleanupRepository) ClaimCleanup(ctx context.Context, ttl time.Duration) (*cleanup.Work, error) {
	work, err := r.Store.ClaimCleanup(ctx, ttl)
	if work != nil {
		work.Lease.Until = time.Now()
	}
	return work, err
}
func (r heartbeatCleanupRepository) RenewCleanup(context.Context, cleanup.Work, time.Duration) (time.Time, error) {
	return time.Time{}, r.failure
}

type heartbeatProvider struct {
	provider.Provider
	entered, canceled, release chan struct{}
}

func (p *heartbeatProvider) Verify(ctx context.Context, _ provider.BindingSnapshot) error {
	close(p.entered)
	<-ctx.Done()
	close(p.canceled)
	<-p.release // External callbacks may need time to unwind after cancellation.
	return ctx.Err()
}

func heartbeatFixture(t *testing.T, kind string) (*managedTestFixture, admission.Receipt) {
	t.Helper()
	f := newSystemFixture(t, fake.DefaultScenario())
	if kind != "dispatch" {
		job := f.collectibleJob(t, "heartbeat-job", "automatic")
		if kind == "cleanup" {
			f.collectOnce(t)
			if _, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCleanup, "heartbeat-cleanup", systemControlBody(t, job.AttemptID)); err != nil {
				t.Fatal(err)
			}
		}
		return f, job
	}
	job := f.submitJob(t, "heartbeat-job", f.jobSpec(t, f.connection(t, "heartbeat-connection", "SYNTHETIC_FIRST")))
	f.authorizeJob(t, job)
	return f, job
}

func TestWorkLeaseHeartbeatLossCancelsAndJoinsEveryEngine(t *testing.T) {
	for _, tc := range []struct {
		kind    string
		failure error
	}{
		{"dispatch", sqlite.ErrBusy}, {"collection", sqlite.ErrCorrupt}, {"cleanup", cleanup.ErrLeaseLost},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			f, job := heartbeatFixture(t, tc.kind)
			p := &heartbeatProvider{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
			f.wrapProvider = func(base provider.Provider) provider.Provider { p.Provider = base; return p }
			var run func(context.Context) (bool, error)
			switch tc.kind {
			case "dispatch":
				engine, err := dispatch.New(heartbeatDispatchRepository{f.host.store, f.clock.Now, tc.failure}, f.services.resolver, f.host.inputs, f.clock, dispatch.DefaultConfig())
				requireNoError(t, err)
				run = func(ctx context.Context) (bool, error) { return engine.RunOnce(ctx, "heartbeat") }
			case "collection":
				engine, err := collection.New(heartbeatCollectionRepository{f.host.store, f.clock.Now, tc.failure}, f.services.resolver, f.host.results, f.clock, collection.DefaultConfig())
				requireNoError(t, err)
				run = engine.RunOnce
			default:
				run = f.cleanupEngine(t, heartbeatCleanupRepository{f.host.store, tc.failure}).RunOnce
			}
			before := f.backend.Stats()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(p.release) }) }
			done, finished := make(chan error, 1), make(chan struct{})
			go func() { defer close(finished); _, err := run(ctx); done <- err }()
			defer func() {
				cancel()
				release()
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Error("provider callback did not join during cleanup")
				}
			}()
			select {
			case <-p.entered:
			case <-ctx.Done():
				t.Fatal("provider callback did not start")
			}
			select {
			case <-p.canceled:
			case <-ctx.Done():
				t.Fatal("renewal failure did not cancel provider callback")
			}
			select {
			case err := <-done:
				t.Fatal("engine returned while provider callback remained active", err)
			case <-time.After(25 * time.Millisecond):
			}
			release()
			select {
			case err := <-done:
				if !errors.Is(err, tc.failure) {
					t.Fatal("renewal policy was hidden", err)
				}
			case <-ctx.Done():
				t.Fatal("engine did not join provider callback")
			}
			after := f.backend.Stats()
			if after.PrepareCalls != before.PrepareCalls || after.SubmitCalls != before.SubmitCalls || after.CleanupCalls != before.CleanupCalls {
				t.Fatal("cancellation started a provider mutation")
			}
			if tc.kind == "dispatch" {
				record, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
				if err != nil || record.Attempt.State.Orchestration == domain.OrchestrationNeedsAttention {
					t.Fatal("transient renewal failure failed the job", err)
				}
			}
		})
	}
}

type renewedCollectionRepository struct {
	*sqlite.Store
	work collection.Work
}

func (r *renewedCollectionRepository) ClaimCollection(ctx context.Context, ttl time.Duration, workers int) (*collection.Work, error) {
	work, err := r.Store.ClaimCollection(ctx, ttl, workers)
	if work != nil {
		r.work = *work
	}
	return work, err
}

type renewalTransferProvider struct {
	provider.Provider
	renew func() error
}

func (p renewalTransferProvider) Fetch(ctx context.Context, remote provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	if strings.HasPrefix(file.Path, "outputs/") {
		if err := p.renew(); err != nil {
			return provider.TransferResult{}, err
		}
	}
	return p.Provider.Fetch(ctx, remote, file, dst, limit, progress)
}

func TestCollectionPublicationSurvivesRenewalDuringTransfer(t *testing.T) {
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "renew-transfer-job", "automatic")
	repo := &renewedCollectionRepository{Store: f.host.store}
	renewals := 0
	f.wrapProvider = func(base provider.Provider) provider.Provider {
		return renewalTransferProvider{Provider: base, renew: func() error {
			f.advance(t, 10*time.Second)
			_, err := f.host.store.RenewCollection(t.Context(), repo.work, time.Minute)
			renewals++
			return err
		}}
	}
	engine, err := collection.New(repo, f.services.resolver, f.host.results, f.clock, collection.DefaultConfig())
	requireNoError(t, err)
	if worked, err := engine.RunOnce(t.Context()); !worked || err != nil {
		t.Fatal("collection after renewal", worked, err)
	}
	result, err := f.host.store.ReadCollection(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	if err != nil || len(result.Files) != 2 || renewals != 1 {
		t.Fatal("renewal invalidated verified proof", renewals, err)
	}
}

type systemCorruptBundle struct {
	dispatch.BlobStore
	object domain.ObjectID
}

func (b *systemCorruptBundle) Open(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	if id == b.object {
		return io.NopCloser(strings.NewReader("not-a-bundle")), nil
	}
	return b.BlobStore.Open(ctx, w, id)
}
func TestSystemDispatchMalformedBundleNeverStages(t *testing.T) {
	f, _, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	bad := &systemCorruptBundle{BlobStore: f.host.inputs, object: f.dispatchRecord(t, id).Request.Spec().Bundle.ObjectID}
	var err error
	f.services.dispatcher, err = dispatch.New(f.wrapDispatch(f.host.store), f.services.resolver, bad, f.fixtureClock(), dispatch.DefaultConfig())
	requireNoError(t, err)
	if err = f.dispatchStep(t); err == nil {
		t.Fatal("corrupt bundle accepted")
	}
	if trace.journal(t, id).Phase != dispatch.Failed || f.backend.Stats().PrepareCalls != 0 {
		t.Fatal("invalid bytes reached staging")
	}
}
