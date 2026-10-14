package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestSystemDispatchLostSubmitResponseRestartAndTerminalCollectionGate(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.AcceptLoseResponse
	f, effects, trace := newSystemDispatch(t, scenario)
	id := f.dispatchJob(t, "app", 1)
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if trace.journal(t, id).Phase != dispatch.Ready || f.backend.Stats().Executions != 0 {
		t.Fatal("prepare allocated execution")
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	j := trace.journal(t, id)
	if j.Phase != dispatch.Submitting || !j.SubmitStarted || !j.Problem.ComputeMayHaveStarted {
		t.Fatal("lost response not recorded")
	}
	f.reopen(t)
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	j = trace.journal(t, id)
	if j.Phase != dispatch.Submitted || j.Remote == nil {
		t.Fatal("reference not rediscovered")
	}
	if err := effects.advance(t, f, *j.Remote); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if f.dispatchState(t, id).Orchestration != domain.OrchestrationRunning {
		t.Fatal("running evidence lost")
	}
	if err := effects.advance(t, f, *j.Remote); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
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
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	j := trace.journal(t, id)
	if j.Phase != dispatch.Staging || j.Prepared == nil || j.Prepared.Ready || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("unready staging submitted")
	}
	if err := effects.markReady(t, f, j.Plan.Job.Identity); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
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
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
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

func TestSystemDispatchPublicStagingAndProvenRejection(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(fmt.Sprint(public), func(t *testing.T) {
			scenario := fake.DefaultScenario()
			scenario.Mode = fake.Reject
			f, effects, trace := newSystemDispatch(t, scenario)
			id := f.dispatchJob(t, "app", 1)
			effects.public = public
			if err := f.dispatchStep(t); err != nil {
				t.Fatal(err)
			}
			if !public {
				if err := f.dispatchStep(t); err != nil {
					t.Fatal(err)
				}
			}
			j := trace.journal(t, id)
			if public {
				if j.Phase != dispatch.Attention || j.Prepared == nil || j.Prepared.Private || j.Problem.Code != domain.CodePrivateStagingUnavailable {
					t.Fatal("public resource not preserved/stopped")
				}
			} else if j.Phase != dispatch.Rejected || j.Problem.ComputeMayHaveStarted || f.dispatchState(t, id).Execution != domain.ExecutionNotSubmitted {
				t.Fatal("proven rejection confused with remote failure")
			}
			before := f.backend.Stats()
			f.advance(t, time.Hour)
			_, err := f.services.dispatcher.RunOnce(context.Background(), "again")
			if err != nil {
				t.Fatal(err)
			}
			if f.backend.Stats() != before {
				t.Fatal("rejected/public work retried")
			}
		})
	}
}

func TestSystemDispatchPauseAndAccountPolicyBlockNewCallsButAllowObservation(t *testing.T) {
	f, _, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	settings := scheduler.DefaultSettings()
	settings.Paused = true
	if err := f.host.store.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); !errors.Is(err, dispatch.ErrPolicy) {
		t.Fatalf("paused submit: %v", err)
	}
	if f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("paused configuration called submit")
	}
	settings.Paused = false
	_ = f.host.store.ConfigureScheduler(context.Background(), settings)
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	_ = f.host.store.ConfigureAccount(context.Background(), f.dispatchRecord(t, id).Profile.AccountScope, scheduler.AccountPolicy{Disabled: true, MaxActive: 1})
	if err := f.dispatchStep(t); err != nil {
		t.Fatal("disabling new dispatch blocked observation", err)
	}
	if trace.journal(t, id).Observation == nil {
		t.Fatal("observation recovery disappeared")
	}
}

func TestSystemDispatchProviderPanicAfterAcceptanceIsReconciled(t *testing.T) {
	f, effects, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := f.dispatchJob(t, "app", 1)
	effects.panicSubmit = true
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	j := trace.journal(t, id)
	raw, _ := json.Marshal(j)
	if j.Phase != dispatch.Submitting || bytes.Contains(raw, []byte("secret-canary")) {
		t.Fatal("panic escaped ambiguity/redaction")
	}
	f.reopen(t)
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if f.backend.Stats().SubmitCalls != 1 || trace.journal(t, id).Remote == nil {
		t.Fatal("panic retried compute")
	}
}

func TestSystemDispatchDoesNotExecutePayloadOnHost(t *testing.T) {
	f, _, _ := newSystemDispatch(t, fake.DefaultScenario())
	f.dispatchJob(t, "app", 1)
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(f.host.root.Path, "answer.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unexpected local workload output")
	}
	if f.backend.Stats().Executions != 0 {
		t.Fatal("preparation triggered execution")
	}
}

func TestSystemDispatchCommitAcknowledgementLossDoesNotRepeatMutation(t *testing.T) {
	for _, kind := range []dispatch.Kind{dispatch.BeginPreparation, dispatch.BeginSubmission, dispatch.SubmissionSeen} {
		t.Run(string(kind), func(t *testing.T) {
			f, _, trace := newSystemDispatch(t, fake.DefaultScenario())
			id := f.dispatchJob(t, "app", 1)
			trace.lose = kind
			if kind != dispatch.BeginPreparation {
				if err := f.dispatchStep(t); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.dispatchStep(t); err == nil {
				t.Fatal("lost ACK returned success")
			}
			f.reopen(t)
			if kind == dispatch.SubmissionSeen {
				if err := f.dispatchStep(t); err != nil {
					t.Fatal(err)
				}
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
	if err != nil {
		t.Fatal(err)
	}
	if err = f.dispatchStep(t); err == nil {
		t.Fatal("corrupt bundle accepted")
	}
	if trace.journal(t, id).Phase != dispatch.Failed || f.backend.Stats().PrepareCalls != 0 {
		t.Fatal("invalid bytes reached staging")
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
func TestSystemDispatchRejectedQuotaLatchesWithoutInventedAllowance(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Reject
	f, effects, trace := newSystemDispatch(t, scenario)
	effects.rejectQuota = true
	id := f.dispatchJob(t, "app", 1)
	f.dispatchJob(t, "other", 2)
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if j := trace.journal(t, id); j.Problem == nil || j.Problem.Code != domain.CodeQuotaExhausted {
		t.Fatal("quota rejection hidden")
	}
	f.advance(t, time.Hour)
	worked, err := f.services.dispatcher.RunOnce(t.Context(), "quota-blocked")
	if err != nil || worked {
		t.Fatal("exhausted account admitted another job", worked, err)
	}
	if stats := f.backend.Stats(); stats.SubmitCalls != 1 || stats.Executions != 0 {
		t.Fatal(stats)
	}
}
func TestSystemDispatchCachedProblemIsAuthorizedAndObservational(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.AcceptLoseResponse
	f, _, _ := newSystemDispatch(t, scenario)
	id := f.dispatchJob(t, "app", 1)
	reader, _ := f.principal(t, "app", auth.Read)
	foreign, _ := f.principal(t, "other", auth.Read)
	if r, err := f.host.store.ReadJob(t.Context(), "app", reader.TokenID(), id.JobID); err != nil || r.Problem != nil {
		t.Fatal("ordinary queued job has a problem", err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	if err := f.dispatchStep(t); err != nil {
		t.Fatal(err)
	}
	before := f.backend.Stats()
	for range 3 {
		r, err := f.host.store.ReadJob(t.Context(), "app", reader.TokenID(), id.JobID)
		if err != nil {
			t.Fatal(err)
		}
		p := r.Problem
		if p == nil || p.Code != domain.CodeProviderSubmissionUnknown || !p.ComputeMayHaveStarted || p.SafeOperationRetry || p.RecommendedAction != domain.RecommendedActionReconcile {
			t.Fatal("cached uncertainty missing")
		}
		if strings.Contains(string(systemJSON(t, p)), "canary") {
			t.Fatal("upstream diagnostic leaked")
		}
	}
	if f.backend.Stats() != before {
		t.Fatal("GET called provider")
	}
	if _, err := f.host.store.ReadJob(t.Context(), "app", foreign.TokenID(), id.JobID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("foreign token accepted", err)
	}
	if _, err := f.host.store.ReadJob(t.Context(), "other", foreign.TokenID(), id.JobID); !errors.Is(err, admission.ErrNotFound) {
		t.Fatal("foreign job disclosed", err)
	}
	if err := f.host.access.Revoke(t.Context(), reader.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.host.store.ReadJob(t.Context(), "app", reader.TokenID(), id.JobID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revoked status access", err)
	}
}
