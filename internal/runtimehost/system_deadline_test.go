package runtimehost

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

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
func TestSystemQueuedDeadlineExpiresWithoutProviderEffects(t *testing.T) {
	f, _, trace := newSystemDispatch(t, fake.DefaultScenario())
	id := systemDeadlineJob(t, f, f.fixtureClock().Now().Add(time.Minute), true)
	f.advance(t, 2*time.Minute)
	if worked, err := f.services.dispatcher.RunOnce(t.Context(), "expired-queue"); err != nil || !worked {
		t.Fatal(worked, err)
	}
	j, state, stats := trace.journal(t, id), f.dispatchState(t, id), f.backend.Stats()
	if j.Phase != dispatch.Prevented || !state.DeadlineExceeded || state.Orchestration != domain.OrchestrationTimedOut || state.Execution != domain.ExecutionNotSubmitted {
		t.Fatal("queued expiry lost local-only facts", j.Phase, state)
	}
	if stats.PrepareCalls != 0 || stats.SubmitCalls != 0 {
		t.Fatal("expired queue mutated provider", stats)
	}
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
func TestSystemDeadlineRefusesNewAuthorizationAndRetry(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint(retry), func(t *testing.T) {
			f, _, _ := newSystemDispatch(t, fake.DefaultScenario())
			id := systemDeadlineJob(t, f, f.fixtureClock().Now().Add(time.Hour), false)
			if retry {
				systemCancelControl(t, f, f.controls(t), f.actor, id)
			}
			f.advance(t, 2*time.Hour)
			if retry {
				if _, err := f.controls(t).Submit(t.Context(), f.actor, "app", id.JobID, domain.OperationRetryCompute, "expired-retry", systemControlBody(t, id.AttemptID)); !errors.Is(err, admission.ErrExpired) {
					t.Fatal("retry extended immutable deadline", err)
				}
			} else {
				if _, err := f.services.authorizations.Authorize(t.Context(), f.actor, "app", id.JobID, "expired-grant", systemAuthorizationRaw(id.AttemptID, 10)); !errors.Is(err, admission.ErrExpired) {
					t.Fatal("expired job received authority", err)
				}
			}
			if current := f.dispatchRecord(t, id); current.Attempt.ID != id.AttemptID || current.Attempt.Number != 1 {
				t.Fatal("expiry created replacement attempt")
			}
			if stats := f.backend.Stats(); stats.PrepareCalls != 0 || stats.SubmitCalls != 0 {
				t.Fatal("refusal mutated provider", stats)
			}
		})
	}
}
func TestSystemDeadlineExpiredPreparedAttemptCanBeCleanedWithoutResult(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.AbsoluteDeadline = true
	f, _, trace := newSystemDispatch(t, scenario)
	id := systemDeadlineJob(t, f, f.fixtureClock().Now().Add(15*time.Minute), true)
	for range 2 {
		f.advance(t, 10*time.Minute)
		if worked, err := f.services.dispatcher.RunOnce(t.Context(), "expire-prepared"); err != nil || !worked {
			t.Fatal(worked, err)
		}
	}
	j := trace.journal(t, id)
	if j.Phase != dispatch.Prevented || j.Prepared == nil || !f.dispatchState(t, id).DeadlineExceeded {
		t.Fatal("prepared expiry not established")
	}
	systemCleanupPrepared(t, f, id, j)
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
