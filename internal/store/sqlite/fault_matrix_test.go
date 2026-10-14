package sqlite

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestUnknownObservationPreservesConfirmedRunningEvidence(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	remote := *f.journal(t, id).Remote
	f.clock.advance(time.Minute)
	claim, err := f.s.ClaimRecovery(t.Context(), "observer")
	if err != nil || claim == nil {
		t.Fatal("missing observation lease", err)
	}
	work, err := f.s.LoadDispatch(t.Context(), *claim)
	requireNoError(t, err)
	running := provider.Observation{
		Remote: remote, Execution: domain.ExecutionRunning,
		RemoteActivity: domain.RemoteActivityActive, ReleaseEvidence: domain.ReleaseEvidenceNotObservable,
		ObservedAt: f.clock.Now().Add(-time.Second), RawState: "RUNNING",
	}
	current, err := f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &running})
	requireNoError(t, err)
	unknown := running
	unknown.Execution = domain.ExecutionUnknown
	unknown.RemoteActivity = domain.RemoteActivityUnknown
	unknown.ReleaseEvidence = domain.ReleaseEvidenceUnknown
	unknown.ObservedAt = f.clock.Now()
	unknown.RawState = "UNKNOWN"
	if _, err := f.s.CommitDispatch(t.Context(), current.Handle, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &unknown}); err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	j, state := f.journal(t, id), f.state(t, id)
	if j.Observation == nil || j.Observation.Execution != domain.ExecutionUnknown ||
		j.Observation.RawState != string(domain.ExecutionUnknown) || j.Observation.Remote != remote ||
		j.Remote == nil || *j.Remote != remote || j.Problem == nil ||
		!j.Problem.ComputeMayHaveStarted || j.Problem.SafeOperationRetry {
		t.Fatal("latest unknown observation or ambiguity was not durable", j)
	}
	if state.Execution != domain.ExecutionRunning || state.RemoteActivity != domain.RemoteActivityActive ||
		state.ReleaseEvidence != domain.ReleaseEvidenceNotObservable {
		t.Fatal("unknown poll erased confirmed running evidence", state)
	}
}

func TestFaultMatrixStalePollCannotRewriteTerminal(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	remote := *f.journal(t, id).Remote
	requireNoError(t, f.adapter.Advance(remote))
	f.clock.advance(time.Minute)
	claim, err := f.s.ClaimRecovery(context.Background(), "observer")
	if err != nil || claim == nil {
		t.Fatal("missing observation lease", err)
	}
	work, err := f.s.LoadDispatch(context.Background(), *claim)
	requireNoError(t, err)
	stale, err := f.adapter.Observe(context.Background(), remote)
	requireNoError(t, err)
	requireNoError(t, f.adapter.Advance(remote))
	f.clock.advance(time.Millisecond)
	terminal, err := f.adapter.Observe(context.Background(), remote)
	requireNoError(t, err)
	current, err := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &terminal})
	requireNoError(t, err)
	events := controlCount(t, f.s, "SELECT count(*) FROM events")
	// Both the old owner version and a current handle must reject the late poll.
	for _, handle := range []dispatch.Handle{work.Handle, current.Handle} {
		if _, err := f.s.CommitDispatch(context.Background(), handle, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &stale}); err == nil {
			t.Fatal("late poll regressed terminal state")
		}
	}
	if !reflect.DeepEqual(f.journal(t, id), current.Journal) || f.state(t, id) != current.Job.Attempt.State || controlCount(t, f.s, "SELECT count(*) FROM events") != events {
		t.Fatal("rejected poll changed durable evidence")
	}
	if f.state(t, id).Execution != domain.ExecutionSucceeded || f.state(t, id).Result != domain.ResultNotAvailable {
		t.Fatal("terminal observation fabricated artifact success")
	}
}

func TestFaultMatrixRestartRetainsSingleAttemptAndSubmissionIntent(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	f.restart(t)
	requireNoError(t, f.step(t))
	if controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 1 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 {
		t.Fatal("restart recreated durable compute rows")
	}
}
