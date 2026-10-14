package sqlite

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestFaultMatrixStalePollCannotRewriteTerminal(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	remote := *f.journal(t, id).Remote
	if err := f.adapter.Advance(remote); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(time.Minute)
	claim, err := f.s.ClaimRecovery(context.Background(), "observer")
	if err != nil || claim == nil {
		t.Fatal("missing observation lease", err)
	}
	work, err := f.s.LoadDispatch(context.Background(), *claim)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := f.adapter.Observe(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.adapter.Advance(remote); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(time.Millisecond)
	terminal, err := f.adapter.Observe(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &terminal})
	if err != nil {
		t.Fatal(err)
	}
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
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 1 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 {
		t.Fatal("restart recreated durable compute rows")
	}
}
