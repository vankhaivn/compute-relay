package ports_test

import (
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

func TestAttemptChangeRequiresMatchingEventAndRevision(t *testing.T) {
	now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	before, err := domain.NewAttempt("att_fixture", "job_fixture", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	state := before.State
	state.Orchestration = domain.OrchestrationPreparing
	after, err := before.Transition(state, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	change := ports.AttemptChange{WorkspaceID: "ws_fixture", Before: before, After: after, Event: domain.Event{
		ID: "event_fixture", Sequence: 1, WorkspaceID: "ws_fixture", JobID: "job_fixture", AttemptID: "att_fixture", Type: domain.EventInputsReady, OccurredAt: after.UpdatedAt,
	}}
	if err := change.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ports.AttemptChange){
		func(c *ports.AttemptChange) { c.After.Revision++ },
		func(c *ports.AttemptChange) { c.After.ID = "att_other" },
		func(c *ports.AttemptChange) { c.Event.WorkspaceID = "ws_other" },
		func(c *ports.AttemptChange) { c.Event.AttemptID = "att_other" },
		func(c *ports.AttemptChange) { c.Event.OccurredAt = time.Time{} },
		func(c *ports.AttemptChange) { c.After = c.Before },
	} {
		bad := change
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatal("inconsistent atomic change accepted")
		}
	}
}
