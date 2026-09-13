package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestOperationAndEventValidationFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC)
	validOperation, err := NewOperation(
		OperationID("op_01"), WorkspaceID("ws_01"), JobID("job_01"),
		AttemptID("attempt_01"), OperationCollect, now,
	)
	if err != nil {
		t.Fatal(err)
	}

	operationMutations := []struct {
		name   string
		mutate func(*Operation)
	}{
		{"operation ID", func(operation *Operation) { operation.ID = "bad/id" }},
		{"workspace ID", func(operation *Operation) { operation.WorkspaceID = "bad/id" }},
		{"job ID", func(operation *Operation) { operation.JobID = "" }},
		{"attempt ID", func(operation *Operation) { operation.AttemptID = "" }},
		{"kind", func(operation *Operation) { operation.Kind = "bad" }},
		{"status", func(operation *Operation) { operation.Status = "bad" }},
		{"revision", func(operation *Operation) { operation.Revision = 0 }},
		{"timestamps", func(operation *Operation) { operation.UpdatedAt = now.Add(-time.Second) }},
	}
	for _, mutation := range operationMutations {
		t.Run("operation "+mutation.name, func(t *testing.T) {
			t.Parallel()
			operation := validOperation
			mutation.mutate(&operation)
			if err := operation.Validate(); err == nil {
				t.Fatal("invalid operation accepted")
			}
		})
	}

	if !OperationSucceeded.Terminal() || OperationRunning.Terminal() {
		t.Fatal("operation terminal classification is incorrect")
	}
	if _, err := validOperation.Transition("bad", nil, now.Add(time.Second)); err == nil {
		t.Fatal("invalid operation transition target accepted")
	}
	if _, err := validOperation.Transition(OperationRunning, nil, time.Time{}); err == nil {
		t.Fatal("zero operation transition time accepted")
	}
	overflow := validOperation
	overflow.Revision = math.MaxUint64
	if _, err := overflow.Transition(OperationRunning, nil, now.Add(time.Second)); err == nil {
		t.Fatal("operation revision overflow accepted")
	}

	problem, err := NewProblem(CodeArtifactCollectionFailed, "failed", FailureStageResults)
	if err != nil {
		t.Fatal(err)
	}
	invalidFailureStatus := validOperation
	invalidFailureStatus.Failure = &problem
	if err := invalidFailureStatus.Validate(); err == nil {
		t.Fatal("failure attached to accepted operation")
	}

	validEvent := Event{
		ID:          EventID("event_01"),
		Sequence:    1,
		WorkspaceID: WorkspaceID("ws_01"),
		JobID:       JobID("job_01"),
		AttemptID:   AttemptID("attempt_01"),
		OperationID: OperationID("op_01"),
		Type:        EventExecutionObserved,
		OccurredAt:  now,
	}
	eventMutations := []struct {
		name   string
		mutate func(*Event)
	}{
		{"event ID", func(event *Event) { event.ID = "bad/id" }},
		{"workspace ID", func(event *Event) { event.WorkspaceID = "bad/id" }},
		{"job ID", func(event *Event) { event.JobID = "bad/id" }},
		{"sequence", func(event *Event) { event.Sequence = 0 }},
		{"attempt ID", func(event *Event) { event.AttemptID = "bad/id" }},
		{"operation ID", func(event *Event) { event.OperationID = "bad/id" }},
		{"type", func(event *Event) { event.Type = "bad" }},
		{"timestamp", func(event *Event) { event.OccurredAt = time.Time{} }},
	}
	for _, mutation := range eventMutations {
		t.Run("event "+mutation.name, func(t *testing.T) {
			t.Parallel()
			event := validEvent
			mutation.mutate(&event)
			if err := event.Validate(); err == nil {
				t.Fatal("invalid event accepted")
			}
		})
	}
}

func TestSameOperationFailureIsIdempotentButChangedDetailsAreRecorded(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC)
	operation, err := NewOperation(
		OperationID("op_01"), WorkspaceID("ws_01"), JobID("job_01"),
		AttemptID("attempt_01"), OperationCollect, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	problem, err := NewProblem(CodeArtifactCollectionFailed, "failed", FailureStageResults)
	if err != nil {
		t.Fatal(err)
	}
	problem = problem.WithDetail("page", "1")
	failed, err := operation.Transition(OperationFailed, &problem, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := failed.Transition(OperationFailed, &problem, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Revision != failed.Revision {
		t.Fatal("identical terminal failure changed revision")
	}

	changedProblem := problem.WithDetail("page", "2")
	if _, err := failed.Transition(OperationFailed, &changedProblem, now.Add(2*time.Second)); err == nil {
		t.Fatal("terminal failure details were rewritten")
	} else {
		var transition TransitionError
		if !errors.As(err, &transition) {
			t.Fatalf("error = %T, want TransitionError", err)
		}
	}
}
