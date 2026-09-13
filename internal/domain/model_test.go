package domain

import (
	"errors"
	"testing"
	"time"
)

func TestNewJobValidatesImmutableResolution(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC)
	job, err := NewJob(Job{
		ID:                   JobID("job_01"),
		WorkspaceID:          WorkspaceID("ws_01"),
		Name:                 "small batch",
		SpecificationVersion: "compute-connector/v1alpha1",
		SpecificationDigest:  SHA256Digest("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Binding: ProviderBinding{
			Profile:               "default-gpu",
			ProviderInstanceID:    ProviderInstanceID("provider_01"),
			ConfigurationRevision: "config_rev_01",
		},
		ActiveAttemptID: AttemptID("attempt_01"),
		CreatedAt:       createdAt,
	})
	if err != nil {
		t.Fatalf("NewJob() error = %v", err)
	}
	if job.Binding.ProviderInstanceID != "provider_01" {
		t.Fatalf("provider binding = %#v", job.Binding)
	}
}

func TestAttemptTransitionIsValueBasedAndRevisioned(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC)
	attempt, err := NewAttempt(AttemptID("attempt_01"), JobID("job_01"), 1, createdAt)
	if err != nil {
		t.Fatalf("NewAttempt() error = %v", err)
	}

	nextState := attempt.State
	nextState.Orchestration = OrchestrationPreparing
	updated, err := attempt.Transition(nextState, createdAt.Add(time.Second))
	if err != nil {
		t.Fatalf("Transition() error = %v", err)
	}
	if attempt.State.Orchestration != OrchestrationQueued {
		t.Fatal("Transition mutated the original attempt")
	}
	if updated.Revision != attempt.Revision+1 {
		t.Fatalf("revision = %d, want %d", updated.Revision, attempt.Revision+1)
	}

	unchanged, err := updated.Transition(updated.State, updated.UpdatedAt)
	if err != nil {
		t.Fatalf("no-op Transition() error = %v", err)
	}
	if unchanged.Revision != updated.Revision || !unchanged.UpdatedAt.Equal(updated.UpdatedAt) {
		t.Fatal("no-op transition changed revision or timestamp")
	}

	outOfOrderState := updated.State
	outOfOrderState.Orchestration = OrchestrationBlocked
	if _, err := updated.Transition(outOfOrderState, createdAt.Add(-time.Second)); err == nil {
		t.Fatal("out-of-order transition timestamp accepted")
	}
}

func TestOperationLifecycleIsMonotonic(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC)
	operation, err := NewOperation(
		OperationID("op_01"),
		WorkspaceID("ws_01"),
		JobID("job_01"),
		AttemptID("attempt_01"),
		OperationReconcile,
		createdAt,
	)
	if err != nil {
		t.Fatalf("NewOperation() error = %v", err)
	}

	running, err := operation.Transition(OperationRunning, nil, createdAt.Add(time.Second))
	if err != nil {
		t.Fatalf("accepted -> running: %v", err)
	}
	succeeded, err := running.Transition(OperationSucceeded, nil, createdAt.Add(2*time.Second))
	if err != nil {
		t.Fatalf("running -> succeeded: %v", err)
	}
	if succeeded.Revision != 3 {
		t.Fatalf("revision = %d, want 3", succeeded.Revision)
	}
	if _, err := succeeded.Transition(OperationRunning, nil, createdAt.Add(3*time.Second)); err == nil {
		t.Fatal("terminal operation moved backward")
	} else {
		var transition TransitionError
		if !errors.As(err, &transition) {
			t.Fatalf("error = %T, want TransitionError", err)
		}
	}
}

func TestFailedOperationRequiresStructuredProblem(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC)
	operation, err := NewOperation(
		OperationID("op_01"), WorkspaceID("ws_01"), JobID("job_01"), AttemptID("attempt_01"),
		OperationCollect, createdAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operation.Transition(OperationFailed, nil, createdAt.Add(time.Second)); err == nil {
		t.Fatal("failed operation without structured problem accepted")
	}

	problem, err := NewProblem(
		CodeArtifactCollectionFailed,
		"artifact download ended before the declared byte count",
		FailureStageResults,
	)
	if err != nil {
		t.Fatal(err)
	}
	problem = problem.WithRetrySemantics(true, false).WithRecommendedAction(RecommendedActionCollect)
	failed, err := operation.Transition(OperationFailed, &problem, createdAt.Add(time.Second))
	if err != nil {
		t.Fatalf("structured failed operation rejected: %v", err)
	}
	if failed.Failure == nil || failed.Failure.Code != CodeArtifactCollectionFailed {
		t.Fatalf("failure = %#v", failed.Failure)
	}
}

func TestEventRequiresSequenceAndKnownType(t *testing.T) {
	t.Parallel()

	event := Event{
		ID:          EventID("event_01"),
		Sequence:    1,
		WorkspaceID: WorkspaceID("ws_01"),
		JobID:       JobID("job_01"),
		AttemptID:   AttemptID("attempt_01"),
		Type:        EventSubmissionUnknown,
		OccurredAt:  time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC),
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}

	event.Sequence = 0
	if err := event.Validate(); err == nil {
		t.Fatal("zero event sequence accepted")
	}
}

func TestCleanupOperationMayBeWorkspaceScoped(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC)
	operation, err := NewOperation(
		OperationID("op_cleanup"), WorkspaceID("ws_01"), "", "",
		OperationCleanup, createdAt,
	)
	if err != nil {
		t.Fatalf("workspace-scoped cleanup rejected: %v", err)
	}
	if operation.JobID != "" || operation.AttemptID != "" {
		t.Fatalf("cleanup scope unexpectedly changed: %#v", operation)
	}
}
