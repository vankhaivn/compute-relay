package domain

import (
	"errors"
	"testing"
)

func TestAttemptStateHappyPath(t *testing.T) {
	t.Parallel()

	states := []AttemptState{
		InitialAttemptState(),
		{
			Orchestration:   OrchestrationPreparing,
			Execution:       ExecutionNotSubmitted,
			Result:          ResultNotAvailable,
			Cancellation:    CancellationNotRequested,
			RemoteActivity:  RemoteActivityNotStarted,
			ReleaseEvidence: ReleaseEvidenceUnknown,
		},
		{
			Orchestration:   OrchestrationDispatching,
			Execution:       ExecutionUnknown,
			Result:          ResultNotAvailable,
			Cancellation:    CancellationNotRequested,
			RemoteActivity:  RemoteActivityPossible,
			ReleaseEvidence: ReleaseEvidenceUnknown,
		},
		{
			Orchestration:   OrchestrationSubmitted,
			Execution:       ExecutionQueued,
			Result:          ResultNotAvailable,
			Cancellation:    CancellationNotRequested,
			RemoteActivity:  RemoteActivityPossible,
			ReleaseEvidence: ReleaseEvidenceUnknown,
		},
		{
			Orchestration:   OrchestrationRunning,
			Execution:       ExecutionRunning,
			Result:          ResultNotAvailable,
			Cancellation:    CancellationNotRequested,
			RemoteActivity:  RemoteActivityActive,
			ReleaseEvidence: ReleaseEvidenceUnknown,
		},
		{
			Orchestration:   OrchestrationCollecting,
			Execution:       ExecutionSucceeded,
			Result:          ResultCollecting,
			Cancellation:    CancellationNotRequested,
			RemoteActivity:  RemoteActivityInactive,
			ReleaseEvidence: ReleaseEvidenceNotObservable,
		},
		{
			Orchestration:   OrchestrationSucceeded,
			Execution:       ExecutionSucceeded,
			Result:          ResultAvailable,
			Cancellation:    CancellationNotRequested,
			RemoteActivity:  RemoteActivityInactive,
			ReleaseEvidence: ReleaseEvidenceNotObservable,
		},
	}

	for index, state := range states {
		if err := state.Validate(); err != nil {
			t.Fatalf("state %d invalid: %v", index, err)
		}
		if index == 0 {
			continue
		}
		if err := ValidateAttemptStateTransition(states[index-1], state); err != nil {
			t.Fatalf("transition %d -> %d rejected: %v", index-1, index, err)
		}
	}
}

func TestRemoteObservationMaySkipIntermediateStates(t *testing.T) {
	t.Parallel()

	current := AttemptState{
		Orchestration:   OrchestrationDispatching,
		Execution:       ExecutionUnknown,
		Result:          ResultNotAvailable,
		Cancellation:    CancellationNotRequested,
		RemoteActivity:  RemoteActivityPossible,
		ReleaseEvidence: ReleaseEvidenceUnknown,
	}
	next := AttemptState{
		Orchestration:   OrchestrationCollecting,
		Execution:       ExecutionSucceeded,
		Result:          ResultCollecting,
		Cancellation:    CancellationNotRequested,
		RemoteActivity:  RemoteActivityInactive,
		ReleaseEvidence: ReleaseEvidenceNotObservable,
	}
	if err := ValidateAttemptStateTransition(current, next); err != nil {
		t.Fatalf("short-job terminal observation rejected: %v", err)
	}
}

func TestTerminalStateCannotMoveBackward(t *testing.T) {
	t.Parallel()

	terminal := successfulState(ResultAvailable)
	running := AttemptState{
		Orchestration:   OrchestrationRunning,
		Execution:       ExecutionRunning,
		Result:          ResultNotAvailable,
		Cancellation:    CancellationNotRequested,
		RemoteActivity:  RemoteActivityActive,
		ReleaseEvidence: ReleaseEvidenceUnknown,
	}

	err := ValidateAttemptStateTransition(terminal, running)
	if err == nil {
		t.Fatal("terminal state moved backward to running")
	}
	var transition TransitionError
	if !errors.As(err, &transition) {
		t.Fatalf("error = %T, want TransitionError", err)
	}
	if transition.Code() != CodeIllegalStateTransition {
		t.Fatalf("transition code = %q", transition.Code())
	}
}

func TestSuccessfulJobRemainsSuccessfulAfterArtifactExpiry(t *testing.T) {
	t.Parallel()

	current := successfulState(ResultAvailable)
	next := successfulState(ResultExpired)
	if err := ValidateAttemptStateTransition(current, next); err != nil {
		t.Fatalf("result expiry rewrote successful execution semantics: %v", err)
	}
}

func TestLocalCancellationPreventsDispatch(t *testing.T) {
	t.Parallel()

	current := InitialAttemptState()
	next := AttemptState{
		Orchestration:   OrchestrationCancelled,
		Execution:       ExecutionNotSubmitted,
		Result:          ResultNotAvailable,
		Cancellation:    CancellationPrevented,
		RemoteActivity:  RemoteActivityNotStarted,
		ReleaseEvidence: ReleaseEvidenceNotApplicable,
	}
	if err := ValidateAttemptStateTransition(current, next); err != nil {
		t.Fatalf("local cancellation rejected: %v", err)
	}
}

func TestRemoteCancellationRequiresEvidence(t *testing.T) {
	t.Parallel()

	running := runningState()
	requested := running
	requested.Orchestration = OrchestrationCancelling
	requested.Cancellation = CancellationRequested
	if err := ValidateAttemptStateTransition(running, requested); err != nil {
		t.Fatalf("cancellation request rejected: %v", err)
	}

	accepted := requested
	accepted.Cancellation = CancellationAccepted
	if err := ValidateAttemptStateTransition(requested, accepted); err != nil {
		t.Fatalf("provider cancel acceptance rejected: %v", err)
	}

	confirmed := AttemptState{
		Orchestration:   OrchestrationCancelled,
		Execution:       ExecutionCancelled,
		Result:          ResultNotAvailable,
		Cancellation:    CancellationConfirmed,
		RemoteActivity:  RemoteActivityInactive,
		ReleaseEvidence: ReleaseEvidenceNotObservable,
	}
	if err := ValidateAttemptStateTransition(accepted, confirmed); err != nil {
		t.Fatalf("confirmed cancellation rejected: %v", err)
	}
}

func TestCancellationCompletionRaceStaysTruthful(t *testing.T) {
	t.Parallel()

	running := runningState()
	requested := running
	requested.Orchestration = OrchestrationCancelling
	requested.Cancellation = CancellationRequested
	if err := ValidateAttemptStateTransition(running, requested); err != nil {
		t.Fatalf("cancellation request rejected: %v", err)
	}

	cancelling := requested
	cancelling.Cancellation = CancellationRequesting
	if err := ValidateAttemptStateTransition(requested, cancelling); err != nil {
		t.Fatalf("requesting cancellation rejected: %v", err)
	}

	collecting := AttemptState{
		Orchestration:   OrchestrationCollecting,
		Execution:       ExecutionSucceeded,
		Result:          ResultCollecting,
		Cancellation:    CancellationTooLate,
		RemoteActivity:  RemoteActivityInactive,
		ReleaseEvidence: ReleaseEvidenceNotObservable,
	}
	if err := ValidateAttemptStateTransition(cancelling, collecting); err != nil {
		t.Fatalf("completion-winning race rejected: %v", err)
	}

	succeeded := successfulState(ResultAvailable)
	succeeded.Cancellation = CancellationTooLate
	if err := ValidateAttemptStateTransition(collecting, succeeded); err != nil {
		t.Fatalf("too-late cancellation changed successful outcome: %v", err)
	}
}
