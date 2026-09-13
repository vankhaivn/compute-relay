package domain

import "testing"

func TestDeadlineEvidenceIsMonotonic(t *testing.T) {
	t.Parallel()

	current := AttemptState{
		Orchestration:    OrchestrationReconciling,
		Execution:        ExecutionUnknown,
		Result:           ResultNotAvailable,
		Cancellation:     CancellationNotRequested,
		RemoteActivity:   RemoteActivityPossible,
		ReleaseEvidence:  ReleaseEvidenceUnknown,
		DeadlineExceeded: true,
	}
	next := current
	next.DeadlineExceeded = false
	if err := ValidateAttemptStateTransition(current, next); err == nil {
		t.Fatal("deadline evidence was cleared")
	}
}

func TestAttemptStateRejectsContradictions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state AttemptState
	}{
		{
			name: "success without results",
			state: func() AttemptState {
				state := successfulState(ResultAvailable)
				state.Result = ResultNotAvailable
				return state
			}(),
		},
		{
			name: "running with inactive remote",
			state: AttemptState{
				Orchestration:   OrchestrationRunning,
				Execution:       ExecutionRunning,
				Result:          ResultNotAvailable,
				Cancellation:    CancellationNotRequested,
				RemoteActivity:  RemoteActivityInactive,
				ReleaseEvidence: ReleaseEvidenceUnknown,
			},
		},
		{
			name: "confirmed cancellation without cancelled execution",
			state: AttemptState{
				Orchestration:   OrchestrationFailed,
				Execution:       ExecutionFailed,
				Result:          ResultNotAvailable,
				Cancellation:    CancellationConfirmed,
				RemoteActivity:  RemoteActivityInactive,
				ReleaseEvidence: ReleaseEvidenceNotObservable,
			},
		},
		{
			name: "timed out without deadline evidence",
			state: AttemptState{
				Orchestration:   OrchestrationTimedOut,
				Execution:       ExecutionTimedOut,
				Result:          ResultNotAvailable,
				Cancellation:    CancellationNotRequested,
				RemoteActivity:  RemoteActivityInactive,
				ReleaseEvidence: ReleaseEvidenceNotObservable,
			},
		},
		{
			name: "failed while remote may still be active",
			state: AttemptState{
				Orchestration:   OrchestrationFailed,
				Execution:       ExecutionUnknown,
				Result:          ResultNotAvailable,
				Cancellation:    CancellationNotRequested,
				RemoteActivity:  RemoteActivityPossible,
				ReleaseEvidence: ReleaseEvidenceUnknown,
			},
		},
		{
			name: "available result while execution is active",
			state: AttemptState{
				Orchestration:   OrchestrationRunning,
				Execution:       ExecutionRunning,
				Result:          ResultAvailable,
				Cancellation:    CancellationNotRequested,
				RemoteActivity:  RemoteActivityActive,
				ReleaseEvidence: ReleaseEvidenceUnknown,
			},
		},
		{
			name: "not-applicable release before terminal",
			state: AttemptState{
				Orchestration:   OrchestrationPreparing,
				Execution:       ExecutionNotSubmitted,
				Result:          ResultNotAvailable,
				Cancellation:    CancellationNotRequested,
				RemoteActivity:  RemoteActivityNotStarted,
				ReleaseEvidence: ReleaseEvidenceNotApplicable,
			},
		},
		{
			name: "failed with unresolved cancellation request",
			state: AttemptState{
				Orchestration:   OrchestrationFailed,
				Execution:       ExecutionFailed,
				Result:          ResultNotAvailable,
				Cancellation:    CancellationRequested,
				RemoteActivity:  RemoteActivityInactive,
				ReleaseEvidence: ReleaseEvidenceNotObservable,
			},
		},
		{
			name: "collecting without terminal execution",
			state: AttemptState{
				Orchestration:   OrchestrationCollecting,
				Execution:       ExecutionRunning,
				Result:          ResultCollecting,
				Cancellation:    CancellationNotRequested,
				RemoteActivity:  RemoteActivityActive,
				ReleaseEvidence: ReleaseEvidenceUnknown,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.state.Validate(); err == nil {
				t.Fatal("Validate() succeeded, want contradiction error")
			}
		})
	}
}

func runningState() AttemptState {
	return AttemptState{
		Orchestration:   OrchestrationRunning,
		Execution:       ExecutionRunning,
		Result:          ResultNotAvailable,
		Cancellation:    CancellationNotRequested,
		RemoteActivity:  RemoteActivityActive,
		ReleaseEvidence: ReleaseEvidenceUnknown,
	}
}

func successfulState(result ResultState) AttemptState {
	return AttemptState{
		Orchestration:   OrchestrationSucceeded,
		Execution:       ExecutionSucceeded,
		Result:          result,
		Cancellation:    CancellationNotRequested,
		RemoteActivity:  RemoteActivityInactive,
		ReleaseEvidence: ReleaseEvidenceNotObservable,
	}
}
