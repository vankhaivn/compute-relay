package domain

import "fmt"

// AttemptState keeps independent state dimensions together without collapsing uncertainty.
type AttemptState struct {
	Orchestration    OrchestrationState
	Execution        ExecutionState
	Result           ResultState
	Cancellation     CancellationState
	RemoteActivity   RemoteActivityState
	ReleaseEvidence  ReleaseEvidence
	DeadlineExceeded bool
}

// InitialAttemptState is the state of a newly admitted attempt before remote side effects.
func InitialAttemptState() AttemptState {
	return AttemptState{
		Orchestration:   OrchestrationQueued,
		Execution:       ExecutionNotSubmitted,
		Result:          ResultNotAvailable,
		Cancellation:    CancellationNotRequested,
		RemoteActivity:  RemoteActivityNotStarted,
		ReleaseEvidence: ReleaseEvidenceUnknown,
	}
}

// Validate rejects contradictions that could create false success, failure, or cancellation.
func (state AttemptState) Validate() error {
	if !state.Orchestration.Valid() {
		return fmt.Errorf("invalid orchestration state %q", state.Orchestration)
	}
	if !state.Execution.Valid() {
		return fmt.Errorf("invalid execution state %q", state.Execution)
	}
	if !state.Result.Valid() {
		return fmt.Errorf("invalid result state %q", state.Result)
	}
	if !state.Cancellation.Valid() {
		return fmt.Errorf("invalid cancellation state %q", state.Cancellation)
	}
	if !state.RemoteActivity.Valid() {
		return fmt.Errorf("invalid remote activity state %q", state.RemoteActivity)
	}
	if !state.ReleaseEvidence.Valid() {
		return fmt.Errorf("invalid release evidence %q", state.ReleaseEvidence)
	}

	if state.Execution.Terminal() && state.RemoteActivity != RemoteActivityInactive {
		return fmt.Errorf("terminal execution %q requires inactive remote activity", state.Execution)
	}
	if state.Execution == ExecutionQueued || state.Execution == ExecutionStarting || state.Execution == ExecutionRunning {
		if state.RemoteActivity == RemoteActivityNotStarted || state.RemoteActivity == RemoteActivityInactive {
			return fmt.Errorf("active execution %q contradicts remote activity %q", state.Execution, state.RemoteActivity)
		}
	}
	if state.Execution == ExecutionNotSubmitted {
		if state.RemoteActivity == RemoteActivityPossible || state.RemoteActivity == RemoteActivityActive {
			return fmt.Errorf("not-submitted execution contradicts remote activity %q", state.RemoteActivity)
		}
	}
	if state.RemoteActivity == RemoteActivityActive && state.Execution.Terminal() {
		return fmt.Errorf("active remote execution cannot have terminal execution state %q", state.Execution)
	}

	if state.Result != ResultNotAvailable && !state.Execution.Terminal() {
		return fmt.Errorf("result state %q requires terminal execution evidence", state.Result)
	}

	switch state.Result {
	case ResultCollecting:
		switch state.Orchestration {
		case OrchestrationCollecting, OrchestrationReconciling, OrchestrationNeedsAttention:
		default:
			return fmt.Errorf("result state %q requires collection or reconciliation orchestration", state.Result)
		}
	case ResultIncomplete, ResultInvalid:
		switch state.Orchestration {
		case OrchestrationCollecting, OrchestrationReconciling, OrchestrationNeedsAttention, OrchestrationFailed:
		default:
			return fmt.Errorf("result state %q requires collection, reconciliation, attention, or failed orchestration", state.Result)
		}
	}

	switch state.Cancellation {
	case CancellationPrevented:
		if state.Execution != ExecutionNotSubmitted {
			return fmt.Errorf("prevented cancellation requires a not-submitted execution")
		}
		if state.RemoteActivity != RemoteActivityNotStarted && state.RemoteActivity != RemoteActivityInactive {
			return fmt.Errorf("prevented cancellation requires no possible remote activity")
		}
	case CancellationConfirmed:
		if state.Execution != ExecutionCancelled || state.RemoteActivity != RemoteActivityInactive {
			return fmt.Errorf("confirmed cancellation requires cancelled execution and inactive remote activity")
		}
	case CancellationTooLate:
		if !state.Execution.Terminal() {
			return fmt.Errorf("too-late cancellation requires terminal execution evidence")
		}
	}

	if state.ReleaseEvidence == ReleaseEvidenceNotApplicable {
		if state.Execution != ExecutionNotSubmitted {
			return fmt.Errorf("release evidence is not applicable only when execution was not submitted")
		}
		if !state.Orchestration.Terminal() {
			return fmt.Errorf("not-applicable release evidence is only final after terminal orchestration")
		}
	}
	if state.ReleaseEvidence == ReleaseEvidenceProviderReported || state.ReleaseEvidence == ReleaseEvidenceConfirmed {
		if state.RemoteActivity != RemoteActivityInactive {
			return fmt.Errorf("release evidence %q requires inactive remote activity", state.ReleaseEvidence)
		}
	}

	switch state.Orchestration {
	case OrchestrationSubmitted:
		if state.Execution == ExecutionNotSubmitted || state.RemoteActivity == RemoteActivityNotStarted || state.RemoteActivity == RemoteActivityInactive {
			return fmt.Errorf("submitted orchestration requires accepted or ambiguous remote execution")
		}
	case OrchestrationRunning:
		if state.Execution != ExecutionStarting && state.Execution != ExecutionRunning {
			return fmt.Errorf("running orchestration requires starting or running execution evidence")
		}
	case OrchestrationCollecting:
		if !state.Execution.Terminal() {
			return fmt.Errorf("collecting requires terminal execution evidence")
		}
	case OrchestrationSucceeded:
		if state.Execution != ExecutionSucceeded {
			return fmt.Errorf("succeeded orchestration requires succeeded execution")
		}
		if state.Result != ResultAvailable && state.Result != ResultExpired {
			return fmt.Errorf("succeeded orchestration requires verified or retained-as-expired results")
		}
		if state.Cancellation != CancellationNotRequested && state.Cancellation != CancellationTooLate {
			return fmt.Errorf("succeeded orchestration contradicts cancellation %q", state.Cancellation)
		}
	case OrchestrationFailed:
		if state.Execution != ExecutionNotSubmitted && state.Execution != ExecutionSucceeded && state.Execution != ExecutionFailed {
			return fmt.Errorf("failed orchestration requires proven local, payload, or result failure")
		}
		if state.Cancellation != CancellationNotRequested && state.Cancellation != CancellationTooLate {
			return fmt.Errorf("failed orchestration contradicts cancellation %q", state.Cancellation)
		}
	case OrchestrationCancelled:
		if state.Cancellation != CancellationPrevented && state.Cancellation != CancellationConfirmed {
			return fmt.Errorf("cancelled orchestration requires prevented or confirmed cancellation")
		}
		if state.Execution != ExecutionNotSubmitted && state.Execution != ExecutionCancelled {
			return fmt.Errorf("cancelled orchestration contradicts execution %q", state.Execution)
		}
	case OrchestrationTimedOut:
		if state.Cancellation != CancellationNotRequested && state.Cancellation != CancellationTooLate {
			return fmt.Errorf("timed-out orchestration contradicts cancellation %q", state.Cancellation)
		}
		if !state.DeadlineExceeded {
			return fmt.Errorf("timed-out orchestration requires deadline evidence")
		}
		if state.Execution != ExecutionNotSubmitted && state.Execution != ExecutionTimedOut {
			return fmt.Errorf("timed-out orchestration contradicts execution %q", state.Execution)
		}
	}

	if state.Orchestration.Terminal() {
		if state.RemoteActivity != RemoteActivityInactive && state.RemoteActivity != RemoteActivityNotStarted {
			return fmt.Errorf("terminal orchestration cannot leave remote activity %q", state.RemoteActivity)
		}
	}
	return nil
}
