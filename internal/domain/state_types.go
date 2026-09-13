package domain

// OrchestrationState describes what the local runtime is doing.
type OrchestrationState string

const (
	OrchestrationQueued         OrchestrationState = "queued"
	OrchestrationPreparing      OrchestrationState = "preparing"
	OrchestrationDispatching    OrchestrationState = "dispatching"
	OrchestrationSubmitted      OrchestrationState = "submitted"
	OrchestrationRunning        OrchestrationState = "running"
	OrchestrationCollecting     OrchestrationState = "collecting"
	OrchestrationBlocked        OrchestrationState = "blocked"
	OrchestrationReconciling    OrchestrationState = "reconciling"
	OrchestrationCancelling     OrchestrationState = "cancelling"
	OrchestrationNeedsAttention OrchestrationState = "needs_attention"
	OrchestrationSucceeded      OrchestrationState = "succeeded"
	OrchestrationFailed         OrchestrationState = "failed"
	OrchestrationCancelled      OrchestrationState = "cancelled"
	OrchestrationTimedOut       OrchestrationState = "timed_out"
)

func (state OrchestrationState) Valid() bool {
	_, ok := orchestrationTransitions[state]
	return ok
}

func (state OrchestrationState) Terminal() bool {
	switch state {
	case OrchestrationSucceeded, OrchestrationFailed, OrchestrationCancelled, OrchestrationTimedOut:
		return true
	default:
		return false
	}
}

var orchestrationTransitions = map[OrchestrationState]map[OrchestrationState]struct{}{
	OrchestrationQueued: setOf(
		OrchestrationPreparing, OrchestrationBlocked, OrchestrationCancelled,
		OrchestrationFailed, OrchestrationTimedOut,
	),
	OrchestrationPreparing: setOf(
		OrchestrationDispatching, OrchestrationBlocked, OrchestrationReconciling,
		OrchestrationCancelling, OrchestrationNeedsAttention, OrchestrationFailed,
		OrchestrationCancelled, OrchestrationTimedOut,
	),
	OrchestrationDispatching: setOf(
		OrchestrationSubmitted, OrchestrationRunning, OrchestrationCollecting,
		OrchestrationReconciling, OrchestrationCancelling, OrchestrationNeedsAttention,
		OrchestrationFailed, OrchestrationCancelled, OrchestrationTimedOut,
	),
	OrchestrationSubmitted: setOf(
		OrchestrationRunning, OrchestrationCollecting, OrchestrationReconciling,
		OrchestrationCancelling, OrchestrationNeedsAttention, OrchestrationFailed,
		OrchestrationCancelled, OrchestrationTimedOut,
	),
	OrchestrationRunning: setOf(
		OrchestrationCollecting, OrchestrationReconciling, OrchestrationCancelling,
		OrchestrationNeedsAttention, OrchestrationFailed, OrchestrationCancelled,
		OrchestrationTimedOut,
	),
	OrchestrationCollecting: setOf(
		OrchestrationSucceeded, OrchestrationFailed, OrchestrationCancelled,
		OrchestrationTimedOut, OrchestrationBlocked, OrchestrationReconciling,
		OrchestrationNeedsAttention,
	),
	OrchestrationBlocked: setOf(
		OrchestrationPreparing, OrchestrationDispatching, OrchestrationReconciling,
		OrchestrationCancelling, OrchestrationNeedsAttention, OrchestrationFailed,
		OrchestrationCancelled, OrchestrationTimedOut,
	),
	OrchestrationReconciling: setOf(
		OrchestrationPreparing, OrchestrationDispatching, OrchestrationSubmitted,
		OrchestrationRunning, OrchestrationCollecting, OrchestrationBlocked,
		OrchestrationCancelling, OrchestrationNeedsAttention, OrchestrationFailed,
		OrchestrationCancelled, OrchestrationTimedOut,
	),
	OrchestrationCancelling: setOf(
		OrchestrationCollecting, OrchestrationReconciling, OrchestrationNeedsAttention,
		OrchestrationSucceeded, OrchestrationFailed, OrchestrationCancelled,
		OrchestrationTimedOut,
	),
	OrchestrationNeedsAttention: setOf(
		OrchestrationSubmitted, OrchestrationRunning, OrchestrationCollecting,
		OrchestrationReconciling, OrchestrationCancelling, OrchestrationSucceeded,
		OrchestrationFailed, OrchestrationCancelled, OrchestrationTimedOut,
	),
	OrchestrationSucceeded: {},
	OrchestrationFailed:    {},
	OrchestrationCancelled: {},
	OrchestrationTimedOut:  {},
}

// ExecutionState records what provider or runner evidence has established.
type ExecutionState string

const (
	ExecutionNotSubmitted ExecutionState = "not_submitted"
	ExecutionUnknown      ExecutionState = "unknown"
	ExecutionQueued       ExecutionState = "queued"
	ExecutionStarting     ExecutionState = "starting"
	ExecutionRunning      ExecutionState = "running"
	ExecutionSucceeded    ExecutionState = "succeeded"
	ExecutionFailed       ExecutionState = "failed"
	ExecutionCancelled    ExecutionState = "cancelled"
	ExecutionTimedOut     ExecutionState = "timed_out"
)

func (state ExecutionState) Valid() bool {
	_, ok := executionTransitions[state]
	return ok
}

func (state ExecutionState) Terminal() bool {
	switch state {
	case ExecutionSucceeded, ExecutionFailed, ExecutionCancelled, ExecutionTimedOut:
		return true
	default:
		return false
	}
}

var executionTransitions = map[ExecutionState]map[ExecutionState]struct{}{
	ExecutionNotSubmitted: setOf(
		ExecutionUnknown, ExecutionQueued, ExecutionStarting, ExecutionRunning,
		ExecutionSucceeded, ExecutionFailed, ExecutionCancelled, ExecutionTimedOut,
	),
	ExecutionUnknown: setOf(
		ExecutionNotSubmitted, ExecutionQueued, ExecutionStarting, ExecutionRunning,
		ExecutionSucceeded, ExecutionFailed, ExecutionCancelled, ExecutionTimedOut,
	),
	ExecutionQueued: setOf(
		ExecutionStarting, ExecutionRunning, ExecutionSucceeded, ExecutionFailed,
		ExecutionCancelled, ExecutionTimedOut,
	),
	ExecutionStarting: setOf(
		ExecutionRunning, ExecutionSucceeded, ExecutionFailed, ExecutionCancelled,
		ExecutionTimedOut,
	),
	ExecutionRunning: setOf(
		ExecutionSucceeded, ExecutionFailed, ExecutionCancelled, ExecutionTimedOut,
	),
	ExecutionSucceeded: {},
	ExecutionFailed:    {},
	ExecutionCancelled: {},
	ExecutionTimedOut:  {},
}

// ResultState records verified local artifact availability, independently of execution.
type ResultState string

const (
	ResultNotAvailable ResultState = "not_available"
	ResultCollecting   ResultState = "collecting"
	ResultAvailable    ResultState = "available"
	ResultIncomplete   ResultState = "incomplete"
	ResultInvalid      ResultState = "invalid"
	ResultExpired      ResultState = "expired"
)

func (state ResultState) Valid() bool {
	_, ok := resultTransitions[state]
	return ok
}

var resultTransitions = map[ResultState]map[ResultState]struct{}{
	ResultNotAvailable: setOf(ResultCollecting, ResultExpired),
	ResultCollecting:   setOf(ResultAvailable, ResultIncomplete, ResultInvalid, ResultExpired),
	ResultAvailable:    setOf(ResultExpired),
	ResultIncomplete:   setOf(ResultCollecting, ResultExpired),
	ResultInvalid:      setOf(ResultCollecting, ResultExpired),
	ResultExpired:      {},
}

// CancellationState records intent and evidence, not merely a local request.
type CancellationState string

const (
	CancellationNotRequested CancellationState = "not_requested"
	CancellationRequested    CancellationState = "requested"
	CancellationRequesting    CancellationState = "requesting"
	CancellationAccepted     CancellationState = "accepted"
	CancellationPrevented    CancellationState = "prevented"
	CancellationConfirmed    CancellationState = "confirmed"
	CancellationTooLate      CancellationState = "too_late"
	CancellationManual       CancellationState = "manual_required"
)

func (state CancellationState) Valid() bool {
	_, ok := cancellationTransitions[state]
	return ok
}

var cancellationTransitions = map[CancellationState]map[CancellationState]struct{}{
	CancellationNotRequested: setOf(CancellationRequested, CancellationPrevented),
	CancellationRequested: setOf(
		CancellationRequesting, CancellationAccepted, CancellationPrevented,
		CancellationConfirmed, CancellationTooLate, CancellationManual,
	),
	CancellationRequesting: setOf(
		CancellationAccepted, CancellationConfirmed, CancellationTooLate,
		CancellationManual,
	),
	CancellationAccepted: setOf(
		CancellationConfirmed, CancellationTooLate, CancellationManual,
	),
	CancellationPrevented: {},
	CancellationConfirmed: {},
	CancellationTooLate:   {},
	CancellationManual:    setOf(CancellationConfirmed, CancellationTooLate),
}

// RemoteActivityState records whether this attempt has or may have active remote compute.
type RemoteActivityState string

const (
	RemoteActivityNotStarted RemoteActivityState = "not_started"
	RemoteActivityUnknown    RemoteActivityState = "unknown"
	RemoteActivityPossible   RemoteActivityState = "possible"
	RemoteActivityActive     RemoteActivityState = "active"
	RemoteActivityInactive   RemoteActivityState = "inactive"
)

func (state RemoteActivityState) Valid() bool {
	_, ok := remoteActivityTransitions[state]
	return ok
}

var remoteActivityTransitions = map[RemoteActivityState]map[RemoteActivityState]struct{}{
	RemoteActivityNotStarted: setOf(
		RemoteActivityUnknown, RemoteActivityPossible, RemoteActivityActive,
		RemoteActivityInactive,
	),
	RemoteActivityUnknown:  setOf(RemoteActivityPossible, RemoteActivityActive, RemoteActivityInactive),
	RemoteActivityPossible: setOf(RemoteActivityActive, RemoteActivityInactive),
	RemoteActivityActive:   setOf(RemoteActivityInactive),
	RemoteActivityInactive: {},
}

// ReleaseEvidence describes hardware-release/accounting evidence separately from execution.
type ReleaseEvidence string

const (
	ReleaseEvidenceUnknown          ReleaseEvidence = "unknown"
	ReleaseEvidenceNotApplicable    ReleaseEvidence = "not_applicable"
	ReleaseEvidenceNotObservable    ReleaseEvidence = "not_observable"
	ReleaseEvidenceProviderReported ReleaseEvidence = "provider_reported"
	ReleaseEvidenceConfirmed        ReleaseEvidence = "confirmed"
)

func (evidence ReleaseEvidence) Valid() bool {
	_, ok := releaseEvidenceTransitions[evidence]
	return ok
}

var releaseEvidenceTransitions = map[ReleaseEvidence]map[ReleaseEvidence]struct{}{
	ReleaseEvidenceUnknown: setOf(
		ReleaseEvidenceNotApplicable, ReleaseEvidenceNotObservable,
		ReleaseEvidenceProviderReported, ReleaseEvidenceConfirmed,
	),
	ReleaseEvidenceNotApplicable:    {},
	ReleaseEvidenceNotObservable:    setOf(ReleaseEvidenceProviderReported, ReleaseEvidenceConfirmed),
	ReleaseEvidenceProviderReported: setOf(ReleaseEvidenceConfirmed),
	ReleaseEvidenceConfirmed:        {},
}
