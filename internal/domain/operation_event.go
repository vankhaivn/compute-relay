package domain

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// OperationKind identifies one explicit durable control action.
type OperationKind string

const (
	OperationCancel       OperationKind = "cancel"
	OperationRetryCompute OperationKind = "retry_compute"
	OperationReconcile    OperationKind = "reconcile"
	OperationCollect      OperationKind = "collect"
	OperationCleanup      OperationKind = "cleanup"
)

func (kind OperationKind) Valid() bool {
	switch kind {
	case OperationCancel, OperationRetryCompute, OperationReconcile, OperationCollect, OperationCleanup:
		return true
	default:
		return false
	}
}

// OperationStatus is the local lifecycle of a durable control action.
type OperationStatus string

const (
	OperationAccepted       OperationStatus = "accepted"
	OperationRunning        OperationStatus = "running"
	OperationSucceeded      OperationStatus = "succeeded"
	OperationFailed         OperationStatus = "failed"
	OperationManualRequired OperationStatus = "manual_required"
)

func (status OperationStatus) Valid() bool {
	switch status {
	case OperationAccepted, OperationRunning, OperationSucceeded, OperationFailed, OperationManualRequired:
		return true
	default:
		return false
	}
}

func (status OperationStatus) Terminal() bool {
	switch status {
	case OperationSucceeded, OperationFailed, OperationManualRequired:
		return true
	default:
		return false
	}
}

// Operation records an explicit action separately from job or attempt state.
type Operation struct {
	ID          OperationID
	WorkspaceID WorkspaceID
	JobID       JobID
	AttemptID   AttemptID
	Kind        OperationKind
	Status      OperationStatus
	Revision    uint64
	Failure     *Problem
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

var operationTransitions = map[OperationStatus]map[OperationStatus]struct{}{
	OperationAccepted:       setOf(OperationRunning, OperationSucceeded, OperationFailed, OperationManualRequired),
	OperationRunning:        setOf(OperationSucceeded, OperationFailed, OperationManualRequired),
	OperationSucceeded:      {},
	OperationFailed:         {},
	OperationManualRequired: {},
}

// NewOperation creates an accepted durable control operation.
func NewOperation(id OperationID, workspaceID WorkspaceID, jobID JobID, attemptID AttemptID, kind OperationKind, createdAt time.Time) (Operation, error) {
	operation := Operation{
		ID:          id,
		WorkspaceID: workspaceID,
		JobID:       jobID,
		AttemptID:   attemptID,
		Kind:        kind,
		Status:      OperationAccepted,
		Revision:    1,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
	if err := operation.Validate(); err != nil {
		return Operation{}, err
	}
	return operation, nil
}

func (operation Operation) Validate() error {
	if !operation.ID.Valid() || !operation.WorkspaceID.Valid() {
		return errors.New("operation identity is invalid")
	}
	if !operation.Kind.Valid() {
		return fmt.Errorf("invalid operation kind %q", operation.Kind)
	}
	if operation.Kind == OperationCleanup {
		if operation.JobID != "" && !operation.JobID.Valid() {
			return errors.New("cleanup job ID is invalid")
		}
		if operation.AttemptID != "" && !operation.AttemptID.Valid() {
			return errors.New("cleanup attempt ID is invalid")
		}
	} else {
		if !operation.JobID.Valid() || !operation.AttemptID.Valid() {
			return errors.New("job operation requires valid job and attempt IDs")
		}
	}
	if !operation.Status.Valid() {
		return fmt.Errorf("invalid operation status %q", operation.Status)
	}
	if operation.Revision == 0 {
		return errors.New("operation revision must be positive")
	}
	if operation.CreatedAt.IsZero() || operation.UpdatedAt.IsZero() || operation.UpdatedAt.Before(operation.CreatedAt) {
		return errors.New("operation timestamps are invalid")
	}
	if operation.Failure != nil {
		if err := operation.Failure.Validate(); err != nil {
			return fmt.Errorf("operation failure: %w", err)
		}
		if operation.Status != OperationFailed && operation.Status != OperationManualRequired {
			return errors.New("operation failure is only valid for failed or manual-required operations")
		}
	}
	if operation.Status == OperationFailed && operation.Failure == nil {
		return errors.New("failed operation requires a structured failure")
	}
	return nil
}

// Transition returns an updated operation copy and never rewrites a terminal outcome.
func (operation Operation) Transition(next OperationStatus, failure *Problem, updatedAt time.Time) (Operation, error) {
	if err := operation.Validate(); err != nil {
		return Operation{}, err
	}
	if !next.Valid() {
		return Operation{}, fmt.Errorf("invalid operation status %q", next)
	}
	if operation.Status == next {
		if failuresEqual(operation.Failure, failure) {
			return operation, nil
		}
		if operation.Status.Terminal() {
			return Operation{}, TransitionError{Dimension: "operation", From: string(operation.Status), To: string(next)}
		}
	} else if _, ok := operationTransitions[operation.Status][next]; !ok {
		return Operation{}, TransitionError{Dimension: "operation", From: string(operation.Status), To: string(next)}
	}
	if updatedAt.IsZero() {
		return Operation{}, errors.New("operation transition time must not be zero")
	}
	if updatedAt.Before(operation.UpdatedAt) {
		return Operation{}, errors.New("operation transition time precedes current update time")
	}
	if operation.Revision == math.MaxUint64 {
		return Operation{}, errors.New("operation revision overflow")
	}
	operation.Status = next
	operation.Failure = failure
	operation.Revision++
	operation.UpdatedAt = updatedAt
	if err := operation.Validate(); err != nil {
		return Operation{}, err
	}
	return operation, nil
}

func failuresEqual(left, right *Problem) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Code != right.Code || left.Message != right.Message || left.Stage != right.Stage ||
		left.SafeOperationRetry != right.SafeOperationRetry ||
		left.ComputeMayHaveStarted != right.ComputeMayHaveStarted ||
		left.RecommendedAction != right.RecommendedAction || len(left.Details) != len(right.Details) {
		return false
	}
	for key, value := range left.Details {
		if right.Details[key] != value {
			return false
		}
	}
	return true
}

// EventType is a stable provider-neutral event name.
type EventType string

const (
	EventJobAccepted              EventType = "job.accepted"
	EventInputsReady              EventType = "inputs.ready"
	EventProviderStagingReady     EventType = "provider.staging.ready"
	EventSubmissionIntentRecorded EventType = "submission.intent_recorded"
	EventSubmissionAccepted       EventType = "submission.accepted"
	EventSubmissionUnknown        EventType = "submission.unknown"
	EventExecutionObserved        EventType = "execution.observed"
	EventCancellationRequested    EventType = "cancellation.requested"
	EventCollectionFailed         EventType = "collection.failed"
	EventArtifactVerified         EventType = "artifact.verified"
	EventJobCompleted             EventType = "job.completed"
	EventSchedulerClaimed         EventType = "scheduler.claimed"
	EventSchedulerDeferred        EventType = "scheduler.deferred"
	EventSchedulerReleased        EventType = "scheduler.released"
	EventPreparationIntended      EventType = "preparation.intent_recorded"
	EventPreparationObserved      EventType = "preparation.observed"
	EventSubmissionRejected       EventType = "submission.rejected"
	EventReconciliationDeferred   EventType = "reconciliation.deferred"
)

func (eventType EventType) Valid() bool {
	switch eventType {
	case EventJobAccepted, EventInputsReady, EventProviderStagingReady,
		EventSubmissionIntentRecorded, EventSubmissionAccepted, EventSubmissionUnknown,
		EventExecutionObserved, EventCancellationRequested, EventCollectionFailed,
		EventArtifactVerified, EventJobCompleted, EventSchedulerClaimed, EventSchedulerDeferred, EventSchedulerReleased,
		EventPreparationIntended, EventPreparationObserved, EventSubmissionRejected, EventReconciliationDeferred:
		return true
	default:
		return false
	}
}

// Event is one monotonically sequenced durable fact for a job.
type Event struct {
	ID          EventID
	Sequence    uint64
	WorkspaceID WorkspaceID
	JobID       JobID
	AttemptID   AttemptID
	OperationID OperationID
	Type        EventType
	OccurredAt  time.Time
}

func (event Event) Validate() error {
	if !event.ID.Valid() || !event.WorkspaceID.Valid() || !event.JobID.Valid() {
		return errors.New("event identity is invalid")
	}
	if event.Sequence == 0 {
		return errors.New("event sequence must be positive")
	}
	if event.AttemptID != "" && !event.AttemptID.Valid() {
		return errors.New("event attempt ID is invalid")
	}
	if event.OperationID != "" && !event.OperationID.Valid() {
		return errors.New("event operation ID is invalid")
	}
	if !event.Type.Valid() {
		return fmt.Errorf("invalid event type %q", event.Type)
	}
	if event.OccurredAt.IsZero() {
		return errors.New("event timestamp must not be zero")
	}
	return nil
}
