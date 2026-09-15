package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

// controlState changes the exact target revision and appends its operation-linked
// fact together. Advancing the revision fences any older dispatch callback without
// prematurely releasing its local worker reservation.
func controlState(ctx context.Context, tx *sql.Tx, r controlRecord, a domain.Attempt, state domain.AttemptState, event domain.EventType, now time.Time) (domain.Attempt, error) {
	next, err := a.Transition(state, now.UTC())
	if err != nil || next.Revision > math.MaxInt64 {
		return domain.Attempt{}, operations.ErrState
	}
	if next != a {
		raw, err := json.Marshal(next.State)
		if err != nil {
			return domain.Attempt{}, ErrInvalid
		}
		res, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?,orchestration=?,revision=?,updated_at=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND revision=?`, string(raw), string(next.State.Orchestration), next.Revision, next.UpdatedAt.Format(time.RFC3339Nano), string(r.Operation.WorkspaceID), string(a.JobID), string(a.ID), a.Revision)
		if err != nil {
			return domain.Attempt{}, dbError(err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return domain.Attempt{}, dbError(err)
		}
		if n != 1 {
			return domain.Attempt{}, operations.ErrChanged
		}
	}
	if err = controlEvent(ctx, tx, r.Operation.WorkspaceID, a.JobID, a.ID, r.Operation.ID, event, now); err != nil {
		return domain.Attempt{}, err
	}
	return next, nil
}

func controlJournal(ctx context.Context, tx *sql.Tx, work dispatch.Work, next dispatch.Journal) error {
	if !next.Valid() || next.Version != work.Journal.Version+1 {
		return ErrInvalid
	}
	raw, err := json.Marshal(next)
	if err != nil || len(raw) > dispatch.MaxJournalBytes {
		return ErrInvalid
	}
	if work.Journal.Version == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO dispatch_journals VALUES(?,?,?,?)`, work.Handle.Claim.Sequence, next.Version, string(next.Phase), string(raw))
		return dbError(err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE dispatch_journals SET version=?,phase=?,journal=? WHERE queue_seq=? AND version=?`, next.Version, string(next.Phase), string(raw), work.Handle.Claim.Sequence, work.Journal.Version)
	if err != nil {
		return dbError(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if n != 1 {
		return operations.ErrChanged
	}
	return nil
}

func admitCancellation(ctx context.Context, tx *sql.Tx, target admission.Record, r controlRecord, now time.Time) (controlRecord, error) {
	state := target.Attempt.State
	// A historical terminal orchestration outcome is immutable. Recording an
	// explicit late request does not rewrite it or manufacture a cancellation.
	if state.Orchestration.Terminal() {
		return finishControl(ctx, tx, r, domain.OperationSucceeded, operations.TooLate, nil, now)
	}
	if target.Job.ActiveAttemptID != target.Attempt.ID {
		return r, operations.ErrChanged
	}
	work, err := controlWork(ctx, tx, target)
	if err != nil {
		return r, err
	}
	if state.Execution.Terminal() {
		if state.Cancellation == domain.CancellationNotRequested {
			state.Cancellation = domain.CancellationRequested
			target.Attempt, err = controlState(ctx, tx, r, target.Attempt, state, domain.EventCancellationRequested, now)
			if err != nil {
				return r, err
			}
		}
		effect := operations.TooLate
		state.Cancellation = domain.CancellationTooLate
		if state.Execution == domain.ExecutionCancelled {
			state.Cancellation = domain.CancellationConfirmed
			effect = operations.CancellationConfirmed
		}
		if _, err = controlState(ctx, tx, r, target.Attempt, state, domain.EventCancellationObserved, now); err != nil {
			return r, err
		}
		return finishControl(ctx, tx, r, domain.OperationSucceeded, effect, nil, now)
	}
	if !work.Journal.SubmitStarted {
		next, state, _, err := dispatch.Apply(work.Journal, state, dispatch.Action{Kind: dispatch.PreventDispatch}, now)
		if err != nil {
			return r, err
		}
		if err = controlJournal(ctx, tx, work, next); err != nil {
			return r, err
		}
		if _, err = controlState(ctx, tx, r, target.Attempt, state, domain.EventCancellationRequested, now); err != nil {
			return r, err
		}
		return finishControl(ctx, tx, r, domain.OperationSucceeded, operations.DispatchPrevented, nil, now)
	}
	if state.Cancellation != domain.CancellationNotRequested {
		return r, operations.ErrState
	}
	state.Cancellation = domain.CancellationRequested
	state.Orchestration = domain.OrchestrationCancelling
	if _, err = controlState(ctx, tx, r, target.Attempt, state, domain.EventCancellationRequested, now); err != nil {
		return r, err
	}
	// Attention is not permission to call Submit again. Only safe identity observation
	// is re-enabled; permanent identity/privacy failures still require intervention.
	if work.Journal.Phase == dispatch.Attention {
		next, _, _, applyErr := dispatch.Apply(work.Journal, state, dispatch.Action{Kind: dispatch.RequestReconciliation}, now)
		if applyErr != nil {
			target.Attempt, _, err = loadAttempt(ctx, tx, r.Operation.WorkspaceID, r.Operation.JobID, r.Operation.AttemptID)
			if err != nil {
				return r, err
			}
			state = target.Attempt.State
			state.Cancellation = domain.CancellationManual
			state.Orchestration = domain.OrchestrationNeedsAttention
			if _, err = controlState(ctx, tx, r, target.Attempt, state, domain.EventCancellationObserved, now); err != nil {
				return r, err
			}
			return finishControl(ctx, tx, r, domain.OperationManualRequired, operations.ManualRequired, controlProblem(domain.CodeRemoteExecutionUnresolved, true), now)
		}
		if err = controlJournal(ctx, tx, work, next); err != nil {
			return r, err
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE scheduler_queue SET not_before_ms=0 WHERE queue_seq=?", work.Handle.Claim.Sequence)
	return r, dbError(err)
}

func admitReconciliation(ctx context.Context, tx *sql.Tx, target admission.Record, r controlRecord, now time.Time) (controlRecord, error) {
	if target.Attempt.State.Orchestration.Terminal() || target.Attempt.State.Execution.Terminal() {
		return finishControl(ctx, tx, r, domain.OperationSucceeded, operations.ObservationRefreshed, nil, now)
	}
	if target.Job.ActiveAttemptID != target.Attempt.ID {
		return r, operations.ErrChanged
	}
	work, err := controlWork(ctx, tx, target)
	if err != nil {
		return r, err
	}
	next, state, _, err := dispatch.Apply(work.Journal, target.Attempt.State, dispatch.Action{Kind: dispatch.RequestReconciliation}, now)
	if err != nil {
		return r, operations.ErrState
	}
	if err = controlJournal(ctx, tx, work, next); err != nil {
		return r, err
	}
	if _, err = controlState(ctx, tx, r, target.Attempt, state, domain.EventReconciliationRequested, now); err != nil {
		return r, err
	}
	_, err = tx.ExecContext(ctx, "UPDATE scheduler_queue SET not_before_ms=0 WHERE queue_seq=?", work.Handle.Claim.Sequence)
	return r, dbError(err)
}

func admitCollection(ctx context.Context, tx *sql.Tx, target admission.Record, r controlRecord, now time.Time) (controlRecord, error) {
	state := target.Attempt.State
	if !state.Execution.Terminal() {
		return r, operations.ErrUnresolved
	}
	if state.Result == domain.ResultAvailable {
		return finishControl(ctx, tx, r, domain.OperationSucceeded, operations.ResultsAvailable, nil, now)
	}
	if state.Result == domain.ResultExpired {
		return r, operations.ErrState
	}
	work, err := controlWork(ctx, tx, target)
	if err != nil {
		return r, err
	}
	if work.Journal.Remote == nil || work.Journal.Observation == nil || work.Journal.Observation.Execution != state.Execution || !work.Journal.Observation.Execution.Terminal() {
		return r, operations.ErrUnresolved
	}
	// M3-05 queues the durable transfer-only ticket. No result bytes are published or
	// declared verified here. M3-06 consumes tickets after implementing its verifier.
	if err = controlEvent(ctx, tx, r.Operation.WorkspaceID, r.Operation.JobID, r.Operation.AttemptID, r.Operation.ID, domain.EventCollectionRequested, now); err != nil {
		return r, err
	}
	return r, nil
}

func controlProblem(code domain.ErrorCode, may bool) *domain.Problem {
	message := "Remote execution is unresolved; inspect or reconcile the recorded identity without resubmitting."
	action := domain.RecommendedActionReconcile
	switch code {
	case domain.CodeRemoteCancelUnsupported:
		message = "Remote cancellation is unsupported or unverified for this binding; manual intervention may be required."
		action = domain.RecommendedActionInspectProvider
	case domain.CodeArtifactCollectionFailed:
		message = "Artifact verification did not complete; collect the same attempt without rerunning compute."
		action = domain.RecommendedActionCollect
	case domain.CodeRemoteExecutionUnresolved:
	default:
		return nil
	}
	stage := domain.FailureStageOperation
	if code == domain.CodeArtifactCollectionFailed {
		stage = domain.FailureStageResults
	}
	p, err := domain.NewProblem(code, message, stage)
	if err != nil {
		return nil
	}
	p = p.WithRetrySemantics(false, may).WithRecommendedAction(action)
	return &p
}
