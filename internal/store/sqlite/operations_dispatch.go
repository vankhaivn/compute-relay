package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

var _ dispatch.CancellationRepository = (*Store)(nil)

func pendingCancellation(ctx context.Context, tx *sql.Tx, work dispatch.Work) (*dispatch.CancellationControl, error) {
	var id domain.OperationID
	err := tx.QueryRowContext(ctx, `SELECT operation_id FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind='cancel' AND status IN ('accepted','running')`, string(work.Job.Job.WorkspaceID), string(work.Job.Job.ID), string(work.Job.Attempt.ID)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	r, err := loadControl(ctx, tx, work.Job.Job.WorkspaceID, id)
	if err != nil {
		return nil, err
	}
	return &dispatch.CancellationControl{Operation: r.Operation, Started: r.Started}, nil
}

func (s *Store) CommitCancellation(ctx context.Context, h dispatch.Handle, id domain.OperationID, action dispatch.CancellationAction, now time.Time) (dispatch.Work, error) {
	if !id.Valid() || !scheduler.ValidTime(now) || (action.Begin && (action.Outcome != nil || action.Code != "")) || (!action.Begin && (action.Outcome == nil) == (action.Code == "")) {
		return dispatch.Work{}, dispatch.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return dispatch.Work{}, err
	}
	defer done()
	var result dispatch.Work
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		work, err := loadDispatch(ctx, tx, h.Claim, now)
		if err != nil {
			return err
		}
		if err = checkHandle(work, h); err != nil {
			return err
		}
		if work.Cancellation == nil || work.Cancellation.Operation.ID != id || work.Journal.Phase != dispatch.Submitted || work.Journal.Remote == nil {
			return dispatch.ErrConflict
		}
		r, err := loadControl(ctx, tx, work.Job.Job.WorkspaceID, id)
		if err != nil {
			return err
		}
		state := work.Job.Attempt.State
		event := domain.EventCancellationObserved
		next := r
		switch {
		case action.Begin:
			if r.Started || r.Operation.Status != domain.OperationAccepted || state.Cancellation != domain.CancellationRequested {
				return dispatch.ErrConflict
			}
			next.Operation, err = r.Operation.Transition(domain.OperationRunning, nil, now)
			if err != nil {
				return err
			}
			next.Started = true
			next.Effect = operations.CancellationRequested
			if err = saveControl(ctx, tx, r, next); err != nil {
				return err
			}
			state.Cancellation = domain.CancellationRequesting
			event = domain.EventCancellationInvoked
		case action.Code != "":
			if action.Code != domain.CodeRemoteCancelUnsupported && action.Code != domain.CodeRemoteExecutionUnresolved {
				return dispatch.ErrInvalid
			}
			state.Cancellation = domain.CancellationManual
			state.Orchestration = domain.OrchestrationNeedsAttention
			next, err = finishControl(ctx, tx, r, domain.OperationManualRequired, operations.ManualRequired, controlProblem(action.Code, true), now)
			if err != nil {
				return err
			}
		default:
			if !r.Started || action.Outcome.Validate() != nil {
				return dispatch.ErrInvalid
			}
			switch action.Outcome.Status {
			case domain.CancellationConfirmed:
				obs := provider.Observation{Remote: *work.Journal.Remote, Execution: domain.ExecutionCancelled, RemoteActivity: domain.RemoteActivityInactive, ReleaseEvidence: domain.ReleaseEvidenceNotObservable, ObservedAt: now, RawState: string(domain.ExecutionCancelled)}
				journal, observed, _, err := dispatch.Apply(work.Journal, state, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &obs}, now)
				if err != nil {
					return err
				}
				if err = controlJournal(ctx, tx, work, journal); err != nil {
					return err
				}
				work.Journal = journal
				work.Handle.Version = journal.Version
				state = observed
				next, err = finishControl(ctx, tx, r, domain.OperationSucceeded, operations.CancellationConfirmed, nil, now)
				if err != nil {
					return err
				}
			case domain.CancellationAccepted, domain.CancellationTooLate:
				if state.Cancellation != domain.CancellationRequesting {
					return dispatch.ErrConflict
				}
				// A stop-request acknowledgement or a too-late hint does not contain the
				// terminal execution outcome. Observe it before claiming termination/too_late.
				state.Cancellation = domain.CancellationAccepted
			case domain.CancellationManual:
				state.Cancellation = domain.CancellationManual
				state.Orchestration = domain.OrchestrationNeedsAttention
				next, err = finishControl(ctx, tx, r, domain.OperationManualRequired, operations.ManualRequired, controlProblem(domain.CodeRemoteCancelUnsupported, true), now)
				if err != nil {
					return err
				}
			default:
				return dispatch.ErrInvalid
			}
		}
		evidenceKind := dispatch.Kind("")
		if action.Outcome != nil && action.Outcome.Status == domain.CancellationConfirmed {
			evidenceKind = dispatch.ObservationSeen
		}
		updated, err := controlState(ctx, tx, next, work.Job.Attempt, state, event, now)
		if err != nil {
			return err
		}
		work.Job.Attempt = updated
		work.Handle.Claim.AttemptRevision = updated.Revision
		if err = finishObservedOperations(ctx, tx, &work, evidenceKind, now); err != nil {
			return err
		}
		work.Cancellation, err = pendingCancellation(ctx, tx, work)
		if err != nil {
			return err
		}
		if err = advanceSchedulerClock(ctx, tx, now); err != nil {
			return err
		}
		result = work
		return nil
	})
	if err != nil {
		return dispatch.Work{}, err
	}
	return result, nil
}

// Called in the SAME transaction as dispatch evidence. A terminal operation is
// historical: later observations can improve job evidence, never rewrite its result.
func finishObservedOperations(ctx context.Context, tx *sql.Tx, work *dispatch.Work, kind dispatch.Kind, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `SELECT operation_id FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind IN ('cancel','reconcile') AND status IN ('accepted','running') ORDER BY operation_seq`, string(work.Job.Job.WorkspaceID), string(work.Job.Job.ID), string(work.Job.Attempt.ID))
	if err != nil {
		return dbError(err)
	}
	ids := []domain.OperationID{}
	for rows.Next() {
		var id domain.OperationID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return dbError(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return dbError(err)
	}
	for _, id := range ids {
		r, err := loadControl(ctx, tx, work.Job.Job.WorkspaceID, id)
		if err != nil {
			return err
		}
		state := work.Job.Attempt.State
		status, effect := domain.OperationSucceeded, operations.ObservationRefreshed
		var p *domain.Problem
		if r.Operation.Kind == domain.OperationCancel {
			if kind == dispatch.ObservationSeen && !state.Execution.Terminal() && state.Cancellation != domain.CancellationManual {
				// Bound this request's observation work, even if a provider keeps
				// reporting running. Expiry never proves termination or frees capacity.
				if err = controlEvent(ctx, tx, r.Operation.WorkspaceID, r.Operation.JobID, r.Operation.AttemptID, r.Operation.ID, domain.EventExecutionObserved, now); err != nil {
					return err
				}
				var polls int
				if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM events WHERE workspace_id=? AND operation_id=? AND type='execution.observed'", string(r.Operation.WorkspaceID), string(r.Operation.ID)).Scan(&polls); err != nil {
					return dbError(err)
				}
				if polls >= dispatch.MaxFailures {
					state.Cancellation = domain.CancellationManual
					state.Orchestration = domain.OrchestrationNeedsAttention
					updated, err := controlState(ctx, tx, r, work.Job.Attempt, state, domain.EventCancellationObserved, now)
					if err != nil {
						return err
					}
					work.Job.Attempt = updated
					work.Handle.Claim.AttemptRevision = updated.Revision
				}
			}
			switch state.Cancellation {
			case domain.CancellationConfirmed:
				effect = operations.CancellationConfirmed
			case domain.CancellationPrevented:
				effect = operations.DispatchPrevented
			case domain.CancellationTooLate:
				effect = operations.TooLate
			case domain.CancellationManual:
				status, effect = domain.OperationManualRequired, operations.ManualRequired
				p = controlProblem(domain.CodeRemoteExecutionUnresolved, true)
			default:
				continue
			}
		} else if work.Journal.Phase == dispatch.Attention {
			status, effect = domain.OperationManualRequired, operations.ManualRequired
			p = controlProblem(domain.CodeRemoteExecutionUnresolved, true)
		} else if !(kind == dispatch.SubmissionSeen && work.Journal.Remote != nil) && !(kind == dispatch.ObservationSeen && work.Journal.Observation != nil && work.Journal.Observation.Execution != domain.ExecutionUnknown) {
			continue
		}
		if _, err = finishControl(ctx, tx, r, status, effect, p, now); err != nil {
			return err
		}
	}
	return nil
}
