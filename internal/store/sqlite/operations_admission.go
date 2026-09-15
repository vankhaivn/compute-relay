package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

var _ operations.Repository = (*Store)(nil)

// controlTarget can read a historical attempt without changing the job's active
// pointer. Only retry requires the target to still be the current attempt.
func controlTarget(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID) (admission.Record, error) {
	r, err := readJobRecord(ctx, tx, w, job)
	if err != nil {
		return r, err
	}
	if r.Attempt.ID != attempt {
		r.Attempt, r.AttemptNonce, err = loadAttempt(ctx, tx, w, job, attempt)
	}
	return r, err
}

// controlWork validates immutable dispatch evidence WITHOUT granting a worker
// lease. Its handle contains a queue identity only and cannot authorize provider I/O.
func controlWork(ctx context.Context, tx *sql.Tx, record admission.Record) (dispatch.Work, error) {
	work := dispatch.Work{Job: record}
	var barrier bool
	err := tx.QueryRowContext(ctx, `SELECT queue_seq,dispatch_barrier FROM scheduler_queue WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(record.Job.WorkspaceID), string(record.Job.ID), string(record.Attempt.ID)).Scan(&work.Handle.Claim.Sequence, &barrier)
	if err != nil {
		return work, dbError(err)
	}
	work.Journal, err = loadJournal(ctx, tx, work.Handle.Claim.Sequence)
	if err != nil {
		return work, err
	}
	work.Handle.Version = work.Journal.Version
	if err = tx.QueryRowContext(ctx, "SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&work.InstallationID); err != nil {
		return work, dbError(err)
	}
	if !work.InstallationID.Valid() {
		return work, ErrCorrupt
	}
	if work.Journal.Plan != nil {
		if !barrier || matchesFrozenPlan(work, *work.Journal.Plan, work.Journal.PreparationID) != nil {
			return work, ErrCorrupt
		}
		if err = checkDispatchLedger(ctx, tx, work); err != nil {
			return work, err
		}
	} else if barrier && work.Journal.Phase != dispatch.Failed && work.Journal.Phase != dispatch.Prevented {
		// A legacy barrier without a journal is not proof that nothing was submitted.
		return work, operations.ErrUnresolved
	}
	return work, nil
}

func retryInputs(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, allowed []string, job domain.JobID, attempt domain.AttemptID) (operations.RetryInputs, error) {
	var result operations.RetryInputs
	r, err := controlTarget(ctx, tx, w, job, attempt)
	if err != nil {
		return result, err
	}
	if r.Job.ActiveAttemptID != attempt {
		return result, operations.ErrChanged
	}
	permitted := false
	for _, profile := range allowed {
		if profile == r.Profile.Binding.Profile {
			permitted = true
		}
	}
	if !permitted {
		return result, auth.ErrForbidden
	}
	if err = operations.RetryAllowed(r.Attempt.State); err != nil {
		return result, err
	}
	work, err := controlWork(ctx, tx, r)
	if err != nil {
		return result, err
	}
	j := work.Journal
	if j.SubmitStarted {
		if r.Attempt.State.Execution == domain.ExecutionNotSubmitted {
			if j.Phase != dispatch.Rejected {
				return result, operations.ErrUnresolved
			}
		} else if j.Remote == nil || j.Observation == nil || !j.Observation.Execution.Terminal() || j.Observation.Execution != r.Attempt.State.Execution {
			return result, operations.ErrUnresolved
		}
	} else if r.Attempt.State.Execution != domain.ExecutionNotSubmitted {
		return result, operations.ErrUnresolved
	}
	if r.PendingHTTPS != 0 || len(r.Objects) != 1+len(r.Request.Spec().Inputs) {
		return result, operations.ErrInputs
	}
	for _, ref := range r.Objects {
		var m domain.ObjectMetadata
		err = tx.QueryRowContext(ctx, retainedInputQuery, string(w), string(ref.Object.ID)).Scan(&m.WorkspaceID, &m.ID, &m.Bytes, &m.SHA256)
		if errors.Is(err, sql.ErrNoRows) {
			return result, operations.ErrInputs
		}
		if err != nil {
			return result, dbError(err)
		}
		if m != ref.Object {
			return result, operations.ErrInputs
		}
	}
	return operations.RetryInputs{Attempt: r.Attempt, Objects: r.Objects}, nil
}

func (s *Store) RetryInputs(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, attempt domain.AttemptID) (operations.RetryInputs, error) {
	if !w.Valid() || !job.Valid() || !attempt.Valid() {
		return operations.RetryInputs{}, operations.ErrRequest
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return operations.RetryInputs{}, err
	}
	defer done()
	var result operations.RetryInputs
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		allowed, err := actorInTx(ctx, tx, w, token, auth.Operate)
		if err != nil {
			return err
		}
		result, err = retryInputs(ctx, tx, w, allowed, job, attempt)
		return err
	})
	if err != nil {
		return operations.RetryInputs{}, err
	}
	return result, nil
}

func (s *Store) AdmitOperation(ctx context.Context, c operations.Command) (operations.Record, error) {
	c.Now = c.Now.UTC()
	if c.Validate() != nil || !scheduler.ValidTime(c.Now) {
		return operations.Record{}, operations.ErrRequest
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return operations.Record{}, err
	}
	defer done()
	var result operations.Record
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		allowed, err := actorInTx(ctx, tx, c.WorkspaceID, c.TokenID, auth.Operate)
		if err != nil {
			return err
		}
		prior, err := replayControl(ctx, tx, c.WorkspaceID, c.Kind, c.KeyDigest, c.RequestHash)
		if err != nil {
			return err
		}
		if prior != nil {
			result = *prior
			return nil
		}
		if _, _, err = schedulerControl(ctx, tx, c.Now); err != nil {
			return err
		}
		target, err := controlTarget(ctx, tx, c.WorkspaceID, c.JobID, c.Request.AttemptID)
		if err != nil {
			return err
		}
		if c.Now.Before(target.Attempt.UpdatedAt) {
			return scheduler.ErrClock
		}
		// Coalesce an attempt's cancellation intent (including terminal/manual
		// outcomes), and an already-pending read/collection request. Distinct HTTP
		// keys never multiply in-flight actions against the same attempt.
		var existing string
		query := `SELECT operation_id FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind=? AND status IN ('accepted','running')`
		if c.Kind == domain.OperationCancel {
			query = `SELECT operation_id FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind=?`
		}
		err = tx.QueryRowContext(ctx, query, string(c.WorkspaceID), string(c.JobID), string(c.Request.AttemptID), string(c.Kind)).Scan(&existing)
		if err == nil {
			r, err := loadControl(ctx, tx, c.WorkspaceID, domain.OperationID(existing))
			if err != nil {
				return err
			}
			if err = saveControlReceipt(ctx, tx, c, r.Record); err != nil {
				return err
			}
			result = r.Record
			result.Replay = true
			return advanceSchedulerClock(ctx, tx, c.Now)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		if c.Kind == domain.OperationRetryCompute {
			proof, err := retryInputs(ctx, tx, c.WorkspaceID, allowed, c.JobID, c.Request.AttemptID)
			if err != nil {
				return err
			}
			if c.Inputs.Attempt != proof.Attempt || len(c.Inputs.Objects) != len(proof.Objects) {
				return operations.ErrChanged
			}
			for i := range proof.Objects {
				if c.Inputs.Objects[i] != proof.Objects[i] {
					return operations.ErrInputs
				}
			}
			if err = controlLimit(ctx, tx, c, true); err != nil {
				return err
			}
		} else if c.Kind != domain.OperationCancel {
			if err = controlLimit(ctx, tx, c, false); err != nil {
				return err
			}
		}
		id, err := randomID("op_", 16)
		if err != nil {
			return err
		}
		op, err := domain.NewOperation(domain.OperationID(id), c.WorkspaceID, c.JobID, c.Request.AttemptID, c.Kind, c.Now)
		if err != nil {
			return ErrInvalid
		}
		r := controlRecord{Record: operations.Record{Operation: op, Effect: operations.Pending}}
		if c.Kind == domain.OperationCollect {
			r.Effect = operations.CollectionRequested
		}
		if err = insertControl(ctx, tx, c, r.Record); err != nil {
			return err
		}
		if err = controlEvent(ctx, tx, c.WorkspaceID, c.JobID, c.Request.AttemptID, op.ID, domain.EventOperationAccepted, c.Now); err != nil {
			return err
		}
		switch c.Kind {
		case domain.OperationCancel:
			r, err = admitCancellation(ctx, tx, target, r, c.Now)
		case domain.OperationRetryCompute:
			r, err = createRetry(ctx, tx, target, r, c.Now)
		case domain.OperationReconcile:
			r, err = admitReconciliation(ctx, tx, target, r, c.Now)
		case domain.OperationCollect:
			r, err = admitCollection(ctx, tx, target, r, c.Now)
		}
		if err != nil {
			return err
		}
		if err = saveControlReceipt(ctx, tx, c, r.Record); err != nil {
			return err
		}
		if err = advanceSchedulerClock(ctx, tx, c.Now); err != nil {
			return err
		}
		result = r.Record
		return nil
	})
	if err != nil {
		// No IDs or success escape a failed/uncertain transaction acknowledgement.
		return operations.Record{}, err
	}
	return result, nil
}

func controlLimit(ctx context.Context, tx *sql.Tx, c operations.Command, retry bool) error {
	query := `SELECT count(*),COALESCE(sum(CASE WHEN workspace_id=? THEN 1 ELSE 0 END),0) FROM operations WHERE status IN ('accepted','running')`
	if retry {
		query = `SELECT count(*),COALESCE(sum(CASE WHEN j.workspace_id=? THEN 1 ELSE 0 END),0) FROM jobs j JOIN attempts a ON a.workspace_id=j.workspace_id AND a.job_id=j.job_id AND a.attempt_id=j.active_attempt_id WHERE a.orchestration NOT IN ('succeeded','failed','cancelled','timed_out')`
	}
	var total, workspace int
	if err := tx.QueryRowContext(ctx, query, string(c.WorkspaceID)).Scan(&total, &workspace); err != nil {
		return dbError(err)
	}
	if total >= c.Limits.MaxOutstandingTotal || workspace >= c.Limits.MaxOutstandingWorkspace {
		return admission.ErrLimit
	}
	return nil
}

func createRetry(ctx context.Context, tx *sql.Tx, target admission.Record, r controlRecord, now time.Time) (controlRecord, error) {
	if target.Job.ActiveAttemptID != target.Attempt.ID || target.Attempt.Number == math.MaxUint32 {
		return r, operations.ErrChanged
	}
	id, err := randomID("att_", 16)
	if err != nil {
		return r, err
	}
	nonce, err := randomID("", 32)
	if err != nil {
		return r, err
	}
	a, err := domain.NewAttempt(domain.AttemptID(id), target.Job.ID, target.Attempt.Number+1, now)
	if err != nil {
		return r, ErrInvalid
	}
	state, err := json.Marshal(a.State)
	if err != nil {
		return r, ErrInvalid
	}
	created := now.UTC().Format(time.RFC3339Nano)
	_, err = tx.ExecContext(ctx, `INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?)`, string(target.Job.WorkspaceID), string(target.Job.ID), id, a.Number, nonce, string(state), string(a.State.Orchestration), a.Revision, created, created)
	if err != nil {
		return r, dbError(err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE jobs SET active_attempt_id=? WHERE workspace_id=? AND job_id=? AND active_attempt_id=?`, id, string(target.Job.WorkspaceID), string(target.Job.ID), string(target.Attempt.ID))
	if err != nil {
		return r, dbError(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return r, dbError(err)
	}
	if n != 1 {
		return r, operations.ErrChanged
	}
	before := r
	r.Operation, err = r.Operation.Transition(domain.OperationSucceeded, nil, now)
	if err != nil {
		return r, err
	}
	r.NewAttemptID = a.ID
	r.Effect = operations.NewAttemptCreated
	if err = saveControl(ctx, tx, before, r); err != nil {
		return r, err
	}
	if err = controlEvent(ctx, tx, target.Job.WorkspaceID, target.Job.ID, a.ID, r.Operation.ID, domain.EventAttemptRetried, now); err != nil {
		return r, err
	}
	if err = controlEvent(ctx, tx, target.Job.WorkspaceID, target.Job.ID, target.Attempt.ID, r.Operation.ID, domain.EventOperationCompleted, now); err != nil {
		return r, err
	}
	// scheduler_enqueue creates a new queued item; old queue/lease/journal/history
	// remain unchanged. There is no implicit provider/credential/profile resolution.
	return r, nil
}
