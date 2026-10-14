package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var _ cleanup.WorkRepository = (*Store)(nil)

func (s *Store) ClaimCleanup(ctx context.Context, ttl time.Duration) (*cleanup.Work, error) {
	if ttl < time.Second || ttl > 31*time.Minute {
		return nil, ErrInvalid
	}
	var result *cleanup.Work
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var w domain.WorkspaceID
		var id domain.OperationID
		err := tx.QueryRowContext(ctx, `SELECT c.workspace_id,c.operation_id FROM cleanup_operations c
 JOIN jobs j ON j.workspace_id=c.workspace_id AND j.job_id=c.job_id
 LEFT JOIN work_leases l ON l.kind='cleanup' AND l.key=json_array(c.workspace_id,c.operation_id)
 WHERE NOT EXISTS (SELECT 1 FROM retired_explicit_attempts r WHERE r.workspace_id=c.workspace_id AND r.job_id=c.job_id AND r.attempt_id=c.attempt_id)
 AND (c.status='accepted' OR (c.status='running' AND (l.until_ms IS NULL OR l.held=0 OR l.until_ms<=?)))
 ORDER BY c.cleanup_seq LIMIT 1`, now.UnixMilli()).Scan(&w, &id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		row, err := loadCleanup(ctx, tx, w, id)
		if err != nil {
			return err
		}
		op := row.record.Operation
		target, err := controlTarget(ctx, tx, w, op.JobID, op.AttemptID)
		if err != nil {
			return err
		}
		if now.Before(target.Attempt.UpdatedAt) || now.Before(op.UpdatedAt) {
			return ErrCorrupt
		}
		plan, err := eligibleCleanup(ctx, tx, target, row.request.DiscardUncollected, now, op.ID)
		if errors.Is(err, operations.ErrBusy) || errors.Is(err, operations.ErrState) || errors.Is(err, operations.ErrUnresolved) || errors.Is(err, cleanup.ErrUncollected) {
			return failCleanup(ctx, tx, row.record, now)
		}
		if err != nil {
			return err
		}
		if plan.resource != row.resource {
			return ErrCorrupt
		}
		resources, err := loadCleanupResources(ctx, tx, op)
		if err != nil {
			return err
		}
		if len(resources) == 0 {
			// Upgrade an older pending operation without changing its original targets.
			if err = snapshotCleanupResources(ctx, tx, op, plan.resources); err != nil {
				return err
			}
			if op.Status == domain.OperationRunning {
				// The old attempt-wide callback may already have affected any target.
				// A missing per-resource intent is not proof that deletion never began.
				if _, err = tx.ExecContext(ctx, `UPDATE cleanup_resources SET state='running',reason='legacy_delete_intent_recovered' WHERE workspace_id=? AND operation_id=?`, string(w), string(id)); err != nil {
					return dbError(err)
				}
			}
			resources, err = loadCleanupResources(ctx, tx, op)
			if err != nil {
				return err
			}
		}
		if len(resources) != len(plan.resources) {
			return ErrCorrupt
		}
		for i, resource := range resources {
			if resource.Result.Fingerprint != plan.resources[i].Fingerprint() || resource.Result.ResourceID != plan.resources[i].Request.LedgerID {
				return ErrCorrupt
			}
		}
		current := row.record
		if op.Status == domain.OperationAccepted {
			next, err := op.Transition(domain.OperationRunning, nil, now)
			if err != nil {
				return ErrInvalid
			}
			current = cleanup.Record{Operation: next, Effect: cleanup.Pending}
			if err = saveCleanup(ctx, tx, row.record, current); err != nil {
				return err
			}
		}
		lease, err := claimWorkLease(ctx, tx, workLease{kind: cleanupWork, key: workKey(w, id)}, now, ttl)
		if err != nil {
			return err
		}
		work := cleanup.Work{
			Lease:                 cleanup.Lease{WorkspaceID: w, OperationID: id, Generation: lease.generation, Fence: lease.fence, Until: lease.until},
			Record:                current,
			Binding:               plan.binding,
			Request:               provider.CleanupRequest{Remote: plan.remote, LedgerID: plan.resource, CreationOperationID: plan.creation, ResultsCollected: plan.collected, Mode: provider.CleanupApply},
			Resources:             resources,
			AcceptNameBasedDelete: row.request.AcceptNameBasedDelete,
		}
		if !work.Valid() {
			return ErrCorrupt
		}
		result = &work
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) RenewCleanup(ctx context.Context, w cleanup.Work, ttl time.Duration) (time.Time, error) {
	if !w.Valid() || ttl < time.Second || ttl > 31*time.Minute {
		return time.Time{}, ErrInvalid
	}
	var until time.Time
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := verifyCleanupLease(ctx, tx, w, now); err != nil {
			return err
		}
		var err error
		until, err = renewWorkLease(ctx, tx, cleanupLease(w.Lease), now, ttl)
		return err
	})
	return until, err
}

// failCleanup settles a request whose attempt is no longer eligible. A recovered
// running resource keeps an unknown outcome; confirmed earlier outcomes stay intact.
func failCleanup(ctx context.Context, tx *sql.Tx, before cleanup.Record, now time.Time) error {
	p, err := domain.NewProblem(domain.CodeIllegalStateTransition, "The attempt is no longer eligible for remote cleanup; no further deletion was attempted. Inspect its resource outcomes before requesting cleanup again.", domain.FailureStageOperation)
	if err != nil {
		return ErrInvalid
	}
	p = p.WithRetrySemantics(false, false).WithRecommendedAction(domain.RecommendedActionRetryRead)
	if before.Operation.Status == domain.OperationRunning {
		var pinned bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM cleanup_resources WHERE workspace_id=? AND operation_id=?)`, string(before.Operation.WorkspaceID), string(before.Operation.ID)).Scan(&pinned); err != nil {
			return dbError(err)
		}
		if !pinned {
			p, err = domain.NewProblem(domain.CodeProviderStateUnknown, "The earlier attempt-wide deletion outcome is unknown and current eligibility prevents recovery; no further deletion was attempted.", domain.FailureStageObservation)
			if err != nil {
				return ErrInvalid
			}
			p = p.WithRetrySemantics(false, false).WithRecommendedAction(domain.RecommendedActionInspectProvider)
		}
	}
	next, err := before.Operation.Transition(domain.OperationFailed, &p, now)
	if err != nil {
		return ErrInvalid
	}
	_, err = tx.ExecContext(ctx, `UPDATE cleanup_resources SET state=CASE state WHEN 'running' THEN 'unknown' ELSE 'retained' END,
 reason=CASE state WHEN 'running' THEN 'prior_delete_outcome_unknown' ELSE 'eligibility_changed' END
 WHERE workspace_id=? AND operation_id=? AND state IN ('pending','running')`, string(before.Operation.WorkspaceID), string(before.Operation.ID))
	if err != nil {
		return dbError(err)
	}
	return saveCleanup(ctx, tx, before, cleanup.Record{Operation: next, Effect: cleanup.Failed})
}

// CompleteCleanup records the aggregate of the resource outcomes under w's fence.
func (s *Store) CompleteCleanup(ctx context.Context, w cleanup.Work, o cleanup.Outcome) error {
	if !w.Valid() || !o.Valid() {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		row, err := verifyCleanupLease(ctx, tx, w, now)
		if err != nil {
			return err
		}
		before := row.record
		if before.Operation.Status != domain.OperationRunning || before.Operation.Revision != w.Record.Operation.Revision {
			return cleanup.ErrLeaseLost
		}
		if now.Before(before.Operation.UpdatedAt) {
			return ErrCorrupt
		}
		resources, err := loadCleanupResources(ctx, tx, before.Operation)
		if err != nil {
			return err
		}
		allConfirmed, anyDeleted := true, false
		for _, resource := range resources {
			if resource.Result.State == cleanup.ResourcePending || resource.Result.State == cleanup.ResourceRunning ||
				o.Effect != cleanup.Failed && !resource.Result.State.Confirmed() {
				return ErrInvalid
			}
			allConfirmed = allConfirmed && resource.Result.State.Confirmed()
			anyDeleted = anyDeleted || resource.Result.State == cleanup.ResourceDeleted
		}
		if len(resources) > 0 && (allConfirmed != (o.Effect != cleanup.Failed) || allConfirmed && (anyDeleted != (o.Effect == cleanup.RemoteDeleted))) {
			return ErrInvalid
		}
		status := domain.OperationSucceeded
		if o.Effect == cleanup.Failed {
			status = domain.OperationFailed
		}
		next, err := before.Operation.Transition(status, o.Problem, now)
		if err != nil {
			return ErrInvalid
		}
		if o.Effect == cleanup.RemoteDeleted || o.Effect == cleanup.AlreadyAbsent {
			if err = releaseGateStorage(ctx, tx, string(before.Operation.WorkspaceID), string(before.Operation.JobID), string(before.Operation.AttemptID)); err != nil {
				return err
			}
		}
		if err = releaseWorkLease(ctx, tx, cleanupLease(w.Lease), now); err != nil {
			return err
		}
		return saveCleanup(ctx, tx, before, cleanup.Record{Operation: next, Effect: o.Effect})
	})
}

// saveCleanup advances exactly one revision through an allowed transition.
func saveCleanup(ctx context.Context, tx *sql.Tx, before, after cleanup.Record) error {
	if after.Validate() != nil || after.Replay || after.Operation.Revision != before.Operation.Revision+1 || after.Operation.Revision > math.MaxInt64 ||
		after.Operation.ID != before.Operation.ID || after.Operation.WorkspaceID != before.Operation.WorkspaceID || after.Operation.JobID != before.Operation.JobID ||
		after.Operation.AttemptID != before.Operation.AttemptID || !after.Operation.CreatedAt.Equal(before.Operation.CreatedAt) {
		return ErrInvalid
	}
	want, err := before.Operation.Transition(after.Operation.Status, after.Operation.Failure, after.Operation.UpdatedAt)
	if err != nil || want.Revision != after.Operation.Revision {
		return ErrInvalid
	}
	raw, err := json.Marshal(after)
	if err != nil || len(raw) > 8192 {
		return ErrInvalid
	}
	res, err := tx.ExecContext(ctx, `UPDATE cleanup_operations SET status=?,revision=?,record=? WHERE workspace_id=? AND operation_id=? AND revision=?`, string(after.Operation.Status), after.Operation.Revision, string(raw), string(after.Operation.WorkspaceID), string(after.Operation.ID), before.Operation.Revision)
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
