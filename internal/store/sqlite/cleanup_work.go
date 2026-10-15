package sqlite

import (
	"context"
	"database/sql"
	"errors"
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
		err := tx.QueryRowContext(ctx, `SELECT c.workspace_id,c.operation_id FROM operations c
 JOIN jobs j ON j.workspace_id=c.workspace_id AND j.job_id=c.job_id
 LEFT JOIN work_leases l ON l.kind='cleanup' AND l.key=json_array(c.workspace_id,c.operation_id)
 WHERE c.kind='cleanup' AND NOT EXISTS (SELECT 1 FROM retired_explicit_attempts r WHERE r.workspace_id=c.workspace_id AND r.job_id=c.job_id AND r.attempt_id=c.attempt_id)
 AND (c.status='accepted' OR (c.status='running' AND (l.until_ms IS NULL OR l.held=0 OR l.until_ms<=?)))
 ORDER BY c.operation_seq LIMIT 1`, now.UnixMilli()).Scan(&w, &id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		row, err := loadOperation(ctx, tx, w, id)
		if err != nil {
			return err
		}
		op := row.Record.Operation
		target, err := controlTarget(ctx, tx, w, op.JobID, op.AttemptID)
		if err != nil {
			return err
		}
		if now.Before(target.Attempt.UpdatedAt) || now.Before(op.UpdatedAt) {
			return ErrCorrupt
		}
		plan, err := eligibleCleanup(ctx, tx, target, row.Request.DiscardUncollected, now, op.ID)
		if errors.Is(err, operations.ErrBusy) || errors.Is(err, operations.ErrState) || errors.Is(err, operations.ErrUnresolved) || errors.Is(err, operations.ErrUncollected) {
			return failCleanup(ctx, tx, row.Record, now)
		}
		if err != nil {
			return err
		}
		if plan.resource != row.Resource {
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
		current := row.Record
		if op.Status == domain.OperationAccepted {
			next, err := op.Transition(domain.OperationRunning, nil, now)
			if err != nil {
				return ErrInvalid
			}
			current = operations.Record{Operation: next, Effect: operations.Pending}
			if err = saveOperation(ctx, tx, row, operationRow{Record: current}); err != nil {
				return err
			}
		}
		lease, err := claimWorkLease(ctx, tx, workLease{kind: cleanupWork, key: workKey(w, id)}, now, ttl)
		if err != nil {
			return err
		}
		work := cleanup.Work{
			Lease:     cleanup.Lease{WorkspaceID: w, OperationID: id, Generation: lease.generation, Fence: lease.fence, Until: lease.until},
			Record:    current,
			Binding:   plan.binding,
			Request:   provider.CleanupRequest{Remote: plan.remote, LedgerID: plan.resource, CreationOperationID: plan.creation, ResultsCollected: plan.collected, Mode: provider.CleanupApply},
			Resources: resources,
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
func failCleanup(ctx context.Context, tx *sql.Tx, before operations.Record, now time.Time) error {
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
	return saveOperation(ctx, tx, operationRow{Record: before}, operationRow{Record: operations.Record{Operation: next, Effect: operations.Failed}})
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
		before := row.Record
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
			if resource.Result.State == operations.ResourcePending || resource.Result.State == operations.ResourceRunning ||
				o.Effect != operations.Failed && !resource.Result.State.Confirmed() {
				return ErrInvalid
			}
			allConfirmed = allConfirmed && resource.Result.State.Confirmed()
			anyDeleted = anyDeleted || resource.Result.State == operations.ResourceDeleted
		}
		if len(resources) > 0 && (allConfirmed != (o.Effect != operations.Failed) || allConfirmed && (anyDeleted != (o.Effect == operations.RemoteDeleted))) {
			return ErrInvalid
		}
		status := domain.OperationSucceeded
		if o.Effect == operations.Failed {
			status = domain.OperationFailed
		}
		next, err := before.Operation.Transition(status, o.Problem, now)
		if err != nil {
			return ErrInvalid
		}
		if o.Effect == operations.RemoteDeleted || o.Effect == operations.AlreadyAbsent {
			if err = releaseGateStorage(ctx, tx, string(before.Operation.WorkspaceID), string(before.Operation.JobID), string(before.Operation.AttemptID)); err != nil {
				return err
			}
			if err = releaseHostCachePinsOnCleanup(ctx, tx, before.Operation, resources, now); err != nil {
				return err
			}
			if err = releaseHostApplicationImagePinsOnCleanup(ctx, tx, before.Operation, resources, now); err != nil {
				return err
			}
			if err = releaseAttemptStorageResourcesInTx(ctx, tx, before.Operation.WorkspaceID, before.Operation.JobID, before.Operation.AttemptID, now); err != nil {
				return err
			}
		}
		if err = releaseWorkLease(ctx, tx, cleanupLease(w.Lease), now); err != nil {
			return err
		}
		return saveOperation(ctx, tx, operationRow{Record: before}, operationRow{Record: operations.Record{Operation: next, Effect: o.Effect}})
	})
}
