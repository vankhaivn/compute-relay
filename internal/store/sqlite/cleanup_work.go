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
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

var _ cleanup.WorkRepository = (*Store)(nil)

func (s *Store) cleanupTx(ctx context.Context, now time.Time, fn func(context.Context, *sql.Tx) error) error {
	if !scheduler.ValidTime(now) {
		return ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, _, err := schedulerControl(ctx, tx, now); err != nil {
			return err
		}
		if err := fn(ctx, tx); err != nil {
			return err
		}
		return advanceSchedulerClock(ctx, tx, now)
	})
}

func (s *Store) ClaimCleanup(ctx context.Context, now time.Time, ttl time.Duration) (*cleanup.Work, error) {
	return s.claimCleanup(ctx, now, ttl, false)
}

// ClaimCleanupManaged claims cleanups of attempts frozen to managed connections.
func (s *Store) ClaimCleanupManaged(ctx context.Context, now time.Time, ttl time.Duration) (*cleanup.Work, error) {
	return s.claimCleanup(ctx, now, ttl, true)
}

// claimCleanup commits the running status and a fresh fence before any provider call.
// Eligibility is rechecked against current durable state; a request that is no longer
// eligible fails explicitly instead of reaching the provider.
func (s *Store) claimCleanup(ctx context.Context, now time.Time, ttl time.Duration, managed bool) (*cleanup.Work, error) {
	now = now.UTC()
	if ttl < time.Second || ttl > 31*time.Minute {
		return nil, ErrInvalid
	}
	var result *cleanup.Work
	err := s.cleanupTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		var w domain.WorkspaceID
		var id domain.OperationID
		err := tx.QueryRowContext(ctx, `SELECT c.workspace_id,c.operation_id FROM cleanup_operations c
 JOIN jobs j ON j.workspace_id=c.workspace_id AND j.job_id=c.job_id
 JOIN profile_revisions p ON p.profile=j.profile AND p.revision=j.profile_revision
 LEFT JOIN cleanup_leases l ON l.workspace_id=c.workspace_id AND l.operation_id=c.operation_id
 WHERE (COALESCE(json_extract(p.snapshot,'$.credential_ref'),'') LIKE 'vault:%')=?
 AND (c.status='accepted' OR (c.status='running' AND (l.until_ms IS NULL OR l.until_ms<=?)))
 ORDER BY c.cleanup_seq LIMIT 1`, managed, now.UnixMilli()).Scan(&w, &id)
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
			return scheduler.ErrClock
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
		var generation int64
		err = tx.QueryRowContext(ctx, `SELECT generation FROM cleanup_leases WHERE workspace_id=? AND operation_id=?`, string(w), string(id)).Scan(&generation)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		if generation == math.MaxInt64 {
			return ErrConflict
		}
		generation++
		fence, err := randomID("", 32)
		if err != nil {
			return err
		}
		until := time.UnixMilli(now.Add(ttl).UnixMilli()).UTC()
		// An upsert would run the insert trigger first, so a recovery claim updates.
		if generation == 1 {
			_, err = tx.ExecContext(ctx, `INSERT INTO cleanup_leases VALUES(?,?,?,?,?)`, string(w), string(id), generation, fence, until.UnixMilli())
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE cleanup_leases SET generation=?,fence=?,until_ms=? WHERE workspace_id=? AND operation_id=? AND generation=?`, generation, fence, until.UnixMilli(), string(w), string(id), generation-1)
		}
		if err != nil {
			return dbError(err)
		}
		work := cleanup.Work{
			Lease:   cleanup.Lease{WorkspaceID: w, OperationID: id, Generation: generation, Fence: fence, Until: until},
			Record:  current,
			Binding: plan.binding,
			Request: provider.CleanupRequest{Remote: plan.remote, LedgerID: plan.resource, CreationOperationID: plan.creation, ResultsCollected: plan.collected, Mode: provider.CleanupApply},
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

// failCleanup settles a request whose attempt is no longer eligible. Nothing was sent.
func failCleanup(ctx context.Context, tx *sql.Tx, before cleanup.Record, now time.Time) error {
	p, err := domain.NewProblem(domain.CodeIllegalStateTransition, "The attempt is no longer eligible for remote cleanup; nothing was deleted. Read its current state before requesting cleanup again.", domain.FailureStageOperation)
	if err != nil {
		return ErrInvalid
	}
	p = p.WithRetrySemantics(false, false).WithRecommendedAction(domain.RecommendedActionRetryRead)
	next, err := before.Operation.Transition(domain.OperationFailed, &p, now)
	if err != nil {
		return ErrInvalid
	}
	return saveCleanup(ctx, tx, before, cleanup.Record{Operation: next, Effect: cleanup.Failed})
}

// CompleteCleanup records the outcome of the one apply call made under w's fence.
func (s *Store) CompleteCleanup(ctx context.Context, w cleanup.Work, o cleanup.Outcome, now time.Time) error {
	now = now.UTC()
	if !w.Valid() || !o.Valid() {
		return ErrInvalid
	}
	return s.cleanupTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		l := w.Lease
		var generation, until int64
		var fence string
		err := tx.QueryRowContext(ctx, `SELECT generation,fence,until_ms FROM cleanup_leases WHERE workspace_id=? AND operation_id=?`, string(l.WorkspaceID), string(l.OperationID)).Scan(&generation, &fence, &until)
		if errors.Is(err, sql.ErrNoRows) {
			return cleanup.ErrLeaseLost
		}
		if err != nil {
			return dbError(err)
		}
		if generation != l.Generation || fence != l.Fence || until != l.Until.UnixMilli() || now.UnixMilli() >= until {
			return cleanup.ErrLeaseLost
		}
		row, err := loadCleanup(ctx, tx, l.WorkspaceID, l.OperationID)
		if err != nil {
			return err
		}
		before := row.record
		if before.Operation.Status != domain.OperationRunning || before.Operation.Revision != w.Record.Operation.Revision {
			return cleanup.ErrLeaseLost
		}
		if now.Before(before.Operation.UpdatedAt) {
			return scheduler.ErrClock
		}
		status := domain.OperationSucceeded
		if o.Effect == cleanup.Failed {
			status = domain.OperationFailed
		}
		next, err := before.Operation.Transition(status, o.Problem, now)
		if err != nil {
			return ErrInvalid
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
