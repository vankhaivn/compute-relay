package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// ClaimRecovery returns an observation/staging-readiness lease, never a repeat-mutation
// permit. Pausing new dispatch does not discard the ability to observe existing work.
func (s *Store) ClaimRecovery(ctx context.Context, owner string, now time.Time) (*scheduler.Claim, error) {
	if !domain.ObjectID(owner).Valid() {
		return nil, scheduler.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return nil, err
	}
	defer done()
	var result *scheduler.Claim
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		settings, last, err := schedulerControl(ctx, tx, now)
		if err != nil {
			return err
		}
		rows, err := schedulerSnapshot(ctx, tx, settings, now)
		if err != nil {
			return err
		}
		_, view, err := scheduler.Select(rows, domain.WorkspaceID(last), settings, now)
		if err != nil {
			return err
		}
		if view.LiveWorkers >= settings.MaxWorkers {
			return advanceSchedulerClock(ctx, tx, now)
		}
		// Only known journal phases can recover. An old dispatch barrier without this
		// ledger remains untouched and requires operator inspection, not guessed migration.
		var sequence int64
		err = tx.QueryRowContext(ctx, `SELECT q.queue_seq FROM scheduler_queue q
 JOIN dispatch_journals d ON d.queue_seq=q.queue_seq
 JOIN jobs j ON j.workspace_id=q.workspace_id AND j.job_id=q.job_id AND j.active_attempt_id=q.attempt_id
 JOIN attempts a ON a.workspace_id=q.workspace_id AND a.job_id=q.job_id AND a.attempt_id=q.attempt_id
 LEFT JOIN scheduler_leases l ON l.queue_seq=q.queue_seq
 WHERE q.dispatch_barrier=1 AND d.phase IN ('staging','ready','submitting','submitted')
 AND a.orchestration NOT IN ('succeeded','failed','cancelled','timed_out')
 AND q.not_before_ms<=? AND (l.held IS NULL OR l.held=0 OR l.until_ms<=?)
 ORDER BY q.not_before_ms,q.queue_seq LIMIT 1`, now.UnixMilli(), now.UnixMilli()).Scan(&sequence)
		if errors.Is(err, sql.ErrNoRows) {
			return advanceSchedulerClock(ctx, tx, now)
		}
		if err != nil {
			return dbError(err)
		}
		var row *scheduler.Candidate
		for i := range rows {
			if rows[i].Sequence == sequence {
				row = &rows[i]
				break
			}
		}
		if row == nil {
			return ErrCorrupt
		}
		a, _, err := loadAttempt(ctx, tx, row.WorkspaceID, row.JobID, row.AttemptID)
		if err != nil {
			return err
		}
		if now.Before(a.UpdatedAt) {
			return scheduler.ErrClock
		}
		var generation int64
		err = tx.QueryRowContext(ctx, "SELECT generation FROM scheduler_leases WHERE queue_seq=?", sequence).Scan(&generation)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		if generation == math.MaxInt64 {
			return ErrConflict
		}
		fence, err := randomID("", 32)
		if err != nil {
			return err
		}
		until := time.UnixMilli(now.Add(settings.LeaseDuration).UnixMilli()).UTC()
		if !scheduler.ValidTime(until) {
			return scheduler.ErrInvalid
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO scheduler_leases VALUES(?,?,?,?,?,1) ON CONFLICT(queue_seq) DO UPDATE SET generation=excluded.generation,owner=excluded.owner,fence=excluded.fence,until_ms=excluded.until_ms,held=1`, sequence, generation+1, owner, fence, until.UnixMilli())
		if err != nil {
			return dbError(err)
		}
		c := scheduler.Claim{Identity: row.Identity, Sequence: sequence, Owner: owner, Generation: generation + 1, Fence: fence, ExpiresAt: until, AttemptRevision: a.Revision, AccountScope: row.AccountScope, ProviderInstanceID: row.ProviderInstanceID}
		if _, err = loadDispatch(ctx, tx, c, now); err != nil {
			return err
		}
		if _, err = schedulerTransition(ctx, tx, row.Identity, a, a.State, domain.EventSchedulerClaimed, now); err != nil {
			return err
		}
		if err = advanceSchedulerClock(ctx, tx, now); err != nil {
			return err
		}
		result = &c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (s *Store) RenewDispatch(ctx context.Context, h dispatch.Handle, now time.Time) (dispatch.Handle, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return dispatch.Handle{}, err
	}
	defer done()
	var result dispatch.Handle
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		work, err := loadDispatch(ctx, tx, h.Claim, now)
		if err != nil {
			return err
		}
		if err = checkHandle(work, h); err != nil {
			return err
		}
		settings, _, err := schedulerControl(ctx, tx, now)
		if err != nil {
			return err
		}
		until := time.UnixMilli(now.Add(settings.LeaseDuration).UnixMilli()).UTC()
		if !scheduler.ValidTime(until) {
			return scheduler.ErrInvalid
		}
		if until.Before(h.Claim.ExpiresAt) {
			until = h.Claim.ExpiresAt
		}
		if _, err = tx.ExecContext(ctx, "UPDATE scheduler_leases SET until_ms=? WHERE queue_seq=?", until.UnixMilli(), h.Claim.Sequence); err != nil {
			return dbError(err)
		}
		result = h
		result.Claim.ExpiresAt = until
		return advanceSchedulerClock(ctx, tx, now)
	})
	if err != nil {
		return dispatch.Handle{}, err
	}
	return result, nil
}

// YieldDispatch releases LOCAL worker ownership, never the dispatch barrier or remote
// execution evidence. HoldsAccount continues to reserve possible remote execution.
func (s *Store) YieldDispatch(ctx context.Context, h dispatch.Handle, now time.Time, delay time.Duration) error {
	if delay < 0 || delay > time.Hour {
		return scheduler.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		work, err := loadDispatch(ctx, tx, h.Claim, now)
		if err != nil {
			return err
		}
		if err = checkHandle(work, h); err != nil {
			return err
		}
		notBefore := now.Add(delay)
		if !scheduler.ValidTime(notBefore) {
			return scheduler.ErrInvalid
		}
		if _, err = schedulerTransition(ctx, tx, h.Claim.Identity, work.Job.Attempt, work.Job.Attempt.State, domain.EventSchedulerReleased, now); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE scheduler_leases SET owner='',fence='',until_ms=0,held=0 WHERE queue_seq=?", h.Claim.Sequence); err != nil {
			return dbError(err)
		}
		if _, err = tx.ExecContext(ctx, "UPDATE scheduler_queue SET not_before_ms=? WHERE queue_seq=?", notBefore.UnixMilli(), h.Claim.Sequence); err != nil {
			return dbError(err)
		}
		return advanceSchedulerClock(ctx, tx, now)
	})
}
