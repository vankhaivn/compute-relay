package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// ClaimRecovery returns an observation/staging-readiness lease, never a repeat-mutation
// permit. Pausing new dispatch does not discard the ability to observe existing work.
func (s *Store) ClaimRecovery(ctx context.Context, owner string) (*scheduler.Claim, error) {
	if !domain.ObjectID(owner).Valid() {
		return nil, scheduler.ErrInvalid
	}
	var result *scheduler.Claim
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		settings, last, err := schedulerControl(ctx, tx)
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
			return nil
		}
		// Only known journal phases can recover. An old dispatch barrier without this
		// ledger remains untouched and requires operator inspection, not guessed migration.
		var sequence int64
		err = tx.QueryRowContext(ctx, `SELECT q.queue_seq FROM scheduler_queue q
 JOIN dispatch_journals d ON d.queue_seq=q.queue_seq
 JOIN jobs j ON j.workspace_id=q.workspace_id AND j.job_id=q.job_id AND j.active_attempt_id=q.attempt_id
 JOIN attempts a ON a.workspace_id=q.workspace_id AND a.job_id=q.job_id AND a.attempt_id=q.attempt_id
 LEFT JOIN work_leases l ON l.kind='dispatch' AND l.key=json_array(q.queue_seq)
 LEFT JOIN preparation_authorizations g ON g.workspace_id=q.workspace_id AND g.job_id=q.job_id AND g.attempt_id=q.attempt_id
 LEFT JOIN start_authorizations x ON x.workspace_id=q.workspace_id AND x.job_id=q.job_id AND x.attempt_id=q.attempt_id
 WHERE NOT EXISTS (SELECT 1 FROM retired_explicit_attempts r WHERE r.workspace_id=q.workspace_id AND r.job_id=q.job_id AND r.attempt_id=q.attempt_id)
 AND q.dispatch_barrier=1 AND d.phase IN ('staging','ready','submitting','submitted')
 AND a.orchestration NOT IN ('succeeded','failed','cancelled','timed_out')
 AND (d.phase!='ready' OR g.authorization_id IS NULL OR x.authorization_id IS NOT NULL OR g.ready_until_ms<=?
 OR EXISTS(SELECT 1 FROM operations o WHERE o.workspace_id=q.workspace_id AND o.job_id=q.job_id AND o.attempt_id=q.attempt_id AND o.kind='cancel' AND o.status IN ('accepted','running')))
 AND q.not_before_ms<=? AND (l.held IS NULL OR l.held=0 OR l.until_ms<=?)
 ORDER BY q.not_before_ms,q.queue_seq LIMIT 1`, now.UnixMilli(), now.UnixMilli(), now.UnixMilli()).Scan(&sequence)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
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
			return ErrCorrupt
		}
		lease, err := claimWorkLease(ctx, tx, workLease{kind: dispatchWork, key: workKey(sequence), owner: owner}, now, settings.LeaseDuration)
		if err != nil {
			return err
		}
		c := scheduler.Claim{Identity: row.Identity, Sequence: sequence, Owner: owner, Generation: lease.generation, Fence: lease.fence, ExpiresAt: lease.until, AttemptRevision: a.Revision, AccountScope: row.AccountScope, ProviderInstanceID: row.ProviderInstanceID}
		if _, err = loadDispatch(ctx, tx, c, now); err != nil {
			return err
		}
		if _, err = schedulerTransition(ctx, tx, row.Identity, a, a.State, domain.EventSchedulerClaimed, now); err != nil {
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
func (s *Store) RenewDispatch(ctx context.Context, h dispatch.Handle) (dispatch.Handle, error) {
	var result dispatch.Handle
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		work, err := loadDispatch(ctx, tx, h.Claim, now)
		if err != nil {
			return err
		}
		if err = checkHandle(work, h); err != nil {
			return err
		}
		settings, _, err := schedulerControl(ctx, tx)
		if err != nil {
			return err
		}
		until, err := renewWorkLease(ctx, tx, dispatchLease(h.Claim), now, settings.LeaseDuration)
		if err != nil {
			return err
		}
		result = h
		result.Claim.ExpiresAt = until
		return nil
	})
	if err != nil {
		return dispatch.Handle{}, err
	}
	return result, nil
}

// YieldDispatch releases LOCAL worker ownership, never the dispatch barrier or remote
// execution evidence. HoldsAccount continues to reserve possible remote execution.
func (s *Store) YieldDispatch(ctx context.Context, h dispatch.Handle, delay time.Duration) error {
	if delay < 0 || delay > time.Hour {
		return scheduler.ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
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
		if err = releaseWorkLease(ctx, tx, dispatchLease(h.Claim), now); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE scheduler_queue SET not_before_ms=? WHERE queue_seq=?", notBefore.UnixMilli(), h.Claim.Sequence); err != nil {
			return dbError(err)
		}
		return nil
	})
}
