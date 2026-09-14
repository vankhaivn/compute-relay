package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func ownedLocalClaim(ctx context.Context, tx *sql.Tx, c scheduler.Claim, now time.Time) (domain.Attempt, error) {
	if !c.Valid() || !scheduler.ValidTime(now) {
		return domain.Attempt{}, scheduler.ErrInvalid
	}
	var owner, fence, profile string
	var generation, until int64
	var held, barrier bool
	err := tx.QueryRowContext(ctx, `SELECT l.owner,l.fence,l.generation,l.until_ms,l.held,q.dispatch_barrier,r.snapshot
 FROM scheduler_leases l JOIN scheduler_queue q ON q.queue_seq=l.queue_seq
 JOIN jobs j ON j.workspace_id=q.workspace_id AND j.job_id=q.job_id AND j.active_attempt_id=q.attempt_id
 JOIN profile_revisions r ON r.profile=j.profile AND r.revision=j.profile_revision
 WHERE q.queue_seq=? AND q.workspace_id=? AND q.job_id=? AND q.attempt_id=?`, c.Sequence, string(c.WorkspaceID), string(c.JobID), string(c.AttemptID)).Scan(&owner, &fence, &generation, &until, &held, &barrier, &profile)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Attempt{}, scheduler.ErrLeaseLost
	}
	if err != nil {
		return domain.Attempt{}, dbError(err)
	}
	if !held || owner != c.Owner || fence != c.Fence || generation != c.Generation || until <= now.UnixMilli() || until != c.ExpiresAt.UnixMilli() {
		return domain.Attempt{}, scheduler.ErrLeaseLost
	}
	var p admission.Profile
	if len(profile) > 8192 || json.Unmarshal([]byte(profile), &p) != nil || p.Validate() != nil {
		return domain.Attempt{}, ErrCorrupt
	}
	if p.AccountScope != c.AccountScope || p.Binding.ProviderInstanceID != c.ProviderInstanceID {
		return domain.Attempt{}, scheduler.ErrLeaseLost
	}
	a, _, err := loadAttempt(ctx, tx, c.WorkspaceID, c.JobID, c.AttemptID)
	if err != nil {
		return domain.Attempt{}, err
	}
	if a.Revision != c.AttemptRevision {
		return domain.Attempt{}, scheduler.ErrLeaseLost
	}
	if barrier || !scheduler.LocalOnly(a.State) {
		return domain.Attempt{}, scheduler.ErrPhaseGate
	}
	if now.Before(a.UpdatedAt) {
		return domain.Attempt{}, scheduler.ErrClock
	}
	return a, nil
}

func (s *Store) RenewClaim(ctx context.Context, c scheduler.Claim, now time.Time) (scheduler.Claim, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return scheduler.Claim{}, err
	}
	defer done()
	var result scheduler.Claim
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		settings, _, err := schedulerControl(ctx, tx, now)
		if err != nil {
			return err
		}
		if _, err = ownedLocalClaim(ctx, tx, c, now); err != nil {
			return err
		}
		expires := time.UnixMilli(now.Add(settings.LeaseDuration).UnixMilli()).UTC()
		if !scheduler.ValidTime(expires) {
			return scheduler.ErrInvalid
		}
		// A shorter new policy must not move an already granted deadline backwards.
		if expires.Before(c.ExpiresAt) {
			expires = c.ExpiresAt
		}
		if _, err = tx.ExecContext(ctx, "UPDATE scheduler_leases SET until_ms=? WHERE queue_seq=?", expires.UnixMilli(), c.Sequence); err != nil {
			return dbError(err)
		}
		result = c
		result.ExpiresAt = expires
		return advanceSchedulerClock(ctx, tx, now)
	})
	if err != nil {
		return scheduler.Claim{}, err
	}
	return result, nil
}
func (s *Store) DeferClaim(ctx context.Context, c scheduler.Claim, now time.Time) error {
	return s.releaseLocalClaim(ctx, c, now, true)
}
func (s *Store) ReleaseClaim(ctx context.Context, c scheduler.Claim, now time.Time) error {
	return s.releaseLocalClaim(ctx, c, now, false)
}
func (s *Store) releaseLocalClaim(ctx context.Context, c scheduler.Claim, now time.Time, deferred bool) error {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		settings, _, err := schedulerControl(ctx, tx, now)
		if err != nil {
			return err
		}
		a, err := ownedLocalClaim(ctx, tx, c, now)
		if err != nil {
			return err
		}
		next := a.State
		event := domain.EventSchedulerReleased
		notBefore := int64(0)
		if deferred {
			next.Orchestration = domain.OrchestrationBlocked
			event = domain.EventSchedulerDeferred
			at := now.Add(settings.BlockedRecheck)
			if !scheduler.ValidTime(at) {
				return scheduler.ErrInvalid
			}
			notBefore = at.UnixMilli()
		}
		if _, err = schedulerTransition(ctx, tx, c.Identity, a, next, event, now); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE scheduler_leases SET owner='',fence='',until_ms=0,held=0 WHERE queue_seq=?", c.Sequence); err != nil {
			return dbError(err)
		}
		if _, err = tx.ExecContext(ctx, "UPDATE scheduler_queue SET not_before_ms=? WHERE queue_seq=?", notBefore, c.Sequence); err != nil {
			return dbError(err)
		}
		return advanceSchedulerClock(ctx, tx, now)
	})
}

// checkUnfencedAttempt is used by the legacy CAS port. Once ownership has been leased,
// arbitrary callers must not bypass generation/fence checks through that older seam.
// The next orchestration task adds fenced preparation/intent/observation transactions;
// M3-03 itself deliberately cannot advance a claimed attempt into remote dispatch.
func checkUnfencedAttempt(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, j domain.JobID, a domain.AttemptID) error {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT count(*) FROM scheduler_queue q JOIN scheduler_leases l ON l.queue_seq=q.queue_seq WHERE q.workspace_id=? AND q.job_id=? AND q.attempt_id=?`, string(w), string(j), string(a)).Scan(&count)
	if err != nil {
		return dbError(err)
	}
	if count != 0 {
		return scheduler.ErrLeaseLost
	}
	return nil
}
