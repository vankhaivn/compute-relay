package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

var _ scheduler.Repository = (*Store)(nil)

// schedulerSnapshot reads ALL outstanding attempts plus live worker reservations,
// not just a prefix of eligible jobs. No provider, credential resolver or file is read.
func schedulerSnapshot(ctx context.Context, tx *sql.Tx, settings scheduler.Settings, now time.Time) ([]scheduler.Candidate, error) {
	rows, err := tx.QueryContext(ctx, `SELECT q.queue_seq,q.workspace_id,q.job_id,q.attempt_id,q.dispatch_barrier,q.not_before_ms,
 a.state,a.orchestration,j.active_attempt_id=q.attempt_id,w.enabled,r.snapshot,
 json_extract(j.request,'$.resources.accelerator'),json_extract(j.request,'$.timeouts.remote_wall_seconds'),
 COALESCE(l.until_ms,0),COALESCE(l.held,0)
 FROM scheduler_queue q JOIN attempts a ON a.workspace_id=q.workspace_id AND a.job_id=q.job_id AND a.attempt_id=q.attempt_id
 JOIN jobs j ON j.workspace_id=q.workspace_id AND j.job_id=q.job_id
 JOIN workspaces w ON w.workspace_id=q.workspace_id
 JOIN profile_revisions r ON r.profile=j.profile AND r.revision=j.profile_revision
 LEFT JOIN scheduler_leases l ON l.queue_seq=q.queue_seq
 WHERE a.orchestration NOT IN ('succeeded','failed','cancelled','timed_out') OR (l.held=1 AND l.until_ms>?)
 ORDER BY q.queue_seq LIMIT ?`, now.UnixMilli(), scheduler.ScanLimit+1)
	if err != nil {
		return nil, dbError(err)
	}
	result := []scheduler.Candidate{}
	for rows.Next() {
		var c scheduler.Candidate
		var raw, phase, profile string
		var until, notBefore int64
		var held bool
		if err = rows.Scan(&c.Sequence, &c.WorkspaceID, &c.JobID, &c.AttemptID, &c.DispatchBarrier, &notBefore, &raw, &phase, &c.ActiveAttempt, &c.WorkspaceEnabled, &profile, &c.Resource, &c.WallSeconds, &until, &held); err != nil {
			rows.Close()
			return nil, dbError(err)
		}
		if len(result) >= scheduler.ScanLimit {
			rows.Close()
			return nil, scheduler.ErrScanLimit
		}
		var p admission.Profile
		if len(raw) > 4096 || json.Unmarshal([]byte(raw), &c.State) != nil || string(c.State.Orchestration) != phase || c.State.Validate() != nil || len(profile) > 8192 || json.Unmarshal([]byte(profile), &p) != nil || p.Validate() != nil {
			rows.Close()
			return nil, ErrCorrupt
		}
		c.AccountScope = p.AccountScope
		c.ProviderInstanceID = p.Binding.ProviderInstanceID
		c.Policy = scheduler.AccountPolicy{MaxActive: settings.MaxActivePerAccount}
		if held {
			c.LeaseUntil = time.UnixMilli(until).UTC()
		}
		if notBefore > 0 {
			c.NotBefore = time.UnixMilli(notBefore).UTC()
		}
		result = append(result, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, dbError(err)
	}
	// Close the first result set before more queries on the single SQLite connection.
	policies := map[string]scheduler.AccountPolicy{}
	quotas := map[string]scheduler.Quota{}
	for i := range result {
		c := &result[i]
		if p, ok := policies[c.AccountScope]; ok {
			c.Policy = p
		} else {
			var raw string
			err = tx.QueryRowContext(ctx, "SELECT policy FROM scheduler_accounts WHERE account_scope=?", c.AccountScope).Scan(&raw)
			if err == nil {
				if len(raw) > 2048 || json.Unmarshal([]byte(raw), &c.Policy) != nil || !c.Policy.Valid() {
					return nil, ErrCorrupt
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				return nil, dbError(err)
			}
			policies[c.AccountScope] = c.Policy
		}
		key := c.AccountScope + "/" + c.Resource // validated opaque account IDs cannot contain '/'.
		if q, ok := quotas[key]; ok {
			c.Quota = q
			continue
		}
		var raw string
		var q scheduler.Quota
		var observed int64
		err = tx.QueryRowContext(ctx, "SELECT observation,exhausted,observed_ms FROM scheduler_quotas WHERE account_scope=? AND resource=?", c.AccountScope, c.Resource).Scan(&raw, &q.Exhausted, &observed)
		if err == nil {
			var value provider.QuotaObservation
			if len(raw) > 4096 || json.Unmarshal([]byte(raw), &value) != nil || value.Validate() != nil || value.Resource != c.Resource || !scheduler.ValidTime(value.ObservedAt) || value.ObservedAt.UnixMilli() != observed {
				return nil, ErrCorrupt
			}
			q.Observation = &value
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, dbError(err)
		}
		quotas[key] = q
		c.Quota = q
	}
	return result, nil
}

func (s *Store) InspectScheduler(ctx context.Context, now time.Time) (scheduler.View, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return scheduler.View{}, err
	}
	defer done()
	var view scheduler.View
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		settings, last, err := schedulerControl(ctx, tx, now)
		if err != nil {
			return err
		}
		rows, err := schedulerSnapshot(ctx, tx, settings, now)
		if err != nil {
			return err
		}
		_, view, err = scheduler.Select(rows, domain.WorkspaceID(last), settings, now)
		return err
	})
	if err != nil {
		return scheduler.View{}, err
	}
	return view, nil
}

// ClaimNext reserves a bounded local worker/account slot and advances fairness in the
// SAME transaction as the claim and preparing event. It never changes attempt identity
// or rewrites dispatching/unknown attempts back to queued.
func (s *Store) ClaimNext(ctx context.Context, owner string, now time.Time) (scheduler.Result, error) {
	if !domain.ObjectID(owner).Valid() {
		return scheduler.Result{}, scheduler.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return scheduler.Result{}, err
	}
	defer done()
	var result scheduler.Result
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		settings, last, err := schedulerControl(ctx, tx, now)
		if err != nil {
			return err
		}
		rows, err := schedulerSnapshot(ctx, tx, settings, now)
		if err != nil {
			return err
		}
		index, view, err := scheduler.Select(rows, domain.WorkspaceID(last), settings, now)
		if err != nil {
			return err
		}
		result.View = view
		if index < 0 {
			return advanceSchedulerClock(ctx, tx, now)
		}
		row := rows[index]
		a, _, err := loadAttempt(ctx, tx, row.WorkspaceID, row.JobID, row.AttemptID)
		if err != nil {
			return err
		}
		if now.Before(a.UpdatedAt) {
			return scheduler.ErrClock
		}
		var generation int64
		err = tx.QueryRowContext(ctx, "SELECT generation FROM scheduler_leases WHERE queue_seq=?", row.Sequence).Scan(&generation)
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
		expires := time.UnixMilli(now.Add(settings.LeaseDuration).UnixMilli()).UTC()
		if !scheduler.ValidTime(expires) {
			return scheduler.ErrInvalid
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO scheduler_leases VALUES(?,?,?,?,?,1)
 ON CONFLICT(queue_seq) DO UPDATE SET generation=excluded.generation,owner=excluded.owner,fence=excluded.fence,until_ms=excluded.until_ms,held=1`, row.Sequence, generation+1, owner, fence, expires.UnixMilli())
		if err != nil {
			return dbError(err)
		}
		next := a.State
		next.Orchestration = domain.OrchestrationPreparing
		updated, err := schedulerTransition(ctx, tx, row.Identity, a, next, domain.EventSchedulerClaimed, now)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE scheduler_control SET last_workspace=?,clock_ms=? WHERE singleton=1", string(row.WorkspaceID), now.UnixMilli())
		if err != nil {
			return dbError(err)
		}
		claim := scheduler.Claim{Identity: row.Identity, Sequence: row.Sequence, Owner: owner, Generation: generation + 1, Fence: fence, ExpiresAt: expires, AttemptRevision: updated.Revision, AccountScope: row.AccountScope, ProviderInstanceID: row.ProviderInstanceID}
		for _, head := range view.Heads {
			if head.Sequence == row.Sequence {
				claim.QuotaWarning = head.Warning
			}
		}
		result.Claim = &claim
		return nil
	})
	if err != nil {
		return scheduler.Result{}, err
	}
	return result, nil
}

// schedulerTransition records genuine local scheduler events, not inputs.ready or
// execution.observed. Reclaim/release may leave state/revision unchanged while still
// appending ownership history. State changes use the central domain transition rules.
func schedulerTransition(ctx context.Context, tx *sql.Tx, id scheduler.Identity, a domain.Attempt, next domain.AttemptState, kind domain.EventType, now time.Time) (domain.Attempt, error) {
	if !id.Valid() || a.ID != id.AttemptID || a.JobID != id.JobID || !kind.Valid() || now.Before(a.UpdatedAt) {
		return domain.Attempt{}, scheduler.ErrInvalid
	}
	updated, err := a.Transition(next, now.UTC())
	if err != nil || updated.Revision > math.MaxInt64 {
		return domain.Attempt{}, scheduler.ErrInvalid
	}
	if updated != a {
		raw, _ := json.Marshal(updated.State)
		res, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?,orchestration=?,revision=?,updated_at=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND revision=?`, string(raw), string(updated.State.Orchestration), updated.Revision, updated.UpdatedAt.Format(time.RFC3339Nano), string(id.WorkspaceID), string(id.JobID), string(id.AttemptID), a.Revision)
		if err != nil {
			return domain.Attempt{}, dbError(err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return domain.Attempt{}, dbError(err)
		}
		if n != 1 {
			return domain.Attempt{}, ErrConflict
		}
	}
	var seq int64
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(max(sequence),0) FROM events WHERE workspace_id=? AND job_id=?", string(id.WorkspaceID), string(id.JobID)).Scan(&seq); err != nil {
		return domain.Attempt{}, dbError(err)
	}
	if seq == math.MaxInt64 {
		return domain.Attempt{}, ErrConflict
	}
	event, err := randomID("evt_", 16)
	if err != nil {
		return domain.Attempt{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO events(workspace_id,job_id,event_id,sequence,attempt_id,type,occurred_at) VALUES(?,?,?,?,?,?,?)`, string(id.WorkspaceID), string(id.JobID), event, seq+1, string(id.AttemptID), string(kind), now.UTC().Format(time.RFC3339Nano))
	return updated, dbError(err)
}
