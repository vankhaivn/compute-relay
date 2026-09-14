package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// ConfigureScheduler is LOCAL operator composition, never an application HTTP route.
// Pausing/lowering bounds stops new claims; it cannot cancel or erase existing work.
func (s *Store) ConfigureScheduler(ctx context.Context, settings scheduler.Settings) error {
	if !settings.Valid() {
		return scheduler.ErrInvalid
	}
	raw, _ := json.Marshal(settings)
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	_, err = s.db.ExecContext(ctx, "UPDATE scheduler_control SET settings=? WHERE singleton=1", string(raw))
	return dbError(err)
}
func (s *Store) ConfigureAccount(ctx context.Context, scope string, policy scheduler.AccountPolicy) error {
	if !domain.ObjectID(scope).Valid() || !policy.Valid() {
		return scheduler.ErrInvalid
	}
	raw, _ := json.Marshal(policy)
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	_, err = s.db.ExecContext(ctx, `INSERT INTO scheduler_accounts VALUES(?,?) ON CONFLICT(account_scope) DO UPDATE SET policy=excluded.policy`, scope, string(raw))
	return dbError(err)
}

func schedulerControl(ctx context.Context, tx *sql.Tx, now time.Time) (scheduler.Settings, string, error) {
	var settings scheduler.Settings
	if !scheduler.ValidTime(now) {
		return settings, "", scheduler.ErrInvalid
	}
	var raw, last string
	var clock int64
	if err := tx.QueryRowContext(ctx, "SELECT settings,last_workspace,clock_ms FROM scheduler_control WHERE singleton=1").Scan(&raw, &last, &clock); err != nil {
		return settings, "", dbError(err)
	}
	if len(raw) > 4096 || json.Unmarshal([]byte(raw), &settings) != nil || !settings.Valid() || last != "" && !domain.WorkspaceID(last).Valid() {
		return settings, "", ErrCorrupt
	}
	if now.UnixMilli() < clock {
		return settings, "", scheduler.ErrClock
	}
	return settings, last, nil
}
func advanceSchedulerClock(ctx context.Context, tx *sql.Tx, now time.Time) error {
	_, err := tx.ExecContext(ctx, "UPDATE scheduler_control SET clock_ms=? WHERE singleton=1", now.UnixMilli())
	return dbError(err)
}

// RecordQuota stores an explicit observation; it never polls, estimates account quota,
// assumes reset or changes credential/account identity. Unknown/error observations do
// not clear an exhausted latch. Only newer fresh positive evidence can do so.
func (s *Store) RecordQuota(ctx context.Context, scope, resource string, q provider.QuotaObservation, now time.Time) error {
	if !domain.ObjectID(scope).Valid() || (resource != "cpu" && resource != "gpu") || q.Resource != resource || q.Validate() != nil || !scheduler.ValidTime(now) || !scheduler.ValidTime(q.ObservedAt) || q.ObservedAt.After(now) {
		return scheduler.ErrInvalid
	}
	if q.ResetAt != nil && !scheduler.ValidTime(*q.ResetAt) {
		return scheduler.ErrInvalid
	}
	raw, err := json.Marshal(q)
	if err != nil || len(raw) > 4096 {
		return scheduler.ErrInvalid
	}
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
		var prior int64
		var exhausted bool
		var previous string
		err = tx.QueryRowContext(ctx, "SELECT observed_ms,exhausted,observation FROM scheduler_quotas WHERE account_scope=? AND resource=?", scope, resource).Scan(&prior, &exhausted, &previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		if err == nil && q.ObservedAt.UnixMilli() <= prior {
			if q.ObservedAt.UnixMilli() == prior && previous == string(raw) {
				return nil
			}
			return ErrConflict
		}
		seconds, known := scheduler.Seconds(q)
		if known && (q.Status == provider.QuotaKnown || q.Status == provider.QuotaStale) {
			if seconds == 0 {
				exhausted = true
			} else if q.Status == provider.QuotaKnown && now.Sub(q.ObservedAt) < settings.QuotaFreshFor {
				exhausted = false
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO scheduler_quotas VALUES(?,?,?,?,?) ON CONFLICT(account_scope,resource) DO UPDATE SET observation=excluded.observation,observed_ms=excluded.observed_ms,exhausted=excluded.exhausted`, scope, resource, string(raw), q.ObservedAt.UnixMilli(), exhausted)
		if err != nil {
			return dbError(err)
		}
		return advanceSchedulerClock(ctx, tx, now)
	})
}
