package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type transactionClockKey struct{}

// write bounds the operation lifetime and samples time after BEGIN IMMEDIATE
// acquires the single writer slot.
// The persisted millisecond floor survives wall-clock rollback and process restart.
// Failed and uncertain transactions are never retried here.
func (s *Store) write(ctx context.Context, fn func(context.Context, *sql.Tx, time.Time) error) error {
	ctx, done, err := s.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		wall := time.Now
		if s.options.Clock != nil {
			wall = s.options.Clock
		}
		now := wall().UTC()
		if !scheduler.ValidTime(now) {
			return scheduler.ErrInvalid
		}
		var floor int64
		if err := tx.QueryRowContext(ctx, `SELECT clock_ms FROM scheduler_control WHERE singleton=1`).Scan(&floor); err != nil {
			return dbError(err)
		}
		if floor < 0 {
			return ErrCorrupt
		}
		if bound := time.UnixMilli(floor).UTC(); now.Before(bound) {
			now = bound
		}
		// The floor rounds up so it never understates a full-precision local write.
		// A subsequent rollback can safely reuse it without regressing timestamps.
		nextFloor := now.UnixMilli()
		if now.Nanosecond()%int(time.Millisecond) != 0 {
			nextFloor++
		}
		if !scheduler.ValidTime(time.UnixMilli(nextFloor)) {
			return scheduler.ErrInvalid
		}
		if _, err := tx.ExecContext(ctx, `UPDATE scheduler_control SET clock_ms=? WHERE singleton=1`, nextFloor); err != nil {
			return dbError(err)
		}
		return fn(context.WithValue(ctx, transactionClockKey{}, now), tx, now)
	})
}

func transactionTime(ctx context.Context) time.Time {
	now, _ := ctx.Value(transactionClockKey{}).(time.Time)
	return now
}
