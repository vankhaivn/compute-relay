package runtimehost

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/retention"
)

// Eligibility windows are hours to days, so an hourly pass keeps the bounded stores
// from filling without competing with dispatch for the single SQLite connection.
const (
	retentionInterval = time.Hour
	retentionPageSize = 100
	retentionMaxPages = 100
)

type retentionPass interface {
	SweepOnce(context.Context, retention.Policy, int64, int) (retention.SweepReport, int64, error)
}

func (h *Host) retentionSweeper(now retention.Clock) (*retention.Sweeper, error) {
	sweeper, err := retention.NewSweeper(h.store, h.inputs, h.results, now)
	if err != nil {
		return nil, ErrState
	}
	return sweeper, nil
}

// runRetention sweeps until shutdown and never stops the runtime: tombstones are
// durable, so the next pass resumes whatever a failed pass left pending.
func runRetention(ctx context.Context, sweeper retentionPass, interval time.Duration) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		sweepRetention(ctx, sweeper)
		timer.Reset(interval)
	}
}

// sweepRetention runs one bounded pass with the default policy. Each call expires the next
// page of the durable inventory scan, then deletes a page of committed tombstones. The pass
// ends when the scan wraps with nothing left to delete, or when a call fails before progress.
func sweepRetention(ctx context.Context, sweeper retentionPass) {
	var after int64
	for i := 0; i < retentionMaxPages && ctx.Err() == nil; i++ {
		report, next, err := sweeper.SweepOnce(ctx, retention.DefaultPolicy(), after, retentionPageSize)
		switch {
		case next > after:
			after = next // more tombstones may follow, even when some deletions failed
		case err != nil || report.Examined == 0:
			return
		default:
			after = 0 // the scan has more inventory; recheck tombstones from the start
		}
	}
}
