package runtimehost

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/retention"
	"github.com/vankhaivn/compute-relay/internal/worker"
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

func (h *Host) retentionSweeper() (*retention.Sweeper, error) {
	sweeper, err := retention.NewSweeper(h.store, h.inputs, h.results)
	if err != nil {
		return nil, ErrState
	}
	return sweeper, nil
}

// runRetention retries transient failures from durable tombstones and stops the
// runtime on invariant failures.
func runRetention(ctx context.Context, sweeper retentionPass, interval time.Duration) error {
	err := worker.Run(ctx, worker.Config{Workers: 1, PollDelay: interval}, func(ctx context.Context, _ int) (bool, error) {
		return false, sweepRetention(ctx, sweeper)
	}, worker.Classify)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return ErrState
	}
	return nil
}

// sweepRetention runs one bounded pass with the default policy. Each call expires the next
// page of the durable inventory scan, then deletes a page of committed tombstones. The pass
// ends when the scan wraps, reaches its page bound, fails before progress, or reports
// an invariant failure. Transient deletion failures still allow cursor progress.
func sweepRetention(ctx context.Context, sweeper retentionPass) error {
	var failures []error
	var after int64
	for i := 0; i < retentionMaxPages && ctx.Err() == nil; i++ {
		report, next, err := sweeper.SweepOnce(ctx, retention.DefaultPolicy(), after, retentionPageSize)
		if err != nil {
			failures = append(failures, err)
			if worker.Classify(err) == worker.Stop {
				return errors.Join(failures...)
			}
		}
		switch {
		case next > after:
			after = next // more tombstones may follow, even when some deletions failed
		case err != nil || report.Examined == 0:
			return errors.Join(failures...)
		default:
			after = 0 // the scan has more inventory; recheck tombstones from the start
		}
	}
	return errors.Join(failures...)
}
