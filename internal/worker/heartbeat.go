package worker

import (
	"context"
	"errors"
	"time"
)

const LeaseDuration = 30 * time.Second

// HeartbeatError preserves processing failures alongside a failed renewal. Local
// cancellation caused by that renewal contributes no separate fatal failure.
func HeartbeatError(processing, renewal error) error {
	if renewal == nil {
		return processing
	}
	return errors.Join(withoutCancellation(processing), renewal)
}

func withoutCancellation(err error) error {
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		causes := make([]error, 0, len(joined.Unwrap()))
		for _, cause := range joined.Unwrap() {
			causes = append(causes, withoutCancellation(cause))
		}
		return errors.Join(causes...)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Heartbeat cancels local work on any failed renewal. stop cancels and joins the
// renewer, returning its original error so the worker can classify it. Callbacks
// still run synchronously and must finish before their worker returns.
func Heartbeat(parent context.Context, until time.Time, now func() time.Time, renew func(context.Context) (time.Time, error)) (context.Context, func() error) {
	if now == nil {
		now = time.Now
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	var failure error
	go func() {
		defer close(done)
		for {
			interval := min(max(until.Sub(now())/3, 10*time.Millisecond), 5*time.Second)
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			bounded, stop := context.WithTimeout(ctx, 5*time.Second)
			updated, err := renew(bounded)
			stop()
			if err != nil {
				if ctx.Err() != nil {
					err = withoutCancellation(err)
				}
				if err != nil {
					failure = err
					cancel()
				}
				return
			}
			until = updated
		}
	}()
	return ctx, func() error {
		cancel()
		<-done
		return failure
	}
}
