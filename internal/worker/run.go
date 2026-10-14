// Package worker runs cooperative durable workers with a shared retry policy.
package worker

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

type Decision uint8

const (
	Stop Decision = iota
	Continue
	Retry
)

type Config struct {
	Workers   int
	PollDelay time.Duration
	Drain     bool
}

// Classify retries only identified transient failures. Unknown errors remain fatal.
// Store adapters can expose Temporary without exporting driver diagnostics.
func Classify(err error) Decision {
	if err == nil {
		return Continue
	}
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		decision := Continue
		for _, cause := range joined.Unwrap() {
			classified := Classify(cause)
			if classified == Stop {
				return Stop
			}
			if classified == Retry {
				decision = Retry
			}
		}
		return decision
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Retry
	}
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) && temporary.Temporary() {
		return Retry
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) && (coded.Code()&255 == 5 || coded.Code()&255 == 6) {
		return Retry
	}
	return Stop
}

// Run retries by taking a fresh durable claim, never by replaying a callback that
// failed after a side effect. It joins every callback before returning, including
// callbacks that ignore cancellation. A fatal error cancels the other workers.
func Run(ctx context.Context, cfg Config, step func(context.Context, int) (bool, error), classify func(error) Decision) error {
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	failures := make(chan error, cfg.Workers)
	for index := 0; index < cfg.Workers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			backoff := 10 * time.Millisecond
			for run.Err() == nil {
				worked, err := step(run, index)
				if run.Err() != nil {
					return
				}
				decision := classify(err)
				if decision == Stop {
					failures <- err
					cancel()
					return
				}
				delay := cfg.PollDelay
				if decision == Retry {
					// Equal jitter keeps retries bounded and avoids synchronized contention.
					delay = backoff/2 + time.Duration(rand.Int64N(int64(backoff/2)+1))
					backoff = min(backoff*2, time.Second)
				} else {
					backoff = 10 * time.Millisecond
					if worked && err == nil && cfg.Drain {
						continue
					}
				}
				timer := time.NewTimer(delay)
				select {
				case <-run.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}(index)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		return err
	}
	return ctx.Err()
}
