package scheduler

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// LocalProcessor is trusted runtime code, NOT an uploaded command executor. It must
// do only bounded local preparation, honor cancellation and make no remote mutations.
// It must never treat this claim as permission to call Provider.Prepare/Submit.
// Provider staging/submission and their intent ledger belong to M3-04.
type LocalProcessor interface {
	PrepareLocal(context.Context, Claim) error
}

// RunLocal uses a fixed number of goroutines. A timed-out processor is cancelled but
// never replaced by another goroutine until it returns. Thus uncooperative code cannot
// cause an unbounded worker explosion. A process should wire exactly one pool per Store.
// Return values never invent inputs-ready or successful compute: every completed local
// pass is deferred for the future preparation/intent orchestrator. No production serve
// command starts this pool implicitly.
func (s *Service) RunLocal(ctx context.Context, ownerPrefix string, workers int, interval time.Duration, p LocalProcessor) error {
	if !domain.ObjectID(ownerPrefix).Valid() || len(ownerPrefix) > 115 || workers < 1 || workers > 64 || interval < 10*time.Millisecond || interval > time.Minute || p == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	view, err := s.Inspect(ctx)
	if err != nil {
		return err
	}
	if workers > view.MaxWorkers || view.MaxWorkers < 1 {
		return ErrInvalid
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.localWorker(ctx, ownerPrefix+"_"+strconv.Itoa(i), interval, p)
			if err != nil && !errors.Is(err, context.Canceled) {
				errs <- err
				cancel()
			}
		}(i)
	}
	// Do not claim bounded shutdown for a processor that ignores its context. The
	// runtime must wait for local code to stop; it never cancels remote compute here.
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return ctx.Err()
}
func (s *Service) localWorker(ctx context.Context, owner string, interval time.Duration, p LocalProcessor) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		result, err := s.Next(ctx, owner)
		if err != nil {
			return err
		}
		if result.Claim != nil {
			claim := *result.Claim
			budget := claim.ExpiresAt.Sub(s.clock.Now())
			if budget <= 0 {
				timer.Reset(interval)
				continue
			}
			work, cancel := context.WithTimeout(ctx, budget)
			// The error is deliberately not logged: processor errors can contain private paths.
			// Both success and failure defer, rather than implying all preparation gates passed.
			_ = prepareSafely(work, p, claim)
			cancel()
			if ctx.Err() != nil {
				return ctx.Err()
			} // retain bounded lease; shutdown is not evidence.
			err = s.Defer(ctx, claim)
			if err != nil && !errors.Is(err, ErrLeaseLost) {
				return err
			}
		}
		timer.Reset(interval)
	}
}
func prepareSafely(ctx context.Context, p LocalProcessor, c Claim) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrInvalid
		}
	}()
	return p.PrepareLocal(ctx, c)
}
