package dispatch

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type workerFaultRepo struct {
	Repository
	faults []error
	calls  int
	cancel context.CancelFunc
}

func (r *workerFaultRepo) ClaimRecovery(ctx context.Context, _ string) (*scheduler.Claim, error) {
	r.calls++
	if len(r.faults) > 0 {
		err := r.faults[0]
		r.faults = r.faults[1:]
		return nil, err
	}
	r.cancel()
	return nil, ctx.Err()
}

type workerBusy struct{}

func (workerBusy) Error() string   { return "busy" }
func (workerBusy) Temporary() bool { return true }

func TestWorkerResumesAfterTransientStoreErrors(t *testing.T) {
	problem := provider.Problem(domain.CodeProviderSubmissionUnknown, domain.FailureStageSubmission, "submission outcome unknown")
	for name, faults := range map[string][]error{
		"store":             {workerBusy{}, context.DeadlineExceeded},
		"provider-value":    {problem},
		"provider-pointer":  {&problem},
		"wrapped-provider":  {fmt.Errorf("job: %w", problem)},
		"provider-and-busy": {errors.Join(problem, workerBusy{})},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			wantCalls := len(faults) + 1
			repo := &workerFaultRepo{faults: faults, cancel: cancel}
			cfg := DefaultConfig()
			cfg.Workers = 1
			cfg.PollDelay = time.Millisecond
			e := &Engine{repo: repo, clock: systemClock{}, config: cfg, slots: make(chan struct{}, 1)}
			started := time.Now()
			if err := e.Run(ctx); !errors.Is(err, context.Canceled) || repo.calls != wantCalls {
				t.Fatal("job failure stopped worker", repo.calls, err)
			}
			if name == "store" && time.Since(started) < 15*time.Millisecond {
				t.Fatal("store backoff skipped")
			}
		})
	}
}

func TestWorkerStopsOnInvariantError(t *testing.T) {
	fatal := errors.New("invalid durable evidence")
	repo := &workerFaultRepo{faults: []error{fatal}}
	cfg := DefaultConfig()
	cfg.Workers = 1
	cfg.PollDelay = time.Millisecond
	e := &Engine{repo: repo, clock: systemClock{}, config: cfg, slots: make(chan struct{}, 1)}
	if err := e.Run(context.Background()); !errors.Is(err, fatal) || repo.calls != 1 {
		t.Fatal("invariant swallowed", repo.calls, err)
	}
}
