package runtimehost

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type managedCleanupProbe struct {
	explicit, managed, completes int
	claimErr                     error
}

func (p *managedCleanupProbe) ClaimCleanup(context.Context, time.Time, time.Duration) (*cleanup.Work, error) {
	p.explicit++
	return nil, nil
}
func (p *managedCleanupProbe) ClaimCleanupManaged(context.Context, time.Time, time.Duration) (*cleanup.Work, error) {
	p.managed++
	return nil, p.claimErr
}
func (p *managedCleanupProbe) CompleteCleanup(context.Context, cleanup.Work, cleanup.Outcome, time.Time) error {
	p.completes++
	if p.completes == 1 {
		return scheduler.ErrClock // A definite rollback before commit.
	}
	return nil
}

func TestManagedCleanupClaimsOnlyManagedAttemptsAndRetriesClock(t *testing.T) {
	probe := &managedCleanupProbe{claimErr: scheduler.ErrClock}
	repo := managedCleanupRepository{probe}
	if w, err := repo.ClaimCleanup(context.Background(), time.Now().UTC(), time.Minute); w != nil || err != nil || probe.managed != 1 || probe.explicit != 0 {
		t.Fatal("managed cleanup claim did not use the managed partition", w, err)
	}
	probe.claimErr = errors.New("store unavailable")
	if _, err := repo.ClaimCleanup(context.Background(), time.Now().UTC(), time.Minute); !errors.Is(err, probe.claimErr) {
		t.Fatal("store error hidden", err)
	}
	if err := repo.CompleteCleanup(context.Background(), cleanup.Work{}, cleanup.Outcome{}, time.Now().UTC()); err != nil || probe.completes != 2 {
		t.Fatal("definite clock rollback was not retried once", err, probe.completes)
	}
	resolver := provider.NewSnapshotRegistry()
	if engine, err := cleanupEngine(repo, resolver); err != nil || engine == nil {
		t.Fatal("cleanup worker composition failed", err)
	}
	if _, err := cleanupEngine(nil, resolver); !errors.Is(err, ErrState) {
		t.Fatal("incomplete cleanup worker composed", err)
	}
}
