package runtimehost

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// cleanupEngine applies accepted cleanup requests with the same frozen-binding resolver as
// the collector of the active mode. It is never composed in admission-only mode.
func cleanupEngine(repo cleanup.WorkRepository, resolver cleanup.Resolver) (*cleanup.Engine, error) {
	engine, err := cleanup.New(repo, resolver, clock{}, cleanup.DefaultConfig())
	if err != nil {
		return nil, ErrState
	}
	return engine, nil
}

type managedCleanupStore interface {
	cleanup.WorkRepository
	ClaimCleanupManaged(context.Context, time.Time, time.Duration) (*cleanup.Work, error)
}

// managedCleanupRepository claims only attempts frozen to managed connections and retries
// a definite clock rollback like the other managed workers.
type managedCleanupRepository struct{ managedCleanupStore }

func (r managedCleanupRepository) ClaimCleanup(ctx context.Context, now time.Time, ttl time.Duration) (*cleanup.Work, error) {
	result, err := r.ClaimCleanupManaged(ctx, now, ttl)
	if errors.Is(err, scheduler.ErrClock) {
		return nil, nil
	}
	return result, err
}
func (r managedCleanupRepository) CompleteCleanup(ctx context.Context, work cleanup.Work, outcome cleanup.Outcome, now time.Time) error {
	return retryManagedWrite(ctx, now, func(at time.Time) error { return r.managedCleanupStore.CompleteCleanup(ctx, work, outcome, at) })
}

func (r managedCleanupRepository) BeginCleanupResource(ctx context.Context, work cleanup.Work, resource cleanup.ResourceWork, now time.Time) error {
	repo, ok := r.managedCleanupStore.(cleanup.ResourceRepository)
	if !ok {
		return cleanup.ErrInvalid
	}
	return retryManagedWrite(ctx, now, func(at time.Time) error { return repo.BeginCleanupResource(ctx, work, resource, at) })
}

func (r managedCleanupRepository) CompleteCleanupResource(ctx context.Context, work cleanup.Work, result cleanup.ResourceResult, now time.Time) error {
	repo, ok := r.managedCleanupStore.(cleanup.ResourceRepository)
	if !ok {
		return cleanup.ErrInvalid
	}
	return retryManagedWrite(ctx, now, func(at time.Time) error { return repo.CompleteCleanupResource(ctx, work, result, at) })
}
