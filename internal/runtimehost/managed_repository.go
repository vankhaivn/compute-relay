package runtimehost

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

const managedClockAttempts = 16

// A worker may sample time before waiting behind a newer SQLite transaction.
// ErrClock is returned before Commit, so retry only that definite rollback with
// fresh wall time. Never retry an uncertain acknowledgement or provider call.
// Exact claims, evidence and finite policy remain unchanged; the store rechecks
// expiry and every fence. A persistent backwards clock is surfaced after this
// bounded retry, without advancing or bypassing the store's monotonic guard.
func retryManagedClock[T any](ctx context.Context, now time.Time, operation func(time.Time) (T, error)) (T, error) {
	for attempt := 0; ; attempt++ {
		result, err := operation(now)
		if !errors.Is(err, scheduler.ErrClock) || attempt+1 >= managedClockAttempts {
			return result, err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			var zero T
			return zero, ctx.Err()
		case <-timer.C:
		}
		now = time.Now().UTC()
	}
}

func retryManagedWrite(ctx context.Context, now time.Time, operation func(time.Time) error) error {
	_, err := retryManagedClock(ctx, now, func(at time.Time) (struct{}, error) { return struct{}{}, operation(at) })
	return err
}

type managedConnectionRepository struct{ connections.Repository }

func (r managedConnectionRepository) FinishConnection(ctx context.Context, record connections.Record, result connections.Completion) error {
	return retryManagedWrite(ctx, result.Now, func(at time.Time) error {
		result.Now = at
		return r.Repository.FinishConnection(ctx, record, result)
	})
}
func (r managedConnectionRepository) ReadConnection(ctx context.Context, workspace domain.WorkspaceID, token, id string, now time.Time) (connections.Connection, error) {
	return retryManagedClock(ctx, now, func(at time.Time) (connections.Connection, error) {
		return r.Repository.ReadConnection(ctx, workspace, token, id, at)
	})
}
func (r managedConnectionRepository) ListConnections(ctx context.Context, workspace domain.WorkspaceID, token string, now time.Time) ([]connections.Connection, error) {
	return retryManagedClock(ctx, now, func(at time.Time) ([]connections.Connection, error) {
		return r.Repository.ListConnections(ctx, workspace, token, at)
	})
}

type managedAuthorizationRepository struct{ executionauth.Repository }

func (r managedAuthorizationRepository) AuthorizeExecution(ctx context.Context, command executionauth.Command) (executionauth.Receipt, error) {
	return retryManagedClock(ctx, command.Now, func(at time.Time) (executionauth.Receipt, error) {
		command.Now = at
		return r.Repository.AuthorizeExecution(ctx, command)
	})
}

type managedDispatchStore interface {
	dispatch.Repository
	ClaimNextManaged(context.Context, string, time.Time) (scheduler.Result, error)
	ClaimRecoveryManaged(context.Context, string, time.Time) (*scheduler.Claim, error)
}
type managedDispatchRepository struct{ managedDispatchStore }

// Claims have no retained provider outcome; a later poll can safely retry them.
func (r managedDispatchRepository) ClaimNext(ctx context.Context, owner string, now time.Time) (scheduler.Result, error) {
	result, err := r.ClaimNextManaged(ctx, owner, now)
	if errors.Is(err, scheduler.ErrClock) {
		return scheduler.Result{}, nil
	}
	return result, err
}
func (r managedDispatchRepository) ClaimRecovery(ctx context.Context, owner string, now time.Time) (*scheduler.Claim, error) {
	result, err := r.ClaimRecoveryManaged(ctx, owner, now)
	if errors.Is(err, scheduler.ErrClock) {
		return nil, nil
	}
	return result, err
}
func (r managedDispatchRepository) LoadDispatch(ctx context.Context, claim scheduler.Claim, now time.Time) (dispatch.Work, error) {
	return retryManagedClock(ctx, now, func(at time.Time) (dispatch.Work, error) { return r.managedDispatchStore.LoadDispatch(ctx, claim, at) })
}
func (r managedDispatchRepository) ConsumeAuthorization(ctx context.Context, handle dispatch.Handle, now time.Time) (dispatch.Work, error) {
	return retryManagedClock(ctx, now, func(at time.Time) (dispatch.Work, error) {
		return r.managedDispatchStore.ConsumeAuthorization(ctx, handle, at)
	})
}
func (r managedDispatchRepository) FreezeInput(ctx context.Context, handle dispatch.Handle, index int, object domain.ObjectMetadata, now time.Time) error {
	return retryManagedWrite(ctx, now, func(at time.Time) error { return r.managedDispatchStore.FreezeInput(ctx, handle, index, object, at) })
}
func (r managedDispatchRepository) CommitDispatch(ctx context.Context, handle dispatch.Handle, action dispatch.Action, now time.Time) (dispatch.Work, error) {
	return retryManagedClock(ctx, now, func(at time.Time) (dispatch.Work, error) {
		return r.managedDispatchStore.CommitDispatch(ctx, handle, action, at)
	})
}
func (r managedDispatchRepository) RenewDispatch(ctx context.Context, handle dispatch.Handle, now time.Time) (dispatch.Handle, error) {
	return retryManagedClock(ctx, now, func(at time.Time) (dispatch.Handle, error) {
		return r.managedDispatchStore.RenewDispatch(ctx, handle, at)
	})
}
func (r managedDispatchRepository) YieldDispatch(ctx context.Context, handle dispatch.Handle, now time.Time, delay time.Duration) error {
	return retryManagedWrite(ctx, now, func(at time.Time) error { return r.managedDispatchStore.YieldDispatch(ctx, handle, at, delay) })
}

type managedCollectionStore interface {
	collection.Repository
	ClaimCollectionManaged(context.Context, time.Time, time.Duration, int) (*collection.Work, error)
}
type managedCollectionRepository struct{ managedCollectionStore }

func (r managedCollectionRepository) ClaimCollection(ctx context.Context, now time.Time, ttl time.Duration, workers int) (*collection.Work, error) {
	result, err := r.ClaimCollectionManaged(ctx, now, ttl, workers)
	if errors.Is(err, scheduler.ErrClock) {
		return nil, nil
	}
	return result, err
}
func (r managedCollectionRepository) PinCollection(ctx context.Context, work collection.Work, snapshot collection.Snapshot, now time.Time) error {
	return retryManagedWrite(ctx, now, func(at time.Time) error { return r.managedCollectionStore.PinCollection(ctx, work, snapshot, at) })
}
func (r managedCollectionRepository) CompleteCollection(ctx context.Context, work collection.Work, proof collection.Verified, now time.Time) error {
	return retryManagedWrite(ctx, now, func(at time.Time) error { return r.managedCollectionStore.CompleteCollection(ctx, work, proof, at) })
}
func (r managedCollectionRepository) FailCollection(ctx context.Context, work collection.Work, failure collection.Failure, now time.Time) error {
	return retryManagedWrite(ctx, now, func(at time.Time) error { return r.managedCollectionStore.FailCollection(ctx, work, failure, at) })
}
