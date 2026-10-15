package cleanup

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

// StorageWork has no job or attempt foreign key. Its immutable target and
// binding come from the sibling journal, while the cleanup engine provides
// the same fenced claim, Begin-before-call and unknown-recovery algorithm.
type StorageWork struct {
	Lease   Lease
	Binding provider.BindingSnapshot
	Target  hoststorage.Target
	State   string
}

func (w StorageWork) Valid() bool {
	return w.Lease.Valid() && w.Binding.Valid() && w.Target.Valid() &&
		(w.State == "pending" || w.State == "running")
}

type StorageRepository interface {
	ClaimStorageCleanup(context.Context, time.Duration) (*StorageWork, error)
	RenewStorageCleanup(context.Context, StorageWork, time.Duration) (time.Time, error)
	BeginStorageCleanup(context.Context, StorageWork) error
	CompleteStorageCleanup(context.Context, StorageWork, hoststorage.DeleteOutcome) error
}

type storageAdapter interface {
	ObserveStorageTarget(context.Context, hoststorage.Target) (hoststorage.TargetObservation, error)
	CleanupStorageTarget(context.Context, hoststorage.Target, bool) (hoststorage.DeleteOutcome, error)
}

func resolveStorageAdapter(resolver Resolver, binding provider.BindingSnapshot) (adapter provider.Provider, err error) {
	defer func() {
		if recover() != nil {
			adapter, err = nil, ErrInvalid
		}
	}()
	return resolver.Resolve(binding)
}

func verifyStorageAdapter(ctx context.Context, adapter provider.Provider, binding provider.BindingSnapshot) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrInvalid
		}
	}()
	return adapter.Verify(ctx, binding)
}

func observeStorageTarget(ctx context.Context, adapter storageAdapter, target hoststorage.Target) (observed hoststorage.TargetObservation, err error) {
	defer func() {
		if recover() != nil {
			observed, err = hoststorage.TargetObservation{}, ErrInvalid
		}
	}()
	return adapter.ObserveStorageTarget(ctx, target)
}

func callStorageDeletion(ctx context.Context, adapter storageAdapter, target hoststorage.Target, recoverOnly bool) (outcome hoststorage.DeleteOutcome, err error) {
	outcome = hoststorage.DeleteUnknown
	defer func() {
		if recover() != nil {
			outcome, err = hoststorage.DeleteUnknown, nil
		}
	}()
	return adapter.CleanupStorageTarget(ctx, target, recoverOnly)
}

func (e *Engine) runStorageOnce(ctx context.Context) (worked bool, resultErr error) {
	w, err := e.storage.ClaimStorageCleanup(ctx, worker.LeaseDuration)
	if err != nil || w == nil {
		return false, err
	}
	if !w.Valid() {
		return true, ErrInvalid
	}
	owned, stop := worker.Heartbeat(ctx, w.Lease.Until, nil, func(c context.Context) (time.Time, error) {
		return e.storage.RenewStorageCleanup(c, *w, worker.LeaseDuration)
	})
	defer func() { resultErr = worker.HeartbeatError(resultErr, stop()) }()
	call, cancel := context.WithTimeout(owned, e.config.Timeout)
	defer cancel()
	providerAdapter, err := resolveStorageAdapter(e.resolver, w.Binding)
	if err != nil {
		return true, err
	}
	if err := verifyStorageAdapter(call, providerAdapter, w.Binding); err != nil {
		if err == ErrInvalid || provider.IsLocalStateError(err) {
			return true, err
		}
		if w.State == "running" {
			return true, err
		}
		if err := stop(); err != nil {
			return true, err
		}
		return true, e.storage.CompleteStorageCleanup(ctx, *w, hoststorage.DeleteRetained)
	}
	adapter, ok := providerAdapter.(storageAdapter)
	if !ok {
		return true, ErrInvalid
	}
	outcome := hoststorage.DeleteUnknown
	if w.State == "running" {
		// A lost acknowledgement is observation-only. A present object remains
		// unknown and is never removed by an automatic retry.
		outcome, err = callStorageDeletion(call, adapter, w.Target, true)
	} else {
		var observed hoststorage.TargetObservation
		observed, err = observeStorageTarget(call, adapter, w.Target)
		if err != nil {
			if err == ErrInvalid || provider.IsLocalStateError(err) {
				return true, err
			}
			// Observation is read-only; a failed observation cannot have deleted
			// the target and must not invent an ambiguous delete outcome.
			outcome, err = hoststorage.DeleteRetained, nil
		} else {
			switch {
			case !observed.Present:
				outcome = hoststorage.AbsentConfirmed
			case !observed.Eligible:
				outcome = hoststorage.DeleteRetained
			default:
				if err = e.storage.BeginStorageCleanup(call, *w); errors.Is(err, hoststorage.ErrIneligible) {
					outcome, err = hoststorage.DeleteRetained, nil
				} else if err != nil {
					return true, err
				}
				if err == nil && outcome != hoststorage.DeleteRetained {
					outcome, err = callStorageDeletion(call, adapter, w.Target, false)
				}
			}
		}
	}
	if err != nil {
		if owned.Err() != nil {
			return true, owned.Err()
		}
		if err == ErrInvalid || provider.IsLocalStateError(err) || errors.Is(err, hoststorage.ErrIneligible) {
			return true, err
		}
		// An ambiguous provider result is durable unknown, not a new permit.
		outcome = hoststorage.DeleteUnknown
	}
	if owned.Err() != nil {
		return true, owned.Err()
	}
	if err := stop(); err != nil {
		return true, err
	}
	return true, e.storage.CompleteStorageCleanup(ctx, *w, outcome)
}
