package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

// A previously cached expiry remains usable after a renewal. Keeping that metadata
// short exercises heartbeat scheduling without waiting through a full lease period.
type heartbeatDispatchRepository struct {
	*Store
	now     func() time.Time
	failure error
}

func (r heartbeatDispatchRepository) ClaimNext(ctx context.Context, owner string) (scheduler.Result, error) {
	result, err := r.Store.ClaimNext(ctx, owner)
	if result.Claim != nil {
		result.Claim.ExpiresAt = r.now()
	}
	return result, err
}
func (r heartbeatDispatchRepository) RenewDispatch(context.Context, dispatch.Handle) (dispatch.Handle, error) {
	return dispatch.Handle{}, r.failure
}

type heartbeatCollectionRepository struct {
	*Store
	now     func() time.Time
	failure error
}

func (r heartbeatCollectionRepository) ClaimCollection(ctx context.Context, ttl time.Duration, workers int) (*collection.Work, error) {
	w, err := r.Store.ClaimCollection(ctx, ttl, workers)
	if w != nil {
		w.Lease.Until = r.now()
	}
	return w, err
}
func (r heartbeatCollectionRepository) RenewCollection(context.Context, collection.Work, time.Duration) (time.Time, error) {
	return time.Time{}, r.failure
}

type heartbeatCleanupRepository struct {
	*Store
	failure error
}

func (r heartbeatCleanupRepository) ClaimCleanup(ctx context.Context, ttl time.Duration) (*cleanup.Work, error) {
	w, err := r.Store.ClaimCleanup(ctx, ttl)
	if w != nil {
		w.Lease.Until = time.Now()
	}
	return w, err
}
func (r heartbeatCleanupRepository) RenewCleanup(context.Context, cleanup.Work, time.Duration) (time.Time, error) {
	return time.Time{}, r.failure
}

type heartbeatProvider struct {
	provider.Provider
	entered, canceled, release chan struct{}
}

func (p *heartbeatProvider) VerifyBinding(ctx context.Context, _ provider.BindingSnapshot) error {
	close(p.entered)
	<-ctx.Done()
	close(p.canceled)
	<-p.release // A provider may ignore cancellation while unwinding an external call.
	return ctx.Err()
}
func (p *heartbeatProvider) DescribeCleanup(q provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	return p.Provider.(provider.ResourceCleaner).DescribeCleanup(q)
}
func (p *heartbeatProvider) CleanupResource(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	return p.Provider.(provider.ResourceCleaner).CleanupResource(ctx, q)
}

func TestWorkLeaseHeartbeatLossCancelsAndJoinsEveryEngine(t *testing.T) {
	for _, tc := range []struct {
		kind    workKind
		failure error
	}{
		{dispatchWork, ErrBusy}, {collectionWorkKind, ErrCorrupt}, {cleanupWork, cleanup.ErrLeaseLost},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			var f *dispatchFixture
			var adapter provider.Provider
			var run func(context.Context) (bool, error)
			p := &heartbeatProvider{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
			switch tc.kind {
			case dispatchWork:
				f = newDispatchFixture(t, fake.DefaultScenario())
				f.seed(t, "a", 1, false)
				adapter = f.adapter
				e, err := dispatch.New(heartbeatDispatchRepository{f.s, f.clock.Now, tc.failure}, cleanupResolver{p}, f.blobs, f.clock, dispatch.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				run = func(ctx context.Context) (bool, error) { return e.RunOnce(ctx, "heartbeat") }
			case collectionWorkKind:
				var src *collectionProvider
				var blobs collection.BlobStore
				f, _, src, blobs = collectionFixture(t)
				adapter = src
				e, err := collection.New(heartbeatCollectionRepository{f.s, f.clock.Now, tc.failure}, cleanupResolver{p}, blobs, f.clock, collection.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				run = e.RunOnce
			default:
				var id scheduler.Identity
				f, id = collectedFixture(t)
				admitCleanup(t, f, id, "heartbeat-loss", false)
				adapter = f.adapter
				e, err := cleanup.New(heartbeatCleanupRepository{f.s, tc.failure}, cleanupResolver{p}, cleanup.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				run = e.RunOnce
			}
			p.Provider = adapter
			before := f.backend.Stats()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := run(ctx); done <- err }()
			select {
			case <-p.entered:
			case <-ctx.Done():
				t.Fatal("provider callback did not start")
			}
			select {
			case <-p.canceled:
			case <-ctx.Done():
				t.Fatal("renewal failure did not cancel provider callback")
			}
			select {
			case err := <-done:
				t.Fatal("engine returned while provider callback remained active", err)
			case <-time.After(25 * time.Millisecond):
			}
			close(p.release)
			select {
			case err := <-done:
				if !errors.Is(err, tc.failure) {
					t.Fatal("renewal policy was hidden", err)
				}
			case <-ctx.Done():
				t.Fatal("engine did not join provider callback")
			}
			after := f.backend.Stats()
			if after.PrepareCalls != before.PrepareCalls || after.SubmitCalls != before.SubmitCalls || after.CleanupCalls != before.CleanupCalls {
				t.Fatal("cancellation started a provider mutation")
			}
			if tc.kind == dispatchWork && controlCount(t, f.s, "SELECT count(*) FROM attempts WHERE orchestration='needs_attention'") != 0 {
				t.Fatal("transient renewal failure failed the job")
			}
		})
	}
}

type dualFailureDispatchRepository struct{ heartbeatDispatchRepository }

func (r dualFailureDispatchRepository) YieldDispatch(context.Context, dispatch.Handle, time.Duration) error {
	return ErrCorrupt
}

type dualFailureCollectionRepository struct{ heartbeatCollectionRepository }

func (r dualFailureCollectionRepository) PinCollection(ctx context.Context, _ collection.Work, _ collection.Snapshot) error {
	<-ctx.Done()
	return ErrCorrupt
}

type dualFailureCleanupRepository struct{ heartbeatCleanupRepository }

func (r dualFailureCleanupRepository) BeginCleanupResource(ctx context.Context, _ cleanup.Work, _ cleanup.ResourceWork) error {
	<-ctx.Done()
	return ErrCorrupt
}

func TestWorkLeaseHeartbeatAndProcessingFailuresBothSurvive(t *testing.T) {
	for _, kind := range []workKind{dispatchWork, collectionWorkKind, cleanupWork} {
		t.Run(string(kind), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var err error
			switch kind {
			case dispatchWork:
				f := newDispatchFixture(t, fake.DefaultScenario())
				f.seed(t, "a", 1, false)
				p := &heartbeatProvider{Provider: f.adapter, entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
				close(p.release)
				repo := dualFailureDispatchRepository{heartbeatDispatchRepository{f.s, f.clock.Now, ErrBusy}}
				e, newErr := dispatch.New(repo, cleanupResolver{p}, f.blobs, f.clock, dispatch.DefaultConfig())
				if newErr != nil {
					t.Fatal(newErr)
				}
				_, err = e.RunOnce(ctx, "dual_failure")
			case collectionWorkKind:
				f, _, p, blobs := collectionFixture(t)
				repo := dualFailureCollectionRepository{heartbeatCollectionRepository{f.s, f.clock.Now, ErrBusy}}
				e, newErr := collection.New(repo, cleanupResolver{p}, blobs, f.clock, collection.DefaultConfig())
				if newErr != nil {
					t.Fatal(newErr)
				}
				_, err = e.RunOnce(ctx)
			default:
				f, id := collectedFixture(t)
				admitCleanup(t, f, id, "dual_failure", false)
				repo := dualFailureCleanupRepository{heartbeatCleanupRepository{f.s, ErrBusy}}
				e, newErr := cleanup.New(repo, cleanupResolver{f.adapter}, cleanup.DefaultConfig())
				if newErr != nil {
					t.Fatal(newErr)
				}
				_, err = e.RunOnce(ctx)
			}
			if !errors.Is(err, ErrCorrupt) || !errors.Is(err, ErrBusy) || worker.Classify(err) != worker.Stop {
				t.Fatal("heartbeat masked a concurrent fatal processing error", err)
			}
		})
	}
}
