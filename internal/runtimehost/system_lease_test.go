package runtimehost

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

// Cached expiry metadata can be shortened without changing the persisted claim authority.
type heartbeatDispatchRepository struct {
	*sqlite.Store
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
	*sqlite.Store
	now     func() time.Time
	failure error
}

func (r heartbeatCollectionRepository) ClaimCollection(ctx context.Context, ttl time.Duration, workers int) (*collection.Work, error) {
	work, err := r.Store.ClaimCollection(ctx, ttl, workers)
	if work != nil {
		work.Lease.Until = r.now()
	}
	return work, err
}
func (r heartbeatCollectionRepository) RenewCollection(context.Context, collection.Work, time.Duration) (time.Time, error) {
	return time.Time{}, r.failure
}

type heartbeatCleanupRepository struct {
	*sqlite.Store
	failure error
}

func (r heartbeatCleanupRepository) ClaimCleanup(ctx context.Context, ttl time.Duration) (*cleanup.Work, error) {
	work, err := r.Store.ClaimCleanup(ctx, ttl)
	if work != nil {
		work.Lease.Until = time.Now()
	}
	return work, err
}
func (r heartbeatCleanupRepository) RenewCleanup(context.Context, cleanup.Work, time.Duration) (time.Time, error) {
	return time.Time{}, r.failure
}

type heartbeatProvider struct {
	provider.Provider
	entered, canceled, release chan struct{}
}

func (p *heartbeatProvider) Verify(ctx context.Context, _ provider.BindingSnapshot) error {
	close(p.entered)
	<-ctx.Done()
	close(p.canceled)
	<-p.release // External callbacks may need time to unwind after cancellation.
	return ctx.Err()
}

func heartbeatFixture(t *testing.T, kind string) (*managedTestFixture, admission.Receipt) {
	t.Helper()
	f := newSystemFixture(t, fake.DefaultScenario())
	if kind != "dispatch" {
		job := f.collectibleJob(t, "heartbeat-job", "automatic")
		if kind == "cleanup" {
			f.collectOnce(t)
			if _, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCleanup, "heartbeat-cleanup", systemControlBody(t, job.AttemptID)); err != nil {
				t.Fatal(err)
			}
		}
		return f, job
	}
	job := f.submitJob(t, "heartbeat-job", f.jobSpec(t, f.connection(t, "heartbeat-connection", "SYNTHETIC_FIRST")))
	f.authorizeJob(t, job)
	return f, job
}

func TestWorkLeaseHeartbeatLossCancelsAndJoinsEveryEngine(t *testing.T) {
	for _, tc := range []struct {
		kind    string
		failure error
	}{
		{"dispatch", sqlite.ErrBusy}, {"collection", sqlite.ErrCorrupt}, {"cleanup", cleanup.ErrLeaseLost},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			f, job := heartbeatFixture(t, tc.kind)
			p := &heartbeatProvider{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
			f.wrapProvider = func(base provider.Provider) provider.Provider { p.Provider = base; return p }
			var run func(context.Context) (bool, error)
			switch tc.kind {
			case "dispatch":
				engine, err := dispatch.New(heartbeatDispatchRepository{f.host.store, f.clock.Now, tc.failure}, f.services.resolver, f.host.inputs, f.clock, dispatch.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				run = func(ctx context.Context) (bool, error) { return engine.RunOnce(ctx, "heartbeat") }
			case "collection":
				engine, err := collection.New(heartbeatCollectionRepository{f.host.store, f.clock.Now, tc.failure}, f.services.resolver, f.host.results, f.clock, collection.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				run = engine.RunOnce
			default:
				run = f.cleanupEngine(t, heartbeatCleanupRepository{f.host.store, tc.failure}).RunOnce
			}
			before := f.backend.Stats()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(p.release) }) }
			done, finished := make(chan error, 1), make(chan struct{})
			go func() { defer close(finished); _, err := run(ctx); done <- err }()
			defer func() {
				cancel()
				release()
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Error("provider callback did not join during cleanup")
				}
			}()
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
			release()
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
			if tc.kind == "dispatch" {
				record, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
				if err != nil || record.Attempt.State.Orchestration == domain.OrchestrationNeedsAttention {
					t.Fatal("transient renewal failure failed the job", err)
				}
			}
		})
	}
}

type dualFailureDispatchRepository struct{ heartbeatDispatchRepository }

func (r dualFailureDispatchRepository) YieldDispatch(context.Context, dispatch.Handle, time.Duration) error {
	return sqlite.ErrCorrupt
}

type dualFailureCollectionRepository struct{ heartbeatCollectionRepository }

func (r dualFailureCollectionRepository) PinCollection(ctx context.Context, _ collection.Work, _ collection.Snapshot) error {
	<-ctx.Done()
	return sqlite.ErrCorrupt
}

type dualFailureCleanupRepository struct{ heartbeatCleanupRepository }

func (r dualFailureCleanupRepository) BeginCleanupResource(ctx context.Context, _ cleanup.Work, _ cleanup.ResourceWork) error {
	<-ctx.Done()
	return sqlite.ErrCorrupt
}

func TestWorkLeaseHeartbeatAndProcessingFailuresBothSurvive(t *testing.T) {
	for _, kind := range []string{"dispatch", "collection", "cleanup"} {
		t.Run(kind, func(t *testing.T) {
			f, _ := heartbeatFixture(t, kind)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var run func(context.Context) (bool, error)
			switch kind {
			case "dispatch":
				p := &heartbeatProvider{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
				close(p.release)
				f.wrapProvider = func(base provider.Provider) provider.Provider { p.Provider = base; return p }
				repo := dualFailureDispatchRepository{heartbeatDispatchRepository{f.host.store, f.clock.Now, sqlite.ErrBusy}}
				engine, err := dispatch.New(repo, f.services.resolver, f.host.inputs, f.clock, dispatch.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				run = func(ctx context.Context) (bool, error) { return engine.RunOnce(ctx, "dual_failure") }
			case "collection":
				repo := dualFailureCollectionRepository{heartbeatCollectionRepository{f.host.store, f.clock.Now, sqlite.ErrBusy}}
				engine, err := collection.New(repo, f.services.resolver, f.host.results, f.clock, collection.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				run = engine.RunOnce
			default:
				repo := dualFailureCleanupRepository{heartbeatCleanupRepository{f.host.store, sqlite.ErrBusy}}
				run = f.cleanupEngine(t, repo).RunOnce
			}
			_, err := run(ctx)
			if !errors.Is(err, sqlite.ErrCorrupt) || !errors.Is(err, sqlite.ErrBusy) || worker.Classify(err) != worker.Stop {
				t.Fatal("heartbeat masked a concurrent fatal processing error", err)
			}
		})
	}
}

type renewedCollectionRepository struct {
	*sqlite.Store
	work collection.Work
}

func (r *renewedCollectionRepository) ClaimCollection(ctx context.Context, ttl time.Duration, workers int) (*collection.Work, error) {
	work, err := r.Store.ClaimCollection(ctx, ttl, workers)
	if work != nil {
		r.work = *work
	}
	return work, err
}

type renewalTransferProvider struct {
	provider.Provider
	renew func() error
}

func (p renewalTransferProvider) Fetch(ctx context.Context, remote provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	if strings.HasPrefix(file.Path, "outputs/") {
		if err := p.renew(); err != nil {
			return provider.TransferResult{}, err
		}
	}
	return p.Provider.Fetch(ctx, remote, file, dst, limit, progress)
}

func TestCollectionPublicationSurvivesRenewalDuringTransfer(t *testing.T) {
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "renew-transfer-job", "automatic")
	repo := &renewedCollectionRepository{Store: f.host.store}
	renewals := 0
	f.wrapProvider = func(base provider.Provider) provider.Provider {
		return renewalTransferProvider{Provider: base, renew: func() error {
			f.advance(t, 10*time.Second)
			_, err := f.host.store.RenewCollection(t.Context(), repo.work, time.Minute)
			renewals++
			return err
		}}
	}
	engine, err := collection.New(repo, f.services.resolver, f.host.results, f.clock, collection.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := engine.RunOnce(t.Context()); !worked || err != nil {
		t.Fatal("collection after renewal", worked, err)
	}
	result, err := f.host.store.ReadCollection(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	if err != nil || len(result.Files) != 2 || renewals != 1 {
		t.Fatal("renewal invalidated verified proof", renewals, err)
	}
}
