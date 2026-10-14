package sqlite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// cleanupProbe wraps the fake adapter. It checks durable intent before every apply call
// and can replace the adapter result; it never deletes anything outside the fake.
type cleanupProbe struct {
	*probeProvider
	mu           sync.Mutex
	requests     []provider.CleanupRequest
	verifyErr    error
	verifies     int
	err          error
	resourceHook func(context.Context, provider.CleanupResourceRequest) (provider.CleanupOutcome, error)
}

func (p *cleanupProbe) Verify(ctx context.Context, b provider.BindingSnapshot) error {
	p.mu.Lock()
	p.verifies++
	p.mu.Unlock()
	if p.verifyErr != nil {
		return p.verifyErr
	}
	return p.probeProvider.Verify(ctx, b)
}
func (p *cleanupProbe) CleanupResource(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	if request.Mode == provider.CleanupDryRun {
		if p.resourceHook != nil {
			return p.resourceHook(ctx, request)
		}
		return p.probeProvider.CleanupResource(ctx, request)
	}
	var running int
	if err := p.store.db.QueryRowContext(ctx, `SELECT count(*) FROM cleanup_resources WHERE resource_id=? AND state='running'`, string(request.LedgerID)).Scan(&running); err != nil || running < 1 {
		panic("resource cleanup called without durable intent")
	}
	p.mu.Lock()
	p.requests = append(p.requests, request.CleanupRequest)
	p.mu.Unlock()
	if p.resourceHook != nil {
		return p.resourceHook(ctx, request)
	}
	if p.err != nil {
		return provider.CleanupOutcome{}, p.err
	}
	return p.probeProvider.CleanupResource(ctx, request)
}

type cleanupResolver struct{ p provider.Provider }

func (r cleanupResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	return r.p, nil
}

func cleanupWorker(t *testing.T, f *dispatchFixture) (*cleanup.Engine, *cleanupProbe) {
	t.Helper()
	probe := &cleanupProbe{probeProvider: f.adapter}
	engine, err := cleanup.New(f.s, cleanupResolver{probe}, cleanup.DefaultConfig())
	requireNoError(t, err)
	return engine, probe
}
func runCleanup(t *testing.T, engine *cleanup.Engine) {
	t.Helper()
	worked, err := engine.RunOnce(context.Background())
	if err != nil || !worked {
		t.Fatalf("cleanup: worked=%v err=%v", worked, err)
	}
}
func admitCleanup(t *testing.T, f *dispatchFixture, id scheduler.Identity, key string, discard bool) (*operations.Service, auth.Principal, operations.Record) {
	t.Helper()
	service, p := cleanupService(t, f, "a", auth.Read, auth.Operate)
	r, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationCleanup, key, cleanupBody(id.AttemptID, discard, ""))
	requireNoError(t, err)
	return service, p, r
}
func TestCleanupLeaseFencesStaleWorkers(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, p, r := admitCleanup(t, f, id, "cleanup-fence", false)
	f.clock.advance(time.Second)
	first, err := f.s.ClaimCleanup(ctx, 2*time.Second)
	if err != nil || first == nil || first.Lease.Generation != 1 || first.Record.Operation.Status != domain.OperationRunning {
		t.Fatal("claim did not commit running intent", first, err)
	}
	if again, err := f.s.ClaimCleanup(ctx, 2*time.Second); again != nil || err != nil {
		t.Fatal("live claim was taken twice", again, err)
	}
	f.clock.advance(3 * time.Second)
	second, err := f.s.ClaimCleanup(ctx, 2*time.Second)
	if err != nil || second == nil || second.Lease.Generation != 2 || second.Lease.Fence == first.Lease.Fence || second.Record.Operation != first.Record.Operation {
		t.Fatal("expired claim was not recovered with a new fence", second, err)
	}
	deleted := cleanup.Outcome{Effect: operations.RemoteDeleted}
	if err = f.s.CompleteCleanup(ctx, *first, deleted); !errors.Is(err, cleanup.ErrLeaseLost) {
		t.Fatal("stale worker completed cleanup", err)
	}
	if err = f.s.CompleteCleanup(ctx, *second, cleanup.Outcome{Effect: operations.Failed}); !errors.Is(err, ErrInvalid) {
		t.Fatal("failure without a problem accepted", err)
	}
	finishCleanupResourceFixtures(t, f, *second)
	err = f.s.CompleteCleanup(ctx, *second, deleted)
	requireNoError(t, err)
	if err = f.s.CompleteCleanup(ctx, *second, cleanup.Outcome{Effect: operations.AlreadyAbsent}); !errors.Is(err, cleanup.ErrLeaseLost) {
		t.Fatal("terminal cleanup was rewritten", err)
	}
	current, err := service.Get(ctx, p, "a", r.Operation.ID)
	if err != nil || current.Effect != operations.RemoteDeleted || current.Operation.Revision != 3 {
		t.Fatal(current, err)
	}
	if f.backend.Stats().CleanupCalls != 0 {
		t.Fatal("store claims called the provider")
	}
}

// Store-only tests attest a result only after committing each fenced resource intent.
func finishCleanupResourceFixtures(t *testing.T, f *dispatchFixture, w cleanup.Work) {
	t.Helper()
	for _, item := range w.Resources {
		requireNoError(t, f.s.BeginCleanupResource(context.Background(), w, item))
		result := item.Result
		result.State, result.Reason = operations.ResourceDeleted, "fixture_deletion_confirmed"
		requireNoError(t, f.s.CompleteCleanupResource(context.Background(), w, result))
	}
}
