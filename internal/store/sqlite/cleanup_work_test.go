package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	mu        sync.Mutex
	requests  []provider.CleanupRequest
	verifyErr error
	result    *provider.CleanupOutcome
	err       error
}

func (p *cleanupProbe) VerifyBinding(ctx context.Context, b provider.BindingSnapshot) error {
	if p.verifyErr != nil {
		return p.verifyErr
	}
	return p.probeProvider.VerifyBinding(ctx, b)
}
func (p *cleanupProbe) Cleanup(ctx context.Context, r provider.CleanupRequest) (provider.CleanupOutcome, error) {
	var running int
	if err := p.store.db.QueryRowContext(ctx, `SELECT count(*) FROM cleanup_operations c JOIN cleanup_leases l
 ON l.workspace_id=c.workspace_id AND l.operation_id=c.operation_id WHERE c.status='running'`).Scan(&running); err != nil || running != 1 {
		panic("cleanup called without a committed running claim")
	}
	p.mu.Lock()
	p.requests = append(p.requests, r)
	p.mu.Unlock()
	if p.result != nil || p.err != nil {
		var result provider.CleanupOutcome
		if p.result != nil {
			result = *p.result
		}
		return result, p.err
	}
	return p.probeProvider.Cleanup(ctx, r)
}

type cleanupResolver struct{ p provider.Provider }

func (r cleanupResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	return r.p, nil
}

func cleanupWorker(t *testing.T, f *dispatchFixture) (*cleanup.Engine, *cleanupProbe) {
	t.Helper()
	probe := &cleanupProbe{probeProvider: f.adapter}
	engine, err := cleanup.New(f.s, cleanupResolver{probe}, f.clock, cleanup.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return engine, probe
}
func runCleanup(t *testing.T, engine *cleanup.Engine) {
	t.Helper()
	worked, err := engine.RunOnce(context.Background())
	if err != nil || !worked {
		t.Fatalf("cleanup: worked=%v err=%v", worked, err)
	}
}
func admitCleanup(t *testing.T, f *dispatchFixture, id scheduler.Identity, key string, discard bool) (*cleanup.Service, auth.Principal, cleanup.Record) {
	t.Helper()
	service, p := cleanupService(t, f, "a", auth.Read, auth.Operate)
	r, err := service.Submit(context.Background(), p, "a", id.JobID, key, cleanupBody(id.AttemptID, discard, ""))
	if err != nil {
		t.Fatal(err)
	}
	return service, p, r
}
func stagingLedger(t *testing.T, f *dispatchFixture, id scheduler.Identity) domain.ProviderResourceID {
	t.Helper()
	return domain.ProviderResourceID("res_staging_" + string(f.journal(t, id).Prepared.Identity.IntentID))
}

func TestCleanupWorkerDeletesOnceAndKeepsReceipts(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, p, original := admitCleanup(t, f, id, "cleanup-apply", false)
	engine, probe := cleanupWorker(t, f)
	f.clock.advance(time.Second)
	runCleanup(t, engine)
	current, err := service.Get(ctx, p, "a", original.Operation.ID)
	if err != nil || current.Operation.Status != domain.OperationSucceeded || current.Effect != cleanup.RemoteDeleted || current.Operation.Revision != 3 || current.Operation.Failure != nil {
		t.Fatal("deletion was not recorded", current, err)
	}
	j := f.journal(t, id)
	want := provider.CleanupRequest{Remote: *j.Remote, LedgerID: stagingLedger(t, f, id), CreationOperationID: j.PreparationID, ResultsCollected: true, Mode: provider.CleanupApply}
	if len(probe.requests) != 1 || probe.requests[0] != want || f.backend.Stats().CleanupCalls != 1 {
		t.Fatal("cleanup did not apply exactly the recorded ownership", probe.requests)
	}
	if worked, err := engine.RunOnce(ctx); worked || err != nil {
		t.Fatal("completed cleanup repeated", worked, err)
	}
	replay, err := service.Submit(ctx, p, "a", id.JobID, "cleanup-apply", cleanupBody(id.AttemptID, false, ""))
	if err != nil || !replay.Replay || replay.Operation.Status != domain.OperationAccepted || replay.Effect != cleanup.Pending {
		t.Fatal("replay rewrote the original receipt", replay, err)
	}
	again, err := service.Submit(ctx, p, "a", id.JobID, "cleanup-after-apply", cleanupBody(id.AttemptID, false, ""))
	if err != nil || again.Operation.ID != original.Operation.ID || again.Effect != cleanup.RemoteDeleted || !again.Replay {
		t.Fatal("applied cleanup was requested again", again, err)
	}
	if s := f.state(t, id); s.Result != domain.ResultAvailable || s.Orchestration != domain.OrchestrationSucceeded {
		t.Fatal("cleanup changed local results or attempt state", s)
	}
	assertNoNewCompute(t, f)
}

func TestCleanupWorkerReportsAlreadyAbsent(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	j := f.journal(t, id)
	// Deleted out of band before the request; the adapter proves absence, not deletion.
	if out, err := f.adapter.Cleanup(ctx, provider.CleanupRequest{Remote: *j.Remote, LedgerID: stagingLedger(t, f, id), CreationOperationID: j.PreparationID, ResultsCollected: true, Mode: provider.CleanupApply}); err != nil || !out.Deleted {
		t.Fatal(out, err)
	}
	service, p, r := admitCleanup(t, f, id, "cleanup-absent", false)
	engine, _ := cleanupWorker(t, f)
	runCleanup(t, engine)
	current, err := service.Get(ctx, p, "a", r.Operation.ID)
	if err != nil || current.Operation.Status != domain.OperationSucceeded || current.Effect != cleanup.AlreadyAbsent {
		t.Fatal("absence was not recorded", current, err)
	}
}

func TestCleanupWorkerFailuresAreExplicit(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name   string
		setup  func(*cleanupProbe)
		code   domain.ErrorCode
		action domain.RecommendedAction
		calls  int
	}{
		{"adapter refuses uncollected results", func(*cleanupProbe) {}, domain.CodeProviderStateUnknown, domain.RecommendedActionInspectProvider, 0},
		{"binding verification fails", func(p *cleanupProbe) { p.verifyErr = errors.New("binding-secret-canary") }, domain.CodeRemoteIdentityMismatch, domain.RecommendedActionInspectProvider, 0},
		{"adapter unsupported", func(p *cleanupProbe) {
			p.err = fmt.Errorf("adapter-secret-canary: %w", provider.ErrCleanupUnsupported)
		}, domain.CodeUnsupportedCapability, domain.RecommendedActionInspectProvider, 0},
		{"adapter returns no evidence", func(p *cleanupProbe) { p.result = &provider.CleanupOutcome{WouldDelete: true} }, domain.CodeProviderStateUnknown, domain.RecommendedActionInspectProvider, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, id := uncollectedFixture(t)
			service, p, r := admitCleanup(t, f, id, "cleanup-failure", true)
			engine, probe := cleanupWorker(t, f)
			c.setup(probe)
			runCleanup(t, engine)
			current, err := service.Get(ctx, p, "a", r.Operation.ID)
			if err != nil || current.Operation.Status != domain.OperationFailed || current.Effect != cleanup.Failed {
				t.Fatal("failure was not recorded", current, err)
			}
			problem := current.Operation.Failure
			if problem == nil || problem.Code != c.code || problem.RecommendedAction != c.action || problem.Validate() != nil || len(problem.Details) != 0 {
				t.Fatal("unexpected public problem", problem)
			}
			for _, canary := range []string{"secret-canary", "collected results"} {
				if strings.Contains(problem.Message, canary) {
					t.Fatal("provider text leaked into the receipt", problem.Message)
				}
			}
			if f.backend.Stats().CleanupCalls != c.calls {
				t.Fatal("unexpected provider deletion", f.backend.Stats().CleanupCalls)
			}
			// A failed cleanup never blocks recovery or an explicit new request.
			controls, cp, _ := controlService(t, f, "a", auth.Operate)
			if _, err = controls.Submit(ctx, cp, "a", id.JobID, domain.OperationCollect, "collect-after-failure", controlRequest(id.AttemptID)); err != nil {
				t.Fatal("failed cleanup blocked collection", err)
			}
		})
	}
	t.Run("new request after failure", func(t *testing.T) {
		f, id := uncollectedFixture(t)
		service, p, first := admitCleanup(t, f, id, "cleanup-first", true)
		engine, probe := cleanupWorker(t, f)
		probe.err = errors.New("transient")
		runCleanup(t, engine)
		probe.err, probe.result = nil, &provider.CleanupOutcome{Deleted: true}
		second, err := service.Submit(ctx, p, "a", id.JobID, "cleanup-second", cleanupBody(id.AttemptID, true, ""))
		if err != nil || second.Operation.ID == first.Operation.ID || second.Replay {
			t.Fatal("failed cleanup absorbed a new explicit request", second, err)
		}
		runCleanup(t, engine)
		if current, err := service.Get(ctx, p, "a", second.Operation.ID); err != nil || current.Effect != cleanup.RemoteDeleted {
			t.Fatal(current, err)
		}
		controls, cp, _ := controlService(t, f, "a", auth.Operate)
		if _, err = controls.Submit(ctx, cp, "a", id.JobID, domain.OperationCollect, "collect-after-delete", controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrState) {
			t.Fatal("collection admitted after remote deletion", err)
		}
	})
}

func TestCleanupWorkerTargetsPreparedResource(t *testing.T) {
	ctx := context.Background()
	f, id := preparedFixture(t)
	service, p, r := admitCleanup(t, f, id, "cleanup-prepared", false)
	engine, probe := cleanupWorker(t, f)
	probe.result = &provider.CleanupOutcome{Deleted: true}
	runCleanup(t, engine)
	j := f.journal(t, id)
	want := provider.CleanupRequest{
		Remote:   provider.RemoteReference{Identity: j.Prepared.Identity, Resource: j.Prepared.Resource, Version: "prepared"},
		LedgerID: stagingLedger(t, f, id), CreationOperationID: j.PreparationID, ResultsCollected: false, Mode: provider.CleanupApply,
	}
	if len(probe.requests) != 1 || probe.requests[0] != want {
		t.Fatal("prepared cleanup did not target the staging resource", probe.requests)
	}
	if current, err := service.Get(ctx, p, "a", r.Operation.ID); err != nil || current.Effect != cleanup.RemoteDeleted {
		t.Fatal(current, err)
	}
	if s := f.backend.Stats(); s.SubmitCalls != 0 || s.Executions != 0 {
		t.Fatal("cleanup submitted compute", s)
	}
}

func TestCleanupLeaseFencesStaleWorkers(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, p, r := admitCleanup(t, f, id, "cleanup-fence", false)
	f.clock.advance(time.Second)
	first, err := f.s.ClaimCleanup(ctx, f.clock.Now(), 2*time.Second)
	if err != nil || first == nil || first.Lease.Generation != 1 || first.Record.Operation.Status != domain.OperationRunning {
		t.Fatal("claim did not commit running intent", first, err)
	}
	if again, err := f.s.ClaimCleanup(ctx, f.clock.Now(), 2*time.Second); again != nil || err != nil {
		t.Fatal("live claim was taken twice", again, err)
	}
	if managed, err := f.s.ClaimCleanupManaged(ctx, f.clock.Now(), 2*time.Second); managed != nil || err != nil {
		t.Fatal("managed worker claimed an explicit attempt", managed, err)
	}
	f.clock.advance(3 * time.Second)
	second, err := f.s.ClaimCleanup(ctx, f.clock.Now(), 2*time.Second)
	if err != nil || second == nil || second.Lease.Generation != 2 || second.Lease.Fence == first.Lease.Fence || second.Record.Operation != first.Record.Operation {
		t.Fatal("expired claim was not recovered with a new fence", second, err)
	}
	deleted := cleanup.Outcome{Effect: cleanup.RemoteDeleted}
	if err = f.s.CompleteCleanup(ctx, *first, deleted, f.clock.Now()); !errors.Is(err, cleanup.ErrLeaseLost) {
		t.Fatal("stale worker completed cleanup", err)
	}
	if err = f.s.CompleteCleanup(ctx, *second, cleanup.Outcome{Effect: cleanup.Failed}, f.clock.Now()); !errors.Is(err, ErrInvalid) {
		t.Fatal("failure without a problem accepted", err)
	}
	if err = f.s.CompleteCleanup(ctx, *second, deleted, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err = f.s.CompleteCleanup(ctx, *second, cleanup.Outcome{Effect: cleanup.AlreadyAbsent}, f.clock.Now()); !errors.Is(err, cleanup.ErrLeaseLost) {
		t.Fatal("terminal cleanup was rewritten", err)
	}
	current, err := service.Get(ctx, p, "a", r.Operation.ID)
	if err != nil || current.Effect != cleanup.RemoteDeleted || current.Operation.Revision != 3 {
		t.Fatal(current, err)
	}
	if f.backend.Stats().CleanupCalls != 0 {
		t.Fatal("store claims called the provider")
	}
}
