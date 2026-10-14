package cleanup

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type stubRepo struct {
	mu       sync.Mutex
	work     *Work
	claimErr error
	complete error
	claims   int
	outcomes []Outcome
}

func (r *stubRepo) ClaimCleanup(context.Context, time.Duration) (*Work, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claims++
	w := r.work
	r.work = nil
	return w, r.claimErr
}
func (r *stubRepo) RenewCleanup(context.Context, Work, time.Duration) (time.Time, error) {
	return time.Now().Add(time.Minute), nil
}

func (r *stubRepo) CompleteCleanup(_ context.Context, _ Work, o Outcome) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, o)
	return r.complete
}

// stubTarget implements only the methods the engine may call; anything else panics.
type stubTarget struct {
	provider.Provider
	verify  error
	result  provider.CleanupOutcome
	err     error
	panics  bool
	block   bool
	applied int
}

func (s *stubTarget) Verify(context.Context, provider.BindingSnapshot) error { return s.verify }
func (s *stubTarget) CleanupResource(ctx context.Context, r provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	s.applied++
	if s.panics {
		panic("provider-secret-canary")
	}
	if s.block {
		<-ctx.Done()
		return provider.CleanupOutcome{}, ctx.Err()
	}
	return s.result, s.err
}

func (s *stubTarget) DescribeCleanup(provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	return provider.CleanupDescription{Kind: "fake_resource", ID: "resource", Name: "resource", Version: "1", Scope: "fake_attempt", Support: domain.CapabilitySupportSupported, Reason: "offline_fixture"}, nil
}
func (r *stubRepo) BeginCleanupResource(context.Context, Work, ResourceWork) error {
	return nil
}
func (r *stubRepo) CompleteCleanupResource(context.Context, Work, operations.ResourceResult) error {
	return nil
}

type stubResolver struct {
	p   provider.Provider
	err error
}

func (r stubResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) { return r.p, r.err }

func stubWork(t *testing.T) *Work {
	t.Helper()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	op, err := domain.NewOperation("op_1", "a", "job_1", "att_1", domain.OperationCleanup, now)
	if err != nil {
		t.Fatal(err)
	}
	if op, err = op.Transition(domain.OperationRunning, nil, now); err != nil {
		t.Fatal(err)
	}
	binding := provider.BindingSnapshot{Binding: domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "fake_1", ConfigurationRevision: "one"}, AccountScope: "account", CredentialRef: "env:FIXTURE"}
	identity := provider.Identity{InstallationID: "rt_1", WorkspaceID: "a", JobID: "job_1", AttemptID: "att_1", InstanceID: "fake_1", IntentID: "int_1",
		ResourceKey: "key", Nonce: strings.Repeat("n", 64), BundleSHA256: domain.SHA256Digest(strings.Repeat("a", 64)), InputManifestSHA256: domain.SHA256Digest(strings.Repeat("b", 64))}
	w := &Work{
		Lease:   Lease{WorkspaceID: "a", OperationID: "op_1", Generation: 1, Fence: strings.Repeat("f", 64), Until: now.Add(time.Minute)},
		Record:  operations.Record{Operation: op, Effect: operations.Pending},
		Binding: binding,
		Request: provider.CleanupRequest{Remote: provider.RemoteReference{Identity: identity, Resource: "resource", Version: "1"}, LedgerID: "res_staging_int_1", CreationOperationID: "op_prepare", ResultsCollected: true, Mode: provider.CleanupApply},
	}
	resource := Resource{Request: provider.CleanupResourceRequest{CleanupRequest: w.Request, Purpose: "staging", OwnershipOperationID: w.Request.CreationOperationID, PlanSHA256: provider.Digest([]byte("plan"))}}
	w.Resources = []ResourceWork{{Resource: resource, Result: operations.ResourceResult{ResourceID: w.Request.LedgerID, Purpose: "staging", Fingerprint: resource.Fingerprint(), State: operations.ResourcePending, Reason: "cleanup_requested"}}}
	if !w.Valid() {
		t.Fatal("invalid stub work")
	}
	return w
}

func TestEngineMapsEveryOutcome(t *testing.T) {
	for _, c := range []struct {
		name     string
		target   *stubTarget
		resolver error
		effect   operations.Effect
		code     domain.ErrorCode
		applied  int
	}{
		{"deleted", &stubTarget{result: provider.CleanupOutcome{Deleted: true}}, nil, operations.RemoteDeleted, "", 1},
		{"already absent", &stubTarget{result: provider.CleanupOutcome{AlreadyAbsent: true}}, nil, operations.AlreadyAbsent, "", 1},
		{"contradictory", &stubTarget{result: provider.CleanupOutcome{Deleted: true, AlreadyAbsent: true}}, nil, operations.Failed, domain.CodeProviderStateUnknown, 1},
		{"dry-run evidence only", &stubTarget{result: provider.CleanupOutcome{WouldDelete: true}}, nil, operations.Failed, domain.CodeProviderStateUnknown, 1},
		{"unsupported", &stubTarget{err: errors.Join(errors.New("adapter-canary"), provider.ErrCleanupUnsupported)}, nil, operations.Failed, domain.CodeUnsupportedCapability, 1},
		{"identity", &stubTarget{err: provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "adapter-canary")}, nil, operations.Failed, domain.CodeRemoteIdentityMismatch, 1},
		{"unresolved", &stubTarget{err: provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation, "adapter-canary")}, nil, operations.Failed, domain.CodeRemoteExecutionUnresolved, 1},
		{"unknown", &stubTarget{err: errors.New("adapter-canary")}, nil, operations.Failed, domain.CodeProviderStateUnknown, 1},
		{"panic", &stubTarget{panics: true}, nil, operations.Failed, domain.CodeProviderStateUnknown, 1},
		{"binding", &stubTarget{verify: errors.New("adapter-canary")}, nil, operations.Failed, domain.CodeRemoteIdentityMismatch, 0},
		{"unconfigured", &stubTarget{}, errors.New("adapter-canary"), operations.Failed, domain.CodeConfigurationInvalid, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			repo := &stubRepo{work: stubWork(t)}
			engine, err := New(repo, stubResolver{c.target, c.resolver}, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			worked, err := engine.RunOnce(context.Background())
			if !worked || err != nil || len(repo.outcomes) != 1 || c.target.applied != c.applied {
				t.Fatal("one pass must record one outcome", worked, err, repo.outcomes, c.target.applied)
			}
			o := repo.outcomes[0]
			if !o.Valid() || o.Effect != c.effect {
				t.Fatal("unexpected outcome", o)
			}
			if c.code != "" && (o.Problem.Code != c.code || strings.Contains(o.Problem.Message, "canary") || o.Problem.SafeOperationRetry) {
				t.Fatal("unexpected public problem", o.Problem)
			}
		})
	}
}

func TestEngineShutdownLeavesClaimForRecovery(t *testing.T) {
	repo := &stubRepo{work: stubWork(t)}
	target := &stubTarget{block: true}
	engine, err := New(repo, stubResolver{target, nil}, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if worked, err := engine.RunOnce(ctx); !worked || !errors.Is(err, context.Canceled) || len(repo.outcomes) != 0 {
		t.Fatal("shutdown recorded an outcome", worked, err, repo.outcomes)
	}
}

func TestEngineRejectsInvalidWorkAndConfig(t *testing.T) {
	w := stubWork(t)
	w.Request.Mode = provider.CleanupDryRun
	repo := &stubRepo{work: w}
	target := &stubTarget{}
	engine, err := New(repo, stubResolver{target, nil}, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := engine.RunOnce(context.Background()); !worked || !errors.Is(err, ErrInvalid) || target.applied != 0 || len(repo.outcomes) != 0 {
		t.Fatal("invalid work reached the provider", worked, err)
	}
	for _, cfg := range []Config{{}, {Workers: 1, Timeout: time.Hour, PollDelay: time.Second}, {Workers: 1, Timeout: time.Minute}} {
		if _, err := New(repo, stubResolver{target, nil}, cfg); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid config accepted", cfg)
		}
	}
	if _, err := New(nil, stubResolver{target, nil}, DefaultConfig()); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing repository accepted")
	}
}

func TestRunSurvivesLostFenceAndStopsOnStoreErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PollDelay = time.Millisecond
	repo := &stubRepo{work: stubWork(t), complete: ErrLeaseLost}
	engine, err := New(repo, stubResolver{&stubTarget{result: provider.CleanupOutcome{Deleted: true}}, nil}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err = engine.Run(ctx); !errors.Is(err, context.DeadlineExceeded) || repo.claims < 2 {
		t.Fatal("lost fence stopped the worker", err, repo.claims)
	}
	broken := errors.New("store unavailable")
	repo = &stubRepo{claimErr: broken}
	if engine, err = New(repo, stubResolver{&stubTarget{}, nil}, cfg); err != nil {
		t.Fatal(err)
	}
	if err = engine.Run(context.Background()); !errors.Is(err, broken) {
		t.Fatal("store error was hidden", err)
	}
}

func TestEngineEmptyInventorySettlesWithoutProviderCall(t *testing.T) {
	w := stubWork(t)
	w.Resources = nil
	repo := &stubRepo{work: w}
	target := &stubTarget{panics: true}
	engine, err := New(repo, stubResolver{target, nil}, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	worked, err := engine.RunOnce(context.Background())
	if !worked || err != nil || target.applied != 0 || len(repo.outcomes) != 1 || repo.outcomes[0].Effect != operations.AlreadyAbsent {
		t.Fatalf("empty inventory did not settle locally: %v %v %+v", worked, err, repo.outcomes)
	}
}
