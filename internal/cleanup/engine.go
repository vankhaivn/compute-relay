package cleanup

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

// Lease is a fenced claim on one running cleanup. A claim after expiry advances the
// generation and fence, so a late completion from an older owner is rejected.
type Lease struct {
	WorkspaceID domain.WorkspaceID
	OperationID domain.OperationID
	Generation  int64
	Fence       string
	Until       time.Time
}

func (l Lease) Valid() bool {
	return l.WorkspaceID.Valid() && l.OperationID.Valid() && l.Generation > 0 && len(l.Fence) == 64 &&
		domain.SHA256Digest(l.Fence).Valid() && !l.Until.IsZero()
}

// Work is rebuilt from the durable ledger and dispatch journal at claim time. Request
// always uses CleanupApply; ResultsCollected is the local attestation, never a guess.
type Work struct {
	Lease                 Lease
	Record                operations.Record
	Binding               provider.BindingSnapshot
	Request               provider.CleanupRequest
	Resources             []ResourceWork
	AcceptNameBasedDelete bool
}

func (w Work) Valid() bool {
	op, id := w.Record.Operation, w.Request.Remote.Identity
	if len(w.Resources) > operations.MaxCleanupResources {
		return false
	}
	seen := map[domain.ProviderResourceID]bool{}
	for _, resource := range w.Resources {
		q, r := resource.Resource.Request, resource.Result
		if !q.Valid() || q.AcceptNameBasedDelete || q.RecoverOnly || !r.Valid() || resource.Resource.Reason != "" || resource.Resource.Fingerprint() != r.Fingerprint || q.LedgerID != r.ResourceID ||
			q.Remote.Identity != id || q.Purpose != r.Purpose || seen[r.ResourceID] {
			return false
		}
		seen[r.ResourceID] = true
	}
	return w.Lease.Valid() && w.Record.Validate() == nil && op.Kind == domain.OperationCleanup && !w.Record.Replay && op.Status == domain.OperationRunning &&
		op.ID == w.Lease.OperationID && op.WorkspaceID == w.Lease.WorkspaceID && w.Binding.Valid() &&
		w.Request.Remote.Validate() == nil && id.WorkspaceID == op.WorkspaceID && id.JobID == op.JobID && id.AttemptID == op.AttemptID &&
		id.InstanceID == w.Binding.Binding.ProviderInstanceID && w.Request.LedgerID.Valid() && w.Request.CreationOperationID.Valid() &&
		w.Request.Mode == provider.CleanupApply
}

// Outcome is the only result a worker records. A failure carries a public problem.
type Outcome struct {
	Effect  operations.Effect
	Problem *domain.Problem
}

func (o Outcome) Valid() bool {
	switch o.Effect {
	case operations.RemoteDeleted, operations.AlreadyAbsent:
		return o.Problem == nil
	case operations.Failed:
		return o.Problem != nil && o.Problem.Validate() == nil
	}
	return false
}

// WorkRepository claims accepted cleanups and records exactly one terminal outcome.
// ClaimCleanup commits the running status and lease before any provider call.
type WorkRepository interface {
	ClaimCleanup(context.Context, time.Duration) (*Work, error)
	RenewCleanup(context.Context, Work, time.Duration) (time.Time, error)
	CompleteCleanup(context.Context, Work, Outcome) error
}
type Resolver interface {
	Resolve(provider.BindingSnapshot) (provider.Provider, error)
}
type Config struct {
	Workers   int
	Timeout   time.Duration
	PollDelay time.Duration
}

func DefaultConfig() Config {
	return Config{Workers: 1, Timeout: 5 * time.Minute, PollDelay: time.Second}
}
func (c Config) Valid() bool {
	return c.Workers >= 1 && c.Workers <= 4 && c.Timeout >= time.Second && c.Timeout <= 30*time.Minute &&
		c.PollDelay >= time.Millisecond && c.PollDelay <= time.Minute
}

type Engine struct {
	repo     WorkRepository
	resolver Resolver
	config   Config
	slots    chan struct{}
}

func New(repo WorkRepository, resolver Resolver, cfg Config) (*Engine, error) {
	if repo == nil || resolver == nil || !cfg.Valid() {
		return nil, ErrInvalid
	}
	return &Engine{repo: repo, resolver: resolver, config: cfg, slots: make(chan struct{}, cfg.Workers)}, nil
}

// RunOnce processes one bounded attempt inventory. If the process stops after a call
// but before its outcome commits, lease expiry permits the original target again;
// adapters must therefore prove absence of an already deleted resource.
func (e *Engine) RunOnce(ctx context.Context) (worked bool, resultErr error) {
	select {
	case e.slots <- struct{}{}:
		defer func() { <-e.slots }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	w, err := e.repo.ClaimCleanup(ctx, worker.LeaseDuration)
	if err != nil || w == nil {
		return false, err
	}
	if !w.Valid() {
		return true, ErrInvalid
	}
	owned, stop := worker.Heartbeat(ctx, w.Lease.Until, nil, func(c context.Context) (time.Time, error) {
		return e.repo.RenewCleanup(c, *w, worker.LeaseDuration)
	})
	defer func() {
		resultErr = worker.HeartbeatError(resultErr, stop())
	}()
	call, cancel := context.WithTimeout(owned, e.config.Timeout)
	defer cancel()
	var outcome Outcome
	if len(w.Resources) > 0 {
		outcome, err = e.applyResources(owned, call, *w)
		if err != nil {
			return true, err
		}
	} else {
		outcome = Outcome{Effect: operations.AlreadyAbsent}
	}
	if owned.Err() != nil {
		// Shutdown leaves the running claim for lease-expiry recovery.
		return true, owned.Err()
	}
	if err := stop(); err != nil {
		return true, err
	}
	return true, e.repo.CompleteCleanup(ctx, *w, outcome)
}

func failure(code domain.ErrorCode, message string, action domain.RecommendedAction) Outcome {
	category, _ := code.Category()
	stage := domain.FailureStageOperation
	switch category {
	case domain.ErrorCategoryValidation:
		stage = domain.FailureStageValidation
	case domain.ErrorCategoryObservation:
		stage = domain.FailureStageObservation
	case domain.ErrorCategoryLocalRuntime:
		stage = domain.FailureStageLocalRuntime
	case domain.ErrorCategoryAuthenticationAuthorization:
		stage = domain.FailureStageAuthentication
	}
	p, err := domain.NewProblem(code, message, stage)
	if err != nil {
		p, _ = domain.NewProblem(domain.CodeProviderStateUnknown, message, domain.FailureStageObservation)
	}
	p = p.WithRetrySemantics(false, false).WithRecommendedAction(action)
	return Outcome{Effect: operations.Failed, Problem: &p}
}

// Run polls until ctx ends. Lost fences continue; transient store failures back off.
func (e *Engine) Run(ctx context.Context) error {
	return worker.Run(ctx, worker.Config{Workers: e.config.Workers, PollDelay: e.config.PollDelay, Drain: true}, func(ctx context.Context, _ int) (bool, error) {
		return e.RunOnce(ctx)
	}, func(err error) worker.Decision {
		if errors.Is(err, ErrLeaseLost) {
			return worker.Continue
		}
		return worker.Classify(err)
	})
}
