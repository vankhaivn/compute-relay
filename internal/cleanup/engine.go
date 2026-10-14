package cleanup

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
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
	Record                Record
	Binding               provider.BindingSnapshot
	Request               provider.CleanupRequest
	Resources             []ResourceWork
	AcceptNameBasedDelete bool
}

func (w Work) Valid() bool {
	op, id := w.Record.Operation, w.Request.Remote.Identity
	if len(w.Resources) > MaxResources {
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
	return w.Lease.Valid() && w.Record.Validate() == nil && !w.Record.Replay && op.Status == domain.OperationRunning &&
		op.ID == w.Lease.OperationID && op.WorkspaceID == w.Lease.WorkspaceID && w.Binding.Valid() &&
		w.Request.Remote.Validate() == nil && id.WorkspaceID == op.WorkspaceID && id.JobID == op.JobID && id.AttemptID == op.AttemptID &&
		id.InstanceID == w.Binding.Binding.ProviderInstanceID && w.Request.LedgerID.Valid() && w.Request.CreationOperationID.Valid() &&
		w.Request.Mode == provider.CleanupApply
}

// Outcome is the only result a worker records. A failure carries a public problem.
type Outcome struct {
	Effect  Effect
	Problem *domain.Problem
}

func (o Outcome) Valid() bool {
	switch o.Effect {
	case RemoteDeleted, AlreadyAbsent:
		return o.Problem == nil
	case Failed:
		return o.Problem != nil && o.Problem.Validate() == nil
	}
	return false
}

// WorkRepository claims accepted cleanups and records exactly one terminal outcome.
// ClaimCleanup commits the running status and lease before any provider call.
type WorkRepository interface {
	ClaimCleanup(context.Context, time.Time, time.Duration) (*Work, error)
	CompleteCleanup(context.Context, Work, Outcome, time.Time) error
}
type Resolver interface {
	Resolve(provider.BindingSnapshot) (provider.Provider, error)
}
type Clock interface{ Now() time.Time }
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

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

// The worker may only verify the frozen binding and apply cleanup.
type target interface {
	provider.BindingVerifier
	Cleanup(context.Context, provider.CleanupRequest) (provider.CleanupOutcome, error)
}

type Engine struct {
	repo     WorkRepository
	resolver Resolver
	clock    Clock
	config   Config
	slots    chan struct{}
}

func New(repo WorkRepository, resolver Resolver, clock Clock, cfg Config) (*Engine, error) {
	if repo == nil || resolver == nil || !cfg.Valid() {
		return nil, ErrInvalid
	}
	if clock == nil {
		clock = wallClock{}
	}
	return &Engine{repo: repo, resolver: resolver, clock: clock, config: cfg, slots: make(chan struct{}, cfg.Workers)}, nil
}

// RunOnce processes one bounded attempt inventory. If the process stops after a call
// but before its outcome commits, lease expiry permits the original target again;
// adapters must therefore prove absence of an already deleted resource.
func (e *Engine) RunOnce(ctx context.Context) (bool, error) {
	select {
	case e.slots <- struct{}{}:
		defer func() { <-e.slots }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	w, err := e.repo.ClaimCleanup(ctx, e.clock.Now().UTC(), e.config.Timeout+30*time.Second)
	if err != nil || w == nil {
		return false, err
	}
	if !w.Valid() {
		return true, ErrInvalid
	}
	call, cancel := context.WithTimeout(ctx, e.config.Timeout)
	defer cancel()
	var outcome Outcome
	if len(w.Resources) > 0 {
		outcome, err = e.applyResources(ctx, call, *w)
		if err != nil {
			return true, err
		}
	} else {
		outcome = e.apply(call, *w)
	}
	if ctx.Err() != nil {
		// Shutdown leaves the running claim for lease-expiry recovery.
		return true, ctx.Err()
	}
	return true, e.repo.CompleteCleanup(ctx, *w, outcome, e.clock.Now().UTC())
}

func (e *Engine) apply(ctx context.Context, w Work) (outcome Outcome) {
	defer func() {
		if recover() != nil {
			outcome = failure(domain.CodeProviderStateUnknown, "remote cleanup did not report a result; owned resources may remain, inspect the provider before requesting cleanup again", domain.RecommendedActionInspectProvider)
		}
	}()
	p, err := e.resolver.Resolve(w.Binding)
	if err != nil || p == nil {
		return failure(domain.CodeConfigurationInvalid, "the attempt's frozen provider binding is not configured in this runtime; nothing was deleted", domain.RecommendedActionConfigure)
	}
	t, ok := p.(target)
	if !ok || t.VerifyBinding(ctx, w.Binding) != nil {
		return failure(domain.CodeRemoteIdentityMismatch, "the provider binding could not be verified for this attempt; nothing was deleted", domain.RecommendedActionInspectProvider)
	}
	result, err := t.Cleanup(ctx, w.Request)
	switch {
	case err == nil && result.Deleted && !result.AlreadyAbsent && !result.WouldDelete:
		return Outcome{Effect: RemoteDeleted}
	case err == nil && result.AlreadyAbsent && !result.Deleted && !result.WouldDelete:
		return Outcome{Effect: AlreadyAbsent}
	case err == nil:
		return failure(domain.CodeProviderStateUnknown, "remote cleanup returned no deletion evidence; owned resources may remain", domain.RecommendedActionInspectProvider)
	case errors.Is(err, provider.ErrCleanupUnsupported):
		return failure(domain.CodeUnsupportedCapability, "remote cleanup is not supported for this attempt's provider binding; owned resources were preserved", domain.RecommendedActionInspectProvider)
	}
	var value domain.Problem
	var pointer *domain.Problem
	code := domain.CodeProviderStateUnknown
	if errors.As(err, &value) {
		code = value.Code
	} else if errors.As(err, &pointer) && pointer != nil {
		code = pointer.Code
	}
	switch code {
	case domain.CodeRemoteIdentityMismatch:
		return failure(code, "the remote resource did not match the recorded attempt identity; nothing was deleted", domain.RecommendedActionInspectProvider)
	case domain.CodeRemoteExecutionUnresolved:
		return failure(code, "the provider reported active or unresolved compute; nothing was deleted", domain.RecommendedActionReconcile)
	case domain.CodeProviderAuthFailed, domain.CodeProviderAccessDenied:
		return failure(code, "the provider refused cleanup for this binding; owned resources were preserved", domain.RecommendedActionConfigure)
	}
	// Provider text is never copied into a public receipt.
	return failure(domain.CodeProviderStateUnknown, "remote cleanup did not complete; owned resources may remain, inspect the provider before requesting cleanup again", domain.RecommendedActionInspectProvider)
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
	return Outcome{Effect: Failed, Problem: &p}
}

// Run polls until ctx ends. Lost fences continue; transient store failures back off.
func (e *Engine) Run(ctx context.Context) error {
	return worker.Run(ctx, worker.Config{Workers: e.config.Workers, PollDelay: e.config.PollDelay, Drain: true}, func(ctx context.Context, _ int) (bool, error) {
		return e.RunOnce(ctx)
	}, func(err error) worker.Decision {
		if errors.Is(err, ErrLeaseLost) {
			return worker.Continue
		}
		if errors.Is(err, scheduler.ErrClock) {
			return worker.Retry
		}
		return worker.Classify(err)
	})
}
