package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type Config struct {
	Workers                                       int
	ControlTimeout, PreparationTimeout, PollDelay time.Duration
}

func DefaultConfig() Config {
	return Config{Workers: 4, ControlTimeout: time.Minute, PreparationTimeout: 5 * time.Minute, PollDelay: 15 * time.Second}
}
func (c Config) Valid() bool {
	return c.Workers > 0 && c.Workers <= 64 && c.ControlTimeout > 0 && c.ControlTimeout <= 5*time.Minute && c.PreparationTimeout >= c.ControlTimeout && c.PreparationTimeout <= time.Hour && c.PollDelay >= time.Millisecond && c.PollDelay <= 5*time.Minute
}

type Engine struct {
	repo      Repository
	providers Resolver
	blobs     BlobStore
	fetcher   HTTPSFetcher
	clock     Clock
	config    Config
	slots     chan struct{}
}

func New(repo Repository, providers Resolver, blobs BlobStore, fetcher HTTPSFetcher, clock Clock, cfg Config) (*Engine, error) {
	if repo == nil || providers == nil || blobs == nil || !cfg.Valid() {
		return nil, ErrInvalid
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &Engine{repo: repo, providers: providers, blobs: blobs, fetcher: fetcher, clock: clock, config: cfg, slots: make(chan struct{}, cfg.Workers)}, nil
}

// RunOnce advances at most one durable phase. Recovery has priority, but never creates
// another preparation/submission for a previously started intent. No implicit CLI wiring.
func (e *Engine) RunOnce(ctx context.Context, owner string) (bool, error) {
	select {
	case e.slots <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	defer func() { <-e.slots }()
	claim, err := e.repo.ClaimRecovery(ctx, owner, e.clock.Now())
	if err != nil {
		return false, err
	}
	if claim == nil {
		r, err := e.repo.ClaimNext(ctx, owner, e.clock.Now())
		if err != nil {
			return false, err
		}
		claim = r.Claim
	}
	if claim == nil {
		return false, nil
	}
	return true, e.process(ctx, *claim)
}

// Run uses a fixed number of cooperative workers. Cancellation never calls remote
// Cancel/Cleanup. It waits for callbacks to return rather than replacing stuck workers
// or claiming shutdown completed while they may still perform a side effect.
func (e *Engine) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	failures := make(chan error, e.config.Workers)
	for i := 0; i < e.config.Workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for {
				_, err := e.RunOnce(ctx, fmt.Sprintf("dispatch_%d", index))
				if err != nil && ctx.Err() == nil {
					var p *domain.Problem
					if !errors.As(err, &p) && !errors.Is(err, ErrPolicy) {
						failures <- err
						cancel()
						return
					}
				}
				timer := time.NewTimer(e.config.PollDelay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		return err
	}
	return ctx.Err()
}

func (e *Engine) process(parent context.Context, claim scheduler.Claim) (resultErr error) {
	work, err := e.repo.LoadDispatch(parent, claim, e.clock.Now())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, e.config.PreparationTimeout)
	s := &session{e: e, current: work, ctx: ctx, cancel: cancel}
	done := make(chan struct{})
	go s.heartbeat(done)
	defer func() {
		cancel()
		<-done
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		w := s.work()
		delay := e.config.PollDelay
		for n := 0; n < w.Journal.Failures && delay < 120*time.Second; n++ {
			delay *= 2
		}
		if delay > 120*time.Second {
			delay = 120 * time.Second
		}
		if errors.Is(resultErr, ErrPolicy) {
			delay = 5 * time.Minute
		}
		// Deterministic per-attempt jitter spreads observations while remaining reproducible.
		if len(w.Handle.Claim.Fence) > 0 {
			delay += time.Duration(w.Handle.Claim.Fence[0]%4) * delay / 10
		}
		if yieldErr := e.repo.YieldDispatch(cleanup, w.Handle, e.clock.Now(), delay); yieldErr != nil && resultErr == nil {
			resultErr = yieldErr
		}
	}()
	binding := provider.BindingSnapshot{Binding: work.Job.Profile.Binding, AccountScope: work.Job.Profile.AccountScope, CredentialRef: work.Job.Profile.CredentialRef}
	p, err := e.providers.Resolve(binding)
	if err != nil {
		return s.fail(domain.CodeConfigurationInvalid)
	}
	checker, ok := p.(provider.BindingVerifier)
	if !ok {
		return s.fail(domain.CodeConfigurationInvalid)
	}
	_, err = control(ctx, e.config.ControlTimeout, func(c context.Context) (struct{}, error) { return struct{}{}, checker.VerifyBinding(c, binding) })
	if err != nil || p.Describe().InstanceID != binding.Binding.ProviderInstanceID {
		return s.fail(domain.CodeProviderAuthFailed)
	}
	switch work.Journal.Phase {
	case Local:
		snapshot, err := e.freeze(ctx, s)
		if err != nil {
			return s.fail(errorCode(err, domain.CodeInputFetchFailed))
		}
		work = s.work()
		job, prepID, err := ResolveJob(work, snapshot)
		if err != nil {
			return s.fail(domain.CodeResourceRequirementUnsatisfied)
		}
		plan, err := control(ctx, e.config.ControlTimeout, func(c context.Context) (provider.Plan, error) { return p.Validate(c, job.Clone()) })
		if err != nil {
			return s.fail(errorCode(err, domain.CodeUnsupportedCapability))
		}
		if err = ValidatedPlan(job, plan, p.Describe()); err != nil {
			return s.fail(domain.CodeUnsupportedCapability)
		}
		if err = s.commit(Action{Kind: BeginPreparation, Plan: &plan, PreparationID: prepID}); err != nil {
			return err
		}
		// This invocation alone received the successful new intent commit. Restart paths
		// enter Staging below and cannot call Prepare, even if no resource is found.
		prepared, err := control(ctx, e.config.PreparationTimeout, func(c context.Context) (provider.Prepared, error) { return p.Prepare(c, plan.Clone(), prepID) })
		if err != nil {
			return s.fail(domain.CodeStagingFailed)
		}
		return s.prepared(prepared)
	case Staging:
		observer, ok := p.(provider.PreparationObserver)
		if !ok {
			return s.fail(domain.CodeConfigurationInvalid)
		}
		journal := work.Journal
		seen, err := control(ctx, e.config.ControlTimeout, func(c context.Context) (provider.PreparationObservation, error) {
			return observer.ReconcilePreparation(c, journal.Plan.Clone(), journal.PreparationID)
		})
		if err != nil {
			return s.fail(domain.CodeStagingFailed)
		}
		if seen.Validate(*journal.Plan, journal.PreparationID) != nil {
			return s.fail(domain.CodeRemoteIdentityMismatch)
		}
		if seen.Status != provider.ReconciliationFound {
			return s.fail(domain.CodeStagingNotReady)
		}
		return s.prepared(*seen.Prepared)
	case Ready:
		if err = s.commit(Action{Kind: BeginSubmission}); err != nil {
			return err
		}
		prepared := *s.work().Journal.Prepared
		outcome, callErr := control(ctx, e.config.ControlTimeout, func(c context.Context) (provider.SubmissionOutcome, error) { return p.Submit(c, prepared), nil })
		if callErr != nil || outcome.Validate(prepared.Identity) != nil {
			outcome = provider.SubmissionOutcome{Status: provider.SubmissionUnknown, Problem: problem(domain.CodeProviderSubmissionUnknown, true)}
		}
		return s.commit(Action{Kind: SubmissionSeen, Submission: &outcome})
	case Submitting:
		identity := work.Journal.Plan.Job.Identity
		seen, err := control(ctx, e.config.ControlTimeout, func(c context.Context) (provider.Reconciliation, error) { return p.ReconcileSubmission(c, identity) })
		if err != nil {
			return s.fail(domain.CodeProviderSubmissionUnknown)
		}
		if seen.Validate(identity) != nil {
			return s.fail(domain.CodeRemoteIdentityMismatch)
		}
		if seen.Status != provider.ReconciliationFound {
			return s.fail(domain.CodeProviderSubmissionUnknown)
		}
		outcome := provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: seen.Remote}
		return s.commit(Action{Kind: SubmissionSeen, Submission: &outcome})
	case Submitted:
		remote := *work.Journal.Remote
		obs, err := control(ctx, e.config.ControlTimeout, func(c context.Context) (provider.Observation, error) { return p.Observe(c, remote) })
		if err != nil {
			return s.fail(domain.CodeProviderUnreachable)
		}
		if obs.Validate(remote) != nil {
			return s.fail(domain.CodeRemoteIdentityMismatch)
		}
		err = s.commit(Action{Kind: ObservationSeen, Observation: &obs})
		if errors.Is(err, ErrStaleObservation) {
			return nil
		}
		return err
	default:
		return ErrConflict
	}
}

func control[T any](ctx context.Context, limit time.Duration, fn func(context.Context) (T, error)) (value T, err error) {
	bounded, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	defer func() {
		if recover() != nil {
			var zero T
			value = zero
			err = errors.New("provider call failed without a usable result")
		}
	}()
	if err := bounded.Err(); err != nil {
		return value, err
	}
	return fn(bounded)
}
func errorCode(err error, fallback domain.ErrorCode) domain.ErrorCode {
	var pointer *domain.Problem
	if errors.As(err, &pointer) && pointer != nil && problem(pointer.Code, false) != nil {
		return pointer.Code
	}
	var value domain.Problem
	if errors.As(err, &value) && problem(value.Code, false) != nil {
		return value.Code
	}
	return fallback
}

type session struct {
	mu      sync.Mutex
	e       *Engine
	current Work
	ctx     context.Context
	cancel  context.CancelFunc
}

func (s *session) work() Work { s.mu.Lock(); defer s.mu.Unlock(); return s.current }
func (s *session) commit(action Action) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := context.Background()
	if action.Kind == BeginPreparation || action.Kind == BeginSubmission {
		base = s.ctx // Cancellation can save an outcome, but can never authorize new work.
	}
	ctx, stop := context.WithTimeout(base, 5*time.Second)
	defer stop()
	work, err := s.e.repo.CommitDispatch(ctx, s.current.Handle, action, s.e.clock.Now())
	if err == nil {
		s.current = work
	}
	return err
}
func (s *session) freeze(index int, m domain.ObjectMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.e.repo.FreezeInput(s.ctx, s.current.Handle, index, m, s.e.clock.Now())
}
func (s *session) prepared(p provider.Prepared) error {
	w := s.work()
	if w.Journal.Plan == nil || p.Validate(*w.Journal.Plan, w.Journal.PreparationID) != nil {
		return s.fail(domain.CodeRemoteIdentityMismatch)
	}
	return s.commit(Action{Kind: PreparationSeen, Prepared: &p})
}
func (s *session) fail(code domain.ErrorCode) error {
	if err := s.commit(Action{Kind: Fault, Code: code}); err != nil {
		return err
	}
	return s.work().Journal.Problem
}
func (s *session) heartbeat(done chan struct{}) {
	defer close(done)
	for {
		w := s.work()
		interval := w.Handle.Claim.ExpiresAt.Sub(s.e.clock.Now()) / 3
		if interval < 10*time.Millisecond {
			interval = 10 * time.Millisecond
		}
		if interval > 5*time.Second {
			interval = 5 * time.Second
		}
		timer := time.NewTimer(interval)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		s.mu.Lock()
		updated, err := s.e.repo.RenewDispatch(s.ctx, s.current.Handle, s.e.clock.Now())
		if err == nil {
			s.current.Handle = updated
		}
		s.mu.Unlock()
		if err != nil {
			s.cancel()
			return
		}
	}
}
