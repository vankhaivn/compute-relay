package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/worker"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

type Config struct {
	Workers                                       int
	ControlTimeout, PreparationTimeout, PollDelay time.Duration
}

// DefaultConfig gives one provider preparation the full hour ceiling: staging can upload every
// frozen input before creating its resource, and that upload is never repeated after a timeout.
func DefaultConfig() Config {
	return Config{Workers: 4, ControlTimeout: time.Minute, PreparationTimeout: time.Hour, PollDelay: 15 * time.Second}
}
func (c Config) Valid() bool {
	return c.Workers > 0 && c.Workers <= 64 && c.ControlTimeout > 0 && c.ControlTimeout <= 5*time.Minute && c.PreparationTimeout >= c.ControlTimeout && c.PreparationTimeout <= time.Hour && c.PollDelay >= time.Millisecond && c.PollDelay <= 5*time.Minute
}

type Engine struct {
	repo      Repository
	providers Resolver
	blobs     BlobStore
	clock     Clock
	config    Config
	slots     chan struct{}
	secrets   *workloadsecret.Pool
	newWork   NewWorkGuard
}

func (e *Engine) WithNewWorkGuard(guard NewWorkGuard) *Engine { e.newWork = guard; return e }

func (e *Engine) WithSecrets(pool *workloadsecret.Pool) *Engine { e.secrets = pool; return e }

func New(repo Repository, providers Resolver, blobs BlobStore, clock Clock, cfg Config) (*Engine, error) {
	if repo == nil || providers == nil || blobs == nil || !cfg.Valid() {
		return nil, ErrInvalid
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &Engine{repo: repo, providers: providers, blobs: blobs, clock: clock, config: cfg, slots: make(chan struct{}, cfg.Workers)}, nil
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
	claim, err := e.repo.ClaimRecovery(ctx, owner)
	if err != nil {
		return false, err
	}
	if claim == nil {
		r, err := e.repo.ClaimNext(ctx, owner)
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
	return worker.Run(ctx, worker.Config{Workers: e.config.Workers, PollDelay: e.config.PollDelay}, func(ctx context.Context, index int) (bool, error) {
		return e.RunOnce(ctx, fmt.Sprintf("dispatch_%d", index))
	}, func(err error) worker.Decision {
		if provider.IsLocalStateError(err) {
			return worker.Classify(err)
		}
		var p *domain.Problem
		if errors.As(err, &p) || newWorkRefusalReason(err) != "" || errors.Is(err, ErrPolicy) || errors.Is(err, scheduler.ErrLeaseLost) || controlContention(err) {
			return worker.Continue
		}
		return worker.Classify(err)
	})
}

// A submitted remote run is observed at most every 15 seconds; control requests still wake it
// at once. Unresolved outcomes after submission start from 30 seconds. Known remote runs
// remain observable through transient outages with bounded calls and backoff.
const (
	submittedPollFloor  = 15 * time.Second
	submittedRetryFloor = 30 * time.Second
)

// nextPassDelay doubles the poll delay per unresolved outcome up to two minutes.
func nextPassDelay(poll time.Duration, j Journal) time.Duration {
	delay := poll
	if j.Remote != nil {
		delay = max(delay, submittedPollFloor)
	}
	if j.SubmitStarted && j.Failures > 0 {
		delay = max(delay, submittedRetryFloor)
	}
	for n := 0; n < j.Failures && delay < 120*time.Second; n++ {
		delay *= 2
	}
	return min(delay, 120*time.Second)
}

func (e *Engine) process(parent context.Context, claim scheduler.Claim) (resultErr error) {
	work, err := e.repo.LoadDispatch(parent, claim)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, e.config.PreparationTimeout)
	s := &session{e: e, current: work}
	ctx, stop := worker.Heartbeat(ctx, work.Handle.Claim.ExpiresAt, e.clock.Now, func(c context.Context) (time.Time, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		updated, err := e.repo.RenewDispatch(c, s.current.Handle)
		if err == nil {
			s.current.Handle = updated
		}
		return updated.Claim.ExpiresAt, err
	})
	s.ctx = ctx
	defer func() {
		resultErr = worker.HeartbeatError(resultErr, stop())
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		s.mu.Lock()
		w, progressErr := s.current, s.progressErr
		s.mu.Unlock()
		resultErr = errors.Join(resultErr, progressErr)
		delay := nextPassDelay(e.config.PollDelay, w.Journal)
		if errors.Is(resultErr, ErrPolicy) {
			delay = 5 * time.Minute
		}
		if newWorkRefusalReason(resultErr) != "" {
			delay = 30 * time.Second
		}
		// Deterministic per-attempt jitter spreads observations while remaining reproducible.
		if len(w.Handle.Claim.Fence) > 0 {
			delay += time.Duration(w.Handle.Claim.Fence[0]%4) * delay / 10
		}
		resultErr = errors.Join(resultErr, e.repo.YieldDispatch(cleanup, w.Handle, delay))
	}()
	// Expiry prohibits a future start; it cannot establish that possible execution
	// stopped. Submitting and submitted attempts retain observation-only recovery.
	expired := work.Job.Request.Spec().Expired(e.clock.Now()) || work.Gate != nil && work.Gate.Expired(e.clock.Now())
	if !work.Journal.SubmitStarted && expired && work.Job.Attempt.State.Cancellation == domain.CancellationNotRequested && !(work.Gate != nil && work.Journal.Phase == Staging && work.Journal.Prepared == nil) {
		return s.commit(Action{Kind: ExpireDispatch})
	}
	preparationLimit := e.config.PreparationTimeout
	if work.Gate != nil {
		preparationLimit = min(preparationLimit, time.Duration(work.Gate.Request.MaxPreparationSeconds)*time.Second)
	}
	if unconfirmedPreparation(work.Journal) {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, min(preparationLimit, e.config.ControlTimeout))
		defer stop()
	}
	binding := provider.BindingSnapshot{Binding: work.Job.Profile.Binding, AccountScope: work.Job.Profile.AccountScope, CredentialRef: work.Job.Profile.CredentialRef}
	p, err := e.providers.Resolve(binding)
	if err != nil {
		if unconfirmedPreparation(work.Journal) {
			return s.endUnconfirmedPreparation(err)
		}
		return s.providerFailure(err, domain.CodeConfigurationInvalid)
	}
	_, err = control(ctx, e.config.ControlTimeout, func(c context.Context) (struct{}, error) { return struct{}{}, p.Verify(c, binding) })
	if err != nil {
		if unconfirmedPreparation(work.Journal) {
			return s.endUnconfirmedPreparation(err)
		}
		code := errorCode(err, domain.CodeProviderAuthFailed)
		if errors.Is(err, context.DeadlineExceeded) {
			code = domain.CodeProviderUnreachable
		}
		return s.providerFailure(err, code)
	}
	if p.Describe().InstanceID != binding.Binding.ProviderInstanceID {
		if unconfirmedPreparation(work.Journal) {
			return s.endUnconfirmedPreparation(nil)
		}
		return s.fail(domain.CodeRemoteIdentityMismatch)
	}
	// Explicit durable job cancellation is separate from Run's shutdown context.
	// It shares this lease, the frozen binding and the one-shot mutation discipline.
	if handled, err := s.cancelOnce(p); handled || err != nil {
		return err
	}
	switch work.Journal.Phase {
	case Local:
		if err := s.guardNewWork(); err != nil {
			return err
		}
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
			return s.providerFailure(err, errorCode(err, domain.CodeUnsupportedCapability))
		}
		if err = ValidatedPlan(job, plan, p.Describe()); err != nil {
			return s.fail(domain.CodeUnsupportedCapability)
		}
		if err = s.consumeAuthorization(); err != nil {
			if newWorkRefusalReason(err) != "" {
				return s.refuseNewWork(err)
			}
			if errors.Is(err, admission.ErrExpired) {
				return s.commit(Action{Kind: ExpireDispatch})
			}
			return err
		}
		if err = s.commit(Action{Kind: BeginPreparation, Plan: &plan, PreparationID: prepID}); err != nil {
			if newWorkRefusalReason(err) != "" {
				return s.refuseNewWork(err)
			}
			if errors.Is(err, admission.ErrExpired) {
				return s.commit(Action{Kind: ExpireDispatch})
			}
			return err
		}
		// This invocation alone received the successful new intent commit. Restart paths
		// enter Staging below and cannot call Prepare, even if no resource is found.
		observed := provider.WithPreparationProgress(ctx, s.progress)
		if work.Gate != nil {
			until, _ := time.Parse(time.RFC3339, work.Gate.Request.ReadyUntil)
			preparationLimit = min(preparationLimit, until.Sub(e.clock.Now()))
		}
		prepared, err := control(observed, preparationLimit, func(c context.Context) (provider.Prepared, error) { return p.Prepare(c, plan.Clone(), prepID) })
		if err != nil {
			if provider.IsLocalStateError(err) {
				return err
			}
			if errors.Is(err, admission.ErrExpired) {
				return s.commit(Action{Kind: ExpireDispatch})
			}
			return s.failWith(domain.CodeStagingFailed, problemReason(err))
		}
		return s.prepared(prepared)
	case Staging:
		journal := work.Journal
		limit := preparationLimit
		if unconfirmedPreparation(journal) {
			limit = min(limit, e.config.ControlTimeout)
		}
		seen, err := control(ctx, limit, func(c context.Context) (provider.PreparationObservation, error) {
			return p.ReconcilePrepare(c, journal.Plan.Clone(), journal.PreparationID)
		})
		if err != nil {
			if unconfirmedPreparation(journal) {
				return s.endUnconfirmedPreparation(err)
			}
			if provider.IsLocalStateError(err) {
				return err
			}
			if expired && work.Gate != nil {
				return s.commit(Action{Kind: ExpireDispatch})
			}
			return s.fail(domain.CodeStagingFailed)
		}
		if seen.Validate(*journal.Plan, journal.PreparationID) != nil {
			if unconfirmedPreparation(journal) {
				return s.endUnconfirmedPreparation(nil)
			}
			return s.fail(domain.CodeRemoteIdentityMismatch)
		}
		if seen.Status != provider.ReconciliationFound {
			if unconfirmedPreparation(journal) {
				return s.endUnconfirmedPreparation(nil)
			}
			if expired && work.Gate != nil {
				return s.commit(Action{Kind: ExpireDispatch})
			}
			return s.fail(domain.CodeStagingNotReady)
		}
		return s.prepared(*seen.Prepared)
	case Ready:
		var material workloadsecret.Material
		defer material.Clear()
		if work.Gate != nil {
			if work.Gate.Start == nil {
				return nil
			}
			if secret := work.Gate.Start.Secrets; secret != nil {
				key := workloadsecret.Key{WorkspaceID: work.Job.Job.WorkspaceID, JobID: work.Job.Job.ID, AttemptID: work.Job.Attempt.ID}
				if !e.secrets.Available(key, secret.AuthorizationID, e.clock.Now()) {
					return nil
				}
			}
			seen, err := control(ctx, preparationLimit, func(c context.Context) (provider.PreparationObservation, error) {
				return p.ReconcilePrepare(c, work.Journal.Plan.Clone(), work.Journal.PreparationID)
			})
			if err != nil {
				return s.providerFailure(err, errorCode(err, domain.CodeStagingNotReady))
			}
			if seen.Validate(*work.Journal.Plan, work.Journal.PreparationID) != nil || seen.Status != provider.ReconciliationFound || seen.Prepared == nil || seen.Prepared.ReadinessRevision() != work.Gate.ReadyRevision {
				return s.fail(domain.CodeRemoteIdentityMismatch)
			}
		}
		if err := s.guardNewWork(); err != nil {
			return err
		}
		if work.Gate != nil && work.Gate.Start.Secrets != nil {
			key := workloadsecret.Key{WorkspaceID: work.Job.Job.WorkspaceID, JobID: work.Job.Job.ID, AttemptID: work.Job.Attempt.ID}
			material, err = e.secrets.Claim(key, work.Gate.Start.Secrets.AuthorizationID, e.clock.Now(), func() error { return s.commit(Action{Kind: BeginSubmission, SecretsAvailable: true}) })
		} else {
			err = s.commit(Action{Kind: BeginSubmission})
		}
		if err != nil {
			if newWorkRefusalReason(err) != "" {
				return s.refuseNewWork(err)
			}
			if errors.Is(err, workloadsecret.ErrUnavailable) {
				return nil
			}
			if errors.Is(err, admission.ErrExpired) {
				return s.commit(Action{Kind: ExpireDispatch})
			}
			return err
		}
		prepared := *s.work().Journal.Prepared
		outcome, callErr := control(ctx, preparationLimit, func(c context.Context) (provider.SubmissionOutcome, error) {
			var authorization *provider.StartAuthorization
			var secrets *workloadsecret.Material
			if start := s.work().Journal.StartAuthorization; start != nil {
				copy := start.Clone()
				authorization = &copy
				if start.Secrets != nil {
					secrets = &material
				}
			}
			return p.Start(c, prepared, authorization, secrets), nil
		})
		if callErr != nil || outcome.Validate(prepared.Identity) != nil {
			outcome = provider.SubmissionOutcome{Status: provider.SubmissionUnknown, Problem: problem(domain.CodeProviderSubmissionUnknown, true)}
		}
		return s.commit(Action{Kind: SubmissionSeen, Submission: &outcome})
	case Submitting:
		identity := work.Journal.Plan.Job.Identity
		seen, err := control(ctx, e.config.ControlTimeout, func(c context.Context) (provider.Reconciliation, error) { return p.Reconcile(c, identity) })
		if err != nil {
			return s.providerFailure(err, domain.CodeProviderSubmissionUnknown)
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
			return s.providerFailure(err, domain.CodeProviderUnreachable)
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

func (s *session) guardNewWork() error {
	if s.e.newWork == nil {
		return nil
	}
	ctx, err := s.e.newWork.BeforeNewWork(s.ctx, s.work().Job)
	if ctx != nil {
		s.ctx = ctx
	}
	if newWorkRefusalReason(err) != "" {
		return s.refuseNewWork(err)
	}
	return err
}

func newWorkRefusalReason(err error) string {
	switch {
	case errors.Is(err, admission.ErrHostUnhealthy):
		return "host_unhealthy"
	case errors.Is(err, hoststorage.ErrHardLimit):
		return "host_storage_limit"
	case errors.Is(err, hoststorage.ErrUnknown):
		return "host_storage_unknown"
	default:
		return ""
	}
}

func (s *session) refuseNewWork(cause error) error {
	reason := newWorkRefusalReason(cause)
	if reason == "" {
		return cause
	}
	w := s.work()
	if w.Journal.Problem == nil || w.Journal.Problem.Details[domain.ReasonDetail] != reason {
		if err := s.commit(Action{Kind: RefuseNewWork, RefusalReason: reason}); err != nil {
			if errors.Is(err, ErrPolicy) {
				return cause
			}
			return err
		}
	}
	return cause
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

// problemReason returns the closed public reason an adapter problem carries, if any.
func problemReason(err error) string {
	var value domain.Problem
	if errors.As(err, &value) {
		return value.PublicDetails()[domain.ReasonDetail]
	}
	var pointer *domain.Problem
	if errors.As(err, &pointer) && pointer != nil {
		return pointer.PublicDetails()[domain.ReasonDetail]
	}
	return ""
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
	mu          sync.Mutex
	e           *Engine
	current     Work
	ctx         context.Context
	sampled     time.Time
	progressErr error
}

func (s *session) work() Work { s.mu.Lock(); defer s.mu.Unlock(); return s.current }
func (s *session) commit(action Action) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := context.Background()
	if action.Kind == BeginPreparation || action.Kind == BeginSubmission {
		base = s.ctx /* Cancellation can save an outcome, but can never authorize new work. */
	} else if action.Kind == RefuseNewWork {
		base = context.WithoutCancel(s.ctx)
	}
	ctx, stop := context.WithTimeout(base, 5*time.Second)
	defer stop()
	work, err := s.e.repo.CommitDispatch(ctx, s.current.Handle, action)
	if err == nil {
		s.current = work
	}
	return err
}
func (s *session) consumeAuthorization() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	work, err := s.e.repo.ConsumeAuthorization(s.ctx, s.current.Handle)
	if err == nil {
		s.current = work
	}
	return err
}
func (s *session) prepared(p provider.Prepared) error {
	w := s.work()
	if w.Journal.Plan == nil || p.Validate(*w.Journal.Plan, w.Journal.PreparationID) != nil {
		return s.fail(domain.CodeRemoteIdentityMismatch)
	}
	return s.commit(Action{Kind: PreparationSeen, Prepared: &p})
}
func (s *session) fail(code domain.ErrorCode) error { return s.failWith(code, "") }

func (s *session) failWith(code domain.ErrorCode, reason string) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if err := s.commit(Action{Kind: Fault, Code: code, Reason: reason}); err != nil {
		return err
	}
	return s.work().Journal.Problem
}

func (s *session) providerFailure(err error, code domain.ErrorCode) error {
	if provider.IsLocalStateError(err) {
		return err
	}
	return s.fail(code)
}

// progress records at most one staging sample per second plus the final one. Transient
// or stale samples are best effort; fatal failures surface after preparation joins.
func (s *session) progress(completed, total int64) {
	now := s.e.clock.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if completed < total && !s.sampled.IsZero() && now.Sub(s.sampled) < time.Second {
		return
	}
	s.sampled = now
	h := s.current.Handle
	sample := domain.PreparationProgress{Scope: "staged_input_bytes", Generation: h.Claim.Generation,
		BytesCompleted: completed, BytesTotal: total, ObservedAt: now}
	ctx, stop := context.WithTimeout(s.ctx, 5*time.Second)
	defer stop()
	err := s.e.repo.RecordPreparationProgress(ctx, h, sample)
	switch err {
	case nil, context.Canceled, scheduler.ErrLeaseLost, ErrInvalid, ErrConflict, ErrStaleObservation:
		return
	}
	if worker.Classify(err) == worker.Stop {
		s.progressErr = errors.Join(s.progressErr, err)
	}
}

func unconfirmedPreparation(j Journal) bool {
	return j.Phase == Staging && j.Plan != nil && j.Prepared == nil && !j.SubmitStarted
}

func (s *session) endUnconfirmedPreparation(cause error) error {
	if provider.IsLocalStateError(cause) {
		return cause
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if err := s.commit(Action{Kind: EndUnconfirmedPreparation}); err != nil {
		return err
	}
	return s.work().Journal.Problem
}
