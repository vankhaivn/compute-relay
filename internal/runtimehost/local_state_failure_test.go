package runtimehost

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
	"github.com/vankhaivn/compute-relay/internal/worker"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

func localFailureFixture(t *testing.T, kind string) (*managedTestFixture, admission.Receipt, domain.OperationID) {
	t.Helper()
	f, job := heartbeatFixture(t, kind)
	if kind != "cleanup" {
		return f, job, ""
	}
	// Replay the fixture's accepted request to retain its exact operation identity.
	r, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCleanup, "heartbeat-cleanup", systemControlBody(t, job.AttemptID))
	requireNoError(t, err)
	return f, job, r.Operation.ID
}

func localFailureStep(t *testing.T, f *managedTestFixture, kind string) func(context.Context) (bool, error) {
	t.Helper()
	switch kind {
	case "dispatch":
		return func(ctx context.Context) (bool, error) {
			return f.services.dispatcher.RunOnce(ctx, "local-read-failure")
		}
	case "collection":
		return f.services.collector.RunOnce
	default:
		return f.cleanupEngine(t, nil).RunOnce
	}
}

func assertLocalFailure(t *testing.T, err, cause error) {
	t.Helper()
	if !provider.IsLocalStateError(err) || !errors.Is(err, cause) || worker.Classify(err) != worker.Classify(cause) {
		t.Fatal("local state error lost its original retry/stop policy", err)
	}
}

func assertLocalWorkPreserved(t *testing.T, f *managedTestFixture, job admission.Receipt, cleanupID domain.OperationID) {
	t.Helper()
	record, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
	if err != nil || record.Problem != nil || record.Attempt.State.Orchestration == domain.OrchestrationNeedsAttention || record.Attempt.State.Result == domain.ResultIncomplete {
		t.Fatal("local failure became terminal provider/collection evidence", err, record.Problem)
	}
	if cleanupID != "" {
		receipt, err := f.controls(t).Get(t.Context(), f.actor, "app", cleanupID)
		if err != nil || receipt.Operation.Status != domain.OperationRunning || receipt.Effect != operations.Pending {
			t.Fatal("local failure finalized cleanup", err, receipt)
		}
		for _, result := range receipt.Resources {
			if result.State != operations.ResourcePending && result.State != operations.ResourceRunning {
				t.Fatal("local failure changed cleanup evidence", result)
			}
		}
	}
}

func TestManagedBindingStoreFailuresPreserveWork(t *testing.T) {
	for _, kind := range []string{"dispatch", "collection", "cleanup"} {
		for _, cause := range []error{sqlite.ErrBusy, context.DeadlineExceeded, sqlite.ErrCorrupt} {
			t.Run(kind+"/"+cause.Error(), func(t *testing.T) {
				f, job, cleanupID := localFailureFixture(t, kind)
				read := f.services.resolver.read
				f.services.resolver.read = func(context.Context, provider.BindingSnapshot) (connections.RuntimeBinding, error) {
					return connections.RuntimeBinding{}, cause
				}
				before := f.backend.Stats()
				step := localFailureStep(t, f, kind)
				worked, err := step(t.Context())
				if !worked {
					t.Fatal("worker did not reach the binding read", err)
				}
				assertLocalFailure(t, err, cause)
				assertLocalWorkPreserved(t, f, job, cleanupID)
				if f.backend.Stats() != before {
					t.Fatal("failed local binding read reached provider effects")
				}
				f.startHTTP(t)
				f.request(t, "GET", "/readyz", "", "", nil, http.StatusOK)
				f.stopHTTP(t)
				f.services.resolver.read = read
				f.advance(t, 2*time.Minute)
				if worked, err := step(t.Context()); !worked || err != nil {
					t.Fatal("original work did not resume after local recovery", worked, err)
				}
				if kind == "collection" {
					result, err := f.host.store.ReadCollection(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
					if err != nil || len(result.Files) == 0 {
						t.Fatal("collection ticket did not recover", err)
					}
				}
			})
		}
	}
}

type localProviderFault struct {
	stage       string
	cause       error
	calls       int
	cancelCalls int
}

func (f *localProviderFault) fail(stage string) error {
	if f.stage != stage {
		return nil
	}
	f.calls++
	return provider.LocalStateError{Err: f.cause}
}

type localFailureProvider struct {
	provider.Provider
	fault *localProviderFault
}

func (p localFailureProvider) Describe() provider.Descriptor {
	descriptor := p.Provider.Describe()
	if p.fault.stage == "cancel" || p.fault.cancelCalls > 0 {
		descriptor.Capabilities.RemoteCancellation = true
	}
	return descriptor
}

func (p localFailureProvider) Verify(ctx context.Context, binding provider.BindingSnapshot) error {
	if err := p.fault.fail("verify"); err != nil {
		return err
	}
	return p.Provider.Verify(ctx, binding)
}
func (p localFailureProvider) Validate(ctx context.Context, job provider.ResolvedJob) (provider.Plan, error) {
	if err := p.fault.fail("validate"); err != nil {
		return provider.Plan{}, err
	}
	return p.Provider.Validate(ctx, job)
}
func (p localFailureProvider) Prepare(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.Prepared, error) {
	if err := p.fault.fail("prepare"); err != nil {
		return provider.Prepared{}, err
	}
	prepared, err := p.Provider.Prepare(ctx, plan, id)
	if err == nil {
		err = p.fault.fail("prepare-after-effect")
	}
	return prepared, err
}
func (p localFailureProvider) ReconcilePrepare(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.PreparationObservation, error) {
	if err := p.fault.fail("reconcile-prepare"); err != nil {
		return provider.PreparationObservation{}, err
	}
	return p.Provider.ReconcilePrepare(ctx, plan, id)
}
func (p localFailureProvider) Reconcile(ctx context.Context, id provider.Identity) (provider.Reconciliation, error) {
	if err := p.fault.fail("reconcile"); err != nil {
		return provider.Reconciliation{}, err
	}
	return p.Provider.Reconcile(ctx, id)
}
func (p localFailureProvider) Observe(ctx context.Context, remote provider.RemoteReference) (provider.Observation, error) {
	if err := p.fault.fail("observe"); err != nil {
		return provider.Observation{}, err
	}
	return p.Provider.Observe(ctx, remote)
}
func (p localFailureProvider) Cancel(ctx context.Context, remote provider.RemoteReference, id domain.OperationID) (provider.CancellationOutcome, error) {
	p.fault.cancelCalls++
	if err := p.fault.fail("cancel"); err != nil {
		return provider.CancellationOutcome{}, err
	}
	return p.Provider.Cancel(ctx, remote, id)
}
func (p localFailureProvider) ListArtifacts(ctx context.Context, remote provider.RemoteReference) ([]provider.Artifact, error) {
	if err := p.fault.fail("catalog"); err != nil {
		return nil, err
	}
	return p.Provider.ListArtifacts(ctx, remote)
}
func (p localFailureProvider) Fetch(ctx context.Context, remote provider.RemoteReference, artifact provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	stage := "manifest"
	if strings.HasPrefix(artifact.Path, "outputs/") {
		stage = "output"
	}
	if err := p.fault.fail(stage); err != nil {
		return provider.TransferResult{}, err
	}
	return p.Provider.Fetch(ctx, remote, artifact, dst, limit, progress)
}
func (p localFailureProvider) CleanupResource(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	if err := p.fault.fail("cleanup"); err != nil {
		return provider.CleanupOutcome{}, err
	}
	return p.Provider.CleanupResource(ctx, request)
}

func TestLocalReadFailuresPreserveDispatchPhases(t *testing.T) {
	for _, stage := range []string{"verify", "validate", "prepare", "reconcile-prepare", "reconcile", "observe"} {
		for _, cause := range []error{sqlite.ErrBusy, context.DeadlineExceeded, sqlite.ErrCorrupt} {
			t.Run(stage+"/"+cause.Error(), func(t *testing.T) {
				f, job, _ := localFailureFixture(t, "dispatch")
				fault := &localProviderFault{cause: cause}
				f.wrapProvider = func(base provider.Provider) provider.Provider { return localFailureProvider{base, fault} }
				if stage == "reconcile-prepare" {
					fault.stage = "prepare-after-effect"
					if _, err := f.services.dispatcher.RunOnce(t.Context(), "local-read-seed"); !errors.Is(err, cause) {
						t.Fatal("preparation fault did not preserve its intent", err)
					}
				} else if stage == "reconcile" || stage == "observe" {
					f.dispatchOnce(t)
					if stage == "reconcile" {
						// Lose only the acknowledgement after the original fake accepts.
						f.wrapProvider = func(base provider.Provider) provider.Provider {
							return localUnknownStartProvider{Provider: localFailureProvider{base, fault}}
						}
					}
					f.dispatchOnce(t)
				}
				beforeRecord, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
				requireNoError(t, err)
				fault.stage, fault.calls = stage, 0
				f.advance(t, 2*time.Minute)
				before := f.backend.Stats()
				worked, err := f.services.dispatcher.RunOnce(t.Context(), "local-read-failure")
				if !worked || fault.calls != 1 {
					t.Fatal("dispatch did not reach the selected local read", worked, fault.calls, err)
				}
				assertLocalFailure(t, err, cause)
				afterRecord, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
				if err != nil || string(systemJSON(t, beforeRecord.Problem)) != string(systemJSON(t, afterRecord.Problem)) || f.backend.Stats() != before {
					t.Fatal("local callback failure consumed provider fault policy or mutated provider", err)
				}
				fault.stage = ""
				f.advance(t, 2*time.Minute)
				if worked, err := f.services.dispatcher.RunOnce(t.Context(), "local-read-recovered"); !worked || err != nil && stage != "prepare" {
					t.Fatal("dispatch did not resume original phase", worked, err)
				}
				stats := f.backend.Stats()
				if stats.PrepareCalls > 1 || stats.SubmitCalls > 1 {
					t.Fatal("local failure rearmed a mutation", stats)
				}
			})
		}
	}
}

type localUnknownStartProvider struct{ provider.Provider }

func (p localUnknownStartProvider) Start(ctx context.Context, prepared provider.Prepared, authority *provider.StartAuthorization, material *workloadsecret.Material) provider.SubmissionOutcome {
	_ = p.Provider.Start(ctx, prepared, authority, material)
	return provider.SubmissionOutcome{Status: provider.SubmissionUnknown, Problem: localSubmissionUnknown()}
}

func localSubmissionUnknown() *domain.Problem {
	p := provider.Problem(domain.CodeProviderSubmissionUnknown, domain.FailureStageSubmission, "submission outcome remains unknown")
	p.ComputeMayHaveStarted = true
	return &p
}

func TestLocalReadFailuresPreserveCollectionAndCleanup(t *testing.T) {
	for _, kind := range []string{"collection", "cleanup"} {
		stages := []string{"verify", "catalog", "manifest", "output"}
		if kind == "cleanup" {
			stages = []string{"verify", "cleanup"}
		}
		for _, stage := range stages {
			for _, cause := range []error{sqlite.ErrBusy, context.DeadlineExceeded, sqlite.ErrCorrupt} {
				t.Run(kind+"/"+stage+"/"+cause.Error(), func(t *testing.T) {
					f, job, cleanupID := localFailureFixture(t, kind)
					fault := &localProviderFault{stage: stage, cause: cause}
					f.wrapProvider = func(base provider.Provider) provider.Provider { return localFailureProvider{base, fault} }
					before := f.backend.Stats()
					worked, err := localFailureStep(t, f, kind)(t.Context())
					if !worked || fault.calls != 1 {
						t.Fatal("worker did not reach selected local read", worked, fault.calls, err)
					}
					assertLocalFailure(t, err, cause)
					assertLocalWorkPreserved(t, f, job, cleanupID)
					after := f.backend.Stats()
					if after.PrepareCalls != before.PrepareCalls || after.SubmitCalls != before.SubmitCalls || after.CleanupCalls != before.CleanupCalls {
						t.Fatal("failed local read reached provider mutation")
					}
					fault.stage = ""
					f.advance(t, 2*time.Minute)
					if worked, err := localFailureStep(t, f, kind)(t.Context()); !worked || err != nil {
						t.Fatal("original collection/cleanup claim did not recover", worked, err)
					}
				})
			}
		}
	}
}

func TestLocalCancellationReadKeepsOneShotIntent(t *testing.T) {
	for _, cause := range []error{sqlite.ErrBusy, context.DeadlineExceeded, sqlite.ErrCorrupt} {
		t.Run(cause.Error(), func(t *testing.T) {
			f, job, _ := localFailureFixture(t, "dispatch")
			f.dispatchOnce(t)
			f.dispatchOnce(t)
			receipt, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCancel, "local-cancel", systemControlBody(t, job.AttemptID))
			requireNoError(t, err)
			fault := &localProviderFault{stage: "cancel", cause: cause}
			f.wrapProvider = func(base provider.Provider) provider.Provider { return localFailureProvider{base, fault} }
			f.advance(t, 2*time.Minute)
			worked, err := f.services.dispatcher.RunOnce(t.Context(), "local-cancel")
			if !worked || fault.cancelCalls != 1 {
				t.Fatal("cancellation callback did not run once", worked, err)
			}
			assertLocalFailure(t, err, cause)
			current, err := f.controls(t).Get(t.Context(), f.actor, "app", receipt.Operation.ID)
			if err != nil || current.Operation.Status != domain.OperationRunning || current.Operation.Failure != nil {
				t.Fatal("local error finalized cancellation", current, err)
			}
			fault.stage = ""
			f.advance(t, 2*time.Minute)
			if _, err := f.services.dispatcher.RunOnce(t.Context(), "local-cancel-recovery"); err != nil || fault.cancelCalls != 1 {
				t.Fatal("local recovery rearmed cancellation", fault.cancelCalls, err)
			}
		})
	}
}

type localStartReadProvider struct {
	provider.Provider
	start func(context.Context, provider.Prepared) provider.SubmissionOutcome
	calls int
}

func (p *localStartReadProvider) Start(ctx context.Context, prepared provider.Prepared, _ *provider.StartAuthorization, _ *workloadsecret.Material) provider.SubmissionOutcome {
	p.calls++
	return p.start(ctx, prepared)
}

func TestLocalAttemptReadAfterSubmissionIntentNeverRearmsStart(t *testing.T) {
	f, job, _ := localFailureFixture(t, "dispatch")
	f.dispatchOnce(t)
	connection, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
	requireNoError(t, err)
	binding := provider.BindingSnapshot{Binding: connection.Profile.Binding, AccountScope: connection.Profile.AccountScope, CredentialRef: connection.Profile.CredentialRef}
	frozen, err := f.host.store.ReadManagedBinding(t.Context(), binding)
	requireNoError(t, err)
	inner := binding
	inner.AccountScope = frozen.CanonicalAccount
	policy := kaggle.DefaultExecutionPolicy()
	policy.MaxWallSeconds = frozen.Profile.MaxRemoteWallSeconds
	adapter, err := kaggle.NewRuntimeAdapter(kaggle.Config{InstanceID: string(inner.Binding.ProviderInstanceID), Revision: inner.Binding.ConfigurationRevision,
		AccountName: inner.AccountScope, CredentialRef: ports.CredentialRef(inner.CredentialRef)}, inner, f.services.connections, f.host.inputs, f.clock,
		func(context.Context, provider.Identity) (provider.Plan, provider.Prepared, error) {
			return provider.Plan{}, provider.Prepared{}, sqlite.ErrBusy
		}, policy, true)
	requireNoError(t, err)
	p := &localStartReadProvider{start: func(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
		record, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
		if err != nil || record.Attempt.State.Execution != domain.ExecutionUnknown || record.Attempt.State.Orchestration != domain.OrchestrationDispatching {
			t.Fatal("Start preceded durable submission intent", err)
		}
		return adapter.Start(ctx, prepared, nil, nil)
	}}
	f.wrapProvider = func(base provider.Provider) provider.Provider { p.Provider = base; return p }
	f.dispatchOnce(t)
	record, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
	if err != nil || record.Problem == nil || record.Problem.Code != domain.CodeProviderSubmissionUnknown || record.Attempt.State.Execution != domain.ExecutionUnknown || p.calls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("unavailable frozen attempt invented a rejection or submit", err, p.calls)
	}
	// Subsequent passes must observe only; the restored fake returns not-found.
	for range 2 {
		f.advance(t, 5*time.Minute)
		_, _ = f.services.dispatcher.RunOnce(t.Context(), "local-start-recovery")
	}
	if p.calls != 1 || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("recovery retried one-shot Start", p.calls, f.backend.Stats())
	}
}

type localCollectionProgressRepository struct {
	*sqlite.Store
	samples int
}

func (r *localCollectionProgressRepository) RecordCollectionProgress(ctx context.Context, work collection.Work, stage string, sample domain.CollectionProgress) error {
	r.samples++
	if r.samples > 1 {
		return context.DeadlineExceeded
	}
	return r.Store.RecordCollectionProgress(ctx, work, stage, sample)
}

func TestCollectionLocalAndProgressFailuresBothSurvive(t *testing.T) {
	f, job, _ := localFailureFixture(t, "collection")
	fault := &localProviderFault{stage: "output", cause: sqlite.ErrCorrupt}
	f.wrapProvider = func(base provider.Provider) provider.Provider { return localFailureProvider{base, fault} }
	repo := &localCollectionProgressRepository{Store: f.host.store}
	engine, err := collection.New(repo, f.services.resolver, f.host.results, f.clock, collection.DefaultConfig())
	requireNoError(t, err)
	worked, err := engine.RunOnce(t.Context())
	if !worked || !errors.Is(err, sqlite.ErrCorrupt) || !errors.Is(err, context.DeadlineExceeded) || worker.Classify(err) != worker.Stop {
		t.Fatal("progress checkpoint replaced the fatal local cause", worked, err)
	}
	assertLocalWorkPreserved(t, f, job, "")
}

type recoveringCollectionProgressRepository struct {
	localReadLoopRepository
	samples int
	failure error
}

func (r *recoveringCollectionProgressRepository) RecordCollectionProgress(ctx context.Context, work collection.Work, stage string, sample domain.CollectionProgress) error {
	r.samples++
	if r.samples == 2 {
		return r.failure
	}
	return r.Store.RecordCollectionProgress(ctx, work, stage, sample)
}

type failingCollectionTransfer struct {
	provider.Provider
	failed bool
}

func (p *failingCollectionTransfer) Fetch(ctx context.Context, remote provider.RemoteReference, artifact provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	if strings.HasPrefix(artifact.Path, "outputs/") && !p.failed {
		p.failed = true
		return provider.TransferResult{}, errors.New("provider transfer unavailable")
	}
	return p.Provider.Fetch(ctx, remote, artifact, dst, limit, progress)
}

func TestCollectionProviderFailureKeepsProgressRetryPolicy(t *testing.T) {
	for _, cause := range []error{sqlite.ErrBusy, context.DeadlineExceeded, sqlite.ErrCorrupt} {
		t.Run(cause.Error(), func(t *testing.T) {
			f, job, _ := localFailureFixture(t, "collection")
			p := &failingCollectionTransfer{}
			f.wrapProvider = func(base provider.Provider) provider.Provider { p.Provider = base; return p }
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			resumed := false
			repo := &recoveringCollectionProgressRepository{failure: cause,
				localReadLoopRepository: localReadLoopRepository{Store: f.host.store, clock: f.clock.(*fixedClock), success: func() { resumed = true; cancel() }}}
			cfg := collection.DefaultConfig()
			cfg.Workers, cfg.PollDelay = 1, time.Millisecond
			engine, err := collection.New(repo, f.services.resolver, f.host.results, f.clock, cfg)
			requireNoError(t, err)
			err = engine.Run(ctx)
			if cause == sqlite.ErrCorrupt {
				if !errors.Is(err, cause) || resumed || repo.samples != 2 {
					t.Fatal("fatal checkpoint did not stop original collection", err, resumed, repo.samples)
				}
				assertLocalWorkPreserved(t, f, job, "")
			} else {
				if !errors.Is(err, context.Canceled) || !resumed {
					t.Fatal("provider failure overrode transient checkpoint recovery", err, resumed)
				}
				result, err := f.host.store.ReadCollection(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
				if err != nil || len(result.Files) == 0 {
					t.Fatal("original accepted collection did not publish", err)
				}
			}
		})
	}
}

type localCanceledProvider struct{ provider.Provider }

func (p localCanceledProvider) Verify(ctx context.Context, _ provider.BindingSnapshot) error {
	<-ctx.Done()
	return provider.LocalStateError{Err: sqlite.ErrCorrupt}
}

func TestLocalReadAndHeartbeatFailuresBothSurvive(t *testing.T) {
	for _, kind := range []string{"dispatch", "collection", "cleanup"} {
		t.Run(kind, func(t *testing.T) {
			f, job, cleanupID := localFailureFixture(t, kind)
			f.wrapProvider = func(base provider.Provider) provider.Provider { return localCanceledProvider{base} }
			var step func(context.Context) (bool, error)
			switch kind {
			case "dispatch":
				engine, err := dispatch.New(heartbeatDispatchRepository{f.host.store, f.clock.Now, sqlite.ErrBusy}, f.services.resolver, f.host.inputs, f.clock, dispatch.DefaultConfig())
				requireNoError(t, err)
				step = func(ctx context.Context) (bool, error) { return engine.RunOnce(ctx, "local-heartbeat") }
			case "collection":
				engine, err := collection.New(heartbeatCollectionRepository{f.host.store, f.clock.Now, sqlite.ErrBusy}, f.services.resolver, f.host.results, f.clock, collection.DefaultConfig())
				requireNoError(t, err)
				step = engine.RunOnce
			default:
				step = f.cleanupEngine(t, heartbeatCleanupRepository{f.host.store, sqlite.ErrBusy}).RunOnce
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			_, err := step(ctx)
			if !errors.Is(err, sqlite.ErrCorrupt) || !errors.Is(err, sqlite.ErrBusy) || worker.Classify(err) != worker.Stop {
				t.Fatal("renewal canceled or replaced a fatal local read failure", err)
			}
			assertLocalWorkPreserved(t, f, job, cleanupID)
		})
	}
}

type localPreparationProgressRepository struct {
	*sqlite.Store
	failure error
}

func (r localPreparationProgressRepository) RecordPreparationProgress(context.Context, dispatch.Handle, domain.PreparationProgress) error {
	return r.failure
}

type localPreparationProgressProvider struct {
	provider.Provider
	sampled, release chan struct{}
	prepared         provider.Prepared
	failure          error
}

func (p *localPreparationProgressProvider) Prepare(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.Prepared, error) {
	prepared, err := p.Provider.Prepare(ctx, plan, id)
	if err != nil {
		return prepared, err
	}
	p.prepared = prepared
	provider.ReportPreparationProgress(ctx, 1, 1)
	close(p.sampled)
	<-p.release
	return prepared, p.failure
}

func TestPreparationProgressFailureJoinsBeforeStoppingAndPreservesOutcome(t *testing.T) {
	for _, cause := range []error{sqlite.ErrBusy, context.DeadlineExceeded, scheduler.ErrLeaseLost, dispatch.ErrInvalid, sqlite.ErrCorrupt, errors.Join(dispatch.ErrInvalid, sqlite.ErrCorrupt)} {
		t.Run(cause.Error(), func(t *testing.T) {
			f, job, _ := localFailureFixture(t, "dispatch")
			p := &localPreparationProgressProvider{sampled: make(chan struct{}), release: make(chan struct{})}
			f.wrapProvider = func(base provider.Provider) provider.Provider { p.Provider = base; return p }
			engine, err := dispatch.New(localPreparationProgressRepository{f.host.store, cause}, f.services.resolver, f.host.inputs, f.clock, dispatch.DefaultConfig())
			requireNoError(t, err)
			done := make(chan error, 1)
			go func() { _, err := engine.RunOnce(t.Context(), "local-progress"); done <- err }()
			select {
			case <-p.sampled:
			case <-time.After(3 * time.Second):
				t.Fatal("preparation did not report progress")
			}
			select {
			case err := <-done:
				t.Fatal("progress error returned before provider callback joined", err)
			default:
			}
			close(p.release)
			err = <-done
			if errors.Is(cause, sqlite.ErrCorrupt) {
				if !errors.Is(err, cause) || worker.Classify(err) != worker.Stop {
					t.Fatal("fatal progress error did not reach the worker", err)
				}
			} else if err != nil {
				t.Fatal("best-effort progress changed preparation outcome", err)
			}
			_, prepared, err := f.host.store.LoadProviderAttempt(t.Context(), p.prepared.Identity)
			if err != nil || prepared != p.prepared {
				t.Fatal("progress failure lost original preparation evidence", err)
			}
			assertLocalWorkPreserved(t, f, job, "")
			f.advance(t, 2*time.Minute)
			if _, err := f.services.dispatcher.RunOnce(t.Context(), "local-progress-recovery"); err != nil || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 1 {
				t.Fatal("progress recovery repeated preparation or failed to use its outcome", err, f.backend.Stats())
			}
		})
	}
}

func TestPreparationProgressAndProviderFailuresStopRuntimeAfterCallbackJoins(t *testing.T) {
	f, _, _ := localFailureFixture(t, "dispatch")
	problem := provider.Problem(domain.CodeStagingFailed, domain.FailureStageProviderPreparation, "preparation acknowledgement unavailable")
	p := &localPreparationProgressProvider{sampled: make(chan struct{}), release: make(chan struct{}), failure: &problem}
	release := sync.OnceFunc(func() { close(p.release) })
	defer release()
	f.wrapProvider = func(base provider.Provider) provider.Provider { p.Provider = base; return p }
	cfg := dispatch.DefaultConfig()
	cfg.Workers, cfg.PollDelay = 1, time.Millisecond
	engine, err := dispatch.New(localPreparationProgressRepository{f.host.store, sqlite.ErrCorrupt}, f.services.resolver, f.host.inputs, f.clock, cfg)
	requireNoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	listener := testListener(t)
	server := &http.Server{Handler: http.NotFoundHandler()}
	done, workerErr := make(chan error, 1), make(chan error, 1)
	go func() {
		done <- runRuntimeServices(ctx, server, listener, "managed-workers", Listening{}, func(Listening) error { return nil }, []func(context.Context) error{
			func(ctx context.Context) error {
				err := engine.Run(ctx)
				workerErr <- err
				return err
			},
		})
	}()
	select {
	case <-p.sampled:
	case err := <-done:
		t.Fatal("preparation callback never reported", err)
	case <-ctx.Done():
		t.Fatal("preparation callback never reported")
	}
	select {
	case err := <-done:
		t.Fatal("runtime returned before preparation callback joined", err)
	default:
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, ErrState) {
			t.Fatal("provider failure masked fatal progress state", err)
		}
	case <-ctx.Done():
		t.Fatal("runtime did not stop after the callback joined")
	}
	original := <-workerErr
	var providerFailure *domain.Problem
	if !errors.Is(original, sqlite.ErrCorrupt) || !errors.As(original, &providerFailure) || worker.Classify(original) != worker.Stop {
		t.Fatal("provider/progress joined causes were lost", original)
	}
	// The failed acknowledgement retains the original preparation intent. A new
	// owner reads that same remote preparation, without issuing another Prepare.
	f.wrapProvider = nil
	f.advance(t, 2*time.Minute)
	if _, err := f.services.dispatcher.RunOnce(t.Context(), "progress-provider-recovery"); err != nil || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("fatal progress recovery repeated preparation", err, f.backend.Stats())
	}
}

type localReadLoopRepository struct {
	*sqlite.Store
	clock   *fixedClock
	success func()
}

func (r localReadLoopRepository) ClaimCollection(ctx context.Context, ttl time.Duration, workers int) (*collection.Work, error) {
	r.clock.advance(2 * time.Minute)
	return r.Store.ClaimCollection(ctx, ttl, workers)
}
func (r localReadLoopRepository) CompleteCollection(ctx context.Context, work collection.Work, proof collection.Verified) error {
	err := r.Store.CompleteCollection(ctx, work, proof)
	if err == nil {
		r.success()
	}
	return err
}
