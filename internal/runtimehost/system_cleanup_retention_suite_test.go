package runtimehost

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/retention"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

func (f *managedTestFixture) cleanupPreview(t *testing.T) *cleanup.Service {
	t.Helper()
	s, err := cleanup.NewService(f.host.access, f.host.store, f.fixtureClock())
	requireNoError(t, err)
	return s.WithPreviewResolver(f.services.resolver)
}

func TestCleanupPreviewEnumeratesOriginalTargetsWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restart bool
	}{{"same-host", false}, {"after-restart", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			f := newSystemFixture(t, fake.DefaultScenario())
			job := f.collectedForCleanup(t, "cleanup-preview")
			probe := f.cleanupProbe(t, "")
			plan, err := f.host.store.PlanCleanup(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID, false)
			requireNoError(t, err)
			before := f.backend.Stats()
			preview, err := f.cleanupPreview(t).Preview(ctx, f.actor, "app", job.JobID, job.AttemptID, false)
			if err != nil || !preview.Eligible || len(preview.Resources) != 2 {
				t.Fatal(preview, err)
			}
			for i, resource := range preview.Resources {
				request := plan.Resources[i].Request
				if resource.Purpose != []string{"execution", "staging"}[i] || resource.ResourceID != request.LedgerID || !resource.Eligible || resource.Observation != "present" || resource.Target == nil || resource.Target.Bytes != nil || resource.Target.ID != request.Remote.Resource || !resource.Fingerprint.Valid() {
					t.Fatal("preview changed target or unknown size", resource)
				}
			}
			if len(probe.requests) != 0 || f.backend.Stats().SubmitCalls != before.SubmitCalls || f.backend.Stats().Executions != before.Executions {
				t.Fatal("preview mutated provider")
			}
			raw := string(systemJSON(t, preview))
			for _, private := range []string{"CredentialRef", "AccountScope", "Nonce", "CreationOperationID", "Remote", "Request", "provider_plan", "name_based_delete", "warning"} {
				if strings.Contains(raw, private) {
					t.Fatal("private or retired evidence exposed", private)
				}
			}
			receipt := f.admitCleanup(t, job, "after-preview", false)
			if tc.restart {
				f.reopen(t)
			}
			probe = f.cleanupProbe(t, receipt.Operation.ID)
			calls := 0
			probe.resourceHook = func(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
				if calls >= len(preview.Resources) || request.RecoverOnly || request.LedgerID != preview.Resources[calls].ResourceID || request.Purpose != preview.Resources[calls].Purpose {
					t.Fatal("target/order changed", request)
				}
				calls++
				return probe.Provider.CleanupResource(ctx, request)
			}
			runSystemCleanup(t, f.cleanupEngine(t, nil))
			current := f.cleanupRecord(t, receipt.Operation.ID)
			if calls != 2 || current.Effect != operations.RemoteDeleted {
				t.Fatal("preview changed cleanup execution", current, calls)
			}
			for i, result := range current.Resources {
				if result.Fingerprint != preview.Resources[i].Fingerprint || result.ResourceID != preview.Resources[i].ResourceID {
					t.Fatal("apply retargeted preview", result)
				}
			}
			f.assertSingleCompute(t, job)
		})
	}
}

func TestCleanupLostAcknowledgementRecoversOriginalTargets(t *testing.T) {
	for _, recovery := range []string{"absence", "binding refusal"} {
		t.Run(recovery, func(t *testing.T) {
			f := newSystemFixture(t, fake.DefaultScenario())
			job := f.collectedForCleanup(t, "cleanup-lost-ack")
			r := f.admitCleanup(t, job, "lost-ack", false)
			p := f.cleanupProbe(t, r.Operation.ID)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p.resourceHook = func(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
				out, err := p.Provider.CleanupResource(ctx, q)
				if err != nil || !out.Deleted {
					t.Fatal("fault must follow real deletion", out, err)
				}
				cancel()
				return provider.CleanupOutcome{}, ctx.Err()
			}
			if worked, err := f.cleanupEngine(t, nil).RunOnce(ctx); !worked || !errors.Is(err, context.Canceled) {
				t.Fatal("outcome commit not interrupted", worked, err)
			}
			before := f.cleanupRecord(t, r.Operation.ID)
			if before.Operation.Status != domain.OperationRunning || len(before.Resources) != 2 || before.Resources[0].State != operations.ResourceRunning || before.Resources[1].State != operations.ResourcePending {
				t.Fatal("durable intent missing", before)
			}
			f.reopen(t)
			f.advance(t, 5*time.Minute+31*time.Second)
			p = f.cleanupProbe(t, r.Operation.ID)
			if recovery == "binding refusal" {
				p.verifyErr = errors.New("unavailable binding")
			}
			runSystemCleanup(t, f.cleanupEngine(t, nil))
			after := f.cleanupRecord(t, r.Operation.ID)
			for i, result := range after.Resources {
				if result.ResourceID != before.Resources[i].ResourceID || result.Fingerprint != before.Resources[i].Fingerprint {
					t.Fatal("recovery retargeted", before, after)
				}
			}
			if recovery == "absence" {
				if after.Operation.Status != domain.OperationSucceeded || after.Effect != operations.AlreadyAbsent || after.Resources[0].State != operations.ResourceAbsent || after.Resources[1].State != operations.ResourceAbsent || len(p.requests) != 2 {
					t.Fatal(after, p.requests)
				}
			} else if after.Operation.Status != domain.OperationFailed || after.Resources[0].State != operations.ResourceUnknown || after.Resources[1].State != operations.ResourceRetained || len(p.requests) != 0 {
				t.Fatal("refusal concealed lost ACK", after, p.requests)
			}
			f.assertSingleCompute(t, job)
		})
	}
}

func TestCleanupProvenRejectedSubmissionKeepsStagingCleanable(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Reject
	f := newSystemFixture(t, scenario)
	connection := f.connection(t, "rejected-cleanup-connection", "SYNTHETIC_FIRST")
	job := f.submitJob(t, "rejected-cleanup", f.jobSpec(t, connection))
	f.authorizeJob(t, job)
	for i := 0; i < 20; i++ {
		r, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
		requireNoError(t, err)
		if r.Attempt.State.Orchestration.Terminal() {
			break
		}
		if i == 19 {
			t.Fatal("submission did not reject")
		}
		f.dispatchOnce(t)
	}
	r := f.admitCleanup(t, job, "rejected-staging", false)
	p := f.cleanupProbe(t, r.Operation.ID)
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	current := f.cleanupRecord(t, r.Operation.ID)
	if current.Effect != operations.RemoteDeleted || len(current.Resources) != 1 || current.Resources[0].Purpose != "staging" || len(p.requests) != 1 {
		t.Fatal(current, p.requests)
	}
	if stats := f.backend.Stats(); stats.SubmitCalls != 1 || stats.Executions != 0 {
		t.Fatal("cleanup reran rejected compute", stats)
	}
}

func TestCleanupProviderTimeoutPersistsUnknownWithoutStoppingWorker(t *testing.T) {
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-timeout")
	r := f.admitCleanup(t, job, "provider-timeout", false)
	p := f.cleanupProbe(t, r.Operation.ID)
	p.resourceHook = func(ctx context.Context, _ provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		<-ctx.Done()
		return provider.CleanupOutcome{}, ctx.Err()
	}
	cfg := cleanup.DefaultConfig()
	cfg.Timeout = time.Second
	e, err := cleanup.New(f.host.store, f.services.resolver, cfg)
	requireNoError(t, err)
	runSystemCleanup(t, e)
	current := f.cleanupRecord(t, r.Operation.ID)
	if current.Operation.Status != domain.OperationFailed || current.Resources[0].State != operations.ResourceUnknown || current.Resources[1].State != operations.ResourceRetained || len(p.requests) != 1 {
		t.Fatal(current, p.requests)
	}
	if worked, err := e.RunOnce(t.Context()); worked || err != nil {
		t.Fatal("timeout automatically repeated", worked, err)
	}
	f.assertSingleCompute(t, job)
}

func TestCleanupRecoveryOnlyReadsOriginalIntent(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "recovery-only")
	r := f.admitCleanup(t, job, "recovery-only-original", false)
	first, err := f.host.store.ClaimCleanup(ctx, time.Second)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	if err = f.host.store.BeginCleanupResource(ctx, *first, first.Resources[0]); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	f.advance(t, 2*time.Second)
	p := f.cleanupProbe(t, r.Operation.ID)
	calls := 0
	p.resourceHook = func(_ context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		calls++
		if !q.RecoverOnly || q.LedgerID != first.Resources[0].Result.ResourceID {
			t.Fatal("recovery could reapply/retarget", q)
		}
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeProviderStateUnknown, domain.FailureStageObservation, "earlier deletion remains unknown")
	}
	e := f.cleanupEngine(t, nil)
	runSystemCleanup(t, e)
	current := f.cleanupRecord(t, r.Operation.ID)
	if calls != 1 || current.Resources[0].State != operations.ResourceUnknown || current.Resources[1].State != operations.ResourceRetained {
		t.Fatal(current, calls)
	}
	if worked, err := e.RunOnce(ctx); worked || err != nil {
		t.Fatal("unknown deletion repeated", worked, err)
	}
	f.assertSingleCompute(t, job)
}

type systemCleanupProvider struct {
	provider.Provider
	requests     []provider.CleanupResourceRequest
	verifyErr    error
	result       *provider.CleanupOutcome
	err          error
	resourceHook func(context.Context, provider.CleanupResourceRequest) (provider.CleanupOutcome, error)
	beforeApply  func(context.Context, provider.CleanupResourceRequest) error
}

func (p *systemCleanupProvider) Verify(ctx context.Context, b provider.BindingSnapshot) error {
	if p.verifyErr != nil {
		return p.verifyErr
	}
	return p.Provider.Verify(ctx, b)
}
func (p *systemCleanupProvider) CleanupResource(ctx context.Context, q provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	if q.Mode == provider.CleanupApply {
		if p.beforeApply != nil {
			if err := p.beforeApply(ctx, q); err != nil {
				return provider.CleanupOutcome{}, err
			}
		}
		p.requests = append(p.requests, q)
	}
	if p.resourceHook != nil {
		return p.resourceHook(ctx, q)
	}
	if q.Mode == provider.CleanupApply {
		if p.result != nil {
			return *p.result, p.err
		}
		if p.err != nil {
			return provider.CleanupOutcome{}, p.err
		}
	}
	return p.Provider.CleanupResource(ctx, q)
}

func (f *managedTestFixture) collectedForCleanup(t *testing.T, key string) admission.Receipt {
	t.Helper()
	job := f.collectibleJob(t, key, "automatic")
	f.collectOnce(t)
	return job
}
func (f *managedTestFixture) uncollectedForCleanup(t *testing.T, key string) admission.Receipt {
	t.Helper()
	job := f.collectibleJob(t, key, "automatic")
	source := f.collectionSource(t)
	source.mode = "corrupt"
	var failure collection.Failure
	if worked, err := f.collectionEngine(t, nil).RunOnce(t.Context()); !worked || !errors.As(err, &failure) {
		t.Fatal("missing verification failure", worked, err)
	}
	return job
}
func (f *managedTestFixture) preparedForCleanup(t *testing.T, key string) admission.Receipt {
	t.Helper()
	connection := f.connection(t, key+"-connection", "SYNTHETIC_FIRST")
	job := f.submitJob(t, key, f.jobSpec(t, connection))
	f.authorizeJob(t, job)
	for range 12 {
		if f.backend.Stats().PrepareCalls != 0 {
			break
		}
		f.dispatchOnce(t)
	}
	if f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("fixture did not stop after preparation", f.backend.Stats())
	}
	r, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCancel, key+"-prevent", systemControlBody(t, job.AttemptID))
	if err != nil || r.Effect != operations.DispatchPrevented {
		t.Fatal("submission not prevented", r, err)
	}
	return job
}
func systemCleanupBody(t *testing.T, attempt domain.AttemptID, discard bool, reason string) []byte {
	t.Helper()
	return systemJSON(t, operations.Request{AttemptID: attempt, DiscardUncollected: discard, Reason: reason})
}
func (f *managedTestFixture) admitCleanup(t *testing.T, job admission.Receipt, key string, discard bool) operations.Record {
	t.Helper()
	r, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCleanup, key, systemCleanupBody(t, job.AttemptID, discard, ""))
	requireNoError(t, err)
	return r
}
func (f *managedTestFixture) cleanupProbe(t *testing.T, op domain.OperationID) *systemCleanupProvider {
	t.Helper()
	probe := &systemCleanupProvider{}
	f.wrapProvider = func(base provider.Provider) provider.Provider { probe.Provider = base; return probe }
	if op != "" {
		probe.beforeApply = func(ctx context.Context, q provider.CleanupResourceRequest) error {
			// This public read must finish before the effect. It proves the running
			// resource intent committed and the callback is outside the transaction.
			call, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			r, err := f.host.store.ReadOperation(call, "app", f.actor.TokenID(), op)
			if err != nil {
				return err
			}
			for _, resource := range r.Resources {
				if resource.ResourceID == q.LedgerID && resource.State == operations.ResourceRunning {
					return nil
				}
			}
			return errors.New("provider effect without committed resource intent")
		}
	}
	return probe
}
func runSystemCleanup(t *testing.T, e *cleanup.Engine) {
	t.Helper()
	if worked, err := e.RunOnce(t.Context()); !worked || err != nil {
		t.Fatal("cleanup", worked, err)
	}
}
func (f *managedTestFixture) cleanupRecord(t *testing.T, id domain.OperationID) operations.Record {
	t.Helper()
	r, err := f.controls(t).Get(t.Context(), f.actor, "app", id)
	requireNoError(t, err)
	return r
}

func TestCleanupWorkerDeletesOnceAndKeepsReceipts(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-once")
	plan, err := f.host.store.PlanCleanup(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID, false)
	requireNoError(t, err)
	r := f.admitCleanup(t, job, "cleanup-original", false)
	probe := f.cleanupProbe(t, r.Operation.ID)
	e := f.cleanupEngine(t, nil)
	runSystemCleanup(t, e)
	current := f.cleanupRecord(t, r.Operation.ID)
	if current.Operation.Status != domain.OperationSucceeded || current.Effect != operations.RemoteDeleted || current.Operation.Revision != 3 || current.Operation.Failure != nil || len(probe.requests) != 2 || f.backend.Stats().CleanupCalls != 2 {
		t.Fatal(current, probe.requests)
	}
	for i, q := range probe.requests {
		if q != plan.Resources[i].Request || !q.ResultsCollected {
			t.Fatal("cleanup changed original target", q, plan.Resources[i])
		}
	}
	if worked, err := e.RunOnce(ctx); worked || err != nil {
		t.Fatal("terminal cleanup repeated", worked, err)
	}
	replay, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCleanup, "cleanup-original", systemCleanupBody(t, job.AttemptID, false, ""))
	if err != nil || !replay.Replay || replay.Operation != r.Operation || replay.Effect != operations.Pending {
		t.Fatal("original receipt changed", replay, err)
	}
	coalesced := f.admitCleanup(t, job, "cleanup-other-key", false)
	if !coalesced.Replay || coalesced.Operation.ID != r.Operation.ID || coalesced.Effect != operations.RemoteDeleted {
		t.Fatal(coalesced)
	}
	state, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	if err != nil || state.Attempt.State.Result != domain.ResultAvailable || state.Attempt.State.Orchestration != domain.OrchestrationSucceeded {
		t.Fatal("remote deletion changed local result", err)
	}
	f.assertSingleCompute(t, job)
}

func TestCleanupWorkerReportsAlreadyAbsent(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "cleanup-absence")
	plan, err := f.host.store.PlanCleanup(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID, false)
	requireNoError(t, err)
	p, err := f.services.resolver.Resolve(plan.Binding)
	requireNoError(t, err)
	for _, target := range plan.Resources {
		q := target.Request
		q.Mode = provider.CleanupApply
		out, err := p.CleanupResource(ctx, q)
		if err != nil || !out.Deleted && !out.AlreadyAbsent {
			t.Fatal(out, err)
		}
	}
	r := f.admitCleanup(t, job, "cleanup-absent", false)
	f.cleanupProbe(t, r.Operation.ID)
	runSystemCleanup(t, f.cleanupEngine(t, nil))
	current := f.cleanupRecord(t, r.Operation.ID)
	if current.Operation.Status != domain.OperationSucceeded || current.Effect != operations.AlreadyAbsent {
		t.Fatal(current)
	}
	f.assertSingleCompute(t, job)
}

func TestRetentionResultExpirySweepAndRootBinding(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "retention-results", "automatic")
	f.collectionSource(t)
	f.collectOnce(t)
	result, err := f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	if err != nil || len(result.Files) == 0 {
		t.Fatal("missing publication", err)
	}
	before, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	requireNoError(t, err)
	f.advance(t, 8*24*time.Hour)
	if _, err = f.host.store.ExpireRetention(ctx, retention.DefaultPolicy(), 100); err != nil {
		t.Fatal(err)
	}
	after, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	want := before.Attempt.State
	want.Result = domain.ResultExpired
	if err != nil || after.Attempt.State != want {
		t.Fatal("expiry changed remote or business outcome", after.Attempt.State, err)
	}
	if _, err = f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, retention.ErrExpired) {
		t.Fatal("expiry hidden as missing", err)
	}
	sweep, err := retention.NewSweeper(f.host.store, f.host.inputs, f.host.results)
	requireNoError(t, err)
	var cursor int64
	for i := 0; i < 20; i++ {
		_, cursor, err = sweep.SweepOnce(ctx, retention.DefaultPolicy(), cursor, 2)
		requireNoError(t, err)
		if cursor == 0 {
			break
		}
		if i == 19 {
			t.Fatal("bounded sweep did not drain")
		}
	}
	for _, file := range result.Files {
		if stream, err := f.host.results.Open(ctx, "app", file.Object.ID); err == nil {
			stream.Close()
			t.Fatal("expired bytes remain")
		}
	}
	page, err := f.host.store.PendingRetention(ctx, 0, 100)
	if err != nil || len(page.Items) != 0 {
		t.Fatal("pending tombstones remain", page, err)
	}
	swapped, err := retention.NewSweeper(f.host.store, f.host.results, f.host.inputs)
	requireNoError(t, err)
	if _, _, err = swapped.SweepOnce(ctx, retention.DefaultPolicy(), 0, 10); !errors.Is(err, retention.ErrChanged) {
		t.Fatal("swapped roots inherited authority", err)
	}
	replacement, err := blobfs.New(filepath.Join(t.TempDir(), "replacement"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	requireNoError(t, err)
	defer replacement.Close()
	changed, err := retention.NewSweeper(f.host.store, f.host.inputs, replacement)
	requireNoError(t, err)
	if _, _, err = changed.SweepOnce(ctx, retention.DefaultPolicy(), 0, 10); !errors.Is(err, retention.ErrChanged) {
		t.Fatal("replacement inherited tombstones", err)
	}
	f.reopen(t)
	if _, err = f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, retention.ErrExpired) {
		t.Fatal("restart forgot expiry", err)
	}
	f.assertSingleCompute(t, job)
}

type systemRetentionRepository struct {
	*sqlite.Store
	stage string
	fired bool
}

func (r *systemRetentionRepository) ExpireRetention(ctx context.Context, p retention.Policy, limit int) (retention.SweepReport, error) {
	report, err := r.Store.ExpireRetention(ctx, p, limit)
	if err == nil && r.stage == "expiry-ack" && !r.fired {
		r.fired = true
		return retention.SweepReport{}, sqlite.ErrUnavailable
	}
	return report, err
}
func (r *systemRetentionRepository) CompleteRetention(ctx context.Context, d retention.Deletion) error {
	err := r.Store.CompleteRetention(ctx, d)
	if err == nil && r.stage == "completion-ack" && !r.fired {
		r.fired = true
		return sqlite.ErrUnavailable
	}
	return err
}

type systemRetentionBlobs struct {
	*blobfs.Store
	calls int
	lose  bool
	fired bool
}

func (b *systemRetentionBlobs) Delete(ctx context.Context, m domain.ObjectMetadata) (bool, error) {
	b.calls++
	absent, err := b.Store.Delete(ctx, m)
	if err == nil && b.lose && !b.fired {
		b.fired = true
		return false, sqlite.ErrUnavailable
	}
	return absent, err
}
