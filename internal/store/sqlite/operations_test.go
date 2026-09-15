package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func controlService(t testing.TB, f *dispatchFixture, w domain.WorkspaceID, scopes ...auth.Scope) (*operations.Service, auth.Principal, string) {
	t.Helper()
	access, err := auth.New(f.s, f.s, nil)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := access.Issue(context.Background(), w, scopes, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := access.Authenticate(context.Background(), secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	service, err := operations.New(access, f.s, f.blobs, f.clock, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return service, p, secret.Reveal()
}
func controlRequest(id domain.AttemptID) []byte {
	raw, _ := json.Marshal(operations.Request{AttemptID: id, Reason: "explicit operator request"})
	return raw
}
func cancelControl(t testing.TB, f *dispatchFixture, service *operations.Service, p auth.Principal, id scheduler.Identity) operations.Record {
	t.Helper()
	r, err := service.Submit(context.Background(), p, id.WorkspaceID, id.JobID, domain.OperationCancel, "cancel-original-key", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func submittedControl(t testing.TB, f *dispatchFixture, id scheduler.Identity) {
	t.Helper()
	for i := 0; i < 8; i++ {
		if f.journal(t, id).Phase == dispatch.Submitted {
			return
		}
		if err := f.step(t); err != nil {
			var p *domain.Problem
			if !errors.As(err, &p) {
				t.Fatal(err)
			}
		}
	}
	t.Fatal("fixture never reached submitted")
}
func controlCount(t testing.TB, s *Store, query string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestControlsPreventDispatchFenceOldWorkerAndReplayAfterRestart(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p, secret := controlService(t, f, "a", auth.Read, auth.Operate)
	work := f.claim(t)
	accepted := cancelControl(t, f, service, p, id)
	if accepted.Effect != operations.DispatchPrevented || accepted.TerminationConfirmed || accepted.Operation.Status != domain.OperationSucceeded {
		t.Fatal("prevention is not remote termination", accepted)
	}
	plan, prep := storePlan(t, work)
	if _, err := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: prep}, f.clock.Now()); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("stale worker escaped cancellation", err)
	}
	if f.state(t, id).Orchestration != domain.OrchestrationCancelled || f.journal(t, id).Phase != dispatch.Prevented {
		t.Fatal("dispatch was not durably prevented")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM provider_resources") != 0 {
		t.Fatal("cancel created remote resources")
	}
	// The running callback's lease is not falsely released by the HTTP request.
	if controlCount(t, f.s, "SELECT count(*) FROM scheduler_leases WHERE held=1") != 1 {
		t.Fatal("local ownership released prematurely")
	}
	before := controlCount(t, f.s, "SELECT count(*) FROM events")
	f.restart(t)
	access, _ := auth.New(f.s, f.s, nil)
	p, err := access.Authenticate(context.Background(), secret)
	if err != nil {
		t.Fatal(err)
	}
	service, err = operations.New(access, f.s, f.blobs, f.clock, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationCancel, "cancel-original-key", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	expected := accepted
	expected.Replay = true
	if !reflect.DeepEqual(replay, expected) || controlCount(t, f.s, "SELECT count(*) FROM events") != before {
		t.Fatal("restart/replay changed the receipt or emitted duplicate events")
	}
	if _, err = service.Submit(context.Background(), p, "a", id.JobID, domain.OperationCancel, "cancel-original-key", []byte(`{"attempt_id":"different"}`)); !errors.Is(err, operations.ErrConflict) {
		t.Fatal("conflicting key accepted", err)
	}
}

func TestControlsConcurrentRetryKeepsFrozenHistoryAndNewIdentity(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	cancelControl(t, f, service, p, id)
	old, err := f.s.ReadJob(context.Background(), "a", p.TokenID(), id.JobID)
	if err != nil {
		t.Fatal(err)
	}
	changed := f.profile
	changed.Binding.ConfigurationRevision = "two"
	changed.Binding.ProviderInstanceID = "another_provider"
	changed.AccountScope = "another_account"
	if err = f.s.PutProfile(context.Background(), changed, true); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan operations.Record, 20)
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "retry-concurrent-key", controlRequest(id.AttemptID))
			if err != nil {
				failures <- err
			} else {
				results <- r
			}
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	var first operations.Record
	for r := range results {
		if first.NewAttemptID == "" {
			first = r
		}
		if r.Operation.ID != first.Operation.ID || r.NewAttemptID != first.NewAttemptID || r.Operation.AttemptID != id.AttemptID {
			t.Fatal("duplicate retry identity")
		}
	}
	if first.Effect != operations.NewAttemptCreated || first.NewAttemptID == id.AttemptID || first.NewAttemptID == "" {
		t.Fatal("retry did not create a distinct attempt")
	}
	current, err := f.s.ReadJob(context.Background(), "a", p.TokenID(), id.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Attempt.Number != 2 || current.Attempt.ID != first.NewAttemptID || current.AttemptNonce == old.AttemptNonce || current.Profile != old.Profile || !reflect.DeepEqual(current.Objects, old.Objects) || string(current.Request.Canonical()) != string(old.Request.Canonical()) {
		t.Fatal("retry changed immutable inputs/binding or reused the nonce")
	}
	historical, err := f.s.LoadAttempt(context.Background(), "a", id.JobID, id.AttemptID)
	if err != nil || historical != old.Attempt {
		t.Fatal("retry rewrote historical attempt", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempts") != 2 || controlCount(t, f.s, "SELECT count(*) FROM scheduler_queue") != 2 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='retry_compute'") != 1 {
		t.Fatal("retry duplicated rows")
	}
	if _, err = service.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "retry-different-key", controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrChanged) {
		t.Fatal("new key targeted superseded attempt", err)
	}
	if _, err = service.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "retry-running-key", controlRequest(first.NewAttemptID)); !errors.Is(err, operations.ErrUnresolved) {
		t.Fatal("queued attempt retried", err)
	}
}

type cancelProbe struct {
	*probeProvider
	clock     *dispatchClock
	calls     *atomic.Int32
	supported bool
	outcome   provider.CancellationOutcome
	lost      bool
	execution domain.ExecutionState
}

func (p *cancelProbe) Describe() provider.Descriptor {
	d := p.probeProvider.Describe()
	caps := []domain.CapabilityStatus{}
	for _, c := range d.Capabilities {
		if c.Name != domain.CapabilityRemoteCancellation {
			caps = append(caps, c)
		}
	}
	support := domain.CapabilitySupportUnsupported
	if p.supported {
		support = domain.CapabilitySupportSupported
	}
	d.Capabilities = append(caps, domain.CapabilityStatus{Name: domain.CapabilityRemoteCancellation, Support: support, Evidence: domain.EvidenceImplementedOffline, Reason: "nonexecuting test fixture"})
	return d
}
func (p *cancelProbe) Cancel(ctx context.Context, r provider.RemoteReference, id domain.OperationID) (provider.CancellationOutcome, error) {
	var n int
	if err := p.store.db.QueryRowContext(ctx, `SELECT count(*) FROM operations WHERE operation_id=? AND attempt_id=? AND cancel_started=1 AND status='running'`, string(id), string(r.Identity.AttemptID)).Scan(&n); err != nil || n != 1 {
		return provider.CancellationOutcome{}, fmt.Errorf("missing cancel intent: %w", err)
	}
	p.calls.Add(1)
	if p.lost {
		return provider.CancellationOutcome{}, errors.New("private-provider-response-canary")
	}
	return p.outcome, nil
}
func (p *cancelProbe) Observe(_ context.Context, r provider.RemoteReference) (provider.Observation, error) {
	activity, release := domain.RemoteActivityActive, domain.ReleaseEvidenceUnknown
	if p.execution.Terminal() {
		activity, release = domain.RemoteActivityInactive, domain.ReleaseEvidenceNotObservable
	}
	return provider.Observation{Remote: r, Execution: p.execution, RemoteActivity: activity, ReleaseEvidence: release, ObservedAt: p.clock.Now()}, nil
}
func installCancelProbe(t testing.TB, f *dispatchFixture, supported bool, outcome provider.CancellationOutcome) *cancelProbe {
	t.Helper()
	p := &cancelProbe{probeProvider: f.adapter, clock: f.clock, calls: &atomic.Int32{}, supported: supported, outcome: outcome, execution: domain.ExecutionSucceeded}
	registry := provider.NewSnapshotRegistry()
	binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
	if err := registry.Register(binding, p); err != nil {
		t.Fatal(err)
	}
	var err error
	f.engine, err = dispatch.New(f.s, registry, f.blobs, nil, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestControlsUnsupportedAcceptedAndConfirmedCancellation(t *testing.T) {
	for _, mode := range []string{"unsupported", "accepted", "confirmed", "lost_response"} {
		t.Run(mode, func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			submittedControl(t, f, id)
			service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
			outcome := provider.CancellationOutcome{Status: domain.CancellationAccepted}
			if mode == "confirmed" {
				outcome = provider.CancellationOutcome{Status: domain.CancellationConfirmed, TerminationConfirmed: true}
			}
			probe := installCancelProbe(t, f, mode != "unsupported", outcome)
			probe.lost = mode == "lost_response"
			accepted := cancelControl(t, f, service, p, id)
			if accepted.Operation.Status != domain.OperationAccepted || accepted.TerminationConfirmed {
				t.Fatal("HTTP response claimed termination")
			}
			if err := f.step(t); err != nil {
				t.Fatal(err)
			}
			current, err := service.Get(context.Background(), p, "a", accepted.Operation.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "unsupported", "lost_response":
				if current.Operation.Status != domain.OperationManualRequired || current.TerminationConfirmed || f.state(t, id).RemoteActivity == domain.RemoteActivityInactive {
					t.Fatal("manual request invented termination")
				}
				raw, _ := json.Marshal(current)
				if strings.Contains(string(raw), "canary") {
					t.Fatal("upstream diagnostic leaked")
				}
			case "accepted":
				if current.Operation.Status != domain.OperationRunning || current.TerminationConfirmed || f.state(t, id).Cancellation != domain.CancellationAccepted {
					t.Fatal("acceptance reported completion")
				}
			case "confirmed":
				if current.Operation.Status != domain.OperationSucceeded || !current.TerminationConfirmed || f.state(t, id).Execution != domain.ExecutionCancelled {
					t.Fatal("terminal cancellation evidence was lost")
				}
			}
			if mode != "confirmed" {
				if err = f.step(t); err != nil {
					t.Fatal(err)
				}
				if f.state(t, id).Execution != domain.ExecutionSucceeded || f.state(t, id).Cancellation != domain.CancellationTooLate {
					t.Fatal("completion did not win")
				}
				latest, err := service.Get(context.Background(), p, "a", accepted.Operation.ID)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "accepted" && (latest.Effect != operations.TooLate || latest.TerminationConfirmed) {
					t.Fatal("completion was reported as cancellation")
				}
				if mode != "accepted" && !reflect.DeepEqual(latest, current) {
					t.Fatal("terminal manual operation history was rewritten")
				}
			}
			want := int32(1)
			if mode == "unsupported" {
				want = 0
			}
			if probe.calls.Load() != want {
				t.Fatal("cancel call count", probe.calls.Load())
			}
			replay, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationCancel, "cancel-original-key", controlRequest(id.AttemptID))
			if err != nil || replay.Operation.Status != domain.OperationAccepted || !replay.Replay {
				t.Fatal("original receipt was not stable", err)
			}
		})
	}
}

func TestControlsCancelIntentRestartDoesNotRepeatMutation(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	accepted := cancelControl(t, f, service, p, id)
	f.clock.advance(time.Minute)
	claim, err := f.s.ClaimRecovery(context.Background(), "intent-crash", f.clock.Now())
	if err != nil || claim == nil {
		t.Fatal("claim", err)
	}
	work, err := f.s.LoadDispatch(context.Background(), *claim, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	committed, err := f.s.CommitCancellation(context.Background(), work.Handle, accepted.Operation.ID, dispatch.CancellationAction{Begin: true}, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Lose the successful acknowledgement and restart before sending Cancel.
	if _, err = f.s.CommitCancellation(context.Background(), work.Handle, accepted.Operation.ID, dispatch.CancellationAction{Begin: true}, f.clock.Now()); err == nil {
		t.Fatal("lost acknowledgement rearmed cancel")
	}
	if committed.Cancellation == nil || !committed.Cancellation.Started {
		t.Fatal("gate not persisted")
	}
	f.restart(t)
	probe := installCancelProbe(t, f, true, provider.CancellationOutcome{Status: domain.CancellationAccepted})
	if err = f.step(t); err != nil {
		t.Fatal(err)
	}
	if probe.calls.Load() != 0 || f.state(t, id).Cancellation != domain.CancellationTooLate {
		t.Fatal("restart repeated cancel instead of observing")
	}
}

func TestControlsCancellationDuringStagingAndTransactionalRollback(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	f.adapter.prepareEntered = make(chan struct{})
	f.adapter.prepareRelease = make(chan struct{})
	returned := make(chan error, 1)
	go func() { _, err := f.engine.RunOnce(context.Background(), "staging-race"); returned <- err }()
	select {
	case <-f.adapter.prepareEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("staging was not entered")
	}
	cancelControl(t, f, service, p, id)
	close(f.adapter.prepareRelease)
	if err := <-returned; !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("stale preparation committed", err)
	}
	if f.journal(t, id).Phase != dispatch.Prevented || controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 0 {
		t.Fatal("cancel permitted submit")
	}
	// Roll back every part of a retry if its sequenced event cannot commit.
	before := controlCount(t, f.s, "SELECT count(*) FROM operations")
	if _, err := f.s.db.Exec(`CREATE TRIGGER fail_control_event BEFORE INSERT ON events WHEN NEW.type='attempt.retried' BEGIN SELECT RAISE(ABORT,'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "rollback-retry-key", controlRequest(id.AttemptID)); err == nil {
		t.Fatal("event failure acknowledged")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations") != before || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 || controlCount(t, f.s, "SELECT count(*) FROM operation_idempotency WHERE kind='retry_compute'") != 0 {
		t.Fatal("failed transaction leaked rows")
	}
	if _, err := f.s.db.Exec("DROP TRIGGER fail_control_event"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "rollback-retry-key", controlRequest(id.AttemptID)); err != nil {
		t.Fatal("rollback consumed idempotency key", err)
	}
}

func TestControlsCollectIsTransferOnlyAndReconcileNeverSubmits(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	if _, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "unresolved-retry", controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrUnresolved) {
		t.Fatal("unresolved compute retried", err)
	}
	reconciliation, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationReconcile, "reconcile-original", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	probe := installCancelProbe(t, f, false, provider.CancellationOutcome{})
	if err = f.step(t); err != nil {
		t.Fatal(err)
	}
	r, err := service.Get(context.Background(), p, "a", reconciliation.Operation.ID)
	if err != nil || r.Effect != operations.ObservationRefreshed {
		t.Fatal("reconciliation not acknowledged", err)
	}
	if _, err = service.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "successful-retry", controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrCollect) {
		t.Fatal("successful compute rerun for missing artifacts", err)
	}
	collect, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationCollect, "collect-original-key", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	again, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationCollect, "collect-another-key", controlRequest(id.AttemptID))
	if err != nil || again.Operation.ID != collect.Operation.ID {
		t.Fatal("parallel collection ticket duplicated", err)
	}
	pending, err := f.s.PendingCollections(context.Background(), 10)
	if err != nil || len(pending) != 1 || pending[0].Operation.ID != collect.Operation.ID {
		t.Fatal("durable collection queue missing", err)
	}
	if _, err = f.s.CompleteCollectionOperation(context.Background(), "a", collect.Operation.ID, collect.Operation.Revision, f.clock.Now()); !errors.Is(err, operations.ErrState) {
		t.Fatal("unverified results were acknowledged", err)
	}
	if f.state(t, id).Result != domain.ResultNotAvailable || probe.calls.Load() != 0 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 || controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 1 {
		t.Fatal("control replay started compute or invented artifacts")
	}
}

type blockedInputs struct {
	operations.BlobReader
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockedInputs) Open(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	b.once.Do(func() {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	})
	return b.BlobReader.Open(ctx, w, id)
}
func TestControlsAuthorizationRecheckedAfterRetryIO(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p, secret := controlService(t, f, "a", auth.Read, auth.Operate)
	cancelControl(t, f, service, p, id)
	access, _ := auth.New(f.s, f.s, nil)
	blocked := &blockedInputs{BlobReader: f.blobs, entered: make(chan struct{}), release: make(chan struct{})}
	service, err := operations.New(access, f.s, blocked, f.clock, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "revoked-during-read", controlRequest(id.AttemptID))
		result <- err
	}()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("retry did not verify blobs")
	}
	// Test-only concurrent revocation, using the same persisted authority consulted at commit.
	if _, err = f.s.db.Exec("UPDATE api_token_hashes SET revoked=1 WHERE token_id=?", p.TokenID()); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	if err = <-result; err == nil {
		t.Fatal("revoked authority committed retry")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 {
		t.Fatal("revocation lost the commit race")
	}
	if _, err = access.Authenticate(context.Background(), secret); err == nil {
		t.Fatal("token was not revoked")
	}
	readOnly, reader, _ := controlService(t, f, "a", auth.Read)
	if _, err = readOnly.Submit(context.Background(), reader, "a", id.JobID, domain.OperationCancel, "read-only-operation", controlRequest(id.AttemptID)); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope operated", err)
	}
}
