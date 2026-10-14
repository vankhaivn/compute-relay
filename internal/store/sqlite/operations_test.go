package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	access, err := auth.New(f.s, f.s)
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
	service, err := operations.New(access, f.s, f.blobs, admission.DefaultLimits())
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
	d.Capabilities.RemoteCancellation = p.supported
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
	return provider.Observation{Remote: r, Execution: p.execution, RemoteActivity: activity, ReleaseEvidence: release, ObservedAt: p.clock.Now(), RawState: string(p.execution)}, nil
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
	f.engine, err = dispatch.New(f.s, registry, f.blobs, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestControlsCancellationFencesWorkerWithoutReleasingItsLease(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	work := f.claim(t)
	cancelControl(t, f, service, p, id)
	plan, prep := storePlan(t, work)
	if _, err := f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: prep}); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("stale worker escaped cancellation", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM provider_resources") != 0 || controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 0 {
		t.Fatal("cancellation created remote intent")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM work_leases WHERE kind='dispatch' AND held=1") != 1 {
		t.Fatal("callback ownership prematurely released")
	}
}
func TestControlsRetryPreservesHistoricalRowsAndOneReceipt(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	cancelControl(t, f, service, p, id)
	old, _, err := loadAttempt(t.Context(), f.s.db, "a", id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err = service.Submit(t.Context(), p, "a", id.JobID, domain.OperationRetryCompute, "retry-storage", controlRequest(id.AttemptID)); err != nil {
			t.Fatal(err)
		}
	}
	historical, _, err := loadAttempt(t.Context(), f.s.db, "a", id.JobID, id.AttemptID)
	if err != nil || historical != old {
		t.Fatal("retry rewrote historical attempt", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempts") != 2 || controlCount(t, f.s, "SELECT count(*) FROM scheduler_queue") != 2 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='retry_compute'") != 1 || controlCount(t, f.s, "SELECT count(*) FROM receipts WHERE namespace='operation' AND scope='retry_compute'") != 1 {
		t.Fatal("retry duplicated durable rows")
	}
}
func TestControlsRetryEventFailureRollsBackAndLeavesKeyReusable(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	cancelControl(t, f, service, p, id)
	before := controlCount(t, f.s, "SELECT count(*) FROM operations")
	if _, err := f.s.db.Exec(`CREATE TRIGGER fail_control_event BEFORE INSERT ON events WHEN NEW.type='attempt.retried' BEGIN SELECT RAISE(ABORT,'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(t.Context(), p, "a", id.JobID, domain.OperationRetryCompute, "rollback-retry-key", controlRequest(id.AttemptID)); err == nil {
		t.Fatal("event failure acknowledged")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations") != before || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 || controlCount(t, f.s, "SELECT count(*) FROM receipts WHERE namespace='operation' AND scope='retry_compute'") != 0 {
		t.Fatal("failed transaction leaked rows")
	}
	if _, err := f.s.db.Exec("DROP TRIGGER fail_control_event"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(t.Context(), p, "a", id.JobID, domain.OperationRetryCompute, "rollback-retry-key", controlRequest(id.AttemptID)); err != nil {
		t.Fatal("rollback consumed idempotency key", err)
	}
}
func TestControlsRawCommandRechecksAuthorization(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	_, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	req := operations.Request{AttemptID: id.AttemptID}
	hash, err := req.Digest(id.JobID, domain.OperationCancel)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := admission.KeyDigest("revoked-raw-command")
	if err = f.s.RevokeToken(t.Context(), p.TokenID()); err != nil {
		t.Fatal(err)
	}
	_, err = f.s.AdmitOperation(t.Context(), operations.Command{WorkspaceID: "a", TokenID: p.TokenID(), JobID: id.JobID, Kind: domain.OperationCancel, Request: req, KeyDigest: key, RequestHash: hash, Limits: admission.DefaultLimits()})
	if !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("raw command skipped authority", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations") != 0 || controlCount(t, f.s, "SELECT count(*) FROM receipts WHERE namespace='operation'") != 0 {
		t.Fatal("revoked command committed rows")
	}
}
func TestControlsCancellationIntentIsDurableBeforeCallback(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	probe := installCancelProbe(t, f, true, provider.CancellationOutcome{Status: domain.CancellationAccepted})
	cancelControl(t, f, service, p, id)
	// cancelProbe checks the private cancel_started row before any callback effect.
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if probe.calls.Load() != 1 {
		t.Fatal("missing committed cancellation callback")
	}
}

func TestControlsReplayLeavesEventAndReceiptRowsUnchanged(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	cancelControl(t, f, service, p, id)
	events := controlCount(t, f.s, "SELECT count(*) FROM events")
	receipts := controlCount(t, f.s, "SELECT count(*) FROM receipts")
	for range 3 {
		if _, err := service.Submit(t.Context(), p, "a", id.JobID, domain.OperationCancel, "cancel-original-key", controlRequest(id.AttemptID)); err != nil {
			t.Fatal(err)
		}
	}
	if controlCount(t, f.s, "SELECT count(*) FROM events") != events || controlCount(t, f.s, "SELECT count(*) FROM receipts") != receipts {
		t.Fatal("replay wrote events or receipts")
	}
}
