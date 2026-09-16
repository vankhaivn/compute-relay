package kaggle

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func quotaJournal(t *testing.T, f *stagingJournalFixture) scheduler.Quota {
	t.Helper()
	var q scheduler.Quota
	f.inspect(t, func(db *sql.DB) {
		var raw string
		var observation provider.QuotaObservation
		if err := db.QueryRow("SELECT observation,exhausted FROM scheduler_quotas WHERE account_scope=? AND resource='gpu'", f.profile.AccountScope).Scan(&raw, &q.Exhausted); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(raw), &observation); err != nil || observation.Validate() != nil {
			t.Fatal("invalid stored quota", err)
		}
		q.Observation = &observation
	})
	return q
}

func TestMonitorM3QuotaLatchSurvivesUnknownStaleAndRestart(t *testing.T) {
	ctx := context.Background()
	f := newStagingJournalFixture(t)
	resolver, err := credentials.NewEnvironment([]ports.CredentialRef{f.config.CredentialRef}, func(string) (string, bool) { return "SYNTHETIC_TOKEN", true })
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewMonitor(f.config, resolver, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	m.local = func(context.Context, Config, Mode, []byte) (Report, error) { return baseline(Local), nil }
	// This integration uses the real frozen account configuration and store, but
	// synthetic helper results. Pinned-SDK transport tests are a separate tier.
	reply := knownQuota("10000000000", "10000000000", "0")
	reads := 0
	m.run = func(_ context.Context, c Config, mode string, _ []byte, request monitorRequest) (monitorResponse, error) {
		reads++
		if mode != "quota" || c.AccountName != f.profile.AccountScope || request.Owner != c.AccountName || request.Execution != nil {
			t.Fatal("quota remapped its account")
		}
		return reply, nil
	}
	read := func() provider.QuotaObservation {
		t.Helper()
		f.clock.mu.Lock()
		f.clock.at = f.clock.at.Add(time.Second)
		f.clock.mu.Unlock()
		q, err := m.ReadQuota(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	record := func(q provider.QuotaObservation) {
		t.Helper()
		if err := f.store.RecordQuota(ctx, f.profile.AccountScope, "gpu", q, f.clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	zero := read()
	record(zero)
	f.restart(t)
	if q := quotaJournal(t, f); !q.Exhausted || *q.Observation.Remaining != 0 {
		t.Fatal("restart lost exhaustion")
	}
	for _, r := range []monitorResponse{{Protocol: 1, Status: "unknown", Reason: "missing_quota"}, {Protocol: 1, Status: "unavailable", Reason: "read_unavailable"}} {
		reply = r
		record(read())
		q := quotaJournal(t, f)
		if !q.Exhausted || q.Observation.Remaining != nil {
			t.Fatal("missing quota erased exhaustion or invented zero")
		}
	}
	reply = knownQuota("20000000000", "0", "1000000000")
	oldPositive := read()
	f.clock.advance()
	stale, err := AgeQuota(oldPositive, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	record(stale)
	if !quotaJournal(t, f).Exhausted {
		t.Fatal("stale positive cleared exhaustion")
	}
	fresh := read()
	record(fresh)
	f.restart(t)
	stored := quotaJournal(t, f)
	if stored.Exhausted || stored.Observation.Precision != "lower_bound" || *stored.Observation.Remaining != 19 || stored.Observation.ResetAt != nil {
		t.Fatal("fresh reservation-aware observation not persisted")
	}
	if reason, _ := stored.Decide("gpu", 19, true, QuotaFreshFor, f.clock.Now()); reason != scheduler.Eligible {
		t.Fatal(reason)
	}
	if reason, _ := stored.Decide("gpu", 20, true, QuotaFreshFor, f.clock.Now()); reason != scheduler.QuotaInsufficient {
		t.Fatal("reserved capacity ignored", reason)
	}
	if err := f.store.RecordQuota(ctx, f.profile.AccountScope, "gpu", zero, f.clock.Now()); err == nil {
		t.Fatal("old observation replaced fresh evidence")
	}
	if reads != 5 || f.creates != 0 {
		t.Fatal("unexpected reads or staging side effects", reads, f.creates)
	}
	f.inspect(t, func(db *sql.DB) {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM submission_intents").Scan(&n); err != nil || n != 0 {
			t.Fatal("quota created compute", err)
		}
	})
}

// Test-only adapter: retain fixture batch capabilities needed by the durable
// engine, but take cancellation evidence from the concrete M4-04 descriptor.
type operationalJournalAdapter struct {
	*executionJournalAdapter
	config      Config
	cancelCalls int
}

func (p *operationalJournalAdapter) Describe() provider.Descriptor {
	d := p.Bound.Describe()
	actual, err := OperationalDescriptor(p.config)
	if err != nil {
		panic(err)
	}
	for i := range d.Capabilities {
		if d.Capabilities[i].Name == domain.CapabilityRemoteCancellation {
			for _, c := range actual.Capabilities {
				if c.Name == domain.CapabilityRemoteCancellation {
					d.Capabilities[i] = c
				}
			}
		}
	}
	return d
}
func (p *operationalJournalAdapter) Cancel(ctx context.Context, ref provider.RemoteReference, op domain.OperationID) (provider.CancellationOutcome, error) {
	p.cancelCalls++
	return p.executor.Cancel(ctx, ref, op)
}

func operationalService(t *testing.T, f *stagingJournalFixture) (*operations.Service, auth.Principal) {
	t.Helper()
	ctx := context.Background()
	access, err := auth.New(f.store, f.store, nil)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := access.Issue(ctx, "workspace", []auth.Scope{auth.Read, auth.Operate}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	actor, err := access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	service, err := operations.New(access, f.store, f.blobs, f.clock, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return service, actor
}

func TestOperationalM3ManualCancellationPreservesActivityAndReceipt(t *testing.T) {
	ctx := context.Background()
	x := newExecutionJournalFixture(t)
	x.step(t, false)
	x.step(t, false)
	f := x.f
	binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
	backend, err := fake.NewBackend(fake.DefaultScenario())
	if err != nil {
		t.Fatal(err)
	}
	bound, err := fake.NewBound(backend, f.clock, binding)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &operationalJournalAdapter{executionJournalAdapter: &executionJournalAdapter{journalStagingAdapter: &journalStagingAdapter{Bound: bound, stage: x.executor.stager}, executor: x.executor}, config: f.config}
	registry := provider.NewSnapshotRegistry()
	if err := registry.Register(binding, adapter); err != nil {
		t.Fatal(err)
	}
	// Use the concrete store so its CancellationRepository method is available.
	f.engine, err = dispatch.New(f.store, registry, f.blobs, nil, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	service, actor := operationalService(t, f)
	raw, _ := json.Marshal(map[string]any{"attempt_id": f.receipt.AttemptID})
	receipt, err := service.Submit(ctx, actor, "workspace", f.receipt.JobID, domain.OperationCancel, "m404-cancel-key", raw)
	if err != nil {
		t.Fatal(err)
	}
	x.step(t, false)
	current, err := service.Get(ctx, actor, "workspace", receipt.Operation.ID)
	if err != nil || current.Operation.Status != domain.OperationManualRequired || current.Effect != operations.ManualRequired || adapter.cancelCalls != 0 {
		t.Fatal("unsupported capability invoked cancel or claimed success", current, err, adapter.cancelCalls)
	}
	attempt, err := f.store.LoadAttempt(ctx, "workspace", f.receipt.JobID, f.receipt.AttemptID)
	if err != nil || attempt.State.Cancellation != domain.CancellationManual || attempt.State.Execution != domain.ExecutionRunning || attempt.State.RemoteActivity != domain.RemoteActivityActive || attempt.State.Orchestration.Terminal() {
		t.Fatal("manual action fabricated remote termination", attempt.State, err)
	}
	x.restart(t)
	service, actor = operationalService(t, f)
	replay, err := service.Submit(ctx, actor, "workspace", f.receipt.JobID, domain.OperationCancel, "m404-cancel-key", raw)
	if err != nil || !replay.Replay || replay.Operation.ID != receipt.Operation.ID || replay.Operation.Status != receipt.Operation.Status || replay.Effect != receipt.Effect {
		t.Fatal("manual outcome replaced original receipt", err)
	}
	x.raw = "COMPLETE"
	x.step(t, false)
	later, err := service.Get(ctx, actor, "workspace", receipt.Operation.ID)
	if err != nil || !reflect.DeepEqual(later.Operation, current.Operation) || later.Effect != current.Effect {
		t.Fatal("completion rewrote control history", err)
	}
	attempt, err = f.store.LoadAttempt(ctx, "workspace", f.receipt.JobID, f.receipt.AttemptID)
	if err != nil || attempt.State.Execution != domain.ExecutionSucceeded || attempt.State.Cancellation == domain.CancellationConfirmed || attempt.State.ReleaseEvidence != domain.ReleaseEvidenceNotObservable || x.saves != 1 {
		t.Fatal("completion/cancel evidence conflated", err)
	}
	x.counts(t, 1)
}
