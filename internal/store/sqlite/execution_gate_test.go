package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func (p *probeProvider) Start(ctx context.Context, prepared provider.Prepared, start provider.StartAuthorization) provider.SubmissionOutcome {
	var count int
	err := p.store.db.QueryRowContext(ctx, `SELECT count(*) FROM start_authorizations g JOIN scheduler_queue q ON q.workspace_id=g.workspace_id AND q.job_id=g.job_id AND q.attempt_id=g.attempt_id JOIN submission_intents s ON s.queue_seq=q.queue_seq WHERE g.authorization_id=? AND g.consumed_at IS NOT NULL AND s.intent_id=? AND s.status='started'`, string(start.AuthorizationID), string(prepared.Identity.IntentID)).Scan(&count)
	if err != nil || count != 1 {
		panic("controlled start lacks atomic consumed authority and intent")
	}
	outcome := p.Bound.Start(ctx, prepared, start)
	if p.panicSubmit {
		panic("uncertain controlled-start response")
	}
	return outcome
}

func (p *probeProvider) ReconcilePreparation(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.PreparationObservation, error) {
	seen, err := p.Bound.ReconcilePreparation(ctx, plan, op)
	if p.afterPreparationRead != nil {
		p.afterPreparationRead()
	}
	return seen, err
}
func attachGate(t testing.TB, f *dispatchFixture) {
	t.Helper()
	attachManaged(t, f)
	cfg := dispatch.DefaultConfig()
	cfg.PollDelay = time.Millisecond
	var err error
	f.engine, err = dispatch.New(managedDispatchRepository{f.s}, f.registry, f.blobs, f.clock, cfg)
	if err != nil {
		t.Fatal(err)
	}
}
func gateFixture(t testing.TB, mode fake.SubmitMode, maxActive ...int) *dispatchFixture {
	t.Helper()
	scenario := fake.DefaultScenario()
	scenario.SeparateStart = true
	scenario.Mode = mode
	f := managedFixture(t, scenario, maxActive...)
	attachGate(t, f)
	return f
}
func gateSeed(t testing.TB, f *dispatchFixture, n int) scheduler.Identity {
	t.Helper()
	return f.seedRaw(t, "a", n, false, strings.Replace(dispatchJSON, `"name":"dispatch-fixture"`, `"name":"dispatch-fixture","execution_control":"prepare_then_start"`, 1))
}
func gateStep(t testing.TB, f *dispatchFixture) (bool, error) {
	t.Helper()
	f.clock.advance(10 * time.Millisecond)
	return f.engine.RunOnce(context.Background(), "gate_worker")
}
func prepareGrant(t testing.TB, f *dispatchFixture, id scheduler.Identity) (*executionauth.Service, auth.Principal, executionauth.GateReceipt, []byte) {
	t.Helper()
	service, p := authorizationService(t, f, "a", auth.Execute, auth.Read)
	raw, _ := json.Marshal(executionauth.PrepareRequest{AttemptID: id.AttemptID, MaxPreparationSeconds: 60, ReadyUntil: f.clock.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339), MaxStagedBytes: 1 << 20, AuthorizePrivateStaging: true})
	receipt, err := service.Prepare(context.Background(), p, "a", id.JobID, "prepare-key-"+string(id.JobID), raw)
	if err != nil {
		t.Fatal(err)
	}
	return service, p, receipt, raw
}
func readyGate(t testing.TB, f *dispatchFixture, id scheduler.Identity) (*executionauth.Service, auth.Principal, executionauth.Readiness) {
	t.Helper()
	service, p, _, _ := prepareGrant(t, f, id)
	if worked, err := gateStep(t, f); err != nil || !worked {
		t.Fatal("prepare phase", worked, err)
	}
	ready, err := service.Readiness(context.Background(), p, "a", id.JobID)
	if err != nil || ready.State != "ready" || !ready.Revision.Valid() || ready.StagedBytes <= 0 {
		t.Fatal("readiness", ready, err)
	}
	return service, p, ready
}
func startGrantRaw(f *dispatchFixture, id scheduler.Identity, ready executionauth.Readiness) []byte {
	raw, _ := json.Marshal(executionauth.StartRequest{AttemptID: id.AttemptID, ReadinessRevision: ready.Revision, MaxRemoteWallSeconds: 10, ExecutionDeadline: f.clock.Now().Add(9 * time.Second).UTC().Format(time.RFC3339), AuthorizeCompute: true})
	return raw
}

func TestExecutionGateReadySurvivesRestartWithoutStarting(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	id := gateSeed(t, f, 1)
	service, p, ready := readyGate(t, f, id)
	before := f.backend.Stats()
	for range 3 {
		if worked, err := gateStep(t, f); err != nil || worked {
			t.Fatal("ready hold occupied worker", worked, err)
		}
	}
	if f.backend.Stats() != before {
		t.Fatal("ready polling changed provider")
	}
	f.restart(t)
	attachGate(t, f)
	service, p = authorizationService(t, f, "a", auth.Execute, auth.Read)
	again, err := service.Readiness(context.Background(), p, "a", id.JobID)
	if err != nil || again.Revision != ready.Revision {
		t.Fatal("readiness lost across restart", err)
	}
	if worked, err := gateStep(t, f); worked || err != nil {
		t.Fatal("restart inferred consent", worked, err)
	}
	if _, err = service.Authorize(context.Background(), p, "a", id.JobID, "forbidden-combined", authorizationRequest(id.AttemptID, 10)); !errors.Is(err, executionauth.ErrState) {
		t.Fatal("combined path bypass", err)
	}
	raw := startGrantRaw(f, id, ready)
	original, err := service.Start(context.Background(), p, "a", id.JobID, "explicit-start", raw)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := gateStep(t, f); !worked || err != nil {
		t.Fatal("start phase", worked, err)
	}
	j := f.journal(t, id)
	if j.Phase != dispatch.Submitted || j.StartAuthorization == nil || j.StartAuthorization.AuthorizationID != original.AuthorizationID || f.backend.Stats().Executions != 1 {
		t.Fatal("missing exact start", j.Phase, f.backend.Stats())
	}
	replay, err := service.Start(context.Background(), p, "a", id.JobID, "explicit-start", raw)
	if err != nil || !replay.Replay || replay.Status != executionauth.Granted {
		t.Fatal("original receipt changed", err)
	}
	current, err := service.GateAuthorization(context.Background(), p, "a", id.JobID, original.AuthorizationID)
	if err != nil || current.Status != executionauth.Consumed || current.ConsumedAt == nil {
		t.Fatal("consumption missing", err)
	}
}

func TestExecutionGateConcurrentStartReplayConflictAndLostAck(t *testing.T) {
	f := gateFixture(t, fake.AcceptLoseResponse)
	id := gateSeed(t, f, 1)
	service, p, ready := readyGate(t, f, id)
	raw := startGrantRaw(f, id, ready)
	var wg sync.WaitGroup
	results := make(chan executionauth.GateReceipt, 8)
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := service.Start(context.Background(), p, "a", id.JobID, "concurrent-start", raw)
			results <- r
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first domain.OperationID
	originals := 0
	for r := range results {
		if first == "" {
			first = r.AuthorizationID
		}
		if r.AuthorizationID != first {
			t.Fatal("duplicate start grant")
		}
		if !r.Replay {
			originals++
		}
	}
	if originals != 1 {
		t.Fatal("multiple original receipts", originals)
	}
	if _, err := service.Start(context.Background(), p, "a", id.JobID, "different-key", raw); !errors.Is(err, executionauth.ErrConflict) {
		t.Fatal("second grant", err)
	}
	changed := strings.Replace(string(raw), `"max_remote_wall_seconds":10`, `"max_remote_wall_seconds":9`, 1)
	if _, err := service.Start(context.Background(), p, "a", id.JobID, "concurrent-start", []byte(changed)); !errors.Is(err, executionauth.ErrConflict) {
		t.Fatal("changed replay", err)
	}
	if _, err := gateStep(t, f); err != nil {
		t.Fatal(err)
	}
	if f.journal(t, id).Phase != dispatch.Submitting {
		t.Fatal("lost start ack lost uncertainty")
	}
	f.restart(t)
	attachGate(t, f)
	if _, err := gateStep(t, f); err != nil {
		t.Fatal(err)
	}
	if f.journal(t, id).Phase != dispatch.Submitted || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 1 {
		t.Fatal("recovery repeated original start", f.backend.Stats())
	}
}

func TestExecutionGateExpiryReplayAndCancellationPreventStart(t *testing.T) {
	for _, kind := range []string{"queued_expiry", "ready_expiry", "cancel_after_grant", "cancel_before_grant"} {
		t.Run(kind, func(t *testing.T) {
			f := gateFixture(t, fake.Accept)
			id := gateSeed(t, f, 1)
			service, p, receipt, prepRaw := prepareGrant(t, f, id)
			if kind != "queued_expiry" {
				if _, err := gateStep(t, f); err != nil {
					t.Fatal(err)
				}
			}
			ready, err := service.Readiness(context.Background(), p, "a", id.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "cancel_after_grant" {
				if _, err = service.Start(context.Background(), p, "a", id.JobID, "start-before-cancel", startGrantRaw(f, id, ready)); err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasPrefix(kind, "cancel_") {
				control, op, _ := controlService(t, f, "a", auth.Operate)
				cancelControl(t, f, control, op, id)
			} else {
				f.clock.advance(31 * time.Minute)
				if _, err = gateStep(t, f); err != nil {
					t.Fatal(err)
				}
			}
			state := f.state(t, id)
			if !state.Orchestration.Terminal() || state.Execution != domain.ExecutionNotSubmitted || f.backend.Stats().SubmitCalls != 0 {
				t.Fatal("gate prevention failed", state)
			}
			replay, err := service.Prepare(context.Background(), p, "a", id.JobID, "prepare-key-"+string(id.JobID), prepRaw)
			if err != nil || !replay.Replay || replay.AuthorizationID != receipt.AuthorizationID {
				t.Fatal("expired/cancelled receipt changed", err)
			}
			if _, err = service.Start(context.Background(), p, "a", id.JobID, "late-start", startGrantRaw(f, id, ready)); err == nil {
				t.Fatal("prevented gate reopened")
			}
		})
	}
}

func TestExecutionGateBoundsAndScopedReadiness(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	service, p := authorizationService(t, f, "a", auth.Execute, auth.Read)
	for n := 1; n <= executionauth.MaxHeldPerAccount+1; n++ {
		id := gateSeed(t, f, n)
		raw, _ := json.Marshal(executionauth.PrepareRequest{AttemptID: id.AttemptID, MaxPreparationSeconds: 60, ReadyUntil: f.clock.Now().Add(time.Minute).UTC().Format(time.RFC3339), MaxStagedBytes: 1 << 20, AuthorizePrivateStaging: true})
		_, err := service.Prepare(context.Background(), p, "a", id.JobID, "bounded-"+string(id.JobID), raw)
		if n <= executionauth.MaxHeldPerAccount && err != nil || n > executionauth.MaxHeldPerAccount && !errors.Is(err, admission.ErrLimit) {
			t.Fatal("unbounded holds", n, err)
		}
	}
	id := scheduler.Identity{WorkspaceID: "a", JobID: "job_1", AttemptID: "attempt_1"}
	readOnly, rp := authorizationService(t, f, "a", auth.Read)
	state, err := readOnly.Readiness(context.Background(), rp, "a", id.JobID)
	if err != nil || state.State != "queued" || state.StagedBytes != 0 {
		t.Fatal("uploaded bytes became remote readiness", state, err)
	}
	if _, err = readOnly.Start(context.Background(), rp, "a", id.JobID, "read-must-not-start", []byte(`{}`)); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope started work", err)
	}
	foreign, fp := authorizationService(t, f, "b", auth.Execute, auth.Read)
	if _, err = foreign.Readiness(context.Background(), fp, "b", id.JobID); !errors.Is(err, executionauth.ErrNotFound) {
		t.Fatal("cross-workspace readiness", err)
	}
	control, op, _ := controlService(t, f, "a", auth.Operate)
	cancelControl(t, f, control, op, id)
	if n := controlCount(t, f.s, "SELECT count(*) FROM preparation_authorizations WHERE storage_released=0"); n != executionauth.MaxHeldPerAccount-1 {
		t.Fatal("never-staged reservation leaked", n)
	}
}

func TestExecutionGateLostPreparationAckNeverRestagesOrStarts(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	id := gateSeed(t, f, 1)
	service, p, _, _ := prepareGrant(t, f, id)
	f.adapter.losePrepare = true
	if _, err := gateStep(t, f); err == nil {
		t.Fatal("lost prepare ack not surfaced")
	}
	f.restart(t)
	attachGate(t, f)
	f.clock.advance(time.Minute)
	if _, err := gateStep(t, f); err != nil {
		t.Fatal(err)
	}
	ready, err := service.Readiness(context.Background(), p, "a", id.JobID)
	// The old service points to the closed store; a new composition is required.
	if err == nil {
		t.Fatal("closed store remained usable", ready)
	}
	service, p = authorizationService(t, f, "a", auth.Execute, auth.Read)
	ready, err = service.Readiness(context.Background(), p, "a", id.JobID)
	if err != nil || ready.State != "ready" || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("preparation recovery inferred start", ready, err, f.backend.Stats())
	}
}

func TestExecutionGateCrashAtDurableBoundaries(t *testing.T) {
	ctx := context.Background()
	f := gateFixture(t, fake.Accept)
	id := gateSeed(t, f, 1)
	_, _, original, _ := prepareGrant(t, f, id)
	work := managedClaim(t, f)
	if _, err := f.s.ConsumeAuthorization(ctx, work.Handle, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	// Consumption survived but no preparation intent or provider mutation existed.
	f.restart(t)
	attachGate(t, f)
	f.clock.advance(time.Minute)
	if worked, err := gateStep(t, f); err != nil || !worked {
		t.Fatal("consumed preparation recovery", worked, err)
	}
	service, p := authorizationService(t, f, "a", auth.Execute, auth.Read)
	consumed, err := service.GateAuthorization(ctx, p, "a", id.JobID, original.AuthorizationID)
	if err != nil || consumed.Status != executionauth.Consumed || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("preparation consent was lost or reused", consumed, err)
	}
	ready, err := service.Readiness(ctx, p, "a", id.JobID)
	if err != nil || ready.State != "ready" {
		t.Fatal(ready, err)
	}
	raw := startGrantRaw(f, id, ready)
	start, err := service.Start(ctx, p, "a", id.JobID, "crash-start", raw)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.advance(10 * time.Millisecond)
	claim, err := f.s.ClaimRecoveryManaged(ctx, "crash_start_worker", f.clock.Now())
	if err != nil || claim == nil {
		t.Fatal("start claim", err)
	}
	work, err = f.s.LoadDispatch(ctx, *claim, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.CommitDispatch(ctx, work.Handle, dispatch.Action{Kind: dispatch.BeginSubmission}, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	// The process died after committing intent and before it could call Start.
	f.restart(t)
	attachGate(t, f)
	f.clock.advance(time.Minute)
	if _, err = gateStep(t, f); err == nil || !strings.HasPrefix(err.Error(), "PROVIDER_SUBMISSION_UNKNOWN:") {
		t.Fatal("missing original start lost uncertainty", err)
	}
	service, p = authorizationService(t, f, "a", auth.Execute, auth.Read)
	current, err := service.GateAuthorization(ctx, p, "a", id.JobID, start.AuthorizationID)
	if err != nil || current.Status != executionauth.Consumed || f.backend.Stats().SubmitCalls != 0 || !f.journal(t, id).SubmitStarted || f.state(t, id).RemoteActivity != domain.RemoteActivityPossible {
		t.Fatal("crash rearmed start authority", current, err, f.state(t, id))
	}
	if replay, err := service.Start(ctx, p, "a", id.JobID, "crash-start", raw); err != nil || !replay.Replay || replay.Status != executionauth.Granted {
		t.Fatal("expired start replay changed original receipt", replay, err)
	}
}

func TestExecutionGateExpiredLostPreparationRetainsExactCleanup(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	id := gateSeed(t, f, 1)
	prepareGrant(t, f, id)
	f.adapter.losePrepare = true
	if _, err := gateStep(t, f); err == nil {
		t.Fatal("lost preparation was hidden")
	}
	f.restart(t)
	attachGate(t, f)
	f.clock.advance(31 * time.Minute)
	for range 2 {
		if worked, err := gateStep(t, f); err != nil || !worked {
			t.Fatal("expired preparation recovery", worked, err)
		}
	}
	j := f.journal(t, id)
	if j.Phase != dispatch.Prevented || j.Prepared == nil || j.Problem == nil || j.Problem.Details[domain.ReasonDetail] != "preparation_expired" || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("expiry discarded original resource or repeated work", j)
	}
	service, p, receipt := admitCleanup(t, f, id, "expired-gate-cleanup", false)
	ctx := context.Background()
	w, err := f.s.ClaimCleanupManaged(ctx, f.clock.Now(), time.Minute)
	if err != nil || w == nil || w.Request.Remote.Resource != j.Prepared.Resource || w.Request.ResultsCollected || w.Request.Remote.Version != "prepared" {
		t.Fatal("never-started cleanup required a result or changed identity", w, err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM preparation_authorizations WHERE storage_released=0") != 1 {
		t.Fatal("cleanup request guessed released storage")
	}
	finishCleanupResourceFixtures(t, f, *w)
	if err = f.s.CompleteCleanup(ctx, *w, cleanup.Outcome{Effect: cleanup.RemoteDeleted}, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if current, err := service.Get(ctx, p, "a", receipt.Operation.ID); err != nil || current.Effect != cleanup.RemoteDeleted || controlCount(t, f.s, "SELECT count(*) FROM preparation_authorizations WHERE storage_released=0") != 0 {
		t.Fatal("confirmed deletion did not release hold", current, err)
	}
}

func TestExecutionGateReadyYieldsWorkerButPreservesAccountCapacity(t *testing.T) {
	f := gateFixture(t, fake.Accept, 2)
	settings := scheduler.DefaultSettings()
	settings.MaxWorkers, settings.MaxActivePerAccount = 1, 2
	if err := f.s.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= 2; n++ {
		id := gateSeed(t, f, n)
		readyGate(t, f, id)
	}
	id := gateSeed(t, f, 3)
	prepareGrant(t, f, id)
	if worked, err := gateStep(t, f); err != nil || worked || f.backend.Stats().PrepareCalls != 2 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("ready hold lost worker or account bound", worked, err, f.backend.Stats())
	}
}

func TestExecutionGateCancelDuringReadinessVerificationFencesStart(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	id := gateSeed(t, f, 1)
	service, p, ready := readyGate(t, f, id)
	grant, err := service.Start(context.Background(), p, "a", id.JobID, "start-races-cancel", startGrantRaw(f, id, ready))
	if err != nil {
		t.Fatal(err)
	}
	control, op, _ := controlService(t, f, "a", auth.Operate)
	f.adapter.afterPreparationRead = func() { cancelControl(t, f, control, op, id) }
	if _, err = gateStep(t, f); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("cancel failed to fence the old readiness worker", err)
	}
	current, err := service.GateAuthorization(context.Background(), p, "a", id.JobID, grant.AuthorizationID)
	if err != nil || current.Status != executionauth.Granted || f.journal(t, id).Phase != dispatch.Prevented || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("cancelled start consumed authority or executed", current, err, f.backend.Stats())
	}
}

func TestExecutionGateUnsupportedProviderHasNoRemoteEffects(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	attachGate(t, f)
	id := gateSeed(t, f, 1)
	prepareGrant(t, f, id)
	if _, err := gateStep(t, f); err == nil {
		t.Fatal("unsupported preparation mode passed")
	}
	j := f.journal(t, id)
	if j.Problem == nil || j.Problem.Code != domain.CodeUnsupportedCapability || f.backend.Stats().PrepareCalls != 0 || f.backend.Stats().SubmitCalls != 0 || controlCount(t, f.s, "SELECT count(*) FROM preparation_authorizations WHERE storage_released=0") != 0 {
		t.Fatal("unsupported provider mutated or retained a hold", j)
	}
}
