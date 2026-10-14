package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func (p *probeProvider) startControlled(ctx context.Context, prepared provider.Prepared, start provider.StartAuthorization) provider.SubmissionOutcome {
	var count int
	err := p.store.db.QueryRowContext(ctx, `SELECT count(*) FROM attempt_grants g JOIN scheduler_queue q ON q.workspace_id=g.workspace_id AND q.job_id=g.job_id AND q.attempt_id=g.attempt_id JOIN submission_intents s ON s.queue_seq=q.queue_seq WHERE g.kind='start' AND g.authorization_id=? AND g.consumed_at IS NOT NULL AND s.intent_id=? AND s.status='started'`, string(start.AuthorizationID), string(prepared.Identity.IntentID)).Scan(&count)
	if err != nil || count != 1 {
		panic("controlled start lacks atomic consumed authority and intent")
	}
	outcome := p.Bound.Start(ctx, prepared, &start, nil)
	return outcome
}

func attachGate(t testing.TB, f *dispatchFixture) {
	t.Helper()
	attachManaged(t, f)
	cfg := dispatch.DefaultConfig()
	cfg.PollDelay = time.Millisecond
	var err error
	f.engine, err = dispatch.New(f.s, f.registry, f.blobs, f.clock, cfg)
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

func TestExecutionGateStartGrantConsumptionAndIntentAreAtomic(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	id := gateSeed(t, f, 1)
	service, p, ready := readyGate(t, f, id)
	start, err := service.Start(t.Context(), p, "a", id.JobID, "atomic-start", startGrantRaw(f, id, ready))
	if err != nil {
		t.Fatal(err)
	}
	// probeProvider.startControlled checks consumed authority and the committed
	// submission intent inside the provider callback, before its remote effect.
	if worked, err := gateStep(t, f); err != nil || !worked {
		t.Fatal(worked, err)
	}
	var consumed, intents int
	if err = f.s.db.QueryRow("SELECT count(*) FROM attempt_grants WHERE kind='start' AND authorization_id=? AND consumed_at IS NOT NULL", start.AuthorizationID).Scan(&consumed); err != nil {
		t.Fatal(err)
	}
	if err = f.s.db.QueryRow("SELECT count(*) FROM submission_intents WHERE status='accepted'").Scan(&intents); err != nil || consumed != 1 || intents != 1 || f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("start grant and intent did not commit together", consumed, intents, err)
	}
}
func TestExecutionGateReadyReleasesWorkerLeaseAndRetainsAccountHolds(t *testing.T) {
	f := gateFixture(t, fake.Accept, 2)
	settings := scheduler.DefaultSettings()
	settings.MaxWorkers, settings.MaxActivePerAccount = 3, 2
	if err := f.s.ConfigureScheduler(t.Context(), settings); err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= 2; n++ {
		id := gateSeed(t, f, n)
		readyGate(t, f, id)
		if held := controlCount(t, f.s, "SELECT count(*) FROM work_leases WHERE kind='dispatch' AND held=1"); held != 0 {
			t.Fatal("ready attempt retained worker lease", held)
		}
		if holds := controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='prepare' AND storage_released=0"); holds != n {
			t.Fatal("ready attempt lost storage hold", holds, n)
		}
	}
}
func TestExecutionGateStorageReleasedOnlyAfterConfirmedCleanup(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	id := gateSeed(t, f, 1)
	prepareGrant(t, f, id)
	f.adapter.losePrepare = true
	if _, err := gateStep(t, f); err == nil {
		t.Fatal("lost ACK hidden")
	}
	f.restart(t)
	attachGate(t, f)
	f.clock.advance(31 * time.Minute)
	for range 2 {
		if worked, err := gateStep(t, f); err != nil || !worked {
			t.Fatal(worked, err)
		}
	}
	_, _, _ = admitCleanup(t, f, id, "expired-gate-cleanup", false)
	w, err := f.s.ClaimCleanup(t.Context(), time.Minute)
	if err != nil || w == nil {
		t.Fatal(err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='prepare' AND storage_released=0") != 1 {
		t.Fatal("admission released unconfirmed storage")
	}
	finishCleanupResourceFixtures(t, f, *w)
	if err = f.s.CompleteCleanup(t.Context(), *w, cleanup.Outcome{Effect: operations.RemoteDeleted}); err != nil {
		t.Fatal(err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='prepare' AND storage_released=0") != 0 {
		t.Fatal("confirmed cleanup retained storage hold")
	}
}
func TestExecutionGateUnstagedPreventionReleasesStorageHold(t *testing.T) {
	for _, unsupported := range []bool{false, true} {
		t.Run(fmt.Sprint(unsupported), func(t *testing.T) {
			var f *dispatchFixture
			if unsupported {
				f = managedFixture(t, fake.DefaultScenario())
				attachGate(t, f)
			} else {
				f = gateFixture(t, fake.Accept)
			}
			id := gateSeed(t, f, 1)
			prepareGrant(t, f, id)
			if unsupported {
				if _, err := gateStep(t, f); err == nil {
					t.Fatal("unsupported gate passed")
				}
			} else {
				service, p, _ := controlService(t, f, "a", auth.Operate)
				cancelControl(t, f, service, p, id)
			}
			if controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='prepare' AND storage_released=0") != 0 {
				t.Fatal("unstaged prevention leaked storage hold")
			}
		})
	}
}
