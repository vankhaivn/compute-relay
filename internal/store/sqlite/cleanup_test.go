package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func cleanupService(t testing.TB, f *dispatchFixture, w domain.WorkspaceID, scopes ...auth.Scope) (*operations.Service, auth.Principal) {
	t.Helper()
	access, err := auth.New(f.s, f.s)
	requireNoError(t, err)
	secret, _, err := access.Issue(context.Background(), w, scopes, time.Time{})
	requireNoError(t, err)
	p, err := access.Authenticate(context.Background(), secret.Reveal())
	requireNoError(t, err)
	service, err := operations.New(access, f.s, nil, admission.DefaultLimits())
	requireNoError(t, err)
	return service, p
}
func cleanupBody(id domain.AttemptID, discard bool, reason string) []byte {
	raw, _ := json.Marshal(operations.Request{AttemptID: id, DiscardUncollected: discard, Reason: reason})
	return raw
}

// collectedFixture is a terminal attempt whose verified results were published locally.
func collectedFixture(t *testing.T) (*dispatchFixture, scheduler.Identity) {
	t.Helper()
	f, id, p, blobs := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	if s := f.state(t, id); s.Orchestration != domain.OrchestrationSucceeded || s.Result != domain.ResultAvailable {
		t.Fatal("fixture results were not collected", s)
	}
	return f, id
}

// uncollectedFixture is a terminal attempt whose collection failed verification.
func uncollectedFixture(t *testing.T) (*dispatchFixture, scheduler.Identity) {
	t.Helper()
	f, id, p, blobs := collectionFixture(t)
	p.mode = "corrupt"
	if worked, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(context.Background()); !worked || err == nil {
		t.Fatal("collection failure was not recorded", err)
	}
	if s := f.state(t, id); s.Orchestration != domain.OrchestrationNeedsAttention || s.Result == domain.ResultAvailable {
		t.Fatal("fixture unexpectedly collected", s)
	}
	return f, id
}

// preparedFixture stages one attempt and prevents its submission.
func preparedFixture(t *testing.T) (*dispatchFixture, scheduler.Identity) {
	t.Helper()
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	for i := 0; f.journal(t, id).Phase != dispatch.Ready; i++ {
		if i == 6 {
			t.Fatal("fixture never staged")
		}
		requireNoError(t, f.step(t))
	}
	service, p, _ := controlService(t, f, "a", auth.Operate)
	if r := cancelControl(t, f, service, p, id); r.Effect != operations.DispatchPrevented {
		t.Fatal("submission was not prevented", r)
	}
	j := f.journal(t, id)
	if j.Phase != dispatch.Prevented || j.SubmitStarted || j.Prepared == nil || f.state(t, id).Orchestration != domain.OrchestrationCancelled {
		t.Fatal("fixture is not a prepared, never-submitted attempt", j.Phase)
	}
	return f, id
}

func TestCleanupAdmissionPersistsOneOperationAndPerKeyReceipts(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, actor := cleanupService(t, f, "a", auth.Read, auth.Operate)
	first, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCleanup, "cleanup-original", cleanupBody(id.AttemptID, false, ""))
	requireNoError(t, err)
	if _, err = service.Submit(ctx, actor, "a", id.JobID, domain.OperationCleanup, "cleanup-second-key", cleanupBody(id.AttemptID, false, "")); err != nil {
		t.Fatal(err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup'") != 1 || controlCount(t, f.s, "SELECT count(*) FROM receipts WHERE namespace='operation' AND scope='cleanup'") != 2 {
		t.Fatal("coalescing lost operation/receipt cardinality")
	}
	if unified, err := f.s.ReadOperation(ctx, "a", actor.TokenID(), first.Operation.ID); err != nil || unified.Operation != first.Operation {
		t.Fatal("shared operation record changed", unified, err)
	}
}

func TestCleanupPreparedAdmissionStoresStagingLedgerIdentity(t *testing.T) {
	ctx := context.Background()
	f, id := preparedFixture(t)
	service, actor := cleanupService(t, f, "a", auth.Operate)
	if _, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCleanup, "cleanup-prepared", cleanupBody(id.AttemptID, false, "")); err != nil {
		t.Fatal(err)
	}
	var resource string
	if err := f.s.db.QueryRow("SELECT resource_id FROM operations WHERE kind='cleanup'").Scan(&resource); err != nil || resource != "res_staging_"+string(f.journal(t, id).Prepared.Identity.IntentID) {
		t.Fatal("cleanup did not pin staging ledger identity", resource, err)
	}
}

func TestCleanupSchemaFourteenUpgradePreservesState(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	before := f.journal(t, id)
	state := f.state(t, id)
	counts := map[string]int{}
	for _, table := range []string{"operations", "receipts", "provider_resources", "events", "artifacts", "collection_publications"} {
		counts[table] = controlCount(t, f.s, "SELECT count(*) FROM "+table)
	}
	f.root = copyFixtureAtSchema(t, f.s, 14)
	f.restart(t)
	if n := controlCount(t, f.s, "SELECT count(*) FROM schema_migrations"); n != len(migrations) {
		t.Fatal("upgrade did not apply the cleanup migration", n)
	}
	for table, n := range counts {
		if got := controlCount(t, f.s, "SELECT count(*) FROM "+table); got != n {
			t.Fatal("upgrade changed existing rows", table, got, n)
		}
	}
	if j := f.journal(t, id); j.Version != before.Version || *j.Remote != *before.Remote || f.state(t, id) != state {
		t.Fatal("upgrade changed dispatch or attempt state")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup'") != 0 {
		t.Fatal("upgrade invented cleanup intent")
	}
	service, p := cleanupService(t, f, "a", auth.Operate)
	if r, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-after-upgrade", cleanupBody(id.AttemptID, false, "")); err != nil || r.Operation.Status != domain.OperationAccepted {
		t.Fatal("upgraded store refused cleanup", err)
	}
}

func TestCleanupStorageRejectsForgedRows(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, p := cleanupService(t, f, "a", auth.Operate)
	r, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-forgery", cleanupBody(id.AttemptID, false, ""))
	requireNoError(t, err)
	for _, statement := range []string{
		"DELETE FROM operations WHERE kind='cleanup'",
		"DELETE FROM receipts WHERE namespace='operation' AND scope='cleanup'",
		"UPDATE receipts SET receipt=receipt WHERE namespace='operation' AND scope='cleanup'",
		"UPDATE operations SET resource_id='res_execution_x' WHERE kind='cleanup'",
		"UPDATE operations SET status='succeeded',revision=2 WHERE kind='cleanup'",
		"INSERT INTO work_leases(kind,key,generation,owner,fence,until_ms,held) VALUES('cleanup',json_array('a','" + string(r.Operation.ID) + "'),1,'','0000000000000000000000000000000000000000000000000000000000000000',1,1)",
	} {
		if _, err := f.s.db.Exec(statement); err == nil {
			t.Fatal("storage accepted a forged cleanup change", statement)
		}
	}
	var execution string
	err = f.s.db.QueryRow("SELECT resource_id FROM provider_resources WHERE purpose='execution'").Scan(&execution)
	requireNoError(t, err)
	if _, err = f.s.db.Exec(`INSERT INTO operations(workspace_id,operation_id,job_id,attempt_id,resource_id,status,revision,created_at,request_sha256,request_json,record,kind)
 SELECT workspace_id,'op_forged',job_id,attempt_id,?,status,revision,created_at,request_sha256,request_json,replace(record,operation_id,'op_forged'),kind FROM operations WHERE kind='cleanup'`, execution); err == nil {
		t.Fatal("cleanup accepted a non-staging target")
	}
}

func cleanupPreviewService(t testing.TB, f *dispatchFixture) *cleanup.Service {
	t.Helper()
	access, err := auth.New(f.s, f.s)
	requireNoError(t, err)
	service, err := cleanup.NewService(access, f.s, nil)
	requireNoError(t, err)
	return service
}

func TestCleanupDoesNotAppendControlEventRows(t *testing.T) {
	f, id := collectedFixture(t)
	events := controlCount(t, f.s, "SELECT count(*) FROM events")
	_, _, receipt := admitCleanup(t, f, id, "cleanup-without-control-events", false)
	if controlCount(t, f.s, "SELECT count(*) FROM events") != events {
		t.Fatal("cleanup admission appended control events")
	}
	worker, _ := cleanupWorker(t, f)
	runCleanup(t, worker)
	if controlCount(t, f.s, "SELECT count(*) FROM events") != events || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup' AND status='succeeded'") != 1 {
		t.Fatal("cleanup completion changed control event history", receipt)
	}
}
