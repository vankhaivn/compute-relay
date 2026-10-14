package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
	service, err := operations.New(access, f.s, nil, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
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
		if err := f.step(t); err != nil {
			t.Fatal(err)
		}
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

func TestCleanupAdmissionReplayConflictAndCoalescing(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, p := cleanupService(t, f, "a", auth.Read, auth.Operate)
	body := cleanupBody(id.AttemptID, false, "explicit operator request")
	original, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-original", body)
	if err != nil {
		t.Fatal(err)
	}
	op := original.Operation
	if op.Kind != domain.OperationCleanup || op.Status != domain.OperationAccepted || original.Effect != operations.Pending || original.Replay || op.AttemptID != id.AttemptID || op.Revision != 1 {
		t.Fatal("unexpected cleanup receipt", original)
	}
	replay, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-original", body)
	expected := original
	expected.Replay = true
	if err != nil || !reflect.DeepEqual(replay, expected) {
		t.Fatal("replay changed the receipt", replay, err)
	}
	if _, err = service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-original", cleanupBody(id.AttemptID, true, "explicit operator request")); !errors.Is(err, operations.ErrConflict) {
		t.Fatal("changed payload reused a key", err)
	}
	if _, err = service.Submit(ctx, p, "a", "job_2", domain.OperationCleanup, "cleanup-original", body); !errors.Is(err, operations.ErrConflict) {
		t.Fatal("key reused against another job", err)
	}
	coalesced, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-another-key", cleanupBody(id.AttemptID, false, ""))
	if err != nil || coalesced.Operation.ID != op.ID || !coalesced.Replay {
		t.Fatal("concurrent cleanup was not coalesced", coalesced, err)
	}
	read, err := service.Get(ctx, p, "a", op.ID)
	if err != nil || read.Operation != op || read.Replay {
		t.Fatal("cleanup receipt not readable", read, err)
	}
	if unified, err := f.s.ReadOperation(ctx, "a", p.TokenID(), op.ID); err != nil || unified.Operation != op {
		t.Fatal("cleanup is not readable through the shared operation record", unified, err)
	}
	controls, cp, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	if _, err = controls.Submit(ctx, cp, "a", id.JobID, domain.OperationReconcile, "reconcile-during-cleanup", controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrBusy) {
		t.Fatal("control admitted while cleanup is pending", err)
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup'"); n != 1 {
		t.Fatal("coalescing created extra cleanups", n)
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM receipts WHERE namespace='operation' AND scope='cleanup'"); n != 2 {
		t.Fatal("each accepted key needs its own durable receipt", n)
	}
	if f.backend.Stats().CleanupCalls != 0 {
		t.Fatal("admission called the provider")
	}
	reader, rp := cleanupService(t, f, "a", auth.Read)
	if _, err = reader.Submit(ctx, rp, "a", id.JobID, domain.OperationCleanup, "cleanup-read-only", body); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope admitted cleanup", err)
	}
	other, op2 := cleanupService(t, f, "b", auth.Read, auth.Operate)
	if _, err = other.Get(ctx, op2, "b", op.ID); !errors.Is(err, operations.ErrNotFound) {
		t.Fatal("cleanup visible across workspaces", err)
	}
	for _, raw := range []string{`{}`, `{"attempt_id":null}`, `{"attempt_id":"` + string(id.AttemptID) + `","discard_uncollected":"true"}`, `{"attempt_id":"` + string(id.AttemptID) + `","extra":1}`, `{"attempt_id":"` + string(id.AttemptID) + `"} {}`} {
		if _, err = service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-invalid", []byte(raw)); !errors.Is(err, operations.ErrRequest) {
			t.Fatal("invalid request accepted", raw, err)
		}
	}
}

func TestCleanupRefusesUnresolvedBusyAndIneligibleAttempts(t *testing.T) {
	ctx := context.Background()
	t.Run("queued without remote resources", func(t *testing.T) {
		f := newDispatchFixture(t, fake.DefaultScenario())
		id := f.seed(t, "a", 1, false)
		service, p := cleanupService(t, f, "a", auth.Operate)
		if _, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-queued", cleanupBody(id.AttemptID, true, "")); !errors.Is(err, operations.ErrState) {
			t.Fatal("queued attempt admitted", err)
		}
		if _, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-foreign", cleanupBody("attempt_404", true, "")); err == nil {
			t.Fatal("unknown attempt admitted")
		}
		f.claim(t)
		if _, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-claimed", cleanupBody(id.AttemptID, true, "")); !errors.Is(err, operations.ErrBusy) {
			t.Fatal("cleanup admitted while dispatch holds the attempt", err)
		}
	})
	t.Run("cancelled before preparation", func(t *testing.T) {
		f := newDispatchFixture(t, fake.DefaultScenario())
		id := f.seed(t, "a", 1, false)
		controls, cp, _ := controlService(t, f, "a", auth.Operate)
		cancelControl(t, f, controls, cp, id)
		service, p := cleanupService(t, f, "a", auth.Operate)
		if _, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-unprepared", cleanupBody(id.AttemptID, true, "")); !errors.Is(err, operations.ErrState) {
			t.Fatal("attempt without a recorded resource admitted", err)
		}
	})
	t.Run("submitted and active", func(t *testing.T) {
		f := newDispatchFixture(t, fake.DefaultScenario())
		id := f.seed(t, "a", 1, false)
		submittedControl(t, f, id)
		service, p := cleanupService(t, f, "a", auth.Operate)
		if _, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-active", cleanupBody(id.AttemptID, true, "")); !errors.Is(err, operations.ErrUnresolved) {
			t.Fatal("active remote execution admitted", err)
		}
	})
	t.Run("collection pending", func(t *testing.T) {
		f, id, _, _ := collectionFixture(t)
		service, p := cleanupService(t, f, "a", auth.Operate)
		if _, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-collecting", cleanupBody(id.AttemptID, true, "")); !errors.Is(err, operations.ErrState) {
			t.Fatal("cleanup admitted before orchestration settled", err)
		}
	})
	t.Run("collection transfer holds the attempt", func(t *testing.T) {
		f, id, p, blobs := collectionFixture(t)
		service, principal := cleanupService(t, f, "a", auth.Operate)
		var refused error
		p.hook = func() error {
			_, refused = service.Submit(ctx, principal, "a", id.JobID, domain.OperationCleanup, "cleanup-during-transfer", cleanupBody(id.AttemptID, true, ""))
			return nil
		}
		runCollection(t, collectionEngine(t, f, p, blobs, f.s))
		if !errors.Is(refused, operations.ErrBusy) {
			t.Fatal("cleanup admitted during a held collection lease", refused)
		}
	})
	t.Run("uncollected requires discard confirmation", func(t *testing.T) {
		f, id := uncollectedFixture(t)
		service, p := cleanupService(t, f, "a", auth.Operate)
		if _, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-uncollected", cleanupBody(id.AttemptID, false, "")); !errors.Is(err, operations.ErrUncollected) {
			t.Fatal("uncollected results deleted without confirmation", err)
		}
		controls, cp, _ := controlService(t, f, "a", auth.Operate)
		ticket, err := controls.Submit(ctx, cp, "a", id.JobID, domain.OperationCollect, "collect-again", controlRequest(id.AttemptID))
		if err != nil || ticket.Operation.Status != domain.OperationAccepted {
			t.Fatal(err)
		}
		if _, err = service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-pending-collect", cleanupBody(id.AttemptID, true, "")); !errors.Is(err, operations.ErrBusy) {
			t.Fatal("cleanup admitted beside an accepted control", err)
		}
	})
	t.Run("uncollected with discard", func(t *testing.T) {
		f, id := uncollectedFixture(t)
		service, p := cleanupService(t, f, "a", auth.Operate)
		r, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-discard", cleanupBody(id.AttemptID, true, "results are not needed"))
		if err != nil || r.Operation.Status != domain.OperationAccepted {
			t.Fatal("confirmed discard refused", err)
		}
		controls, cp, _ := controlService(t, f, "a", auth.Operate)
		if _, err = controls.Submit(ctx, cp, "a", id.JobID, domain.OperationCollect, "collect-after-cleanup", controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrBusy) {
			t.Fatal("collection admitted while cleanup is pending", err)
		}
	})
	t.Run("prepared and never submitted", func(t *testing.T) {
		f, id := preparedFixture(t)
		service, p := cleanupService(t, f, "a", auth.Operate)
		r, err := service.Submit(ctx, p, "a", id.JobID, domain.OperationCleanup, "cleanup-prepared", cleanupBody(id.AttemptID, false, ""))
		if err != nil || r.Operation.Status != domain.OperationAccepted {
			t.Fatal("prepared attempt refused", err)
		}
		var resource string
		if err = f.s.db.QueryRow("SELECT resource_id FROM operations WHERE kind='cleanup'").Scan(&resource); err != nil || resource != "res_staging_"+string(f.journal(t, id).Prepared.Identity.IntentID) {
			t.Fatal("cleanup does not target the staging ledger entry", resource, err)
		}
	})
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err = f.s.db.QueryRow("SELECT resource_id FROM provider_resources WHERE purpose='execution'").Scan(&execution); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.db.Exec(`INSERT INTO operations(workspace_id,operation_id,job_id,attempt_id,resource_id,status,revision,created_at,request_sha256,request_json,record,kind)
 SELECT workspace_id,'op_forged',job_id,attempt_id,?,status,revision,created_at,request_sha256,request_json,replace(record,operation_id,'op_forged'),kind FROM operations WHERE kind='cleanup'`, execution); err == nil {
		t.Fatal("cleanup accepted a non-staging target")
	}
}

func cleanupPreviewService(t testing.TB, f *dispatchFixture) *cleanup.Service {
	t.Helper()
	access, err := auth.New(f.s, f.s)
	if err != nil {
		t.Fatal(err)
	}
	service, err := cleanup.NewService(access, f.s, nil)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestCleanupSharesOperationsWithoutChangingControlCapacityOrEvents(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	other := f.seed(t, "a", 2, false)
	submittedControl(t, f, other)
	access, err := auth.New(f.s, f.s)
	if err != nil {
		t.Fatal(err)
	}
	service, err := operations.New(access, f.s, nil, admission.Limits{MaxOutstandingTotal: 1, MaxOutstandingWorkspace: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, actor := cleanupService(t, f, "a", auth.Read, auth.Operate)
	cancellation, err := service.Submit(ctx, actor, "a", other.JobID, domain.OperationCancel, "pending-control", controlRequest(other.AttemptID))
	if err != nil || cancellation.Operation.Status != domain.OperationAccepted {
		t.Fatal("missing pending control fixture", cancellation, err)
	}
	events := controlCount(t, f.s, "SELECT count(*) FROM events")
	receipt, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCleanup, "cleanup-at-control-cap", cleanupBody(id.AttemptID, false, ""))
	if err != nil || receipt.Operation.Status != domain.OperationAccepted {
		t.Fatal("cleanup acquired the control admission limit", receipt, err)
	}
	if got := controlCount(t, f.s, "SELECT count(*) FROM events"); got != events {
		t.Fatal("cleanup admission appended control events", got, events)
	}
	// Finish the unrelated cancellation. Cleanup remains pending, and must neither
	// become dispatch control work nor consume the ordinary control-only capacity.
	for i := 0; i < 12; i++ {
		current, err := service.Get(ctx, actor, "a", cancellation.Operation.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Operation.Status.Terminal() {
			break
		}
		if i == 11 {
			t.Fatal("cancellation did not settle")
		}
		if err = f.step(t); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = service.Submit(ctx, actor, "a", other.JobID, domain.OperationReconcile, "control-with-cleanup-pending", controlRequest(other.AttemptID)); err != nil {
		t.Fatal("cleanup consumed ordinary control capacity", err)
	}
	current, err := service.Get(ctx, actor, "a", receipt.Operation.ID)
	if err != nil || current.Operation != receipt.Operation {
		t.Fatal("dispatch claimed cleanup as ordinary control", current, err)
	}
	events = controlCount(t, f.s, "SELECT count(*) FROM events")
	worker, _ := cleanupWorker(t, f)
	runCleanup(t, worker)
	current, err = service.Get(ctx, actor, "a", receipt.Operation.ID)
	if err != nil || current.Effect != operations.RemoteDeleted {
		t.Fatal("own cleanup blocked its eligibility guard", current, err)
	}
	if got := controlCount(t, f.s, "SELECT count(*) FROM events"); got != events {
		t.Fatal("cleanup completion appended control events", got, events)
	}
}
