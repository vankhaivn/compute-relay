package sqlite

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func failCancellationPreflight(t *testing.T, f *dispatchFixture, id scheduler.Identity, probe *cancelProbe) {
	t.Helper()
	probe.verifyError = provider.Problem(domain.CodeProviderUnreachable, domain.FailureStageObservation, "Fixture transport is unavailable.")
	for range dispatch.MaxFailures {
		err := f.step(t)
		var problem *domain.Problem
		if !errors.As(err, &problem) || problem.Code != domain.CodeProviderUnreachable {
			t.Fatal("expected preflight failure", err)
		}
	}
	if f.state(t, id).Cancellation != domain.CancellationManual || probe.calls.Load() != 0 {
		t.Fatal("preflight failure invoked cancellation or failed to settle")
	}
	view, err := f.s.ClaimNext(t.Context(), "check-capacity")
	requireNoError(t, err)
	if view.Claim != nil || view.View.AccountReservations[f.profile.AccountScope] != 1 {
		t.Fatal("unresolved execution released compute capacity")
	}
}

func TestCancellationRecoveryAfterRepeatedPreflightOutage(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	journal := f.journal(t, id)
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	probe := installCancelProbe(t, f, true, provider.CancellationOutcome{Status: domain.CancellationConfirmed, TerminationConfirmed: true})
	first := cancelControl(t, f, service, actor, id)
	failCancellationPreflight(t, f, id, probe)
	historyQuery := fmt.Sprintf(`SELECT o.record,r.receipt FROM operations o JOIN receipts r ON r.subject_id=o.operation_id WHERE o.operation_id='%s' AND r.namespace='operation'`, first.Operation.ID)
	history := upgradeRows(t, f.s.db, historyQuery)
	replay := cancelControl(t, f, service, actor, id)
	if !replay.Replay || replay.Operation != first.Operation {
		t.Fatal("same key did not replay the original accepted receipt", replay)
	}
	second, err := service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationCancel, "explicit-second-cancel", controlRequest(id.AttemptID))
	requireNoError(t, err)
	if second.Replay || second.Operation.ID == first.Operation.ID || second.Operation.Status != domain.OperationAccepted {
		t.Fatal("fresh explicit request did not recover the uninvoked cancellation", second)
	}
	failCancellationPreflight(t, f, id, probe)
	// Reopen durable state before recovery: no in-memory eligibility is sufficient.
	f.restart(t)
	service, actor, _ = controlService(t, f, "a", auth.Read, auth.Operate)
	probe = installCancelProbe(t, f, true, provider.CancellationOutcome{Status: domain.CancellationConfirmed, TerminationConfirmed: true})
	var wg sync.WaitGroup
	results := make(chan operations.Record, 8)
	failures := make(chan error, 8)
	for i := range 8 {
		wg.Go(func() {
			r, err := service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationCancel, fmt.Sprintf("concurrent-recovery-%d", i), controlRequest(id.AttemptID))
			results <- r
			failures <- err
		})
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		requireNoError(t, err)
	}
	var recovered domain.OperationID
	for r := range results {
		if recovered == "" {
			recovered = r.Operation.ID
		}
		if r.Operation.ID != recovered || r.Operation.ID == first.Operation.ID || r.Operation.ID == second.Operation.ID {
			t.Fatal("concurrent fresh requests did not coalesce one new operation", r)
		}
	}
	requireNoError(t, f.step(t))
	if probe.calls.Load() != 1 || f.state(t, id).Cancellation != domain.CancellationConfirmed {
		t.Fatal("recovery did not invoke exactly one successful stop")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 ||
		controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 1 ||
		controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cancel'") != 3 {
		t.Fatal("recovery resubmitted compute or duplicated control work")
	}
	if !reflect.DeepEqual(journal.Remote, f.journal(t, id).Remote) {
		t.Fatal("recovery changed the frozen remote identity")
	}
	if !reflect.DeepEqual(history, upgradeRows(t, f.s.db, historyQuery)) {
		t.Fatal("recovery rewrote the original operation or receipt")
	}
	settled, err := service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationCancel, "after-confirmation", controlRequest(id.AttemptID))
	requireNoError(t, err)
	if settled.Operation.ID != recovered || probe.calls.Load() != 1 {
		t.Fatal("confirmed cancellation was rearmed")
	}
	// The scheduler accounts fresh inactive evidence independently of remote cleanup.
	view, err := f.s.ClaimNext(t.Context(), "after-stop")
	requireNoError(t, err)
	if view.View.AccountReservations[f.profile.AccountScope] != 0 {
		t.Fatal("confirmed inactive execution retained compute capacity")
	}
}

func TestCancellationRecoveryNeverRearmsStartedStop(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	probe := installCancelProbe(t, f, true, provider.CancellationOutcome{})
	probe.lost = true
	first := cancelControl(t, f, service, actor, id)
	requireNoError(t, f.step(t))
	if probe.calls.Load() != 1 || f.state(t, id).Cancellation != domain.CancellationManual {
		t.Fatal("lost stop response did not preserve uncertainty")
	}
	f.restart(t)
	service, actor, _ = controlService(t, f, "a", auth.Read, auth.Operate)
	probe = installCancelProbe(t, f, true, provider.CancellationOutcome{Status: domain.CancellationConfirmed, TerminationConfirmed: true})
	for i := range 3 {
		r, err := service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationCancel, fmt.Sprintf("uncertain-retry-%d", i), controlRequest(id.AttemptID))
		requireNoError(t, err)
		if r.Operation.ID != first.Operation.ID || r.Operation.Status != domain.OperationManualRequired {
			t.Fatal("uncertain stop acquired another mutation permit", r)
		}
	}
	_, err := service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationReconcile, "observe-uncertain-stop", controlRequest(id.AttemptID))
	requireNoError(t, err)
	requireNoError(t, f.step(t))
	if probe.calls.Load() != 0 || !f.state(t, id).Execution.Terminal() {
		t.Fatal("reconciliation repeated stop or missed terminal observation")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cancel'") != 1 {
		t.Fatal("started stop history multiplied")
	}
}

func TestCancellationRecoveryFencesExistingWorker(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	probe := installCancelProbe(t, f, true, provider.CancellationOutcome{})
	cancelControl(t, f, service, actor, id)
	failCancellationPreflight(t, f, id, probe)
	_, err := service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationReconcile, "claim-before-retry", controlRequest(id.AttemptID))
	requireNoError(t, err)
	claim, err := f.s.ClaimRecovery(t.Context(), "old-observer")
	requireNoError(t, err)
	if claim == nil {
		t.Fatal("missing observation lease")
	}
	work, err := f.s.LoadDispatch(t.Context(), *claim)
	requireNoError(t, err)
	_, err = service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationCancel, "fence-recovery", controlRequest(id.AttemptID))
	requireNoError(t, err)
	obs, err := probe.Observe(t.Context(), *work.Journal.Remote)
	requireNoError(t, err)
	_, err = f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &obs})
	if !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("old callback escaped the recovery fence", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM work_leases WHERE kind='dispatch' AND held=1") != 1 {
		t.Fatal("recovery prematurely released the old worker lease")
	}
}

func TestCancellationRecoveryPreservesIdentityMismatch(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	probe := installCancelProbe(t, f, true, provider.CancellationOutcome{})
	probe.verifyError = provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "Fixture identity changed.")
	cancelControl(t, f, service, actor, id)
	var problem *domain.Problem
	if err := f.step(t); !errors.As(err, &problem) || problem.Code != domain.CodeRemoteIdentityMismatch {
		t.Fatal("identity mismatch was hidden", err)
	}
	probe.verifyError = nil
	retry, err := service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationCancel, "identity-mismatch-retry", controlRequest(id.AttemptID))
	requireNoError(t, err)
	if retry.Operation.Status != domain.OperationManualRequired || probe.calls.Load() != 0 || f.journal(t, id).Phase != dispatch.Attention {
		t.Fatal("fresh cancellation bypassed frozen-identity intervention", retry)
	}
}

func TestCancellationPreflightUnclassifiedFailureRemainsUnknown(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	probe := installCancelProbe(t, f, true, provider.CancellationOutcome{})
	probe.verifyError = errors.New("private-provider-failure")
	cancelControl(t, f, service, actor, id)
	var problem *domain.Problem
	if err := f.step(t); !errors.As(err, &problem) || problem.Code != domain.CodeProviderStateUnknown {
		t.Fatal("unclassified verification failure asserted an unsupported cause", err)
	}
	if probe.calls.Load() != 0 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE cancel_started=1") != 0 {
		t.Fatal("failed verification consumed the provider stop permit")
	}
}

func TestCancellationRecoveryMigrationPreservesHistoryAndStopFence(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(fmt.Sprint(started), func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			submittedControl(t, f, id)
			service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
			probe := installCancelProbe(t, f, true, provider.CancellationOutcome{})
			first := cancelControl(t, f, service, actor, id)
			if started {
				probe.lost = true
				requireNoError(t, f.step(t))
			} else {
				failCancellationPreflight(t, f, id, probe)
			}
			const query = `SELECT o.*,r.receipt FROM operations o JOIN receipts r ON r.subject_id=o.operation_id WHERE o.kind='cancel' AND r.namespace='operation' ORDER BY o.operation_seq`
			before := upgradeRows(t, f.s.db, query)
			f.root = copyFixtureAtSchema(t, f.s, 36)
			f.restart(t)
			if !reflect.DeepEqual(before, upgradeRows(t, f.s.db, query)) {
				t.Fatal("migration rewrote historical cancellation or receipt bytes")
			}
			service, actor, _ = controlService(t, f, "a", auth.Read, auth.Operate)
			replay := cancelControl(t, f, service, actor, id)
			if !replay.Replay || replay.Operation != first.Operation {
				t.Fatal("migration changed original-key replay")
			}
			retry, err := service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationCancel, "post-upgrade-recovery", controlRequest(id.AttemptID))
			requireNoError(t, err)
			if started {
				if retry.Operation.ID != first.Operation.ID {
					t.Fatal("migration lost the previous stop permit")
				}
				// Bypass admission in a rolled-back synthetic transaction to prove
				// the database itself rejects a second stop permit for this attempt.
				tx, err := f.s.db.BeginTx(t.Context(), nil)
				requireNoError(t, err)
				defer tx.Rollback()
				const clone = `INSERT INTO operations(workspace_id,operation_id,job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record,new_attempt_id,cancel_started,resource_id)
 SELECT workspace_id,'op_duplicate',job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,json_set(record,'$.Operation.ID','op_duplicate'),new_attempt_id,?,resource_id
 FROM operations WHERE operation_id=?`
				if _, err := tx.ExecContext(t.Context(), clone, 1, string(first.Operation.ID)); err == nil {
					t.Fatal("database granted a second stop permit")
				}
				// The same historical row is legal without a consumed stop permit.
				_, err = tx.ExecContext(t.Context(), clone, 0, string(first.Operation.ID))
				requireNoError(t, err)
			} else if retry.Operation.ID == first.Operation.ID || retry.Operation.Status != domain.OperationAccepted {
				t.Fatal("upgraded preflight failure could not recover", retry)
			}
		})
	}
}
