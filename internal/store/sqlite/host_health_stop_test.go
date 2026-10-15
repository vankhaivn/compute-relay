package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestHostHealthStopIdentityIsReservedAndExactAttemptScoped(t *testing.T) {
	first := hostHealthStopID("installation_a", "workspace_a", "job_a", "attempt_a")
	if !first.Valid() || !strings.HasPrefix(string(first), "op_health_") || first != hostHealthStopID("installation_a", "workspace_a", "job_a", "attempt_a") {
		t.Fatal("system stop identity is not stable and valid")
	}
	for _, other := range []domain.OperationID{
		hostHealthStopID("installation_b", "workspace_a", "job_a", "attempt_a"),
		hostHealthStopID("installation_a", "workspace_b", "job_a", "attempt_a"),
		hostHealthStopID("installation_a", "workspace_a", "job_b", "attempt_a"),
		hostHealthStopID("installation_a", "workspace_a", "job_a", "attempt_b"),
	} {
		if first == other {
			t.Fatal("system stop identity escaped exact attempt or installation scope")
		}
	}
}

func TestHostHealthStopProjectionRequiresConfirmedExactCancellation(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	journal := f.journal(t, id)
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	var systemID domain.OperationID
	var installation domain.RuntimeInstallationID
	err := f.s.write(t.Context(), func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if err := tx.QueryRowContext(ctx, "SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&installation); err != nil {
			return err
		}
		systemID = hostHealthStopID(installation, id.WorkspaceID, id.JobID, id.AttemptID)
		op, err := domain.NewOperation(systemID, id.WorkspaceID, id.JobID, id.AttemptID, domain.OperationCancel, now)
		if err != nil {
			return err
		}
		request := operations.Request{AttemptID: id.AttemptID, Reason: "host_critical_stopped"}
		hash, err := request.Digest(id.JobID, domain.OperationCancel)
		if err != nil {
			return err
		}
		accepted := operationRow{Record: operations.Record{Operation: op, Effect: operations.Pending}, Request: request}
		if err := insertOperation(ctx, tx, operations.Command{WorkspaceID: id.WorkspaceID, JobID: id.JobID, Kind: domain.OperationCancel, Request: request, RequestHash: hash}, accepted); err != nil {
			return err
		}
		running := accepted
		running.Operation, err = op.Transition(domain.OperationRunning, nil, now)
		if err != nil {
			return err
		}
		running.Effect, running.Started = operations.CancellationRequested, true
		if err := saveOperation(ctx, tx, accepted, running); err != nil {
			return err
		}
		_, err = finishControl(ctx, tx, running, domain.OperationManualRequired, operations.ManualRequired, controlProblem(domain.CodeRemoteExecutionUnresolved, true), now)
		return err
	})
	requireNoError(t, err)
	for _, execution := range []domain.ExecutionState{domain.ExecutionRunning, domain.ExecutionFailed, domain.ExecutionSucceeded, domain.ExecutionCancelled} {
		t.Run(string(execution), func(t *testing.T) {
			candidate := journal
			activity, release := domain.RemoteActivityActive, domain.ReleaseEvidenceUnknown
			if execution.Terminal() {
				activity, release, candidate.Phase = domain.RemoteActivityInactive, domain.ReleaseEvidenceProviderReported, dispatch.Collectible
			}
			candidate.Observation = &provider.Observation{Remote: *candidate.Remote, Execution: execution, RemoteActivity: activity, ReleaseEvidence: release, ObservedAt: f.clock.Now(), RawState: string(execution)}
			if !candidate.Valid() {
				t.Fatal("invalid observation fixture")
			}
			err := withTx(t.Context(), f.s.db, func(tx *sql.Tx) error {
				problem, err := hostCriticalStopProblem(t.Context(), tx, id.WorkspaceID, id.JobID, id.AttemptID, candidate)
				if err != nil || (problem != nil) != (execution == domain.ExecutionCancelled) {
					t.Fatal("termination was inferred from failure or pending control", execution, problem, err)
				}
				if problem != nil && problem.Details[domain.ReasonDetail] != "host_critical_stopped" {
					t.Fatal("confirmed observation lost stable reason")
				}
				return nil
			})
			requireNoError(t, err)
			if execution == domain.ExecutionCancelled {
				raw, err := json.Marshal(candidate)
				requireNoError(t, err)
				var foreign dispatch.Journal
				requireNoError(t, json.Unmarshal(raw, &foreign))
				foreign.Plan.Job.Identity.Nonce = strings.Repeat("f", 64)
				foreign.Prepared.Identity = foreign.Plan.Job.Identity
				foreign.Prepared.PlanSHA256 = foreign.Plan.Digest()
				foreign.Remote.Identity = foreign.Plan.Job.Identity
				foreign.Observation.Remote.Identity = foreign.Plan.Job.Identity
				if !foreign.Valid() {
					t.Fatal("invalid foreign identity fixture")
				}
				err = withTx(t.Context(), f.s.db, func(tx *sql.Tx) error {
					problem, err := hostCriticalStopProblem(t.Context(), tx, id.WorkspaceID, id.JobID, id.AttemptID, foreign)
					if !errors.Is(err, ErrCorrupt) || problem != nil {
						t.Fatal("foreign ownership nonce claimed confirmed stop", problem, err)
					}
					return nil
				})
				requireNoError(t, err)
			}
		})
	}
	original, err := service.Get(t.Context(), actor, "a", systemID)
	if err != nil || original.Operation.Status != domain.OperationManualRequired || original.TerminationConfirmed || original.Effect != operations.ManualRequired {
		t.Fatal("projection rewrote the original ambiguous operation", original.Operation.Status, err)
	}
}
