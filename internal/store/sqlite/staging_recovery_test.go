package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

type stagingRecoveryProbe struct {
	*probeProvider
	observeCalls  int
	unavailable   bool
	planDigest    domain.SHA256Digest
	preparationID domain.OperationID
}

func (p *stagingRecoveryProbe) ReconcilePreparation(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.PreparationObservation, error) {
	p.observeCalls++
	if plan.Digest() != p.planDigest || id != p.preparationID {
		return provider.PreparationObservation{}, errors.New("original staging identity changed")
	}
	if p.unavailable {
		return provider.PreparationObservation{}, errors.New("synthetic staging observation failure")
	}
	return p.probeProvider.ReconcilePreparation(ctx, plan, id)
}

func TestReconcileExhaustedStagingPreservesConsumedPermitAndNeverPreparesAgain(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "pending"
		if ready {
			name = "ready"
		}
		t.Run(name, func(t *testing.T) {
			scenario := fake.DefaultScenario()
			scenario.PreparationReady = ready
			f := managedFixture(t, scenario)
			id := f.seed(t, "a", 1, false)
			grant, authService, authPrincipal := grantAuthorization(t, f, id)
			f.adapter.losePrepare = true
			if err := f.step(t); err == nil {
				t.Fatal("lost preparation acknowledgement did not report a problem")
			}
			original := f.journal(t, id)
			probe := &stagingRecoveryProbe{probeProvider: f.adapter, unavailable: true, planDigest: original.Plan.Digest(), preparationID: original.PreparationID}
			f.registry = provider.NewSnapshotRegistry()
			if err := f.registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe); err != nil {
				t.Fatal(err)
			}
			var err error
			f.engine, err = dispatch.New(managedDispatchRepository{f.s}, f.registry, f.blobs, nil, f.clock, dispatch.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			for i := 1; i < dispatch.MaxFailures; i++ {
				if err := f.step(t); err == nil {
					t.Fatal("failed observation was cleared")
				}
			}
			before := f.journal(t, id)
			if before.Phase != dispatch.Attention || before.Failures != dispatch.MaxFailures || before.SubmitStarted || before.Prepared != nil {
				t.Fatalf("wrong exhausted fixture: %+v", before)
			}
			permit, err := authService.Get(context.Background(), authPrincipal, "a", id.JobID, grant.AuthorizationID)
			if err != nil || permit.Status != executionauth.Consumed {
				t.Fatal("original authorization was not consumed", err)
			}
			service, principal, _ := controlService(t, f, "a", auth.Read, auth.Operate)
			accepted, err := service.Submit(context.Background(), principal, "a", id.JobID, domain.OperationReconcile, "staging-original-key", controlRequest(id.AttemptID))
			if err != nil || accepted.Operation.Status != domain.OperationAccepted {
				t.Fatal("exhausted preparation reconcile rejected", err)
			}
			queued := f.journal(t, id)
			if queued.Phase != dispatch.Staging || queued.SubmitStarted || queued.PreparationID != before.PreparationID || !reflect.DeepEqual(queued.Plan, before.Plan) {
				t.Fatal("control changed original frozen preparation")
			}
			for _, key := range []string{"staging-original-key", "staging-coalesced-key"} {
				replay, err := service.Submit(context.Background(), principal, "a", id.JobID, domain.OperationReconcile, key, controlRequest(id.AttemptID))
				if err != nil || replay.Operation.ID != accepted.Operation.ID || !replay.Replay {
					t.Fatal("reconcile replay multiplied pending operations", err)
				}
			}
			probe.unavailable = false
			if err := f.step(t); err != nil {
				t.Fatal(err)
			}
			current := f.journal(t, id)
			finished, err := service.Get(context.Background(), principal, "a", accepted.Operation.ID)
			if err != nil || finished.Operation.Status != domain.OperationSucceeded || finished.Effect != operations.ObservationRefreshed {
				t.Fatal("staging observation left the reconcile operation pending", err, finished)
			}
			expectedPhase := dispatch.Staging
			if ready {
				expectedPhase = dispatch.Ready
			}
			if current.Phase != expectedPhase || current.SubmitStarted || current.PreparationID != before.PreparationID || !reflect.DeepEqual(current.Plan, before.Plan) {
				t.Fatal("staging observation invented another intent or submitted")
			}
			afterPermit, err := authService.Get(context.Background(), authPrincipal, "a", id.JobID, grant.AuthorizationID)
			if err != nil || !reflect.DeepEqual(afterPermit, permit) || controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations") != 1 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 || controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 0 {
				t.Fatal("recovery changed permit, attempt or submission history", err)
			}
			stats := f.backend.Stats()
			if stats.PrepareCalls != 1 || stats.SubmitCalls != 0 || stats.Executions != 0 || probe.observeCalls != dispatch.MaxFailures {
				t.Fatalf("recovery repeated provider mutations or skipped observation: %+v reads=%d", stats, probe.observeCalls)
			}
		})
	}
}
