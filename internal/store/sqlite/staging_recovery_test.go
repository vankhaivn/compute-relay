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

func (p *stagingRecoveryProbe) ReconcilePrepare(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.PreparationObservation, error) {
	p.observeCalls++
	if plan.Digest() != p.planDigest || id != p.preparationID {
		return provider.PreparationObservation{}, errors.New("original staging identity changed")
	}
	if p.unavailable {
		return provider.PreparationObservation{}, errors.New("synthetic staging observation failure")
	}
	return p.probeProvider.ReconcilePrepare(ctx, plan, id)
}

func TestStagingReconcilePreservesGrantAndAttemptRows(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	grant, authService, p := grantAuthorization(t, f, id)
	f.adapter.losePrepare = true
	if err := f.step(t); err == nil {
		t.Fatal("lost ACK hidden")
	}
	original := f.journal(t, id)
	probe := &stagingRecoveryProbe{probeProvider: f.adapter, unavailable: true, planDigest: original.Plan.Digest(), preparationID: original.PreparationID}
	registry := provider.NewSnapshotRegistry()
	if err := registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe); err != nil {
		t.Fatal(err)
	}
	var err error
	f.engine, err = dispatch.New(f.s, registry, f.blobs, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < dispatch.MaxFailures; i++ {
		_ = f.step(t)
	}
	permit, err := authService.Get(t.Context(), p, "a", id.JobID, grant.AuthorizationID)
	if err != nil || permit.Status != executionauth.Consumed {
		t.Fatal(err)
	}
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	if _, err = service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationReconcile, "staging-storage", controlRequest(id.AttemptID)); err != nil {
		t.Fatal(err)
	}
	queued := f.journal(t, id)
	if queued.Phase != dispatch.Staging || queued.SubmitStarted || queued.PreparationID != original.PreparationID || !reflect.DeepEqual(queued.Plan, original.Plan) {
		t.Fatal("reconcile changed original journal identity")
	}
	probe.unavailable = false
	if err = f.step(t); err != nil {
		t.Fatal(err)
	}
	after, err := authService.Get(t.Context(), p, "a", id.JobID, grant.AuthorizationID)
	if err != nil || !reflect.DeepEqual(after, permit) || controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='execution'") != 1 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 || controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 0 {
		t.Fatal("reconcile changed durable history", err)
	}
}
