package runtimehost

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func newSystemGate(t *testing.T, mode fake.SubmitMode, maxActive ...int) (*managedTestFixture, *systemDispatchEffects, *systemDispatchTrace) {
	t.Helper()
	scenario := fake.DefaultScenario()
	scenario.SeparateStart = true
	scenario.Mode = mode
	f, e, trace := newSystemDispatch(t, scenario)
	if len(maxActive) > 0 {
		f.adapter.profileHook = func(p admission.Profile) admission.Profile { p.MaxActiveAttempts = maxActive[0]; return p }
	}
	return f, e, trace
}
func systemGateSeed(t *testing.T, f *managedTestFixture, n int) scheduler.Identity {
	t.Helper()
	connection := f.connection(t, "gate-connection-"+fmt.Sprint(n), "SYNTHETIC_FIRST")
	if connection.Selection.MaxActiveAttempts > 1 {
		profile, _, err := f.host.store.ReadProfile(t.Context(), connection.Selection.Profile)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.host.store.ConfigureAccount(t.Context(), profile.AccountScope, scheduler.AccountPolicy{MaxActive: connection.Selection.MaxActiveAttempts}); err != nil {
			t.Fatal(err)
		}
	}
	spec := f.jobSpec(t, connection)
	spec.ExecutionControl = "prepare_then_start"
	job := f.submitJob(t, "gate-job-"+fmt.Sprint(n), spec)
	return scheduler.Identity{WorkspaceID: "app", JobID: job.JobID, AttemptID: job.AttemptID}
}
func systemGateStep(t *testing.T, f *managedTestFixture) (bool, error) {
	t.Helper()
	f.advance(t, 10*time.Millisecond)
	return f.services.dispatcher.RunOnce(t.Context(), "gate-system-worker")
}
func systemAuthorizationService(t *testing.T, f *managedTestFixture, w domain.WorkspaceID, scopes ...auth.Scope) (*executionauth.Service, auth.Principal) {
	t.Helper()
	actor, _ := f.principal(t, w, scopes...)
	return f.services.authorizations, actor
}
func systemPrepareGrant(t *testing.T, f *managedTestFixture, id scheduler.Identity) (*executionauth.Service, auth.Principal, executionauth.GateReceipt, []byte) {
	t.Helper()
	service, p := systemAuthorizationService(t, f, "app", auth.Execute, auth.Read)
	raw := systemJSON(t, executionauth.PrepareRequest{AttemptID: id.AttemptID, MaxPreparationSeconds: 60, ReadyUntil: f.fixtureClock().Now().Add(30 * time.Minute).UTC().Format(time.RFC3339), MaxStagedBytes: 1 << 20, AuthorizePrivateStaging: true})
	receipt, err := service.Prepare(t.Context(), p, "app", id.JobID, "prepare-key-"+string(id.JobID), raw)
	if err != nil {
		t.Fatal(err)
	}
	return service, p, receipt, raw
}
func systemReadyGate(t *testing.T, f *managedTestFixture, id scheduler.Identity) (*executionauth.Service, auth.Principal, executionauth.Readiness) {
	t.Helper()
	s, p, _, _ := systemPrepareGrant(t, f, id)
	if worked, err := systemGateStep(t, f); err != nil || !worked {
		t.Fatal("prepare phase", worked, err)
	}
	ready, err := s.Readiness(t.Context(), p, "app", id.JobID)
	if err != nil || ready.State != "ready" || !ready.Revision.Valid() || ready.StagedBytes <= 0 {
		t.Fatal("readiness", ready, err)
	}
	return s, p, ready
}
func systemStartGrantRaw(f *managedTestFixture, id scheduler.Identity, ready executionauth.Readiness) []byte {
	raw, _ := json.Marshal(executionauth.StartRequest{AttemptID: id.AttemptID, ReadinessRevision: ready.Revision, MaxRemoteWallSeconds: 10, ExecutionDeadline: f.fixtureClock().Now().Add(9 * time.Second).UTC().Format(time.RFC3339), AuthorizeCompute: true})
	return raw
}
func systemClaim(t *testing.T, f *managedTestFixture) dispatch.Work {
	t.Helper()
	r, err := f.host.store.ClaimNext(context.Background(), "system-boundary")
	if err != nil || r.Claim == nil {
		t.Fatal("claim", err)
	}
	w, err := f.host.store.LoadDispatch(t.Context(), *r.Claim)
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func systemControlService(t *testing.T, f *managedTestFixture, w domain.WorkspaceID, scopes ...auth.Scope) (*operations.Service, auth.Principal, string) {
	t.Helper()
	p, secret := f.principal(t, w, scopes...)
	return f.controls(t), p, secret
}
func systemCancelControl(t *testing.T, f *managedTestFixture, s *operations.Service, p auth.Principal, id scheduler.Identity) operations.Record {
	t.Helper()
	r, err := s.Submit(t.Context(), p, id.WorkspaceID, id.JobID, domain.OperationCancel, "cancel-original-key", systemControlBody(t, id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
