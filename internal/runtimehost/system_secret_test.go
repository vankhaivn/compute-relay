package runtimehost

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

const systemWorkloadCanary = "synthetic-workload-canary/with+url?characters=only"

func systemSecretFixture(t *testing.T, mode fake.SubmitMode) (*managedTestFixture, *systemDispatchEffects, *systemDispatchTrace, scheduler.Identity, *executionauth.Service, auth.Principal, executionauth.Readiness, *workloadsecret.Service, *workloadsecret.Pool) {
	t.Helper()
	scenario := fake.DefaultScenario()
	scenario.SeparateStart = true
	scenario.EphemeralSecrets = true
	scenario.Mode = mode
	f, e, trace := newSystemDispatch(t, scenario)
	connection := f.connection(t, "secret-connection", "SYNTHETIC_FIRST")
	spec := f.jobSpec(t, connection)
	spec.ExecutionControl = "prepare_then_start"
	spec.WorkloadSecrets = []workloadsecret.Slot{{Name: "ingest", Exposure: "file"}}
	job := f.submitJob(t, "secret-job", spec)
	id := scheduler.Identity{WorkspaceID: "app", JobID: job.JobID, AttemptID: job.AttemptID}
	gate, p, ready := systemReadyGate(t, f, id)
	e.start = func(_ context.Context, _ provider.Prepared, _ *provider.StartAuthorization, m *workloadsecret.Material, out provider.SubmissionOutcome) provider.SubmissionOutcome {
		if m == nil || string(m.Values["ingest"]) != systemWorkloadCanary {
			t.Error("wrong synthetic material")
		}
		return out
	}
	return f, e, trace, id, gate, p, ready, f.services.secrets, f.host.secretPool
}
func systemSecretRequest(f *managedTestFixture, id scheduler.Identity, ready executionauth.Readiness) []byte {
	raw, _ := json.Marshal(workloadsecret.Supply{AttemptID: id.AttemptID, ReadinessRevision: ready.Revision, ExpiresAt: f.fixtureClock().Now().Add(time.Minute).UTC().Format(time.RFC3339), Values: map[string][]byte{"ingest": []byte(systemWorkloadCanary)}})
	return raw
}
func systemSecretStartRequest(f *managedTestFixture, id scheduler.Identity, ready executionauth.Readiness, secret domain.OperationID) []byte {
	var request executionauth.StartRequest
	_ = json.Unmarshal(systemStartGrantRaw(f, id, ready), &request)
	request.SecretAuthorizationID = secret
	raw, _ := json.Marshal(request)
	return raw
}
func assertSystemSecretSanitized(t *testing.T, f *managedTestFixture, id scheduler.Identity) {
	t.Helper()
	record := f.dispatchRecord(t, id)
	ready, err := f.services.authorizations.Readiness(t.Context(), f.actor, "app", id.JobID)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{record.Problem, record.Preparation, record.Attempt.State, ready} {
		raw := systemJSON(t, value)
		if bytes.Contains(raw, []byte(systemWorkloadCanary)) || bytes.Contains(raw, []byte(base64.StdEncoding.EncodeToString([]byte(systemWorkloadCanary)))) {
			t.Fatal("public status leaked workload material")
		}
	}
}
