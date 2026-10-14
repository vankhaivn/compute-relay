package api_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
)

func TestManagedHTTPExecutionGatesRemainIndependentAndScoped(t *testing.T) {
	f := setupManagedHTTP(t, true)
	connection := verifiedManagedConnection(t, f)
	if err := f.store.CommitObject(context.Background(), domain.ObjectMetadata{ID: "gate_code", WorkspaceID: "a", Bytes: 1, SHA256: domain.SHA256Digest(strings.Repeat("b", 64))}); err != nil {
		t.Fatal(err)
	}
	body := strings.ReplaceAll(strings.Replace(jobBody, "default-gpu", connection.Selection.Profile, 1), `"object_id":"code"`, `"object_id":"gate_code"`)
	body = strings.Replace(body, `"api_version":`, `"execution_control":"prepare_then_start","api_version":`, 1)
	_, raw := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "gate-job", body, 202)
	job := decodeHTTPJSON[admission.Receipt](t, raw)
	base := job.Links.Self
	before := f.adapter.verifies.Load()
	_, raw = f.call(t, "GET", base+"/readiness", "read", "", "", 200)
	wireSchema(t, "execution-gate", raw)
	state := decodeHTTPJSON[executionauth.Readiness](t, raw)
	if state.State != "preparation_required" || state.StagedBytes != 0 || state.Revision != "" {
		t.Fatal("admission became remote readiness", state)
	}
	request, _ := json.Marshal(executionauth.PrepareRequest{AttemptID: job.AttemptID, MaxPreparationSeconds: 30, ReadyUntil: time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339), MaxStagedBytes: 1024, AuthorizePrivateStaging: true})
	for _, who := range []string{"read", "write", "operate", "manage"} {
		f.call(t, "POST", base+"/prepare", who, "gate-prepare", string(request), 403)
		f.call(t, "POST", base+"/start", who, "gate-start", `{}`, 403)
	}
	headers, raw := f.call(t, "POST", base+"/prepare", "execute", "gate-prepare", string(request), 202)
	wireSchema(t, "execution-gate", raw)
	first := decodeHTTPJSON[executionauth.GateReceipt](t, raw)
	if first.Kind != "prepare" || first.Status != executionauth.Granted || first.Replay {
		t.Fatal("preparation receipt", first)
	}
	location := base + "/gate-authorizations/" + string(first.AuthorizationID)
	if headers.Get("Location") != location {
		t.Fatal("incorrect preparation location")
	}
	_, raw = f.call(t, "GET", location, "execute", "", "", 200)
	wireSchema(t, "execution-gate", raw)
	_, raw = f.call(t, "POST", base+"/prepare", "execute", "gate-prepare", string(request), 202)
	replay := decodeHTTPJSON[executionauth.GateReceipt](t, raw)
	if !replay.Replay || replay.AuthorizationID != first.AuthorizationID {
		t.Fatal("preparation replay lost its original authority")
	}
	_, raw = f.call(t, "GET", base+"/readiness", "read", "", "", 200)
	wireSchema(t, "execution-gate", raw)
	state = decodeHTTPJSON[executionauth.Readiness](t, raw)
	if state.State != "queued" || state.StagedBytes != 0 || state.Revision != "" {
		t.Fatal("permission became verified readiness", state)
	}
	start, _ := json.Marshal(executionauth.StartRequest{AttemptID: job.AttemptID, ReadinessRevision: domain.SHA256Digest(strings.Repeat("a", 64)), MaxRemoteWallSeconds: 120, ExecutionDeadline: time.Now().UTC().Add(time.Minute).Format(time.RFC3339), AuthorizeCompute: true})
	f.call(t, "POST", base+"/start", "execute", "too-early-start", string(start), 409)
	f.call(t, "POST", base+"/prepare", "execute", "second-prepare", string(request), 409)
	f.call(t, "POST", base+"/prepare", "execute", "gate-prepare", strings.Replace(string(request), `:30,`, `:31,`, 1), 409)
	for _, route := range []string{"prepare", "start"} {
		f.call(t, "POST", base+"/"+route, "execute", "unknown-field", `{"unknown":true}`, 400)
		f.call(t, "POST", base+"/"+route, "execute", "oversized-gate", strings.Repeat(" ", executionauth.MaxRequestBytes+1), 413)
		f.call(t, "POST", base+"/"+route, "b", "foreign-token", `{}`, 403)
	}
	f.call(t, "POST", strings.Replace(base, "/a/", "/b/", 1)+"/prepare", "b", "foreign-job", string(request), 404)
	f.call(t, "GET", strings.Replace(base, "/a/", "/b/", 1)+"/readiness", "b", "", "", 404)
	f.call(t, "GET", strings.Replace(location, "/a/", "/b/", 1), "b", "", "", 404)
	f.call(t, "GET", location, "read", "", "", 403)
	f.call(t, "GET", base+"/readiness", "execute", "", "", 403)
	f.call(t, "GET", base+"/prepare", "execute", "", "", 405)
	f.call(t, "GET", base+"/start", "execute", "", "", 405)
	f.call(t, "POST", base+"/readiness", "read", "unused-key", `{}`, 405)
	if f.adapter.verifies.Load() != before {
		t.Fatal("gate HTTP request called a provider")
	}
	if err := f.store.RevokeToken(context.Background(), f.tokenIDs["execute"]); err != nil {
		t.Fatal(err)
	}
	f.call(t, "POST", base+"/prepare", "execute", "gate-prepare", string(request), 401)
}
