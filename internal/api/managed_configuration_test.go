package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
)

func TestManagedHTTPLiveConfigurationPreservesOldAdmissionReplay(t *testing.T) {
	f := setupManagedHTTP(t, true)
	connection := verifiedManagedConnection(t, f)
	path := "/v1/workspaces/a/connections/" + connection.ID
	oldJobBody := strings.Replace(jobBody, "default-gpu", connection.Selection.Profile, 1)
	_, raw := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "before-configuration-job", oldJobBody, 202)
	original := decodeHTTPJSON[admission.Receipt](t, raw)
	body := fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":7200}}`, connection.Revision)
	before := f.adapter.verifies.Load()
	for _, who := range []string{"read", "write", "operate", "execute"} {
		f.call(t, "POST", path+"/actions", who, "configuration-scope", body, 403)
	}
	f.call(t, "POST", strings.Replace(path, "/a/", "/b/", 1)+"/actions", "b", "configuration-cross-workspace", body, 404)
	headers, raw := f.call(t, "POST", path+"/actions", "manage", "configuration-save", body, 202)
	saved := readManagedOperation(t, raw)
	if saved.Status != "succeeded" || saved.Action != "configure" || saved.ConnectionRevision != connection.Revision+1 || headers.Get("Location") == "" {
		t.Fatal("configuration was not complete on the same running server")
	}
	_, raw = f.call(t, "GET", headers.Get("Location"), "manage", "", "", 200)
	if got := readManagedOperation(t, raw); got.ID != saved.ID || got.Status != "succeeded" {
		t.Fatal("configuration receipt unavailable")
	}
	_, raw = f.call(t, "GET", path, "read", "", "", 200)
	wireSchema(t, "connection", raw)
	managedSafeResponse(t, raw)
	current := decodeHTTPJSON[connections.Connection](t, raw)
	if current.Configuration == nil || current.Configuration.MaxRemoteWallSeconds != 7200 || current.Selection == nil || current.Selection.MaxRemoteWallSeconds != 7200 || current.Selection.Profile == connection.Selection.Profile {
		t.Fatal("configuration did not expose a new immutable selection")
	}
	_, raw = f.call(t, "POST", path+"/actions", "manage", "configuration-save", body, 202)
	if replay := readManagedOperation(t, raw); !replay.Replay || replay.ID != saved.ID || replay.Status != saved.Status {
		t.Fatal("configuration replay lost its original receipt")
	}
	f.call(t, "POST", path+"/actions", "manage", "configuration-save", strings.Replace(body, "7200", "3600", 1), 409)
	f.call(t, "POST", path+"/actions", "manage", "configuration-stale", body, 409)
	_, raw = f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "before-configuration-job", oldJobBody, 202)
	replay := decodeHTTPJSON[admission.Receipt](t, raw)
	if replay.JobID != original.JobID || !replay.Replay {
		t.Fatal("old admitted job no longer replays")
	}
	f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "compatible-selection-job", oldJobBody, 202)
	newJobBody := strings.Replace(oldJobBody, connection.Selection.Profile, current.Selection.Profile, 1)
	newJobBody = strings.Replace(newJobBody, `"remote_wall_seconds":120`, `"remote_wall_seconds":3600`, 1)
	f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "after-configuration-job", newJobBody, 202)
	if f.adapter.verifies.Load() != before {
		t.Fatal("saving or reading configuration contacted a provider")
	}
	_, raw = f.call(t, "GET", "/v1/info", "read", "", "", 200)
	info := decodeHTTPJSON[struct {
		Features []string `json:"features"`
	}](t, raw)
	if !strings.Contains(strings.Join(info.Features, ","), "connection_configuration") {
		t.Fatal("live configuration capability was not advertised")
	}
}

func TestManagedHTTPConfigurationRejectsInvalidShapeWithoutMutation(t *testing.T) {
	f := setupManagedHTTP(t, true)
	connection := verifiedManagedConnection(t, f)
	path := "/v1/workspaces/a/connections/" + connection.ID
	_, before := f.call(t, "GET", path, "read", "", "", 200)
	for i, fields := range []string{
		`"action":"configure"`,
		`"action":"configure","configuration":null`,
		`"action":"configure","configuration":{}`,
		`"action":"configure","configuration":{"max_remote_wall_seconds":0}`,
		`"action":"configure","configuration":{"max_remote_wall_seconds":86401}`,
		`"action":"configure","configuration":{"max_remote_wall_seconds":1.5}`,
		`"action":"configure","configuration":{"max_remote_wall_seconds":true}`,
		`"action":"configure","configuration":{"max_remote_wall_seconds":7200,"automatic_rotation":true}`,
		`"action":"configure","configuration":{"max_remote_wall_seconds":7200,"max_remote_wall_seconds":3600}`,
		`"action":"configure","configuration":{"Max_Remote_Wall_Seconds":7200}`,
		`"action":"configure","configuration":{"max_remote_wall_seconds":7200},"credentials":{"api_token":"SYNTHETIC"}`,
		`"action":"check","configuration":{"max_remote_wall_seconds":7200}`,
	} {
		body := fmt.Sprintf(`{"expected_revision":%d,%s}`, connection.Revision, fields)
		f.call(t, "POST", path+"/actions", "manage", fmt.Sprintf("invalid-configuration-%d", i), body, 400)
	}
	_, after := f.call(t, "GET", path, "read", "", "", 200)
	if string(before) != string(after) || f.adapter.verifies.Load() != 1 {
		t.Fatal("invalid configuration partially changed the connection")
	}
}

func TestManagedHTTPPendingCheckRetriesExactAdmission(t *testing.T) {
	f := setupManagedHTTP(t, true)
	connection := verifiedManagedConnection(t, f)
	body := strings.Replace(jobBody, "default-gpu", connection.Selection.Profile, 1)
	action := fmt.Sprintf(`{"action":"check","expected_revision":%d}`, connection.Revision)
	f.call(t, "POST", "/v1/workspaces/a/connections/"+connection.ID+"/actions", "manage", "pending-check", action, 202)
	headers, raw := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "pending-job", body, 429)
	var response struct {
		Error struct {
			Code              string            `json:"code"`
			RecommendedAction string            `json:"recommended_action"`
			Details           map[string]string `json:"details"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Error.Code != "REQUEST_LIMIT_EXCEEDED" || response.Error.RecommendedAction != "wait" || response.Error.Details["reason"] != "connection_update_pending" || headers.Get("Retry-After") != "1" {
		t.Fatal("pending update did not expose retry metadata", string(raw))
	}
	if worked, err := f.service.RunOnce(context.Background()); err != nil || !worked {
		t.Fatal("check failed", err)
	}
	f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "pending-job", body, 202)
}
