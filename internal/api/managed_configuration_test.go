package api_test

import (
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
	var original admission.Receipt
	if json.Unmarshal(raw, &original) != nil {
		t.Fatal("invalid original job receipt")
	}
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
	var current connections.Connection
	if json.Unmarshal(raw, &current) != nil || current.Configuration == nil || current.Configuration.MaxRemoteWallSeconds != 7200 || current.Selection == nil || current.Selection.MaxRemoteWallSeconds != 7200 || current.Selection.Profile == connection.Selection.Profile {
		t.Fatal("configuration did not expose a new immutable selection")
	}
	_, raw = f.call(t, "POST", path+"/actions", "manage", "configuration-save", body, 202)
	if replay := readManagedOperation(t, raw); !replay.Replay || replay.ID != saved.ID || replay.Status != saved.Status {
		t.Fatal("configuration replay lost its original receipt")
	}
	f.call(t, "POST", path+"/actions", "manage", "configuration-save", strings.Replace(body, "7200", "3600", 1), 409)
	f.call(t, "POST", path+"/actions", "manage", "configuration-stale", body, 409)
	_, raw = f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "before-configuration-job", oldJobBody, 202)
	var replay admission.Receipt
	if json.Unmarshal(raw, &replay) != nil || replay.JobID != original.JobID || !replay.Replay {
		t.Fatal("old admitted job no longer replays")
	}
	f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "stale-selection-job", oldJobBody, 409)
	newJobBody := strings.Replace(oldJobBody, connection.Selection.Profile, current.Selection.Profile, 1)
	newJobBody = strings.Replace(newJobBody, `"remote_wall_seconds":120`, `"remote_wall_seconds":3600`, 1)
	f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "after-configuration-job", newJobBody, 202)
	if f.adapter.verifies.Load() != before {
		t.Fatal("saving or reading configuration contacted a provider")
	}
	_, raw = f.call(t, "GET", "/v1/info", "read", "", "", 200)
	var info struct {
		Features []string `json:"features"`
	}
	if json.Unmarshal(raw, &info) != nil || !strings.Contains(strings.Join(info.Features, ","), "connection_configuration") {
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
