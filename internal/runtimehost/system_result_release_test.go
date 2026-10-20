package runtimehost

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func TestResultReleaseHTTPExpiresExactAttemptWithoutProviderEffects(t *testing.T) {
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "release-result")
	result, err := f.host.store.ReadCollection(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	requireNoError(t, err)
	var request retention.ReleaseRequest
	for _, file := range result.Files {
		if file.Role == "output" {
			request = retention.ReleaseRequest{AttemptID: job.AttemptID, ArtifactID: file.ID, SHA256: file.Object.SHA256}
			break
		}
	}
	if !request.Valid() {
		t.Fatal("fixture has no output")
	}
	f.startHTTP(t)
	_, info := f.request(t, http.MethodGet, "/v1/info", "", "", nil, http.StatusOK)
	var capabilities struct {
		Features []string `json:"features"`
	}
	requireNoError(t, json.Unmarshal(info, &capabilities))
	found := false
	for _, feature := range capabilities.Features {
		found = found || feature == "result_release"
	}
	if !found {
		t.Fatal("release capability not advertised")
	}
	path := "/v1/workspaces/app/jobs/" + string(job.JobID) + "/release-results"
	body := systemJSON(t, request)
	before := f.backend.Stats()
	f.request(t, http.MethodPost, path, "", "application/json", body, http.StatusBadRequest)
	_, raw := f.request(t, http.MethodPost, path, "release-original", "application/json", body, http.StatusOK)
	var receipt retention.ReleaseReceipt
	requireNoError(t, json.Unmarshal(raw, &receipt))
	if receipt.PendingFiles != 0 || receipt.Files != len(result.Files) || receipt.ReclaimedBytes != receipt.Bytes || receipt.Bytes < 1 {
		t.Fatal("release did not confirm physical reclamation", receipt)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	for _, name := range []string{"common", "result-release", "info"} {
		document, err := schemas.Files.ReadFile(name + ".v1alpha1.schema.json")
		requireNoError(t, err)
		var value any
		requireNoError(t, json.Unmarshal(document, &value))
		requireNoError(t, compiler.AddResource("https://release.invalid/"+name+".v1alpha1.schema.json", value))
	}
	for _, wire := range []struct {
		schema string
		raw    []byte
	}{{"result-release.v1alpha1.schema.json#/$defs/request", body},
		{"result-release.v1alpha1.schema.json#/$defs/receipt", raw}, {"info.v1alpha1.schema.json", info}} {
		schema, err := compiler.Compile("https://release.invalid/" + wire.schema)
		requireNoError(t, err)
		var value any
		requireNoError(t, json.Unmarshal(wire.raw, &value))
		requireNoError(t, schema.Validate(value))
	}
	f.request(t, http.MethodGet, "/v1/workspaces/app/jobs/"+string(job.JobID)+"/artifacts?attempt_id="+string(job.AttemptID),
		"", "", nil, http.StatusGone)
	f.reopen(t)
	f.startHTTP(t)
	_, replay := f.request(t, http.MethodPost, path, "release-original", "application/json", body, http.StatusOK)
	if string(replay) != string(raw) {
		t.Fatal("restart changed exact release receipt", string(raw), string(replay))
	}
	if after := f.backend.Stats(); after != before {
		t.Fatal("local release performed provider work", before, after)
	}
}
