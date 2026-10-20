package runtimehost

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/localstorage"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func TestInputReleaseHTTPPreviewConfirmReplayAndInventory(t *testing.T) {
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectedForCleanup(t, "input-release")
	result, err := f.host.store.ReadCollection(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	requireNoError(t, err)
	request := retention.InputReleaseRequest{}
	for _, file := range result.Files {
		if file.Role == "output" {
			request.Jobs = []retention.InputReleaseJob{{JobID: job.JobID, ReleaseRequest: retention.ReleaseRequest{AttemptID: job.AttemptID, ArtifactID: file.ID, SHA256: file.Object.SHA256}}}
			break
		}
	}
	if len(request.Jobs) != 1 {
		t.Fatal("no output")
	}
	f.startHTTP(t)
	before := f.backend.Stats()
	_, info := f.request(t, http.MethodGet, "/v1/info", "", "", nil, http.StatusOK)
	_, inventory := f.request(t, http.MethodGet, "/v1/workspaces/app/local-storage?refresh=true", "", "", nil, http.StatusOK)
	var sizes localstorage.Inventory
	requireNoError(t, json.Unmarshal(inventory, &sizes))
	if sizes.InputsBytes <= 0 || sizes.ResultsBytes <= 0 || sizes.DatabaseBytes <= 0 || sizes.TotalBytes != sizes.InputsBytes+sizes.ResultsBytes+sizes.DatabaseBytes+sizes.OtherBytes {
		t.Fatal("bad local inventory", sizes)
	}
	path := "/v1/workspaces/app/input-release-preview"
	_, raw := f.request(t, http.MethodPost, path, "", "application/json", systemJSON(t, request), http.StatusOK)
	var preview retention.InputReleasePreview
	requireNoError(t, json.Unmarshal(raw, &preview))
	if preview.Files < 1 || preview.ReleasableBytes <= 0 {
		t.Fatal("no releasable inputs", preview)
	}
	request.PreviewSHA256 = preview.PreviewSHA256
	body := systemJSON(t, request)
	f.request(t, http.MethodPost, "/v1/workspaces/app/input-releases", "", "application/json", body, http.StatusBadRequest)
	_, released := f.request(t, http.MethodPost, "/v1/workspaces/app/input-releases", "input-original", "application/json", body, http.StatusOK)
	var receipt retention.InputReleaseReceipt
	requireNoError(t, json.Unmarshal(released, &receipt))
	if receipt.Bytes != preview.ReleasableBytes || receipt.ReclaimedBytes != receipt.Bytes || receipt.PendingFiles != 0 {
		t.Fatal("preview/receipt mismatch", preview, receipt)
	}
	// Result publication remains readable after only its uploaded inputs are gone.
	_, err = f.host.store.ReadCollection(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	requireNoError(t, err)
	f.reopen(t)
	f.startHTTP(t)
	_, replay := f.request(t, http.MethodPost, "/v1/workspaces/app/input-releases", "input-original", "application/json", body, http.StatusOK)
	if string(replay) != string(released) {
		t.Fatal("restart changed input receipt")
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	for _, name := range []string{"common", "info", "input-release", "local-storage"} {
		document, e := schemas.Files.ReadFile(name + ".v1alpha1.schema.json")
		requireNoError(t, e)
		var value any
		requireNoError(t, json.Unmarshal(document, &value))
		requireNoError(t, compiler.AddResource("https://input.invalid/"+name+".v1alpha1.schema.json", value))
	}
	for _, wire := range []struct {
		schema string
		raw    []byte
	}{{"input-release.v1alpha1.schema.json#/$defs/request", body}, {"input-release.v1alpha1.schema.json#/$defs/preview", raw}, {"input-release.v1alpha1.schema.json#/$defs/receipt", released}, {"local-storage.v1alpha1.schema.json", inventory}, {"info.v1alpha1.schema.json", info}} {
		schema, e := compiler.Compile("https://input.invalid/" + wire.schema)
		requireNoError(t, e)
		var value any
		requireNoError(t, json.Unmarshal(wire.raw, &value))
		requireNoError(t, schema.Validate(value))
	}
	if after := f.backend.Stats(); after != before {
		t.Fatal("local release performed provider effects", before, after)
	}
}
