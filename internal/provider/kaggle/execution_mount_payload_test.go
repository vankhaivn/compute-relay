package kaggle

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestExecutionSourceCarriesDatasetMountIdentity(t *testing.T) {
	c, plan, prepared := executionFixture(t)
	request, err := buildExecutionRequest(c, DefaultStagingPolicy(), DefaultExecutionPolicy(), plan, prepared)
	if err != nil {
		t.Fatal(err)
	}
	prefix := executionBootstrap + "\nrun_remote(json.loads(base64.b64decode(\""
	suffix := "\", validate=True)))\n"
	if !strings.HasPrefix(request.Source, prefix) || !strings.HasSuffix(request.Source, suffix) {
		t.Fatal("execution source framing changed")
	}
	encoded := strings.TrimSuffix(strings.TrimPrefix(request.Source, prefix), suffix)
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		DatasetOwner string `json:"dataset_owner"`
		DatasetSlug  string `json:"dataset_slug"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		t.Fatal("invalid execution bootstrap payload")
	}
	if payload.DatasetOwner != c.AccountName || payload.DatasetOwner+"/"+payload.DatasetSlug != request.Dataset {
		t.Fatal("dataset mount identity is not bound to the staged reference", payload, request.Dataset)
	}
}
