package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/internal/httpsinput"
)

func TestHTTPSIngestFixturesMatchRequestDecoder(t *testing.T) {
	root := repositoryRoot(t)
	var manifest manifest
	if err := readStrictJSON(filepath.Join(root, filepath.FromSlash(manifestPath)), &manifest); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, contract := range manifest.Roots {
		if contract.Schema != "schemas/object-ingest.v1alpha1.schema.json" {
			continue
		}
		found = true
		for _, set := range []struct {
			paths []string
			valid bool
		}{{contract.Valid, true}, {contract.Invalid, false}} {
			for _, path := range set.paths {
				data, err := os.ReadFile(filepath.Join(root, "api", filepath.FromSlash(path)))
				if err != nil {
					t.Fatal(err)
				}
				var request httpsinput.Request
				err = json.Unmarshal(data, &request)
				if (err == nil) != set.valid {
					t.Fatalf("fixture %s: decoder valid=%v, want %v", path, err == nil, set.valid)
				}
			}
		}
	}
	if !found || operationStatus("ingestObject") != "implemented-offline" {
		t.Fatal("ingestion contract or implemented-handler status missing")
	}
}

func TestHTTPSIngestWireShapeAndRuntimePolicyBoundary(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	schema, err := compiler.Compile(filepath.Join(repositoryRoot(t), "api", "schemas", "object-ingest.v1alpha1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []httpsinput.Request{
		{URL: "https://example.org/file"},
		{URL: "https://example.org/file?version=1", SHA256: strings.Repeat("a", 64)},
	} {
		data, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(value); err != nil {
			t.Fatal("serialized request rejected", err)
		}
	}
	// Structural schemas do not authorize hosts. The runtime must additionally reject
	// this structurally valid request before any network/credential access.
	value := map[string]any{"url": "https://127.0.0.1/metadata"}
	if err := schema.Validate(value); err != nil {
		t.Fatal("test expects a structural/runtime policy distinction", err)
	}
	if err := (httpsinput.Request{URL: value["url"].(string)}).Validate(); err == nil {
		t.Fatal("runtime permitted loopback despite the structural-only contract")
	}
}
