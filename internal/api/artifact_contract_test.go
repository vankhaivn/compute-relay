package api

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
)

type artifactNoExternalSchemas struct{}

func (artifactNoExternalSchemas) Load(string) (any, error) {
	return nil, errors.New("external schema access forbidden")
}

func TestArtifactActualHTTPResponsesMatchEmbeddedContract(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	compiler.UseLoader(artifactNoExternalSchemas{})
	for _, name := range []string{"common.v1alpha1.schema.json", "artifact.v1alpha1.schema.json"} {
		raw, err := schemas.Files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if json.Unmarshal(raw, &value) != nil {
			t.Fatal("invalid embedded schema")
		}
		if err := compiler.AddResource("https://artifact.invalid/"+name, value); err != nil {
			t.Fatal(err)
		}
	}
	f, _, _ := artifactHTTPFixture(t, []byte("checked bytes"))
	for _, tc := range []struct{ suffix, definition string }{
		{"?attempt_id=att_one&limit=1", "page"},
		{"/art_output?attempt_id=att_one", "metadata"},
	} {
		schema, err := compiler.Compile("https://artifact.invalid/artifact.v1alpha1.schema.json#/$defs/" + tc.definition)
		if err != nil {
			t.Fatal(err)
		}
		resp, raw := f.request(t, "GET", "/v1/workspaces/a/jobs/job_one/artifacts"+tc.suffix, "read", nil, nil)
		var value map[string]any
		if resp.StatusCode != 200 || json.Unmarshal(raw, &value) != nil || schema.Validate(value) != nil {
			t.Fatal("actual response differs from its public schema", resp.StatusCode, string(raw))
		}
		for _, forbidden := range []string{"object_id", "account_scope", "credential_ref", "provider_reference"} {
			if strings.Contains(string(raw), `"`+forbidden+`"`) {
				t.Fatal("private internal field leaked", forbidden)
			}
		}
		value["object_id"] = "private"
		if schema.Validate(value) == nil {
			t.Fatal("wire schema admitted an internal field")
		}
	}
}
