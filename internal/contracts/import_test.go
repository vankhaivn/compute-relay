package contracts

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/internal/packaging"
)

func TestBundleWireShapeMatchesSchema(t *testing.T) {
	cases := []struct {
		schema string
		value  any
	}{
		{"bundle-manifest.v1", packaging.Manifest{Version: packaging.Version, Files: []packaging.File{{Path: "main.py", Bytes: 0, SHA256: strings.Repeat("a", 64), Executable: false}}}},
	}
	for _, tc := range cases {
		compiler := jsonschema.NewCompiler()
		compiler.DefaultDraft(jsonschema.Draft2020)
		schema, err := compiler.Compile(filepath.Join(repositoryRoot(t), "api", "schemas", tc.schema+".schema.json"))
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(tc.value)
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(value); err != nil {
			t.Fatal(err)
		}
	}
}
