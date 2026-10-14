package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// The runner CI exports real outputs from its synthetic CPU tests, then this check
// validates those exact bytes against the already published result-manifest contract.
// The Python runner check validates its input examples using the runtime decoder.
func TestRunnerGeneratedResultContracts(t *testing.T) {
	directory := os.Getenv("CR_RUNNER_RESULTS_DIR")
	if directory == "" {
		t.Skip("generated runner outputs are validated by the dedicated offline runner job")
	}
	paths, err := filepath.Glob(filepath.Join(directory, "*.json"))
	if err != nil || len(paths) < 10 {
		t.Fatal("runner result corpus missing or incomplete", err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	schema, err := compiler.Compile(filepath.Join(repositoryRoot(t), "api", "schemas", "result-manifest.v1alpha1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(value); err != nil {
			t.Fatalf("actual runner result rejected: %v", err)
		}
		seen[value.(map[string]any)["phase"].(string)] = true
	}
	for _, phase := range []string{"completed", "failed", "setup_failed", "resource_check_failed", "timed_out"} {
		if !seen[phase] {
			t.Fatalf("missing result phase %s", phase)
		}
	}
}
