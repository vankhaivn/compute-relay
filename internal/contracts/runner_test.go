package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestRunnerContractsAndAssetLock(t *testing.T) {
	root := repositoryRoot(t)
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	schema, err := compiler.Compile(filepath.Join(root, "runner", "manifest.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(root, "runner", "examples", "request.*.json"))
	if err != nil || len(paths) < 3 {
		t.Fatal("missing runner contract fixtures", err)
	}
	for _, path := range paths {
		value, err := readJSONValue(path)
		if err != nil {
			t.Fatal(err)
		}
		valid := !strings.Contains(filepath.Base(path), ".invalid.")
		if err := schema.Validate(value); (err == nil) != valid {
			t.Fatalf("runner fixture %s: %v", filepath.Base(path), err)
		}
	}
	type entry struct {
		Path   string `json:"path"`
		Bytes  int    `json:"bytes"`
		SHA256 string `json:"sha256"`
	}
	var lock struct {
		Version int     `json:"lock_version"`
		Files   []entry `json:"files"`
	}
	if err := readStrictJSON(filepath.Join(root, "runner", "assets.lock.json"), &lock); err != nil {
		t.Fatal(err)
	}
	if lock.Version != 1 {
		t.Fatal("unknown runner asset lock version")
	}
	var actual []entry
	err = filepath.WalkDir(filepath.Join(root, "runner"), func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || d.Name() == "assets.lock.json" || (filepath.Ext(path) != ".py" && filepath.Ext(path) != ".json") {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			t.Fatalf("symlinked runner asset: %s", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		actual = append(actual, entry{filepath.ToSlash(rel), len(data), hex.EncodeToString(sum[:])})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(actual, func(i, j int) bool { return actual[i].Path < actual[j].Path })
	if !reflect.DeepEqual(actual, lock.Files) {
		t.Fatal("runner asset lock drift; review and run python runner/check.py --lock")
	}
}

// The runner CI exports real outputs from its synthetic CPU tests, then this check
// validates those exact bytes against the already published result-manifest contract.
// Default Go-only runs still validate the input schema and asset lock above.
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
