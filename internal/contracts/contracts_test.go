package contracts

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestRepositoryContracts(t *testing.T) {
	root := repositoryRoot(t)
	if err := validateLocalRefs(filepath.Join(root, "api", "schemas")); err != nil {
		t.Fatal(err)
	}
	if err := validateDomainEnums(root); err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	schemaPaths, err := filepath.Glob(filepath.Join(root, "api", "schemas", "*.schema.json"))
	if err != nil || len(schemaPaths) == 0 {
		t.Fatal("schema files missing", err)
	}
	schemas := map[string]*jsonschema.Schema{}
	for _, path := range schemaPaths {
		name := strings.Split(filepath.Base(path), ".")[0]
		schema, err := compiler.Compile(path)
		if err != nil {
			t.Fatalf("compile %s: %v", name, err)
		}
		schemas[name] = schema
	}
	// JobSpec fixtures predate the job-spec schema filename.
	schemas["job"] = schemas["job-spec"]
	err = filepath.WalkDir(filepath.Join(root, "api", "examples"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".json" {
			return err
		}
		t.Run(filepath.Base(path), func(t *testing.T) {
			name := strings.Split(entry.Name(), ".")[0]
			schema := schemas[name]
			if schema == nil {
				t.Fatalf("no schema for example %s", path)
			}
			value, err := readJSONValue(path)
			if err != nil {
				t.Fatal(err)
			}
			valid := filepath.Base(filepath.Dir(path)) != "invalid"
			if err := schema.Validate(value); (err == nil) != valid {
				t.Fatalf("valid=%v, want %v: %v", err == nil, valid, err)
			}
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateRefRejectsRemoteAndEscapingLocations(t *testing.T) {
	apiRoot := t.TempDir()
	containing := filepath.Join(apiRoot, "schemas")
	if err := os.MkdirAll(containing, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(containing, "local.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, ref := range []string{
		"https://example.invalid/schema.json",
		"/etc/passwd",
		"../outside.json",
		`..\outside.json`,
	} {
		if err := validateRef(apiRoot, containing, ref); err == nil {
			t.Fatalf("validateRef(%q) succeeded, want rejection", ref)
		}
	}
	if err := validateRef(apiRoot, containing, "local.json#/value"); err != nil {
		t.Fatalf("local ref rejected: %v", err)
	}
	if err := validateRef(apiRoot, containing, "#/$defs/value"); err != nil {
		t.Fatalf("fragment-only ref rejected: %v", err)
	}
}

func TestSameUniqueStringsIgnoresOrderAndRejectsDrift(t *testing.T) {
	tests := []struct {
		name     string
		actual   []string
		expected []string
		want     bool
	}{
		{name: "same order", actual: []string{"a", "b"}, expected: []string{"a", "b"}, want: true},
		{name: "different order", actual: []string{"b", "a"}, expected: []string{"a", "b"}, want: true},
		{name: "missing", actual: []string{"a"}, expected: []string{"a", "b"}},
		{name: "unexpected", actual: []string{"a", "c"}, expected: []string{"a", "b"}},
		{name: "duplicate actual", actual: []string{"a", "a"}, expected: []string{"a", "b"}},
		{name: "duplicate expected", actual: []string{"a", "b"}, expected: []string{"a", "a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sameUniqueStrings(test.actual, test.expected); got != test.want {
				t.Fatalf("sameUniqueStrings(%v, %v) = %v, want %v", test.actual, test.expected, got, test.want)
			}
		})
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}
