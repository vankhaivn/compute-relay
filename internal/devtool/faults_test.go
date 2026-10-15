package devtool

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

func TestFaultCatalogReferencesExistingTests(t *testing.T) {
	cases, err := loadFaultCases(filepath.Join("..", ".."))
	if err != nil || len(cases) != 28 {
		t.Fatal("fault catalog drift", err)
	}
}

func TestFaultCatalogRejectsMissingOrUntraceableEvidence(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"internal/fixture"} {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(dir)), 0700); err != nil {
			t.Fatal(err)
		}
	}
	ref := faultTest{File: "internal/fixture/fixture_test.go", Name: "TestProof"}
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(ref.File)), []byte("package fixture\nimport \"testing\"\nfunc TestProof(t *testing.T) {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cases := make([]faultCase, 28)
	for i := range cases {
		cases[i] = faultCase{Number: i + 1, Scenario: fmt.Sprintf("Case %d.", i+1), Tests: []faultTest{ref}}
	}
	encode := func(cases []faultCase) []byte {
		raw, err := json.Marshal(map[string]any{"version": 1, "cases": cases})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	good := encode(cases)
	if _, err := checkFaultCatalog(root, good); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"count", "number", "blank-scenario", "duplicate-scenario", "empty", "missing-function", "missing-file", "traversal", "duplicate-reference", "unknown-field", "trailing"} {
		t.Run(mode, func(t *testing.T) {
			var copy struct{ Cases []faultCase }
			if err := json.Unmarshal(good, &copy); err != nil {
				t.Fatal(err)
			}
			c := copy.Cases
			switch mode {
			case "count":
				c = c[:27]
			case "number":
				c[0].Number = 2
			case "blank-scenario":
				c[0].Scenario = " "
			case "duplicate-scenario":
				c[1].Scenario = c[0].Scenario
			case "empty":
				c[0].Tests = nil
			case "missing-function":
				c[0].Tests[0].Name = "TestMissing"
			case "missing-file":
				c[0].Tests[0].File = "internal/fixture/missing_test.go"
			case "traversal":
				c[0].Tests[0].File = "internal/../../outside_test.go"
			case "duplicate-reference":
				c[0].Tests = append(c[0].Tests, c[0].Tests[0])
			}
			raw := encode(c)
			if mode == "unknown-field" {
				raw = append([]byte(`{"unknown":1,`), raw[1:]...)
			}
			if mode == "trailing" {
				raw = append(raw, []byte(` {}`)...)
			}
			if _, err := checkFaultCatalog(root, raw); err == nil {
				t.Fatal("invalid catalog accepted")
			}
		})
	}
}

func TestFaultReferenceRequiresGoTestSignature(t *testing.T) {
	for _, source := range []string{
		"func TestProof(t *testing.T) {}",
		"func TestProof() {}",
		"func TestProof(t *testing.B) {}",
		"func TestProof(t *testing.T) int { return 0 }",
		"func TestProof[T any](t *testing.T) {}",
		"func (s suite) TestProof(t *testing.T) {}",
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "fixture_test.go", "package fixture\n"+source, 0)
		if err != nil {
			t.Fatal(err)
		}
		fn := file.Decls[0].(*ast.FuncDecl)
		if got, want := isTestFunction(fn), source == "func TestProof(t *testing.T) {}"; got != want {
			t.Fatalf("%s: got %v, want %v", source, got, want)
		}
	}
}
