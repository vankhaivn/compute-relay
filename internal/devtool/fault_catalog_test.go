package devtool

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

//go:embed fault-matrix.json
var faultCatalog []byte

var faultTestName = regexp.MustCompile(`^Test[A-Z0-9][A-Za-z0-9_]*$`)

const (
	faultModule = "github.com/vankhaivn/compute-relay/"
)

type faultTest struct {
	File string `json:"file"`
	Name string `json:"name"`
}
type faultCase struct {
	Number   int         `json:"number"`
	Scenario string      `json:"scenario"`
	Tests    []faultTest `json:"tests"`
}

func (t faultTest) key() string { return faultModule + path.Dir(t.File) + ":" + t.Name }

// This catalog is reviewed repository evidence, not user-supplied test commands.
// The full Go suite runs these tests; this check keeps exact source references
// valid. Semantic coverage and live-provider limitations still need review.
func loadFaultCases(root string) ([]faultCase, error) {
	return checkFaultCatalog(root, faultCatalog)
}

func checkFaultCatalog(root string, raw []byte) ([]faultCase, error) {
	var catalog struct {
		Version int         `json:"version"`
		Cases   []faultCase `json:"cases"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&catalog) != nil || d.Decode(new(any)) != io.EOF || catalog.Version != 1 || len(catalog.Cases) != 28 {
		return nil, errors.New("fault matrix must contain exactly 28 version-1 cases")
	}
	functions := map[string]map[string]bool{}
	scenarios := map[string]bool{}
	for i, c := range catalog.Cases {
		if c.Number != i+1 || strings.TrimSpace(c.Scenario) == "" || scenarios[c.Scenario] || len(c.Tests) == 0 || len(c.Tests) > 8 {
			return nil, fmt.Errorf("invalid or untraceable fault case %d", i+1)
		}
		scenarios[c.Scenario] = true
		seen := map[string]bool{}
		for _, ref := range c.Tests {
			if !strings.HasPrefix(ref.File, "internal/") || path.Clean(ref.File) != ref.File || strings.ContainsAny(ref.File, "\\:") || !strings.HasSuffix(ref.File, "_test.go") || !faultTestName.MatchString(ref.Name) || seen[ref.key()] {
				return nil, fmt.Errorf("invalid test reference for FM%02d", c.Number)
			}
			seen[ref.key()] = true
			if functions[ref.File] == nil {
				file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, filepath.FromSlash(ref.File)), nil, 0)
				if err != nil {
					return nil, fmt.Errorf("parse %s: %w", ref.File, err)
				}
				functions[ref.File] = map[string]bool{}
				for _, decl := range file.Decls {
					if fn, ok := decl.(*ast.FuncDecl); ok && isTestFunction(fn) {
						functions[ref.File][fn.Name.Name] = true
					}
				}
			}
			if !functions[ref.File][ref.Name] {
				return nil, fmt.Errorf("missing evidence %s::%s", ref.File, ref.Name)
			}
		}
	}
	return catalog.Cases, nil
}

func isTestFunction(fn *ast.FuncDecl) bool {
	if fn.Recv != nil || fn.Type.TypeParams != nil || fn.Type.Results != nil || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
		return false
	}
	param := fn.Type.Params.List[0]
	if len(param.Names) != 1 {
		return false
	}
	pointer, ok := param.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := pointer.X.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "T" {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "testing"
}
