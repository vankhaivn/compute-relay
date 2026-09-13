package provider_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestCoreAndFakeImportBoundaries(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate source")
	}
	internal := filepath.Dir(filepath.Dir(source))
	for _, directory := range []string{"domain", "ports", "provider", "provider/fake"} {
		entries, err := os.ReadDir(filepath.Join(internal, directory))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(internal, directory, entry.Name())
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, spec := range file.Imports {
				name, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					t.Fatal(err)
				}
				if name == "os" || strings.HasPrefix(name, "os/") || name == "net" || strings.HasPrefix(name, "net/") || name == "syscall" || name == "unsafe" || strings.Contains(name, "kaggle") {
					t.Errorf("infrastructure import %q in pure boundary %s", name, path)
				}
				if directory == "domain" && strings.HasPrefix(name, "github.com/vankhaivn/compute-relay/internal/") {
					t.Errorf("domain imports application/infrastructure: %s", name)
				}
			}
		}
	}
}
