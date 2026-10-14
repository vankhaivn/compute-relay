package testsupport

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/packaging"
)

// Bundle builds an offline code archive from logical project paths.
func Bundle(t testing.TB, files map[string][]byte) []byte {
	t.Helper()
	root := t.TempDir()
	paths := make([]string, 0, len(files))
	for name, data := range files {
		file := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, data, 0600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, name)
	}
	sort.Strings(paths)
	project, err := packaging.OpenProject(root, packaging.DefaultLimits(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer project.Close()
	selection, err := project.Prepare(t.Context(), paths)
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if _, err := selection.Write(t.Context(), &archive); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}
