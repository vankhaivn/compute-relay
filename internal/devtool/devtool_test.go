package devtool

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGoFilesDeterministicAndSkipsGeneratedEnvironments(t *testing.T) {
	root := t.TempDir()
	write := func(path string) {
		t.Helper()
		fullPath := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(fullPath, []byte("package example\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	write("z.go")
	write("a/a_test.go")
	write("vendor/ignored.go")
	write(".venv/ignored.go")
	write("notes.txt")

	got, err := GoFiles(root)
	if err != nil {
		t.Fatalf("GoFiles() error = %v", err)
	}
	want := []string{filepath.Join("a", "a_test.go"), "z.go"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GoFiles() = %#v, want %#v", got, want)
	}
}
