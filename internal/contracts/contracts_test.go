package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestRepositoryContracts(t *testing.T) {
	root := repositoryRoot(t)
	if err := Check(root); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}

func TestBuildLockIsDeterministic(t *testing.T) {
	root := repositoryRoot(t)
	first, err := BuildLock(root)
	if err != nil {
		t.Fatalf("BuildLock(first): %v", err)
	}
	second, err := BuildLock(root)
	if err != nil {
		t.Fatalf("BuildLock(second): %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("BuildLock() returned different snapshots for unchanged files")
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
