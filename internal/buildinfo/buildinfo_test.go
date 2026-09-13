package buildinfo

import "testing"

func TestCurrentReturnsSnapshot(t *testing.T) {
	oldVersion, oldCommit, oldBuiltAt := Version, Commit, BuiltAt
	t.Cleanup(func() {
		Version, Commit, BuiltAt = oldVersion, oldCommit, oldBuiltAt
	})

	Version = "v0.1.0"
	Commit = "abc123"
	BuiltAt = "2026-09-13T12:00:00Z"

	got := Current()
	if got.Version != Version || got.Commit != Commit || got.BuiltAt != BuiltAt {
		t.Fatalf("Current() = %#v, want configured build metadata", got)
	}
}
