package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/buildinfo"
)

func TestRunHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := Run(nil, &stdout, &stderr); code != 0 {
		t.Fatalf("Run() code = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "Compute Relay") {
		t.Fatalf("help output = %q, want product name", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"serve"}, &stdout, &stderr); code != 2 {
		t.Fatalf("Run() code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), `unknown command "serve"`) {
		t.Fatalf("stderr = %q, want unknown-command diagnostic", stderr.String())
	}
}

func TestRunVersionJSON(t *testing.T) {
	oldVersion, oldCommit, oldBuiltAt := buildinfo.Version, buildinfo.Commit, buildinfo.BuiltAt
	t.Cleanup(func() {
		buildinfo.Version, buildinfo.Commit, buildinfo.BuiltAt = oldVersion, oldCommit, oldBuiltAt
	})
	buildinfo.Version = "v0.0.1-test"
	buildinfo.Commit = "deadbeef"
	buildinfo.BuiltAt = "2026-09-13T12:00:00Z"

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"version", "--json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run() code = %d, stderr = %q", code, stderr.String())
	}

	var got buildinfo.Info
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode version output: %v", err)
	}
	if got.Version != buildinfo.Version || got.Commit != buildinfo.Commit || got.BuiltAt != buildinfo.BuiltAt {
		t.Fatalf("version output = %#v, want current build metadata", got)
	}
}

func TestRunVersionRejectsPositionals(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"version", "extra"}, &stdout, &stderr); code != 2 {
		t.Fatalf("Run() code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "accepts no positional arguments") {
		t.Fatalf("stderr = %q, want argument diagnostic", stderr.String())
	}
}
