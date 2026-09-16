package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	if code := Run([]string{"SYNTHETIC_SECRET"}, &stdout, &stderr); code != 2 {
		t.Fatalf("Run() code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") || strings.Contains(stderr.String(), "SYNTHETIC_SECRET") {
		t.Fatalf("stderr = %q, want sanitized unknown-command diagnostic", stderr.String())
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

func TestLocalRoutesKeepHelpAndCancellationFreeOfStateWrites(t *testing.T) {
	for _, command := range []string{"init", "state", "serve", "workspace", "token", "validate"} {
		var stdout, stderr bytes.Buffer
		if code := Run([]string{command, "--help"}, &stdout, &stderr); code != 0 || stdout.Len() == 0 || stderr.Len() != 0 {
			t.Fatal("local help routing failed", command, code)
		}
	}
	path := filepath.Join(t.TempDir(), "must-not-create")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	if code := RunContext(ctx, []string{"init", "--root", path}, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
		t.Fatal("cancelled init succeeded", code)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("cancelled action created state")
	}
}
