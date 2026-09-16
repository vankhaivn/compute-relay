package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/kaggleacceptance"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

// Helper-only entry into the real CLI implementation, without a fixture provider.
func TestAcceptanceCLIProcessChild(t *testing.T) {
	if os.Getenv("COMPUTE_RELAY_CLI_CHILD") != "1" {
		t.Skip("subprocess helper only")
	}
	for i, argument := range os.Args {
		if argument == "--" {
			os.Exit(command(context.Background(), os.Args[i+1:], os.Stdout, os.Stderr, kaggleacceptance.Run, hashProgram))
		}
	}
	t.Fatal("missing child command arguments")
}

func TestAcceptanceLocalCommandsReopenActualStateWithoutProviderAccess(t *testing.T) {
	root := filepath.Join(t.TempDir(), "acceptance")
	config := filepath.Join(t.TempDir(), "config.json")
	c := kaggle.Config{InstanceID: "fixture", Revision: "1", AccountName: "fixture_user", CredentialRef: "env:NEVER_RESOLVE_THIS", PythonExecutable: filepath.Join(t.TempDir(), "missing-python.exe")}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, raw, 0600); err != nil {
		t.Fatal(err)
	}
	run := func(arguments ...string) kaggleacceptance.Report {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		args := append([]string{"-test.run=^TestAcceptanceCLIProcessChild$", "--"}, arguments...)
		child := exec.CommandContext(ctx, os.Args[0], args...)
		child.Env = append(os.Environ(), "COMPUTE_RELAY_CLI_CHILD=1", "NEVER_RESOLVE_THIS=SYNTHETIC_SECRET")
		output, err := child.CombinedOutput()
		var report kaggleacceptance.Report
		if err != nil || json.Unmarshal(output, &report) != nil {
			t.Fatal("local CLI failed", err, string(output))
		}
		if report.Status != "prepared-local" || report.Evidence != "not-tested" || report.GPUVerified || report.RestartVerified || report.FullM1Acceptance || strings.Contains(string(output), "SYNTHETIC_SECRET") || strings.Contains(string(output), "fixture_user") {
			t.Fatal("local CLI inflated or exposed evidence", report)
		}
		return report
	}
	prepared := run("prepare", "--root", root, "--config", config, "--machine-shape", "NvidiaTeslaT4")
	before, err := os.ReadFile(filepath.Join(root, "acceptance.json"))
	if err != nil {
		t.Fatal(err)
	}
	status := run("status", "--root", root)
	after, err := os.ReadFile(filepath.Join(root, "acceptance.json"))
	if err != nil || string(before) != string(after) || status.JobID != prepared.JobID || status.AttemptID != prepared.AttemptID || status.AttemptNumber != 1 {
		t.Fatal("local reopen replaced the admitted identity", err)
	}
	for _, name := range []string{"submission-process.json", "resume-process.json"} {
		if _, err := os.Lstat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatal("local command created remote/restart evidence", name, err)
		}
	}
}
