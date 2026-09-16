package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/kaggleacceptance"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

func noHash(t *testing.T) programHash {
	return func(context.Context) (domain.SHA256Digest, error) {
		t.Fatal("invalid command reached executable identity")
		return "", nil
	}
}
func noExecution(t *testing.T) execution {
	return func(context.Context, kaggleacceptance.Options) (kaggleacceptance.Report, error) {
		t.Fatal("command unexpectedly started acceptance")
		return kaggleacceptance.Report{}, nil
	}
}
func testHash(context.Context) (domain.SHA256Digest, error) {
	return provider.Digest([]byte("test executable")), nil
}

func TestAcceptanceCLIRejectsMissingOrCrossModeAuthorizationBeforeWork(t *testing.T) {
	for _, args := range [][]string{
		nil, {"arbitrary"}, {"submit", "--root", "state"},
		{"submit", "--root", "state", "--allow-gpu"},
		{"submit", "--root", "state", "--allow-private-staging"},
		{"resume", "--root", "state"},
		{"status", "--root", "state", "--allow-gpu=false"},
		{"resume", "--root", "state", "--allow-read-only", "--machine-shape", "NvidiaTeslaT4"},
		{"collect", "--root", "state", "--allow-read-only"},
		{"collect", "--root", "state", "--allow-read-only", "--collection-key", "bad"},
		{"status", "--root", "state", "--max-wait", "31m"},
		{"status", "--root", "state", "--max-wait", "0s"},
		{"status", "--root", "state", "--config", "unused"},
		{"status", "--root", "state", "unexpected"},
		{"prepare", "--root", "state", "--config", "unused", "--machine-shape", "CPU"},
		{"submit", "--token", "SYNTHETIC_SECRET"},
	} {
		var out, diagnostic bytes.Buffer
		if code := command(context.Background(), args, &out, &diagnostic, noExecution(t), noHash(t)); code != 2 {
			t.Fatal("invalid command accepted", args, code)
		}
		if out.Len() != 0 || strings.Contains(diagnostic.String(), "SYNTHETIC_SECRET") {
			t.Fatal("invalid input reflected")
		}
	}
	for _, args := range [][]string{{"--help"}, {"submit", "--help"}} {
		var out bytes.Buffer
		if code := command(context.Background(), args, &out, &out, noExecution(t), noHash(t)); code != 0 || !strings.Contains(out.String(), "120-second") {
			t.Fatal("help performed work or hid the budget")
		}
	}
}

func TestAcceptanceCLIMapsAllExplicitModesWithoutConfigurationFallback(t *testing.T) {
	config := filepath.Join(t.TempDir(), "config.json")
	c := kaggle.Config{InstanceID: "fixture", Revision: "1", AccountName: "fixture_user", CredentialRef: "env:EXPLICIT_TOKEN", PythonExecutable: filepath.Join(t.TempDir(), "python.exe")}
	raw, _ := json.Marshal(c)
	if err := os.WriteFile(config, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"prepare", "submit", "resume", "collect", "status"} {
		args := []string{mode, "--root", "fixture-state"}
		switch mode {
		case "prepare":
			args = append(args, "--config", config, "--machine-shape", "NvidiaTeslaT4")
		case "submit":
			args = append(args, "--allow-private-staging", "--allow-gpu")
		case "resume":
			args = append(args, "--allow-read-only")
		case "collect":
			args = append(args, "--allow-read-only", "--collection-key", "explicit-collect")
		}
		var out, diagnostic bytes.Buffer
		calls := 0
		execute := func(_ context.Context, o kaggleacceptance.Options) (kaggleacceptance.Report, error) {
			calls++
			digest, _ := testHash(context.Background())
			if o.Mode != mode || o.Root != "fixture-state" || o.ProgramSHA256 != digest || o.AllowGPU != (mode == "submit") || o.AllowPrivateStaging != (mode == "submit") || o.AllowReadOnly != (mode == "resume" || mode == "collect") {
				t.Fatal("incorrect authorization or binary mapping", o.Mode)
			}
			if mode == "prepare" && o.Config != c || mode != "prepare" && o.Config != (kaggle.Config{}) {
				t.Fatal("configuration was remapped or rediscovered")
			}
			status := "passed-live"
			if mode == "prepare" {
				status = "prepared-local"
			} else if mode == "submit" {
				status = "resume-required"
			}
			// This is a command serializer/exit fixture, not live provider evidence.
			return kaggleacceptance.Report{Protocol: 1, Status: status, Evidence: "passed-live", GPUVerified: true, RestartVerified: true}, nil
		}
		if code := command(context.Background(), args, &out, &diagnostic, execute, testHash); code != 0 || calls != 1 {
			t.Fatal("explicit mode failed", mode, code)
		}
		if strings.Contains(out.String(), "EXPLICIT_TOKEN") || strings.Contains(out.String(), "fixture_user") {
			t.Fatal("configuration leaked into report")
		}
		if mode == "submit" && !strings.Contains(diagnostic.String(), "remote wall 120s") {
			t.Fatal("mutation budget was not displayed")
		}
	}
}

func TestAcceptanceCLISanitizesErrorsAndRejectsFixtureQualification(t *testing.T) {
	for _, status := range []string{"passed-offline", "restart-required", "quota-blocked", "passed-live"} {
		var out, diagnostic bytes.Buffer
		execute := func(context.Context, kaggleacceptance.Options) (kaggleacceptance.Report, error) {
			r := kaggleacceptance.Report{Protocol: 1, Status: status, Evidence: status}
			if status == "passed-live" {
				return r, errors.New("SYNTHETIC_SECRET")
			}
			return r, nil
		}
		if code := command(context.Background(), []string{"resume", "--root", "fixture", "--allow-read-only"}, &out, &diagnostic, execute, testHash); code != 1 {
			t.Fatal("incomplete/failed evidence returned success", status)
		}
		if strings.Contains(out.String()+diagnostic.String(), "SYNTHETIC_SECRET") {
			t.Fatal("internal error escaped")
		}
	}
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte(`{"credential":"SYNTHETIC_SECRET"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	args := []string{"prepare", "--root", "unused", "--config", path, "--machine-shape", "NvidiaTeslaT4"}
	if code := command(context.Background(), args, &out, &out, noExecution(t), noHash(t)); code != 1 || strings.Contains(out.String(), "SYNTHETIC_SECRET") {
		t.Fatal("invalid configuration escaped")
	}
}

func TestAcceptanceExecutableIdentityIsStableAndCancellationAware(t *testing.T) {
	one, err := hashProgram(context.Background())
	if err != nil || !one.Valid() {
		t.Fatal(err)
	}
	two, err := hashProgram(context.Background())
	if err != nil || one != two {
		t.Fatal("running binary identity changed", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := hashProgram(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled binary read did not stop", err)
	}
}
