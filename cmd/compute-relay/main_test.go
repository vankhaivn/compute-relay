package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

// This test-only entry invokes the real main, not an alternate runtime command.
// No test behavior, fixture provider or environment hook is shipped in production.
func TestMain(m *testing.M) {
	if os.Getenv("COMPUTE_RELAY_CLI_TEST_CHILD") == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}
func childCommand(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.Env = append(os.Environ(), "COMPUTE_RELAY_CLI_TEST_CHILD=1")
	return cmd
}
func runChild(t *testing.T, want int, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := childCommand(ctx, args...)
	var out, diagnostic bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &diagnostic
	err := cmd.Run()
	code := 0
	if err != nil {
		if cmd.ProcessState == nil {
			t.Fatal(err)
		}
		code = cmd.ProcessState.ExitCode()
	}
	if code != want {
		t.Fatalf("command %s code %d want %d: %s", args[0], code, want, diagnostic.String())
	}
	if bytes.Contains(out.Bytes(), []byte("cr1_")) || bytes.Contains(diagnostic.Bytes(), []byte("cr1_")) {
		t.Fatal("token reflected on console")
	}
	return out.Bytes()
}
func TestExecutableLocalLifecycleAndSignalReopen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runtime")
	var before, after struct {
		InstallationID string `json:"installation_id"`
		Dispatch       bool   `json:"dispatch_enabled"`
	}
	if json.Unmarshal(runChild(t, 0, "init", "--root", root), &before) != nil || before.InstallationID == "" || before.Dispatch {
		t.Fatal("invalid initialized state")
	}
	runChild(t, 1, "init", "--root", root)
	runChild(t, 0, "workspace", "create", "--root", root, "--id", "app")
	runChild(t, 1, "workspace", "create", "--root", root, "--id", "app")
	// The runtime root is already private on every native platform, outside its stores.
	tokenPath := filepath.Join(root, "app-token")
	var issued struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(runChild(t, 0, "token", "issue", "--root", root, "--workspace", "app", "--scope", "read", "--output", tokenPath), &issued) != nil || issued.ID == "" {
		t.Fatal("invalid token receipt")
	}
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSuffix(string(tokenBytes), "\n")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server := childCommand(ctx, "serve", "--root", root, "--listen", "127.0.0.1:0")
	output, err := server.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic bytes.Buffer
	server.Stderr = &diagnostic
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if server.ProcessState == nil {
			_ = server.Process.Kill()
			_ = server.Wait()
		}
	})
	line := make(chan []byte, 1)
	go func() { raw, _ := bufio.NewReader(io.LimitReader(output, 4096)).ReadBytes('\n'); line <- raw }()
	var announcement struct {
		Address  string `json:"address"`
		Dispatch bool   `json:"dispatch_enabled"`
	}
	select {
	case raw := <-line:
		if json.Unmarshal(raw, &announcement) != nil || announcement.Address == "" || announcement.Dispatch {
			t.Fatal("invalid listening announcement")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not announce")
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, Timeout: 5 * time.Second}
	req, err := http.NewRequest("GET", "http://"+announcement.Address+"/readyz", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("X-Compute-Relay-Mode") != "local-admission-only" {
		t.Fatal("real CLI HTTP boundary failed")
	}
	runChild(t, 1, "state", "--root", root) // OS ownership survives across command entry points.
	if runtime.GOOS == "windows" {
		// Windows os.Process.Signal does not provide POSIX interrupt delivery.
		// This is abrupt-death/reopen evidence, not a Windows graceful-signal claim.
		if err := server.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		if err := server.Wait(); err == nil {
			t.Fatal("killed server exited successfully")
		}
	} else {
		if err := server.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		if err := server.Wait(); err != nil {
			t.Fatal("signal shutdown failed", err, diagnostic.String())
		}
	}
	if bytes.Contains(diagnostic.Bytes(), []byte(token)) {
		t.Fatal("server diagnostic exposed token")
	}
	if json.Unmarshal(runChild(t, 0, "state", "--root", root), &after) != nil || after.InstallationID != before.InstallationID || after.Dispatch {
		t.Fatal("reopen changed installation")
	}
	runChild(t, 0, "token", "revoke", "--root", root, "--id", issued.ID)
	runChild(t, 0, "workspace", "disable", "--root", root, "--id", "app")
}

func TestExecutableManagedHelpAndInvalidModeFlags(t *testing.T) {
	help := runChild(t, 0, "serve", "--help")
	for _, required := range []string{"--managed-python ABS", "--managed-max-workers 2", "separate API execution authorization"} {
		if !bytes.Contains(help, []byte(required)) {
			t.Fatal("executable help omitted managed mode semantics")
		}
	}
	root := filepath.Join(t.TempDir(), "must-not-create")
	python := filepath.Join(t.TempDir(), "synthetic-python3")
	for _, flags := range [][]string{
		{"--managed-python", "relative-python"},
		{"--managed-max-workers", "2"},
		{"--managed-python", python, "--allow-gpu"},
		{"--managed-python", python, "--managed-allow-internet", "--managed-allow-internet=false"},
	} {
		runChild(t, 2, append([]string{"serve", "--root", root}, flags...)...)
		if _, err := os.Lstat(root); !os.IsNotExist(err) {
			t.Fatal("invalid executable flags touched runtime state")
		}
	}
}

// Pins the managed companion surface that installed application clients drive: exact receipts,
// readiness body and feature names, a minimal process environment and owned-state refusal.
func TestExecutableManagedCompanionClientSurface(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("managed workers require the macOS credential store")
	}
	root := filepath.Join(t.TempDir(), "runtime")
	var initialized, reopened struct {
		InstallationID string `json:"installation_id"`
	}
	if json.Unmarshal(runChild(t, 0, "init", "--root", root), &initialized) != nil || initialized.InstallationID == "" {
		t.Fatal("invalid init receipt")
	}
	var workspace map[string]any
	if json.Unmarshal(runChild(t, 0, "workspace", "create", "--root", root, "--id", "app"), &workspace) != nil ||
		workspace["id"] != "app" || workspace["enabled"] != true {
		t.Fatal("invalid workspace receipt", workspace)
	}
	tokenPath := filepath.Join(root, "app-token")
	var issued struct {
		ID          string   `json:"id"`
		WorkspaceID string   `json:"workspace_id"`
		Scopes      []string `json:"scopes"`
		Delivery    string   `json:"delivery"`
		ExpiresAt   string   `json:"expires_at"`
	}
	raw := runChild(t, 0, "token", "issue", "--root", root, "--workspace", "app", "--ttl", "2592000s",
		"--output", tokenPath, "--scope", "read", "--scope", "write", "--scope", "operate")
	if json.Unmarshal(raw, &issued) != nil || issued.ID == "" || issued.WorkspaceID != "app" || issued.Delivery != "written-once" ||
		strings.Join(issued.Scopes, ",") != "read,write,operate" {
		t.Fatal("invalid token receipt", string(raw))
	}
	if _, err := time.Parse(time.RFC3339Nano, issued.ExpiresAt); err != nil {
		t.Fatal("invalid token expiry", issued.ExpiresAt)
	}
	info, err := os.Stat(tokenPath)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatal("token file is not private", err)
	}
	tokenBytes, _ := os.ReadFile(tokenPath)
	token := strings.TrimSuffix(string(tokenBytes), "\n")
	if !tokenPattern.MatchString(token) {
		t.Fatal("token format changed")
	}
	// Never executed: managed startup performs no account read.
	python := filepath.Join(t.TempDir(), "synthetic-python3")
	if err := os.WriteFile(python, []byte("synthetic executable fixture; must never run"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server := exec.CommandContext(ctx, os.Args[0], "serve", "--root", root, "--listen", "127.0.0.1:0",
		"--managed-python", python, "--managed-machine-shape", "cpu")
	server.Env = []string{"LANG=C.UTF-8", "COMPUTE_RELAY_CLI_TEST_CHILD=1"} // No PATH or HOME.
	output, err := server.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic bytes.Buffer
	server.Stderr = &diagnostic
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if server.ProcessState == nil {
			_ = server.Process.Kill()
			_ = server.Wait()
		}
	})
	line := make(chan []byte, 1)
	go func() { raw, _ := bufio.NewReader(io.LimitReader(output, 4096)).ReadBytes('\n'); line <- raw }()
	var announcement struct {
		Status   string `json:"status"`
		Mode     string `json:"mode"`
		Address  string `json:"address"`
		Dispatch bool   `json:"dispatch_enabled"`
	}
	select {
	case raw := <-line:
		if json.Unmarshal(raw, &announcement) != nil || announcement.Status != "listening" || announcement.Mode != "managed-workers" ||
			!announcement.Dispatch || !strings.HasPrefix(announcement.Address, "127.0.0.1:") {
			t.Fatal("invalid managed listening receipt", string(raw), diagnostic.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("managed serve did not announce", diagnostic.String())
	}
	get := func(path string) map[string]any {
		t.Helper()
		client := &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, Timeout: 5 * time.Second}
		req, err := http.NewRequest("GET", "http://"+announcement.Address+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var body map[string]any
		if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&body) != nil {
			t.Fatalf("GET %s failed with %d", path, response.StatusCode)
		}
		return body
	}
	if ready := get("/readyz"); len(ready) != 1 || ready["status"] != "ready" {
		t.Fatal("readiness body changed", ready)
	}
	described := get("/v1/info")
	if described["api_version"] != "compute-connector/v1alpha1" || described["runtime_version"] == "" {
		t.Fatal("invalid runtime info", described)
	}
	features := map[string]bool{}
	list, _ := described["features"].([]any)
	for _, feature := range list {
		if name, ok := feature.(string); ok {
			features[name] = true
		}
	}
	for _, required := range []string{"workspace_auth", "job_admission", "managed_connections", "attempt_authorization",
		"manual_result_collection", "job_logs", "connection_configuration", "job_cleanup"} {
		if !features[required] {
			t.Fatal("managed runtime stopped advertising", required)
		}
	}
	runChild(t, 1, "state", "--root", root)
	if err := server.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := server.Wait(); err != nil {
		t.Fatal("managed signal shutdown failed", err, diagnostic.String())
	}
	if json.Unmarshal(runChild(t, 0, "state", "--root", root), &reopened) != nil || reopened.InstallationID != initialized.InstallationID {
		t.Fatal("reopen changed installation")
	}
}

var tokenPattern = regexp.MustCompile(`^cr1_[A-Za-z0-9_-]{43}$`)
