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
