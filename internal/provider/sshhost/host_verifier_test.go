package sshhost

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestHostVerifierShellIsPOSIX(t *testing.T) {
	if output, err := exec.Command("/bin/sh", "-n", "-c", hostVerifierShell).CombinedOutput(); err != nil {
		t.Fatalf("fixed host command is invalid: %s: %v", output, err)
	}
	docker := filepath.Join(t.TempDir(), "docker")
	const fakeDocker = `#!/bin/sh
if [ "$3" = container ] && [ "$4" = cp ]; then
  printf archive
  exit "${FAKE_CP_STATUS-0}"
fi
if [ "$3" = container ] && [ "$4" = run ]; then
  cat >/dev/null
  printf '{"files":[],"marker":null}\n'
  exit 0
fi
exit 125
`
	if err := os.WriteFile(docker, []byte(fakeDocker), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"0", "7"} {
		command := exec.Command("/bin/sh", "-c", hostVerifierShell, "relay-verifier", docker, "unix:///tmp/fake.sock", strings.Repeat("a", 64), fakeImageID,
			hostVerifierBootstrap, "source", "allowlist", "installation", "attempt", "operation")
		command.Env = append(os.Environ(), "FAKE_CP_STATUS="+status)
		output, err := command.CombinedOutput()
		if err != nil || string(output) != "cp:"+status+"\n{\"files\":[],\"marker\":null}\n" {
			t.Fatalf("pipeline status frame %s = %q, %v", status, output, err)
		}
	}
}

func TestHostVerifierKeepsStagedBytesOffSSHAndTargetStopped(t *testing.T) {
	h, clock := controlledHarness(t)
	plan, prepared := h.prepare(t)
	container := h.host.container(t)
	if _, err := h.adapter.ReconcilePrepare(context.Background(), plan, testOperation); err != nil {
		t.Fatal(err)
	}
	if h.host.starts != 0 || h.host.created != 1 {
		t.Fatal("readiness verification started a target")
	}
	for _, flag := range []string{"--rm -i", "--pull=never", "--user 65534:65534", "--network none", "--cap-drop ALL", "--security-opt no-new-privileges", "--read-only", "--pids-limit 32", "--memory 134217728", "--memory-swap 134217728", "--cpus 1", "--label io.compute-relay.kind=verification", "--label \"io.compute-relay.installation=$8\"", "--label \"io.compute-relay.attempt=$9\"", "--label \"io.compute-relay.operation=${10}\"", "--entrypoint python3"} {
		if !strings.Contains(hostVerifierShell, flag) {
			t.Fatalf("verifier lacks %s", flag)
		}
	}
	if strings.Contains(hostVerifierShell, "--mount") || strings.Contains(hostVerifierShell, "--volumes-from") {
		t.Fatal("stopped-container verifier gained a mount")
	}
	for _, call := range h.host.wireCalls {
		if slices.Contains(call, container.id+":/relay") {
			t.Fatal("full staging archive crossed SSH")
		}
	}
	if len(h.host.verifierOutput) != 2 {
		t.Fatal("Ready and reconciliation did not use the local verifier")
	}
	for _, output := range h.host.verifierOutput {
		if len(output) > 16<<10 || bytes.Contains(output, []byte("code")) || bytes.Contains(output, []byte("relay/")) {
			t.Fatal("staged file contents appeared in the verifier result")
		}
	}
	start := startAuthority(prepared, clock)
	if out := h.adapter.Start(context.Background(), prepared, &start, nil); out.Status != "accepted" || h.host.starts != 1 || len(h.host.verifierOutput) != 3 {
		t.Fatal("Start did not reverify before launching the target", out.Status)
	}
}

func TestHostVerifierRejectsTamperingBeforeReady(t *testing.T) {
	for _, kind := range []string{"tampered", "missing"} {
		t.Run(kind, func(t *testing.T) {
			h, _ := controlledHarness(t)
			h.host.verifierMutate = func(c *fakeContainer) {
				if kind == "missing" {
					delete(c.files, "relay/staging/input-000.bin")
					return
				}
				c.files["relay/staging/input-000.bin"] = fakeFile{data: []byte("xyz"), uid: nobody}
			}
			plan, err := h.adapter.Validate(context.Background(), h.job)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = h.adapter.Prepare(context.Background(), plan, testOperation); !errors.Is(err, ErrIdentity) || h.host.starts != 0 || len(h.host.containers) != 0 {
				t.Fatal("tampered staging became Ready", err)
			}
		})
	}
}

func TestHostVerifierRejectsTamperingBeforeStart(t *testing.T) {
	for _, kind := range []string{"tampered", "missing", "oversized", "link", "unexpected"} {
		t.Run(kind, func(t *testing.T) {
			h, clock := controlledHarness(t)
			_, prepared := h.prepare(t)
			c := h.host.container(t)
			switch kind {
			case "tampered":
				c.files["relay/staging/input-000.bin"] = fakeFile{data: []byte("xyz"), uid: nobody}
			case "missing":
				delete(c.files, "relay/staging/input-000.bin")
			case "oversized":
				c.files["relay/staging/input-000.bin"] = fakeFile{data: []byte("abcd"), uid: nobody}
			case "link":
				c.files["relay/staging/input-000.bin"] = fakeFile{link: true, uid: nobody}
			case "unexpected":
				c.files["relay/staging/extra.bin"] = fakeFile{data: []byte("x"), uid: nobody}
			}
			start := startAuthority(prepared, clock)
			if out := h.adapter.Start(context.Background(), prepared, &start, nil); out.Status != "rejected" || h.host.starts != 0 {
				t.Fatal("tampered staging reached Start", out.Status)
			}
			if _, exists := c.files["relay/start.json"]; exists {
				t.Fatal("start authority was written before verification")
			}
		})
	}
}

func TestHostVerifierRejectsProducerAndArchiveFailures(t *testing.T) {
	for _, kind := range []string{"producer_failure", "missing_status", "verifier_failure", "oversized_stdout", "truncated", "truncated_tail", "trailing_data", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			h, clock := controlledHarness(t)
			_, prepared := h.prepare(t)
			switch kind {
			case "producer_failure":
				h.host.verifierProducerStatus = 17
			case "missing_status":
				h.host.verifierOmitStatus = true
			case "verifier_failure":
				h.host.verifierExit = 2
			case "oversized_stdout":
				h.host.verifierExtraOutput = bytes.Repeat([]byte("x"), 128<<10)
			case "truncated":
				h.host.verifierTarFault = func(data []byte) []byte { return data[:100] }
			case "truncated_tail":
				h.host.verifierTarFault = func(data []byte) []byte { return data[:len(data)-1024] }
			case "trailing_data":
				h.host.verifierTarFault = func(data []byte) []byte { return append(data, []byte("unexpected")...) }
			case "duplicate":
				h.host.verifierTarFault = func(data []byte) []byte {
					var extra bytes.Buffer
					writer := tar.NewWriter(&extra)
					if err := writer.WriteHeader(&tar.Header{Name: "relay/manifest.json", Mode: 0o644, Size: 1}); err != nil {
						t.Fatal(err)
					}
					if _, err := writer.Write([]byte("x")); err != nil {
						t.Fatal(err)
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
					return append(append([]byte(nil), data[:len(data)-1024]...), extra.Bytes()...)
				}
			}
			start := startAuthority(prepared, clock)
			if out := h.adapter.Start(context.Background(), prepared, &start, nil); out.Status != "rejected" || h.host.starts != 0 {
				t.Fatal("failed verifier allowed Start", out.Status)
			}
		})
	}
}
