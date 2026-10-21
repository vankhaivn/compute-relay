package sshhost

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// TestRealDockerOverSSH drives a local Docker engine through the in-process SSH server.
// It pulls the standard image and runs one container, so it never runs by default.
// COMPUTE_RELAY_SSH_DOCKER_PLATFORM (for example linux/amd64) selects an emulated platform.
func TestRealDockerOverSSH(t *testing.T) {
	if os.Getenv("COMPUTE_RELAY_SSH_DOCKER_E2E") != "1" {
		t.Skip("set COMPUTE_RELAY_SSH_DOCKER_E2E=1 to use the local Docker engine")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	environment := os.Environ()
	if platform := os.Getenv("COMPUTE_RELAY_SSH_DOCKER_PLATFORM"); platform != "" {
		environment = append(environment, "DOCKER_DEFAULT_PLATFORM="+platform)
	}
	server := localDockerSSHServer(t, environment)

	target := server.target()
	account, err := Account(target)
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]string{FieldHost: target.Host, FieldPort: strconv.Itoa(target.Port), FieldUsername: target.User, FieldPassword: server.Password}
	raw, err := EncodeFields(fields)
	if err != nil {
		t.Fatal(err)
	}

	client, err := Dial(ctx, target, Credential{Password: []byte(server.Password)})
	if err != nil {
		t.Fatal(err)
	}
	binary, err := findDocker(ctx, client, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = (docker{exec: client, binary: binary}).pull(ctx, StandardImage); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()

	payload := "import os, platform, pathlib\n" +
		"data = pathlib.Path(os.environ['CC_INPUT_DIR'], 'data.txt').read_text()\n" +
		"pathlib.Path(os.environ['CC_OUTPUT_DIR'], 'answer.txt').write_text(data.upper())\n" +
		"print('machine', platform.machine(), 'uid', os.getuid(), flush=True)\n"
	adapter, job, remote := startE2E(ctx, t, account, raw, payload, "ok")
	observed := waitTerminal(ctx, t, adapter, remote)
	files := map[string][]byte{}
	page := provider.PageRequest{Limit: 100}
	listed, err := adapter.ListArtifacts(ctx, remote)
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range listed {
		var dst bytes.Buffer
		if _, err = adapter.Fetch(ctx, remote, artifact, &dst, 32<<20, nil); err != nil {
			t.Fatalf("fetch %s: %v", artifact.Path, err)
		}
		files[artifact.Path] = dst.Bytes()
	}
	logs, err := adapter.Logs(ctx, remote, page)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("observation %s; logs %q; result %s; stderr %q", observed.RawState, logs.Lines, files["control/execution-result.json"], files["control/stderr.log"])
	if observed.Execution != domain.ExecutionSucceeded || string(files["outputs/answer.txt"]) != "ABC" ||
		!bytes.Contains(files["control/execution-result.json"], []byte(`"phase":"completed"`)) {
		t.Fatalf("attempt did not complete: %s", observed.RawState)
	}
	if !strings.Contains(strings.Join(logs.Lines, "\n"), "uid 65534") {
		t.Fatalf("payload did not run as the unprivileged container user: %q", logs.Lines)
	}
	if err = provider.ValidateManifestIdentity(job.Identity, files["control/execution-result.json"]); err != nil {
		t.Fatal(err)
	}

	adapter, _, remote = startE2E(ctx, t, account, raw, "import time\nprint('waiting', flush=True)\ntime.sleep(90)\n", "cancel")
	for observed, err = adapter.Observe(ctx, remote); err == nil && observed.Execution != domain.ExecutionRunning; observed, err = adapter.Observe(ctx, remote) {
		time.Sleep(time.Second)
	}
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	cancelled, err := adapter.Cancel(ctx, remote, "cancel_fixture")
	if err != nil || cancelled.Status != domain.CancellationConfirmed || !cancelled.TerminationConfirmed {
		t.Fatalf("cancel = %+v, %v", cancelled, err)
	}
	if observed = waitTerminal(ctx, t, adapter, remote); observed.Execution != domain.ExecutionCancelled {
		t.Fatalf("cancelled attempt observed as %s", observed.RawState)
	}
	t.Logf("cancelled attempt observed as %s", observed.RawState)
}

// startE2E stages and starts one attempt and removes its container when the test ends.
func startE2E(ctx context.Context, t *testing.T, account string, raw []byte, payload, suffix string) (*Adapter, provider.ResolvedJob, provider.RemoteReference) {
	t.Helper()
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "main.py"), []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	opened, err := packaging.OpenProject(project, packaging.DefaultLimits(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	selection, err := opened.Prepare(ctx, []string{"main.py"})
	if err != nil {
		t.Fatal(err)
	}
	var bundle bytes.Buffer
	if _, err = selection.Write(ctx, &bundle); err != nil {
		t.Fatal(err)
	}
	blobs := testBlobs{}
	job := testJob(t, testSpec, blobs)
	blobs["code"] = bundle.Bytes()
	snapshot := job.Inputs.Clone()
	snapshot.Bundle.Bytes, snapshot.Bundle.SHA256 = int64(bundle.Len()), provider.Digest(bundle.Bytes())
	job.Inputs, job.Identity.BundleSHA256 = &snapshot, snapshot.Bundle.SHA256
	job.Identity.ResourceKey = "cr-e2e-" + suffix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if strings.Contains(payload, "sleep") {
		spec := strings.Replace(testSpec, `"remote_wall_seconds":10`, `"remote_wall_seconds":120`, 1)
		job.Specification, job.SpecificationSHA256, job.WallSeconds = []byte(spec), provider.Digest([]byte(spec)), 120
	}
	if err = job.Validate(); err != nil {
		t.Fatal(err)
	}
	binding := provider.BindingSnapshot{Binding: job.Binding, AccountScope: "scope_fixture", CredentialRef: "vault:ssh-fixture"}
	adapter, err := NewAdapter(Config{InstanceID: "instance", Account: account, CredentialRef: "vault:ssh-fixture", MaxWallSeconds: 120, Image: StandardImage},
		binding, &testVault{raw: raw}, blobs, ports.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	if err = adapter.Verify(ctx, binding); err != nil {
		t.Fatal(err)
	}
	plan, err := adapter.Validate(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := adapter.Prepare(ctx, plan, testOperation)
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := decodeRef(prepared.Resource, job.Identity)
	remote := provider.RemoteReference{Identity: job.Identity, Resource: prepared.Resource, Version: ref.ID}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		request := provider.CleanupRequest{Remote: remote, LedgerID: "ledger_fixture", CreationOperationID: testOperation, ResultsCollected: true, Mode: provider.CleanupApply}
		if outcome, err := adapter.CleanupResource(ctx, resourceRequest(request, plan.Digest())); err != nil || !outcome.Deleted && !outcome.AlreadyAbsent {
			t.Errorf("cleanup = %+v, %v", outcome, err)
		}
	})
	out := adapter.Start(ctx, prepared, nil, nil)
	if out.Status != provider.SubmissionAccepted || *out.Remote != remote {
		t.Fatalf("submit = %+v", out.Problem)
	}
	return adapter, job, remote
}

func waitTerminal(ctx context.Context, t *testing.T, adapter *Adapter, remote provider.RemoteReference) provider.Observation {
	t.Helper()
	for {
		observed, err := adapter.Observe(ctx, remote)
		if err != nil {
			t.Fatal(err)
		}
		if observed.Execution.Terminal() {
			return observed
		}
		time.Sleep(time.Second)
	}
}

func localDockerSSHServer(t *testing.T, environment []string) *testServer {
	t.Helper()
	server := newTestServer(t)
	server.Password = "e2e-password"
	server.Handle = func(command string, stdin io.Reader, stdout, stderr io.Writer) int {
		cmd := exec.Command("/bin/sh", "-c", command)
		cmd.Env, cmd.Stdin, cmd.Stdout, cmd.Stderr = environment, stdin, stdout, stderr
		err := cmd.Run()
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return exit.ExitCode()
		}
		if err != nil {
			return 255
		}
		return 0
	}
	return server
}
