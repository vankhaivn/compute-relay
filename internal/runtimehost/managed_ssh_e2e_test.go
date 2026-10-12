package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
	"github.com/vankhaivn/compute-relay/internal/testsupport/sshtest"
)

// TestManagedSSHDockerPipeline runs one job through admission, authorization, dispatch,
// collection and requested cleanup on the local Docker engine behind the in-process SSH
// server. It pulls the standard image, so it never runs by default.
func TestManagedSSHDockerPipeline(t *testing.T) {
	if os.Getenv("COMPUTE_RELAY_SSH_DOCKER_E2E") != "1" {
		t.Skip("set COMPUTE_RELAY_SSH_DOCKER_E2E=1 to use the local Docker engine")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	environment := os.Environ()
	if platform := os.Getenv("COMPUTE_RELAY_SSH_DOCKER_PLATFORM"); platform != "" {
		environment = append(environment, "DOCKER_DEFAULT_PLATFORM="+platform)
	}
	server := sshtest.New(t)
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
	host, port, _ := server.Start()

	f := newManagedTestFixture(t, true, &managedSSHAdapter{verify: sshhost.VerifyHost, config: ManagedServeConfig{MaxRemoteWallSeconds: 300}})
	request, _ := json.Marshal(map[string]any{"provider_type": "ssh", "label": "Local Docker", "credentials": map[string]string{
		"host": host, "port": fmt.Sprint(port), "username": server.User, "password": server.Password}})
	op, err := f.services.connections.Submit(ctx, f.actor, "app", "", "ssh-e2e-connection", request)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := f.services.connections.RunOnce(ctx); err != nil || !worked {
		t.Fatal("verify connection", err)
	}
	connection, err := f.services.connections.Get(ctx, f.actor, "app", op.ConnectionID)
	if err != nil || connection.Selection == nil {
		op, _ = f.services.connections.Operation(ctx, f.actor, "app", op.ID)
		t.Fatalf("connection not verified: %+v %+v", connection, op)
	}

	project := t.TempDir()
	payload := "import os, pathlib\npathlib.Path(os.environ['CC_OUTPUT_DIR'], 'answer.txt').write_text('42')\nprint('answer written', flush=True)\n"
	if err = os.WriteFile(filepath.Join(project, "main.py"), []byte(payload), 0o644); err != nil {
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
	objectService, err := objects.New(f.host.access, f.host.inputs, f.host.store)
	if err != nil {
		t.Fatal(err)
	}
	uploaded, err := objectService.Upload(ctx, f.actor, "app", int64(bundle.Len()), provider.Digest(bundle.Bytes()), bytes.NewReader(bundle.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	spec := fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"ssh e2e","profile":%q,"bundle":{"object_id":%q},"execution":{"kind":"python","command":["python","main.py"]},"inputs":[],"outputs":[{"path":"answer.txt","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":120,"setup_seconds":30,"finalization_grace_seconds":10}}`, connection.Selection.Profile, uploaded.ID)
	job, err := jobs.Submit(ctx, f.actor, "app", "ssh-e2e-job", []byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ids, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=io.compute-relay.attempt="+string(job.AttemptID)).Output()
		for _, id := range strings.Fields(string(ids)) {
			_ = exec.Command("docker", "container", "rm", "--force", id).Run()
		}
	})
	grant := fmt.Sprintf(`{"attempt_id":%q,"max_remote_wall_seconds":120,"authorize_private_staging":true,"authorize_compute":true}`, job.AttemptID)
	if _, err = f.services.authorizations.Authorize(ctx, f.actor, "app", job.JobID, "ssh-e2e-grant", []byte(grant)); err != nil {
		t.Fatal(err)
	}

	var record admission.Record
	for !record.Attempt.State.Orchestration.Terminal() {
		if _, err = f.services.dispatcher.RunOnce(ctx, "ssh_e2e_worker"); err != nil {
			t.Fatal("dispatch", err)
		}
		if _, err = f.services.collector.RunOnce(ctx); err != nil {
			t.Fatal("collection", err)
		}
		if record, err = jobs.Get(ctx, f.actor, "app", job.JobID); err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatalf("attempt did not finish: %+v", record.Attempt.State)
		}
		time.Sleep(500 * time.Millisecond)
	}
	state := record.Attempt.State
	t.Logf("final attempt state %+v problem %v", state, record.Problem)
	if state.Orchestration != domain.OrchestrationSucceeded || state.Execution != domain.ExecutionSucceeded || state.Result != domain.ResultAvailable {
		t.Fatalf("attempt state %+v problem %v", state, record.Problem)
	}

	cleanups, err := cleanup.NewService(f.host.access, f.host.store, clock{})
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := f.host.access.Issue(ctx, "app", []auth.Scope{auth.Read, auth.Operate}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	operator, err := f.host.access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := cleanups.Submit(ctx, operator, "app", job.JobID, "ssh-e2e-cleanup", []byte(fmt.Sprintf(`{"attempt_id":%q}`, job.AttemptID)))
	if err != nil {
		t.Fatal(err)
	}
	cleaner, err := cleanupEngine(managedCleanupRepository{f.host.store}, f.services.resolver)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := cleaner.RunOnce(ctx); err != nil || !worked {
		t.Fatal("cleanup", worked, err)
	}
	if receipt, err = cleanups.Get(ctx, operator, "app", receipt.Operation.ID); err != nil || receipt.Effect != cleanup.RemoteDeleted {
		t.Fatalf("cleanup receipt %+v %v", receipt, err)
	}
	if ids, err := exec.Command("docker", "ps", "-aq", "--filter", "label=io.compute-relay.attempt="+string(job.AttemptID)).Output(); err != nil || len(strings.TrimSpace(string(ids))) != 0 {
		t.Fatalf("container remains after cleanup: %q %v", ids, err)
	}
}
