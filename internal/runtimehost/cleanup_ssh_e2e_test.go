package runtimehost

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/testsupport/sshtest"
)

type cleanupE2EClock struct{ offset time.Duration }

func (c cleanupE2EClock) Now() time.Time { return time.Now().UTC().Add(c.offset) }

// TestManagedSSHCleanupRecovery requires a separately authorized disposable Docker
// daemon inside the named container. It retains its root and remote fixtures on failure.
// No provider operation runs in ordinary tests or CI.
func TestManagedSSHCleanupRecovery(t *testing.T) {
	container := os.Getenv("COMPUTE_RELAY_CLEANUP_DOCKER_CONTAINER")
	root := os.Getenv("COMPUTE_RELAY_CLEANUP_EVIDENCE")
	if container == "" && root == "" {
		t.Skip("set COMPUTE_RELAY_CLEANUP_DOCKER_CONTAINER and COMPUTE_RELAY_CLEANUP_EVIDENCE for a disposable engine")
	}
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(container) || !filepath.IsAbs(root) {
		t.Fatal("explicit immutable engine container ID and absolute evidence directory required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal("use a new evidence directory; never overwrite an interrupted run", err)
	}
	save := func(name string, value any) {
		t.Helper()
		raw, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(filepath.Join(root, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = file.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err = errors.Join(file.Sync(), file.Close()); err != nil {
			t.Fatal(err)
		}
	}
	docker := func(input []byte, args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(ctx, "docker", append([]string{"exec", "-i", container, "docker"}, args...)...)
		command.Stdin = bytes.NewReader(input)
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("disposable Docker command %q: %v: %s", args, err, out)
		}
		return bytes.TrimSpace(out)
	}
	copyWitness := func(id, destination, content string) {
		t.Helper()
		var stream bytes.Buffer
		writer := tar.NewWriter(&stream)
		if err := writer.WriteHeader(&tar.Header{Name: "sentinel", Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		_, _ = writer.Write([]byte(content))
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		docker(stream.Bytes(), "cp", "-", id+":"+destination)
	}
	info := docker(nil, "info", "--format", "{{json .}}")
	var engine struct{ ID, OSType, Architecture, ServerVersion string }
	if json.Unmarshal(info, &engine) != nil || engine.ID == "" || engine.OSType != "linux" || engine.Architecture != "x86_64" {
		t.Fatal("qualification requires a verified Linux amd64 engine")
	}
	if existing := docker(nil, "ps", "-aq"); len(existing) != 0 {
		t.Fatal("disposable engine must start without containers")
	}
	save("engine", engine)

	var mu sync.Mutex
	var faultTarget string
	var interrupt context.CancelFunc
	removals, withheld := 0, 0
	server := sshtest.New(t)
	server.Password = "synthetic-cleanup-e2e-password"
	server.Handle = func(command string, stdin io.Reader, stdout, stderr io.Writer) int {
		cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container, "sh", "-c", command)
		cmd.Stdin, cmd.Stderr = stdin, stderr
		var output bytes.Buffer
		cmd.Stdout = &output
		err := cmd.Run()
		mu.Lock()
		drop := faultTarget != "" && strings.Contains(command, faultTarget) && regexp.MustCompile(`(^|[' ])rm([' ]|$)`).MatchString(command)
		if drop {
			removals++
		}
		if drop && err == nil && interrupt != nil {
			withheld++
			interrupt()
			interrupt = nil
			mu.Unlock()
			return 255
		}
		mu.Unlock()
		_, _ = stdout.Write(output.Bytes())
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
	state := filepath.Join(root, "runtime")
	if _, err := Initialize(ctx, state); err != nil {
		t.Fatal(err)
	}
	h, err := Open(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Close() }()
	if _, err = h.CreateWorkspace(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	secret, _, err := h.access.Issue(ctx, "app", []auth.Scope{auth.Read, auth.Write, auth.Manage, auth.Execute, auth.Operate}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	actor, err := h.access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	config := ManagedServeConfig{MaxRemoteWallSeconds: 120, MaxWorkers: 1}
	vault := &managedTestVault{entries: map[string][]byte{}}
	services, err := h.composeManaged(ctx, config, vault, newManagedSSHAdapter(config))
	if err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(map[string]any{"provider_type": "ssh", "label": "Disposable cleanup engine", "credentials": map[string]string{
		"host": host, "port": fmt.Sprint(port), "username": server.User, "password": server.Password}})
	op, err := services.connections.Submit(ctx, actor, "app", "", "cleanup-e2e-connection", request)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := services.connections.RunOnce(ctx); err != nil || !worked {
		t.Fatal("connection verification", err)
	}
	connection, err := services.connections.Get(ctx, actor, "app", op.ConnectionID)
	if err != nil || connection.Host == nil || connection.Authentication != "verified" {
		t.Fatal("connection not verified", err)
	}
	install, err := services.connections.Submit(ctx, actor, "app", connection.ID, "cleanup-e2e-image",
		[]byte(fmt.Sprintf(`{"action":"install_image","expected_revision":%d}`, connection.Revision)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = services.connections.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	installed, err := services.connections.Operation(ctx, actor, "app", install.ID)
	if err != nil || installed.Status != "succeeded" {
		t.Fatal("image installation", installed, err)
	}
	connection, err = services.connections.Get(ctx, actor, "app", connection.ID)
	if err != nil || connection.Selection == nil || connection.Host.Image == nil {
		t.Fatal("image selection missing", err)
	}
	imageID := connection.Host.Image.ID
	volume := string(docker(nil, "volume", "create", "cleanup-witness-named"))
	witnessID := string(docker(nil, "create", "--name", "cleanup-witness", "--network", "none", "--mount", "type=volume,src="+volume+",dst=/named", "--mount", "type=volume,dst=/anonymous", imageID, "python", "-c", "pass"))
	copyWitness(witnessID, "/named", "preserve-named-volume")
	copyWitness(witnessID, "/anonymous", "preserve-anonymous-volume")
	copyWitness(witnessID, "/tmp", "preserve-unrelated-container")
	witnesses := map[string]string{}
	for _, name := range []string{"/named/sentinel", "/anonymous/sentinel", "/tmp/sentinel"} {
		witnesses[name] = cleanupE2ETarDigest(t, docker(nil, "cp", witnessID+":"+name, "-"))
	}
	volumes := docker(nil, "volume", "ls", "--format", "{{.Name}}")
	images := docker(nil, "image", "ls", "--no-trunc", "--format", "{{.ID}}")

	project := filepath.Join(root, "project")
	if err = os.Mkdir(project, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := "import os, pathlib\npathlib.Path(os.environ['CC_OUTPUT_DIR'], 'answer.txt').write_text('42')\nprint('cleanup fixture completed', flush=True)\n"
	if err = os.WriteFile(filepath.Join(project, "main.py"), []byte(payload), 0o600); err != nil {
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
	objectService, err := objects.New(h.access, h.inputs, h.store)
	if err != nil {
		t.Fatal(err)
	}
	uploaded, err := objectService.Upload(ctx, actor, "app", int64(bundle.Len()), provider.Digest(bundle.Bytes()), bytes.NewReader(bundle.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := admission.New(h.access, h.store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	spec := fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"cleanup recovery","profile":%q,"bundle":{"object_id":%q},"execution":{"kind":"python","command":["python","main.py"]},"inputs":[],"outputs":[{"path":"answer.txt","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":120,"setup_seconds":30,"finalization_grace_seconds":10}}`, connection.Selection.Profile, uploaded.ID)
	job, err := jobs.Submit(ctx, actor, "app", "cleanup-e2e-job", []byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	save("job", job)
	grant := fmt.Sprintf(`{"attempt_id":%q,"max_remote_wall_seconds":120,"authorize_private_staging":true,"authorize_compute":true}`, job.AttemptID)
	if _, err = services.authorizations.Authorize(ctx, actor, "app", job.JobID, "cleanup-e2e-grant", []byte(grant)); err != nil {
		t.Fatal(err)
	}
	var record admission.Record
	for !record.Attempt.State.Orchestration.Terminal() {
		if _, err = services.dispatcher.RunOnce(ctx, "cleanup_e2e_worker"); err != nil {
			t.Fatal("dispatch", err)
		}
		if _, err = services.collector.RunOnce(ctx); err != nil {
			t.Fatal("collection", err)
		}
		if record, err = jobs.Get(ctx, actor, "app", job.JobID); err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal("bounded attempt did not finish", record.Attempt.State)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if record.Attempt.State.Orchestration != domain.OrchestrationSucceeded || record.Attempt.State.Result != domain.ResultAvailable {
		t.Fatal("fixture did not collect successfully", record.Attempt.State, record.Problem)
	}
	save("completed", record)
	target := string(docker(nil, "ps", "-a", "--no-trunc", "--filter", "label=io.compute-relay.attempt="+string(job.AttemptID), "--format", "{{.ID}}"))
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(target) {
		t.Fatal("expected exactly one original container")
	}
	name := strings.TrimPrefix(string(docker(nil, "inspect", "--format", "{{.Name}}", target)), "/")
	clock := cleanupE2EClock{}
	service, err := operations.New(h.access, h.store, nil, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	previewService, err := cleanup.NewService(h.access, h.store, clock)
	if err != nil {
		t.Fatal(err)
	}
	previewService = previewService.WithPreviewResolver(services.resolver)
	preview, err := previewService.Preview(ctx, actor, "app", job.JobID, job.AttemptID, false, false)
	if err != nil || !preview.Eligible || len(preview.Resources) != 2 {
		t.Fatal("cleanup preview", preview, err)
	}
	for _, resource := range preview.Resources {
		if resource.Target == nil || resource.Target.ID != target || resource.Target.Scope != "container_writable_layer" {
			t.Fatal("preview widened the cleanup target", resource)
		}
	}
	save("preview", preview)
	localBefore := cleanupE2EFiles(t, state)
	raw := []byte(fmt.Sprintf(`{"attempt_id":%q}`, job.AttemptID))
	receipt, err := service.Submit(ctx, actor, "app", job.JobID, domain.OperationCleanup, "cleanup-e2e-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := cleanup.New(h.store, services.resolver, cleanup.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	faultCtx, faultCancel := context.WithCancel(ctx)
	defer faultCancel()
	mu.Lock()
	faultTarget, interrupt = target, faultCancel
	mu.Unlock()
	if worked, err := worker.RunOnce(faultCtx); !worked || !errors.Is(err, context.Canceled) {
		t.Fatal("fault must interrupt outcome commit after real deletion", worked, err)
	}
	before, err := service.Get(ctx, actor, "app", receipt.Operation.ID)
	if err != nil || before.Operation.Status != domain.OperationRunning || before.Resources[0].State != operations.ResourceRunning {
		t.Fatal("missing durable deletion intent", before, err)
	}
	save("interrupted", before)
	if len(docker(nil, "ps", "-aq", "--filter", "id="+target)) != 0 {
		t.Fatal("the fault did not follow a real deletion")
	}
	replacement := string(docker(nil, "create", "--name", name, "--network", "none", imageID, "python", "-c", "pass"))
	copyWitness(replacement, "/tmp", "preserve-reused-name")
	replacementDigest := cleanupE2ETarDigest(t, docker(nil, "cp", replacement+":/tmp/sentinel", "-"))
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	h, err = Open(ctx, state)
	if err != nil {
		t.Fatal("reopen original root", err)
	}
	clock.offset = 6 * time.Minute // Expire the original lease without changing its persisted intent.
	injectHostClock(t, h, clock.Now)
	services, err = h.composeManaged(ctx, config, vault, newManagedSSHAdapter(config))
	if err != nil {
		t.Fatal(err)
	}
	worker, err = cleanup.New(h.store, services.resolver, cleanup.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOnce(ctx); err != nil || !worked {
		t.Fatal("original-target restart recovery", worked, err)
	}
	service, err = operations.New(h.access, h.store, nil, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	after, err := service.Get(ctx, actor, "app", receipt.Operation.ID)
	if err != nil || after.Operation.Status != domain.OperationSucceeded || after.Effect != operations.AlreadyAbsent || len(after.Resources) != 2 {
		t.Fatal("recovery did not prove original absence", after, err)
	}
	for i, resource := range after.Resources {
		if resource.State != operations.ResourceAbsent || resource.ResourceID != before.Resources[i].ResourceID || resource.Fingerprint != before.Resources[i].Fingerprint {
			t.Fatal("recovery changed the target or lost the resource outcome", before, after)
		}
	}
	save("recovered", after)
	replayed, err := service.Submit(ctx, actor, "app", job.JobID, domain.OperationCleanup, "cleanup-e2e-original", raw)
	if err != nil || !replayed.Replay || replayed.Operation.ID != receipt.Operation.ID {
		t.Fatal("repeat request changed original operation", replayed, err)
	}
	if worked, err := worker.RunOnce(ctx); err != nil || worked {
		t.Fatal("repeat cleanup created another worker effect", worked, err)
	}
	if replacementDigest != cleanupE2ETarDigest(t, docker(nil, "cp", replacement+":/tmp/sentinel", "-")) {
		t.Fatal("original-ID recovery touched a replacement at the old name")
	}
	for path, sum := range witnesses {
		if cleanupE2ETarDigest(t, docker(nil, "cp", witnessID+":"+path, "-")) != sum {
			t.Fatal("cleanup changed an unrelated witness", path)
		}
	}
	if !bytes.Equal(volumes, docker(nil, "volume", "ls", "--format", "{{.Name}}")) || !bytes.Equal(images, docker(nil, "image", "ls", "--no-trunc", "--format", "{{.ID}}")) {
		t.Fatal("cleanup changed volume or image inventory")
	}
	if !reflect.DeepEqual(localBefore, cleanupE2EFiles(t, state)) {
		t.Fatal("remote cleanup changed verified local inputs or results")
	}
	mu.Lock()
	counts := map[string]int{"deletion_commands": removals, "withheld_acknowledgements": withheld}
	mu.Unlock()
	if counts["deletion_commands"] != 1 || counts["withheld_acknowledgements"] != 1 {
		t.Fatal("cleanup repeated the deletion command", counts)
	}
	save("passed", map[string]any{"counts": counts, "original_container": target, "replacement_container": replacement, "witness_container": witnessID, "local_files": localBefore, "witnesses": witnesses, "repeat": replayed, "lease_expiry": "controlled clock advanced six minutes after reopening the original store"})
	t.Log("real SSH deletion, deliberately lost acknowledgement, durable reopen/recovery, reused name, idempotent repeat and preservation passed")
}

func cleanupE2ETarDigest(t *testing.T, raw []byte) string {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(raw))
	header, err := reader.Next()
	if err != nil || header.Typeflag != tar.TypeReg || header.Size > 1024 {
		t.Fatal("invalid witness archive", err)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatal("unexpected additional witness entry", err)
	}
	return string(provider.Digest(content))
}

func cleanupE2EFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	for _, area := range []string{"inputs", "results"} {
		if err := filepath.WalkDir(filepath.Join(root, area), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return errors.New("unexpected local artifact symlink")
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, _ := filepath.Rel(root, path)
			files[relative] = string(provider.Digest(raw))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return files
}
