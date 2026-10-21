package sshhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const testSpec = `{"api_version":"compute-connector/v1alpha1","name":"PRIVATE_LABEL","profile":"profile","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","main.py"]},"inputs":[{"name":"input","target":"data.txt","source":{"kind":"object","object_id":"input"}}],"outputs":[{"path":"answer.txt","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`

const testOperation domain.OperationID = "prep_fixture"

type testVault struct {
	raw   []byte
	calls int
}

func (v *testVault) WithCredential(_ context.Context, ref ports.CredentialRef, use func([]byte) error) error {
	v.calls++
	if ref != "vault:ssh-fixture" {
		return errors.New("unexpected credential reference")
	}
	return use(append([]byte(nil), v.raw...))
}

type testBlobs map[domain.ObjectID][]byte

func (b testBlobs) Open(_ context.Context, workspace domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	data, ok := b[id]
	if !ok || workspace != "workspace" {
		return nil, errors.New("missing object")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Second)
	return c.now
}

type harness struct {
	adapter *Adapter
	host    *fakeHost
	vault   *testVault
	blobs   testBlobs
	job     provider.ResolvedJob
	dials   int
	dialErr error
}

func testJob(t *testing.T, spec string, blobs testBlobs) provider.ResolvedJob {
	t.Helper()
	snapshot := provider.InputSnapshot{Bundle: domain.ObjectMetadata{ID: "code", WorkspaceID: "workspace", Bytes: 4, SHA256: provider.Digest([]byte("code"))},
		Inputs: []provider.FrozenInput{{Name: "input", Target: "data.txt", Object: domain.ObjectMetadata{ID: "input", WorkspaceID: "workspace", Bytes: 3, SHA256: provider.Digest([]byte("abc"))}}}}
	blobs["code"], blobs["input"] = []byte("code"), []byte("abc")
	job := provider.ResolvedJob{Identity: provider.Identity{InstallationID: "installation", WorkspaceID: "workspace", JobID: "job", AttemptID: "attempt",
		InstanceID: "instance", IntentID: "si_fixture", ResourceKey: "cr-" + strings.Repeat("ab", 20), Nonce: strings.Repeat("a", 32),
		BundleSHA256: snapshot.Bundle.SHA256, InputManifestSHA256: snapshot.InputDigest()},
		Binding:       domain.ProviderBinding{Profile: "profile", ProviderInstanceID: "instance", ConfigurationRevision: "r1"},
		Specification: []byte(spec), SpecificationSHA256: provider.Digest([]byte(spec)),
		Required:    []domain.CapabilityName{domain.CapabilityBatchExecution, domain.CapabilityPython, domain.CapabilityPrivateInputStaging},
		WallSeconds: 10, Inputs: &snapshot}
	if err := job.Validate(); err != nil {
		t.Fatal(err)
	}
	return job
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	target := Target{Host: "100.64.0.7", Port: 22, User: "kenji", HostKey: testFingerprint}
	account, err := Account(target)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeFields(map[string]string{FieldHost: target.Host, FieldUsername: target.User, FieldPassword: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{host: newFakeHost(t), vault: &testVault{raw: raw}, blobs: testBlobs{}}
	h.job = testJob(t, testSpec, h.blobs)
	binding := provider.BindingSnapshot{Binding: h.job.Binding, AccountScope: "scope_fixture", CredentialRef: "vault:ssh-fixture"}
	h.adapter, err = NewAdapter(Config{InstanceID: "instance", Account: account, CredentialRef: "vault:ssh-fixture", MaxWallSeconds: 60, Image: StandardImage},
		binding, h.vault, h.blobs, &testClock{now: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	h.adapter.copyRetryDelay = time.Millisecond
	h.adapter.dial = func(_ context.Context, got Target, credential Credential) (session, error) {
		h.dials++
		if got != target || string(credential.Password) != "secret" {
			t.Errorf("dialed %+v", got)
		}
		if h.dialErr != nil {
			return nil, h.dialErr
		}
		return h.host, nil
	}
	return h
}

func (h *harness) prepare(t *testing.T) (provider.Plan, provider.Prepared) {
	t.Helper()
	plan, err := h.adapter.Validate(context.Background(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := h.adapter.Prepare(context.Background(), plan, testOperation)
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Validate(plan, testOperation); err != nil {
		t.Fatal(err)
	}
	return plan, prepared
}

func (h *harness) submit(t *testing.T) provider.RemoteReference {
	t.Helper()
	_, prepared := h.prepare(t)
	out := h.adapter.Start(context.Background(), prepared, nil, nil)
	if err := out.Validate(h.job.Identity); err != nil || out.Status != provider.SubmissionAccepted {
		t.Fatalf("submit = %+v, %v", out, err)
	}
	return *out.Remote
}

func resultManifest(id provider.Identity, outputs map[string]string) string {
	var artifacts []map[string]any
	for name, data := range outputs {
		artifacts = append(artifacts, map[string]any{"path": name, "bytes": len(data), "sha256": provider.Digest([]byte(data))})
	}
	raw, _ := json.Marshal(map[string]any{"manifest_version": "1", "job_id": id.JobID, "attempt_id": id.AttemptID, "attempt_nonce": id.Nonce, "runner_version": "fixture", "bundle_sha256": id.BundleSHA256, "input_manifest_sha256": id.InputManifestSHA256, "started_at": "2026-10-14T00:00:00Z", "finished_at": "2026-10-14T00:00:01Z", "phase": "completed", "exit_code": 0, "timed_out": false, "resource_check": map[string]any{"gpu_required": false, "gpu_verified": false}, "artifacts": artifacts, "error": nil})
	return string(raw)
}

func TestLifecycleRunsOneHardenedContainerAndCollectsVerifiedFiles(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if err := h.adapter.Verify(ctx, h.adapter.binding); err != nil {
		t.Fatal(err)
	}
	plan, err := h.adapter.Validate(ctx, h.job)
	if err != nil {
		t.Fatal(err)
	}
	var progress [][2]int64
	prepared, err := h.adapter.Prepare(provider.WithPreparationProgress(ctx, func(done, total int64) {
		progress = append(progress, [2]int64{done, total})
	}), plan, testOperation)
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Validate(plan, testOperation); err != nil || !prepared.Ready || !prepared.Private {
		t.Fatalf("prepared = %+v, %v", prepared, err)
	}
	if len(progress) == 0 || progress[len(progress)-1] != [2]int64{7, 7} {
		t.Fatalf("progress = %v", progress)
	}

	c := h.host.container(t)
	if c.name != "compute-relay-"+h.job.Identity.ResourceKey || c.status != "created" {
		t.Fatalf("container = %s %s", c.name, c.status)
	}
	args := strings.Join(c.args, " ")
	for _, want := range []string{"--user 65534:65534", "--network none", "--cap-drop ALL", "--security-opt no-new-privileges",
		"--pids-limit 4096", "--restart no", "--label io.compute-relay.attempt=attempt", "--label io.compute-relay.preparation=prep_fixture",
		fakeImageID + " timeout -k 30 70 python3 -E -s /relay/runner/run.py --manifest /relay/manifest.json --execute --staging-root /relay/staging --work-root /relay/work --network-mode disabled"} {
		if !strings.Contains(args, want) {
			t.Errorf("create lacks %q: %s", want, args)
		}
	}
	if strings.Contains(args, "secret") || strings.Contains(args, "PRIVATE_LABEL") {
		t.Fatal("create arguments carry private values")
	}
	for name, uid := range map[string]int{"relay/runner/run.py": 0, "relay/runner/relay_runner/main.py": 0, "relay/manifest.json": nobody,
		"relay/staging/code.bin": nobody, "relay/staging/input-000.bin": nobody, "relay/staged.json": nobody} {
		if f, ok := c.files[name]; !ok || f.uid != uid {
			t.Errorf("staged %s = %+v", name, f)
		}
	}
	if string(c.files["relay/staging/code.bin"].data) != "code" || string(c.files["relay/staging/input-000.bin"].data) != "abc" ||
		!bytes.Contains(c.files["relay/manifest.json"].data, []byte(`"path":"code.bin"`)) {
		t.Fatal("staged bytes differ from frozen objects")
	}

	found, err := h.adapter.ReconcilePrepare(ctx, plan, testOperation)
	if err != nil || found.Validate(plan, testOperation) != nil || found.Status != provider.ReconciliationFound || *found.Prepared != prepared {
		t.Fatalf("reconcile preparation = %+v, %v", found, err)
	}
	if lookup, err := h.adapter.Reconcile(ctx, h.job.Identity); err != nil || lookup.Status != provider.ReconciliationNotFound {
		t.Fatalf("unstarted container was found: %+v, %v", lookup, err)
	}

	out := h.adapter.Start(ctx, prepared, nil, nil)
	if err = out.Validate(h.job.Identity); err != nil || out.Status != provider.SubmissionAccepted || out.Remote.Version != c.id {
		t.Fatalf("submit = %+v, %v", out, err)
	}
	remote := *out.Remote
	lookup, err := h.adapter.Reconcile(ctx, h.job.Identity)
	if err != nil || lookup.Validate(h.job.Identity) != nil || lookup.Status != provider.ReconciliationFound || *lookup.Remote != remote {
		t.Fatalf("reconcile = %+v, %v", lookup, err)
	}
	running, err := h.adapter.Observe(ctx, remote)
	if err != nil || running.Execution != domain.ExecutionRunning || running.RemoteActivity != domain.RemoteActivityActive {
		t.Fatalf("observe = %+v, %v", running, err)
	}
	if _, err = h.adapter.ListArtifacts(ctx, remote); !errors.Is(err, ErrNotDone) {
		t.Fatalf("running artifacts: %v", err)
	}

	h.host.finish(0, map[string]string{"control/stdout.log": "out\n", "control/stderr.log": "", "control/environment.json": "{}",
		"control/bundle.tar.gz": "private", "control/execution-result.json": resultManifest(h.job.Identity, map[string]string{"answer.txt": "42"}),
		"outputs/answer.txt": "42", "outputs/undeclared.txt": "x"})
	done, err := h.adapter.Observe(ctx, remote)
	if err != nil || done.Execution != domain.ExecutionSucceeded || done.RemoteActivity != domain.RemoteActivityInactive ||
		done.ReleaseEvidence != domain.ReleaseEvidenceProviderReported || done.RawState != "docker:exited:0" || !done.ObservedAt.After(running.ObservedAt) {
		t.Fatalf("observe = %+v, %v", done, err)
	}

	listed, err := h.adapter.ListArtifacts(ctx, remote)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, artifact := range listed {
		paths = append(paths, artifact.Path)
		var dst bytes.Buffer
		receipt, err := h.adapter.Fetch(ctx, remote, artifact, &dst, 1<<20, nil)
		if err != nil || receipt.Bytes != artifact.Bytes || receipt.SHA256 != artifact.SHA256 || provider.Digest(dst.Bytes()) != artifact.SHA256 {
			t.Fatalf("fetch %s = %+v, %v", artifact.Path, receipt, err)
		}
	}
	if want := []string{"control/environment.json", "control/execution-result.json", "control/stderr.log", "control/stdout.log", "outputs/answer.txt"}; !slices.Equal(paths, want) {
		t.Fatalf("listed %q", paths)
	}
	tampered := listed[4]
	tampered.SHA256 = provider.Digest([]byte("43"))
	if _, err = h.adapter.Fetch(ctx, remote, tampered, io.Discard, 1<<20, nil); !errors.Is(err, ErrArtifact) {
		t.Fatalf("tampered fetch: %v", err)
	}
	foreign := listed[4]
	foreign.Remote.Identity.Nonce = strings.Repeat("b", 32)
	if _, err = h.adapter.Fetch(ctx, remote, foreign, io.Discard, 1<<20, nil); !errors.Is(err, ErrArtifact) {
		t.Fatalf("foreign fetch: %v", err)
	}

	request := provider.CleanupRequest{Remote: remote, LedgerID: "ledger_fixture", CreationOperationID: testOperation, ResultsCollected: true, Mode: provider.CleanupDryRun}
	c.labels[labelPrefix+"attempt"] = "other_attempt"
	if _, err := h.adapter.CleanupResource(ctx, resourceRequest(request, mustPlan(t, h).Digest())); !problemCode(err, domain.CodeRemoteIdentityMismatch) || h.host.commands("rm") != 0 {
		t.Fatalf("changed execution owner reached cleanup: %v", err)
	}
	c.labels[labelPrefix+"attempt"] = string(h.job.Identity.AttemptID)
	if dry, err := h.adapter.CleanupResource(ctx, resourceRequest(request, mustPlan(t, h).Digest())); err != nil || !dry.WouldDelete || dry.Deleted || h.host.commands("rm") != 0 {
		t.Fatalf("dry run = %+v, %v", dry, err)
	}
	request.Mode = provider.CleanupApply
	if applied, err := h.adapter.CleanupResource(ctx, resourceRequest(request, mustPlan(t, h).Digest())); err != nil || !applied.Deleted {
		t.Fatalf("apply = %+v, %v", applied, err)
	}
	if repeat, err := h.adapter.CleanupResource(ctx, resourceRequest(request, mustPlan(t, h).Digest())); err != nil || !repeat.AlreadyAbsent {
		t.Fatalf("repeat = %+v, %v", repeat, err)
	}
	if h.host.starts != 1 || h.host.created != 1 || h.host.commands("rm") != 1 || h.host.closed != h.dials {
		t.Fatalf("starts=%d created=%d closed=%d dials=%d", h.host.starts, h.host.created, h.host.closed, h.dials)
	}
}

func TestSubmitRejectsOnlyBeforeStartAndOtherwiseStaysUnknown(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status provider.SubmissionStatus
		starts int
		reason string
	}{
		{"container missing", provider.SubmissionRejected, 0, "container_missing"},
		{"host unreachable", provider.SubmissionRejected, 0, ""},
		{"cancelled context", provider.SubmissionRejected, 0, ""},
		{"start fails", provider.SubmissionUnknown, 0, ""},
		{"already started", provider.SubmissionUnknown, 1, ""},
		{"foreign preparation", provider.SubmissionRejected, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			_, prepared := h.prepare(t)
			ctx := context.Background()
			switch tc.name {
			case "container missing":
				h.host.containers = map[string]*fakeContainer{}
			case "host unreachable":
				h.dialErr = ErrUnavailable
			case "cancelled context":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			case "start fails":
				h.host.fail = func(args []string) (Result, bool) { return Result{ExitCode: 1}, args[1] == "start" }
			case "already started":
				if out := h.adapter.Start(ctx, prepared, nil, nil); out.Status != provider.SubmissionAccepted {
					t.Fatalf("first submit = %+v", out)
				}
			case "foreign preparation":
				prepared.PreparationID = "prep_other"
			}
			dials := h.dials
			out := h.adapter.Start(ctx, prepared, nil, nil)
			if out.Validate(h.job.Identity) != nil || out.Status != tc.status || h.host.starts != tc.starts {
				t.Fatalf("submit = %+v, starts=%d", out, h.host.starts)
			}
			if tc.name == "cancelled context" && h.dials != dials {
				t.Fatal("cancelled start contacted the host")
			}
			if tc.reason != "" && (out.Problem == nil || out.Problem.PublicDetails()["reason"] != tc.reason) {
				t.Fatalf("submit reason = %+v", out.Problem)
			}
		})
	}
}

func TestPrepareRemovesItsOwnContainerWhenStagingFails(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	plan, err := h.adapter.Validate(ctx, h.job)
	if err != nil {
		t.Fatal(err)
	}
	h.blobs["input"] = []byte("abd")
	if _, err = h.adapter.Prepare(ctx, plan, testOperation); !errors.Is(err, ErrArtifact) {
		t.Fatalf("tampered input: %v", err)
	}
	if len(h.host.containers) != 0 || h.host.created != 1 || h.host.commands("rm") != 1 {
		t.Fatalf("containers=%d created=%d", len(h.host.containers), h.host.created)
	}
	h.blobs["input"] = []byte("abc")
	h.host.image = false
	if _, err = h.adapter.Prepare(ctx, plan, testOperation); !errors.Is(err, ErrRemoteDocker) || h.host.created != 1 {
		t.Fatalf("missing image: %v", err)
	}
	h.host.registry = true
	if _, err = h.adapter.Prepare(ctx, plan, testOperation); err != nil || !h.host.image {
		t.Fatalf("pulled image: %v", err)
	}
	if _, err = h.adapter.Prepare(ctx, plan, testOperation); !errors.Is(err, ErrIdentity) || h.host.created != 2 {
		t.Fatalf("second prepare: %v", err)
	}
	if found, err := h.adapter.ReconcilePrepare(ctx, plan, "prep_other"); !errors.Is(err, ErrIdentity) {
		t.Fatalf("foreign preparation: %+v, %v", found, err)
	}
	delete(h.host.container(t).files, "relay/staged.json")
	if found, err := h.adapter.ReconcilePrepare(ctx, plan, testOperation); err != nil || found.Status != provider.ReconciliationNotFound {
		t.Fatalf("incomplete staging: %+v, %v", found, err)
	}
}

func TestCancelConfirmsTerminationFromDockerState(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	remote := h.submit(t)
	outcome, err := h.adapter.Cancel(ctx, remote, "cancel_fixture")
	if err != nil || outcome.Validate() != nil || outcome.Status != domain.CancellationConfirmed || !outcome.TerminationConfirmed {
		t.Fatalf("cancel = %+v, %v", outcome, err)
	}
	if _, ok := h.host.container(t).files["relay/cancel-requested.json"]; !ok {
		t.Fatal("cancellation request was not recorded")
	}
	observed, err := h.adapter.Observe(ctx, remote)
	if err != nil || observed.Execution != domain.ExecutionCancelled || observed.RemoteActivity != domain.RemoteActivityInactive {
		t.Fatalf("observe = %+v, %v", observed, err)
	}
	again, err := h.adapter.Cancel(ctx, remote, "cancel_fixture")
	if err != nil || again.Status != domain.CancellationTooLate || again.TerminationConfirmed {
		t.Fatalf("second cancel = %+v, %v", again, err)
	}
}

func TestObserveMapsDockerExitEvidence(t *testing.T) {
	for _, tc := range []struct {
		exit int
		oom  bool
		want domain.ExecutionState
	}{{0, false, domain.ExecutionSucceeded}, {1, false, domain.ExecutionFailed}, {124, false, domain.ExecutionTimedOut}, {137, true, domain.ExecutionFailed}, {0, true, domain.ExecutionFailed}} {
		h := newHarness(t)
		remote := h.submit(t)
		h.host.finish(tc.exit, nil)
		h.host.container(t).oom = tc.oom
		observed, err := h.adapter.Observe(context.Background(), remote)
		if err != nil || observed.Execution != tc.want || observed.RawState != fmt.Sprintf("docker:exited:%d", tc.exit) {
			t.Errorf("exit %d oom %v = %+v, %v", tc.exit, tc.oom, observed, err)
		}
	}
	h := newHarness(t)
	remote := h.submit(t)
	h.host.container(t).status = "removing"
	if _, err := h.adapter.Observe(context.Background(), remote); !errors.Is(err, ErrRemoteDocker) {
		t.Fatalf("unknown state: %v", err)
	}
	other := remote
	other.Version = strings.Repeat("f", 64)
	if _, err := h.adapter.Observe(context.Background(), other); !errors.Is(err, ErrIdentity) {
		t.Fatalf("replaced container: %v", err)
	}
}

func TestValidateRefusesWorkBeyondTheConnectionPolicy(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	for name, spec := range map[string]string{
		"gpu":      strings.Replace(testSpec, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`, 1),
		"internet": strings.Replace(testSpec, `"remote_internet":"disabled"`, `"remote_internet":"required"`, 1),
	} {
		job := testJob(t, spec, h.blobs)
		var problem domain.Problem
		if _, err := h.adapter.Validate(ctx, job); !errors.As(err, &problem) || problem.Code != domain.CodeResourceRequirementUnsatisfied {
			t.Errorf("%s: %v", name, err)
		}
	}
	job := h.job
	job.Required = append(slices.Clone(job.Required), domain.CapabilityGPU)
	var problem domain.Problem
	if _, err := h.adapter.Validate(ctx, job); !errors.As(err, &problem) || problem.Code != domain.CodeUnsupportedCapability {
		t.Fatalf("gpu capability: %v", err)
	}
	job = h.job
	job.Binding.ConfigurationRevision = "r2"
	job.Identity.InstanceID = "instance"
	if _, err := h.adapter.Validate(ctx, job); !errors.Is(err, ErrBinding) {
		t.Fatalf("other revision: %v", err)
	}
	h.adapter.config.AllowInternet = true
	internet := testJob(t, strings.Replace(testSpec, `"remote_internet":"disabled"`, `"remote_internet":"required"`, 1), h.blobs)
	h.job = internet
	h.prepare(t)
	if args := strings.Join(h.host.container(t).args, " "); !strings.Contains(args, "--network bridge") || !strings.HasSuffix(args, "--network-mode required") {
		t.Fatalf("internet container = %s", args)
	}
}

func TestChangedHostKeyStopsBeforeLogin(t *testing.T) {
	h := newHarness(t)
	raw, err := EncodeFields(map[string]string{FieldHost: "100.64.0.7", FieldUsername: "kenji", FieldPassword: "secret",
		FieldHostKey: "SHA256:" + strings.Repeat("A", 43)})
	if err != nil {
		t.Fatal(err)
	}
	h.vault.raw = raw
	if err = h.adapter.Verify(context.Background(), h.adapter.binding); !errors.Is(err, ErrHostKeyMismatch) || h.dials != 0 {
		t.Fatalf("check = %v, dials %d", err, h.dials)
	}
	raw, _ = EncodeFields(map[string]string{FieldHost: "100.64.0.8", FieldUsername: "kenji", FieldPassword: "secret"})
	h.vault.raw = raw
	if err = h.adapter.Verify(context.Background(), h.adapter.binding); !errors.Is(err, ErrHostKeyMismatch) || h.dials != 0 {
		t.Fatalf("moved host = %v, dials %d", err, h.dials)
	}
	if err = h.adapter.Verify(context.Background(), provider.BindingSnapshot{}); !errors.Is(err, ErrBinding) {
		t.Fatalf("other binding: %v", err)
	}
}

func TestReadLogsPagesCompleteStdoutLines(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	remote := h.submit(t)
	h.host.container(t).logs = "a\nb\nc\npart"
	first, err := h.adapter.Logs(ctx, remote, provider.PageRequest{Limit: 2})
	if err != nil || !slices.Equal(first.Lines, []string{"a", "b"}) || first.Availability != "live" || first.Source != "provider" || first.NextCursor == "" {
		t.Fatalf("first = %+v, %v", first, err)
	}
	second, err := h.adapter.Logs(ctx, remote, provider.PageRequest{Limit: 2, Cursor: first.NextCursor})
	if err != nil || !slices.Equal(second.Lines, []string{"c"}) || second.NextCursor == "" {
		t.Fatalf("second = %+v, %v", second, err)
	}
	h.host.container(t).logs = "a\nb\nc\npartial line\n" + strings.Repeat("é", maxLogLineBytes) + "\n"
	h.host.finish(0, nil)
	third, err := h.adapter.Logs(ctx, remote, provider.PageRequest{Limit: 5, Cursor: second.NextCursor})
	if err != nil || len(third.Lines) != 2 || third.Lines[0] != "partial line" || !third.Truncated || len(third.Lines[1]) > maxLogLineBytes ||
		third.Availability != "after_completion" || third.NextCursor != "" {
		t.Fatalf("third = %+v, %v", third, err)
	}
	for _, cursor := range []string{"bad", strings.Replace(second.NextCursor, "e", "f", 1)} {
		if _, err = h.adapter.Logs(ctx, remote, provider.PageRequest{Limit: 1, Cursor: cursor}); !errors.Is(err, provider.ErrLogCursorInvalid) {
			t.Errorf("cursor %q: %v", cursor, err)
		}
	}
	h.host.container(t).logs = ""
	if _, err = h.adapter.Logs(ctx, remote, provider.PageRequest{Limit: 1, Cursor: second.NextCursor}); !errors.Is(err, provider.ErrLogCursorReset) {
		t.Fatalf("truncated stream: %v", err)
	}
}

func TestReadTarRefusesLinksAndOversizedFiles(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	remote := h.submit(t)
	h.host.finish(0, map[string]string{"control/stdout.log": "x", "outputs/answer.txt": "42",
		"control/execution-result.json": resultManifest(h.job.Identity, map[string]string{"answer.txt": "42"})})
	c := h.host.container(t)
	c.files["relay/work/control/stderr.log"] = fakeFile{link: true}
	if _, err := h.adapter.ListArtifacts(ctx, remote); !errors.Is(err, ErrArtifact) {
		t.Fatalf("symlink control file: %v", err)
	}
	delete(c.files, "relay/work/control/stderr.log")
	page, err := h.adapter.ListArtifacts(ctx, remote)
	if err != nil {
		t.Fatal(err)
	}
	answer := page[len(page)-1]
	c.files["relay/work/outputs/answer.txt"] = fakeFile{data: []byte("420")}
	var dst bytes.Buffer
	if _, err = h.adapter.Fetch(ctx, remote, answer, &dst, 1<<20, nil); !errors.Is(err, ErrArtifact) || dst.Len() != 0 {
		t.Fatalf("grown output: %v, wrote %d", err, dst.Len())
	}
	if _, err = h.adapter.Fetch(ctx, remote, answer, &dst, 1, nil); !errors.Is(err, ErrArtifact) {
		t.Fatalf("over caller limit: %v", err)
	}
}

func TestCatalogDoesNotTrustInvalidResultOutputs(t *testing.T) {
	for _, name := range []string{"partial", "duplicate", "identity"} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			remote := h.submit(t)
			raw := resultManifest(h.job.Identity, map[string]string{"answer.txt": "42"})
			switch name {
			case "partial":
				raw = `{"phase":"completed","artifacts":[{"path":"answer.txt","bytes":2}]}`
			case "duplicate":
				raw = strings.TrimSuffix(raw, "}") + `,"phase":"failed"}`
			case "identity":
				id := h.job.Identity
				id.JobID = "other"
				raw = resultManifest(id, map[string]string{"answer.txt": "42"})
			}
			h.host.finish(0, map[string]string{"control/execution-result.json": raw, "outputs/answer.txt": "42"})
			page, err := h.adapter.ListArtifacts(context.Background(), remote)
			if err != nil {
				t.Fatal(err)
			}
			for _, artifact := range page {
				if strings.HasPrefix(artifact.Path, "outputs/") {
					t.Fatal("untrusted output listed", artifact.Path)
				}
			}
		})
	}
}

func TestInvalidCancellationOperationNeverCopiesOrStops(t *testing.T) {
	h := newHarness(t)
	remote := h.submit(t)
	copies, stops, dials := h.host.commands("cp"), h.host.commands("stop"), h.dials
	outcome, err := h.adapter.Cancel(context.Background(), remote, "")
	if !errors.Is(err, ErrRequest) || outcome != (provider.CancellationOutcome{}) || h.host.commands("cp") != copies || h.host.commands("stop") != stops || h.dials != dials {
		t.Fatal("invalid cancellation caused effects", outcome, err)
	}
}
