package sshhost

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/hostwatchdog"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
	"golang.org/x/crypto/ssh"
)

func assertFailureReason(t *testing.T, err error, want string) {
	t.Helper()
	if got := provider.Reason(err, ""); got != want || !domain.ValidReason(got) || strings.Contains(err.Error(), "private diagnostic") {
		t.Fatalf("reason=%q want=%q error=%v", got, want, err)
	}
}

func TestDockerFailuresNameTheFailedOperation(t *testing.T) {
	ctx := t.Context()
	generation := testImageGeneration()
	for _, tc := range []struct {
		name, reason string
		run          func(docker) error
	}{
		{"ping", "docker_ping_failed", func(d docker) error { return d.ping(ctx) }},
		{"platform", "docker_platform_failed", func(d docker) error { _, _, e := d.platform(ctx); return e }},
		{"clock", "docker_clock_read_failed", func(d docker) error { _, e := d.systemTime(ctx); return e }},
		{"engine", "docker_engine_read_failed", func(d docker) error { _, e := d.engineID(ctx); return e }},
		{"image inspect", "docker_image_inspect_failed", func(d docker) error { _, _, e := d.inspectImage(ctx, StandardImage); return e }},
		{"image pull", "docker_image_pull_failed", func(d docker) error { return d.pull(ctx, StandardImage) }},
		{"container inspect", "docker_container_inspect_failed", func(d docker) error { _, _, e := d.inspect(ctx, "fixture"); return e }},
		{"container create", "docker_container_create_failed", func(d docker) error { _, e := d.create(ctx, containerSpec{Name: "fixture"}); return e }},
		{"copy in", "docker_copy_in_failed", func(d docker) error { return d.copyIn(ctx, "fixture", strings.NewReader("")) }},
		{"copy out", "docker_copy_out_failed", func(d docker) error { _, e := d.copyOut(ctx, "fixture", "/relay", io.Discard); return e }},
		{"logs", "docker_logs_read_failed", func(d docker) error { _, e := d.logs(ctx, "fixture", io.Discard); return e }},
		{"volume inspect", "cache_volume_inspect_failed", func(d docker) error { _, _, e := d.inspectVolume(ctx, "fixture"); return e }},
		{"volume list", "cache_volume_list_failed", func(d docker) error { _, _, e := d.inspectOwnedCacheVolume(ctx, hostcache.Generation{}); return e }},
		{"volume create", "cache_volume_create_failed", func(d docker) error { return d.createVolume(ctx, hostcache.Generation{}) }},
		{"application image observe", "application_image_observation_failed", func(d docker) error { _, e := d.observeApplicationImage(ctx, generation, "amd64"); return e }},
		{"application image load", "application_image_load_failed", func(d docker) error { return d.loadApplicationArchive(ctx, strings.NewReader("")) }},
		{"application image store", "application_image_store_unverified", func(d docker) error {
			_, e := d.applicationImageID(ctx, &hostimages.Prepared{ImageID: fakeImageID, ManifestID: fakeImageID})
			return e
		}},
		{"secret delivery", "workload_secret_delivery_failed", func(d docker) error {
			return deliverSecrets(ctx, d, "fixture", provider.Prepared{}, workloadsecret.Material{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &scriptedExec{answer: func([]string, io.Reader, io.Writer) (Result, error) {
				return Result{ExitCode: 1, Stderr: []byte("private diagnostic")}, nil
			}}
			err := tc.run(docker{exec: exec, binary: "/usr/bin/docker"})
			assertFailureReason(t, err, tc.reason)
			if !errors.Is(err, ErrRemoteDocker) {
				t.Fatal("Docker classification lost", err)
			}
		})
	}
	exec := &scriptedExec{answer: func([]string, io.Reader, io.Writer) (Result, error) { return Result{ExitCode: 1}, nil }}
	_, err := findDocker(ctx, exec, "/usr/bin/docker")
	assertFailureReason(t, err, "docker_cli_unavailable")
}

func TestDockerReadWrappersKeepSpecificReasonsAndTransportCauses(t *testing.T) {
	cause := provider.WithReason(fmt.Errorf("private diagnostic: %w", ErrUnavailable), "docker_channel_open_failed")
	exec := &scriptedExec{answer: func([]string, io.Reader, io.Writer) (Result, error) { return Result{}, cause }}
	d := docker{exec: exec, binary: "/usr/bin/docker"}
	for _, run := range []func() error{
		func() error { _, e := d.systemTime(t.Context()); return e },
		func() error { _, e := d.engineID(t.Context()); return e },
		func() error { _, _, e := d.platform(t.Context()); return e },
		func() error {
			_, e := d.applicationImageID(t.Context(), &hostimages.Prepared{ImageID: fakeImageID, ManifestID: fakeImageID})
			return e
		},
		func() error { _, e := d.observeApplicationImage(t.Context(), testImageGeneration(), "amd64"); return e },
		func() error { return d.loadApplicationArchive(t.Context(), strings.NewReader("")) },
	} {
		err := run()
		assertFailureReason(t, err, "docker_channel_open_failed")
		if !errors.Is(err, ErrUnavailable) {
			t.Fatal("transport cause lost", err)
		}
	}
}

func TestAdapterOperationsRetainCommandReasons(t *testing.T) {
	for _, operation := range []string{"prepare", "reconcile preparation", "reconcile execution", "observe", "cancel", "list artifacts", "fetch", "logs", "cleanup"} {
		t.Run(operation, func(t *testing.T) {
			h := newHarness(t)
			plan, prepared := h.prepare(t)
			c := h.host.container(t)
			remote := provider.RemoteReference{Identity: prepared.Identity, Resource: prepared.Resource, Version: c.id}
			var err error
			h.host.fail = func(args []string) (Result, bool) {
				if len(args) > 1 && args[0] == "container" && args[1] == "inspect" {
					return Result{ExitCode: 1, Stderr: []byte("private diagnostic")}, true
				}
				return Result{}, false
			}
			switch operation {
			case "prepare":
				_, err = h.adapter.Prepare(t.Context(), plan, testOperation)
			case "reconcile preparation":
				_, err = h.adapter.ReconcilePrepare(t.Context(), plan, testOperation)
			case "reconcile execution":
				_, err = h.adapter.Reconcile(t.Context(), h.job.Identity)
			case "observe":
				_, err = h.adapter.Observe(t.Context(), remote)
			case "cancel":
				_, err = h.adapter.Cancel(t.Context(), remote, "cancel_fixture")
			case "list artifacts":
				_, err = h.adapter.ListArtifacts(t.Context(), remote)
			case "fetch":
				_, err = h.adapter.Fetch(t.Context(), remote, provider.Artifact{Remote: remote, Path: "outputs/answer.txt", Bytes: 1, SHA256: provider.Digest([]byte("x"))}, io.Discard, 1, nil)
			case "logs":
				_, err = h.adapter.Logs(t.Context(), remote, provider.PageRequest{Limit: 1})
			case "cleanup":
				_, err = h.adapter.CleanupResource(t.Context(), provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{Remote: remote, LedgerID: "resource_fixture", CreationOperationID: testOperation, Mode: provider.CleanupApply}, Purpose: "execution", OwnershipOperationID: domain.OperationID(remote.Identity.IntentID), PlanSHA256: plan.Digest()})
			}
			assertFailureReason(t, err, "docker_container_inspect_failed")
			if !errors.Is(err, ErrRemoteDocker) || h.host.starts != 0 || h.host.created != 1 {
				t.Fatal("failure changed classification or issued new work", err)
			}
		})
	}
}

func TestSubmissionUncertaintyRetainsStartFailureAndNeverRestarts(t *testing.T) {
	h := newHarness(t)
	_, prepared := h.prepare(t)
	h.host.fail = func(args []string) (Result, bool) {
		if slices.Equal(args[:min(len(args), 2)], []string{"container", "start"}) {
			return Result{ExitCode: 1}, true
		}
		return Result{}, false
	}
	out := h.adapter.Start(t.Context(), prepared, nil, nil)
	if out.Status != provider.SubmissionUnknown || out.Problem == nil || out.Problem.PublicDetails()[domain.ReasonDetail] != "submission_outcome_unknown" || out.Problem.PublicDetails()[domain.CauseDetail] != "container_start_failed" || out.Problem.SafeOperationRetry || !out.Problem.ComputeMayHaveStarted {
		t.Fatalf("uncertain start lost cause or safety: %+v", out)
	}
	calls := h.host.commands("start")
	_, err := h.adapter.Reconcile(t.Context(), h.job.Identity)
	if err != nil || h.host.commands("start") != calls || h.host.created != 1 {
		t.Fatal("reconcile repeated start", err)
	}
}

func TestHealthIdentityFailuresNameTheirStep(t *testing.T) {
	for _, tc := range []struct{ mode, want string }{
		{"machine read", "ssh_identity_read_failed"},
		{"machine changed", "health_machine_identity_mismatch"},
		{"engine changed", "health_docker_identity_mismatch"},
		{"remote socket", "docker_endpoint_unverified"},
		{"docker absent", "docker_cli_unavailable"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			exec := newHealthTestExecutor(t)
			resource := healthMachineResource()
			engine := resourceDigest("docker-engine-id", exec.engine)
			switch tc.mode {
			case "machine read":
				exec.failMachine = true
			case "machine changed":
				resource = "resource_" + strings.Repeat("b", 64)
			case "engine changed":
				exec.engine = "synthetic-other-engine"
			case "remote socket":
				exec.socket = "ssh://remote.invalid"
			case "docker absent":
				exec.failDocker = true
			}
			_, err := collectHealth(context.Background(), healthBoundedExecutor{exec}, Endpoint{DockerPath: "/usr/local/bin/docker"}, resource, engine)
			assertFailureReason(t, err, tc.want)
			if !errors.Is(err, ErrResourceIdentity) {
				t.Fatal("health lost identity classification", err)
			}
		})
	}
	_, err := CollectHealth(t.Context(), nil, "", "invalid", "")
	assertFailureReason(t, err, "health_binding_mismatch")
}

func TestStorageFailuresNameSummaryAndIncompleteInspection(t *testing.T) {
	scope := hoststorage.Scope{InstallationID: "installation_1", MachineID: domain.SHA256Digest(strings.Repeat("b", 64)), EngineID: domain.SHA256Digest(strings.Repeat("c", 64))}
	for _, tc := range []struct {
		name, want string
		exec       *storageExecutor
	}{
		{"summary invalid", "storage_container_summary_invalid", &storageExecutor{summaryReply: storageHTTPReply("invalid"), foreignID: strings.Repeat("d", 64)}},
		{"summary command failed", "storage_container_summary_failed", &storageExecutor{summaryExit: 1, foreignID: strings.Repeat("d", 64)}},
		{"partial inspect", "storage_inspect_incomplete", &storageExecutor{volumes: []string{"relay-cache-fixture"}, dropVolume: "relay-cache-fixture", foreignID: strings.Repeat("d", 64)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := scanDockerStorage(t.Context(), docker{exec: tc.exec, binary: "/usr/bin/docker"}, scope)
			assertFailureReason(t, err, tc.want)
			if got := StorageScanReason(err); got != tc.want {
				t.Fatal("storage wrapper lost reason", got)
			}
		})
	}
	for _, kind := range []hoststorage.Kind{hoststorage.Container, hoststorage.Volume, hoststorage.Image} {
		_, err := decodeStorageObjects(kind, []byte("invalid"))
		assertFailureReason(t, err, "storage_inspect_invalid")
	}
	_, err := CollectStorage(t.Context(), nil, "", hoststorage.Scope{})
	assertFailureReason(t, err, "storage_scope_invalid")
}

func TestValidationAndProtocolFailuresCarrySafeReasons(t *testing.T) {
	h := newHarness(t)
	job := h.job.Clone()
	job.Identity.InstanceID = "another"
	_, err := h.adapter.Validate(t.Context(), job)
	assertFailureReason(t, err, "job_binding_invalid")
	_, err = decodeRef("invalid", h.job.Identity)
	assertFailureReason(t, err, "remote_reference_invalid")
	err = parseTar(bytes.NewReader(make([]byte, 512)), 1, func(string, io.Reader) error { return nil })
	// An empty archive is valid. A truncated header is not.
	if err != nil {
		t.Fatal(err)
	}
	err = parseTar(strings.NewReader("invalid"), 1, func(string, io.Reader) error { return nil })
	assertFailureReason(t, err, "artifact_archive_invalid")
}

func TestWatchdogCommandFailuresKeepSafeStepReasons(t *testing.T) {
	p, a := watchdogTestPlan(t)
	x := &scriptedExec{answer: func([]string, io.Reader, io.Writer) (Result, error) {
		return Result{ExitCode: 1, Stderr: []byte("private diagnostic")}, nil
	}}
	_, err := watchdogServiceState(t.Context(), x, a, p)
	assertFailureReason(t, err, "watchdog_service_read_failed")
	_, err = watchdogRun(t.Context(), x, "fixed script")
	assertFailureReason(t, err, "watchdog_script_failed")
	err = (watchdogBoundedExecutor{x}).bootout(t.Context(), a, p)
	assertFailureReason(t, err, "watchdog_bootout_failed")
	if !errors.Is(err, hostwatchdog.ErrIntegrity) {
		t.Fatal("watchdog error classification lost", err)
	}
	x.answer = func([]string, io.Reader, io.Writer) (Result, error) { return Result{}, nil }
	_, err = watchdogServiceState(t.Context(), x, a, p)
	assertFailureReason(t, err, "watchdog_service_invalid")
}

func TestAdapterStorageOperationsRetainCommandFailureAndBindingReasons(t *testing.T) {
	for _, operation := range []string{"scan", "observe", "delete"} {
		t.Run(operation, func(t *testing.T) {
			h := newHarness(t)
			scope := hoststorage.Scope{InstallationID: "installation", MachineID: domain.SHA256Digest(resourceDigest("linux-machine-id", h.host.machineID)), EngineID: domain.SHA256Digest(resourceDigest("docker-engine-id", h.host.engineID))}
			h.adapter.config.MachineID, h.adapter.config.EngineID = string(scope.MachineID), string(scope.EngineID)
			target := hoststorage.Target{Scope: scope, Class: hoststorage.Volume, ID: "relay-cache-fixture", Action: hoststorage.OrphanVolume, Labels: map[string]string{storageLabel: "installation"}}
			cause := provider.WithReason(ErrUnavailable, "ssh_channel_open_failed")
			h.dialErr = cause
			var err error
			switch operation {
			case "scan":
				_, err = h.adapter.ScanStorage(t.Context(), scope)
			case "observe":
				_, err = h.adapter.ObserveStorageTarget(t.Context(), target)
			case "delete":
				_, err = h.adapter.CleanupStorageTarget(t.Context(), target, false)
			}
			assertFailureReason(t, err, "ssh_channel_open_failed")
			if h.host.created != 0 || h.host.starts != 0 || h.host.commands("rm") != 0 {
				t.Fatal("unobserved storage led to mutation")
			}
		})
	}
}

func TestCleanupProblemRetainsReasonAndOriginalClassification(t *testing.T) {
	h := newHarness(t)
	request := resourceCleanupRequest(t, h)
	h.host.container(t).status = "running"
	_, err := h.adapter.CleanupResource(t.Context(), request)
	assertFailureReason(t, err, "cleanup_container_not_finished")
	var p domain.Problem
	if !errors.As(err, &p) || p.Code != domain.CodeRemoteExecutionUnresolved || !errors.Is(err, ErrNotDone) {
		t.Fatal("cleanup Problem dropped its cause", err)
	}
}

type transportFaultConn struct {
	ssh.Conn
	stage string
}

func (c transportFaultConn) OpenChannel(string, []byte) (ssh.Channel, <-chan *ssh.Request, error) {
	if c.stage == "open" {
		return nil, nil, errors.New("private diagnostic")
	}
	requests := make(chan *ssh.Request)
	close(requests)
	return transportFaultChannel{accept: c.stage != "start"}, requests, nil
}
func (transportFaultConn) Wait() error  { return io.EOF }
func (transportFaultConn) Close() error { return nil }

type transportFaultChannel struct {
	ssh.Channel
	accept bool
}

func (transportFaultChannel) Read([]byte) (int, error)                         { return 0, io.EOF }
func (transportFaultChannel) Write(p []byte) (int, error)                      { return len(p), nil }
func (transportFaultChannel) Close() error                                     { return nil }
func (transportFaultChannel) CloseWrite() error                                { return nil }
func (c transportFaultChannel) SendRequest(string, bool, []byte) (bool, error) { return c.accept, nil }
func (transportFaultChannel) Stderr() io.ReadWriter                            { return &bytes.Buffer{} }

func TestSSHCommandFailuresNameChannelStartAndWait(t *testing.T) {
	for _, tc := range []struct{ stage, want string }{{"open", "ssh_channel_open_failed"}, {"start", "ssh_command_start_failed"}, {"wait", "ssh_command_wait_failed"}} {
		t.Run(tc.stage, func(t *testing.T) {
			channels := make(chan ssh.NewChannel)
			close(channels)
			requests := make(chan *ssh.Request)
			close(requests)
			client := Client{conn: ssh.NewClient(transportFaultConn{stage: tc.stage}, channels, requests), stop: func() bool { return false }}
			defer client.Close()
			_, err := client.Run(t.Context(), nil, []string{"fixed-command"}, nil, io.Discard)
			assertFailureReason(t, err, tc.want)
			if !errors.Is(err, ErrUnavailable) {
				t.Fatal("SSH command lost transport classification", err)
			}
		})
	}
}

func TestReadinessRefusalKeepsMarkerReadFailure(t *testing.T) {
	h, clock := controlledHarness(t)
	_, prepared := h.prepare(t)
	h.host.fail = func(args []string) (Result, bool) {
		if len(args) > 2 && args[0] == "container" && args[1] == "cp" && strings.HasSuffix(args[2], stagedMarker) {
			return Result{ExitCode: 1}, true
		}
		return Result{}, false
	}
	authority := startAuthority(prepared, clock)
	out := h.adapter.Start(t.Context(), prepared, &authority, nil)
	if out.Status != provider.SubmissionRejected || out.Problem == nil || out.Problem.PublicDetails()[domain.ReasonDetail] != "readiness_invalidated" || out.Problem.PublicDetails()[domain.CauseDetail] != "docker_copy_out_failed" || h.host.starts != 0 {
		t.Fatalf("marker failure lost cause or reached start: %+v", out)
	}
}
