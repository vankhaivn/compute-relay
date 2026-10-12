package sshhost

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func controlledHarness(t *testing.T) (*harness, *deadlineClock) {
	t.Helper()
	h := newHarness(t)
	clock := &deadlineClock{at: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)}
	h.adapter.clock, h.host.engineTime = clock, clock.at
	h.job = testJob(t, strings.Replace(testSpec, `"name":"PRIVATE_LABEL"`, `"name":"PRIVATE_LABEL","execution_control":"prepare_then_start"`, 1), h.blobs)
	h.job.Required = append(h.job.Required, domain.CapabilitySeparateStart)
	return h, clock
}
func startAuthority(p provider.Prepared, clock *deadlineClock) provider.StartAuthorization {
	return provider.StartAuthorization{AuthorizationID: "grant_fixture", ReadinessRevision: p.ReadinessRevision(), MaxRemoteWallSeconds: 10, ExecutionDeadline: clock.at.Add(10 * time.Second).Format(time.RFC3339), StartBefore: clock.at.Add(8 * time.Second).Format(time.RFC3339)}
}

func TestControlledPreparationVerifiesBytesWithoutExecution(t *testing.T) {
	h, clock := controlledHarness(t)
	plan, prepared := h.prepare(t)
	c := h.host.container(t)
	if h.host.starts != 0 || !prepared.ReadinessRevision().Valid() || !slices.Contains(c.args, "--start-control") || !slices.Contains(c.args, "--supervise") {
		t.Fatal("readiness lacks nonexecuting proof")
	}
	before := append([]byte(nil), c.files["relay/manifest.json"].data...)
	observed, err := h.adapter.ReconcilePreparation(context.Background(), plan, testOperation)
	if err != nil || observed.Prepared == nil || *observed.Prepared != prepared || h.host.starts != 0 {
		t.Fatal("readiness reconciliation changed preparation", err)
	}
	if out := h.adapter.Submit(context.Background(), prepared); out.Status != provider.SubmissionRejected || h.host.starts != 0 {
		t.Fatal("legacy submit bypassed start grant")
	}
	control := startAuthority(prepared, clock)
	out := h.adapter.Start(context.Background(), prepared, control)
	if out.Status != provider.SubmissionAccepted || h.host.starts != 1 {
		t.Fatal("controlled start failed", out.Status, out.Problem)
	}
	if !bytes.Equal(before, c.files["relay/manifest.json"].data) {
		t.Fatal("start rewrote staged manifest")
	}
	var start startControl
	if json.Unmarshal(c.files["relay/start.json"].data, &start) != nil || start.StartAuthorization != control || start.ManifestSHA256 != prepared.ManifestSHA256 || start.AttemptNonce != prepared.Identity.Nonce {
		t.Fatal("control is not bound to original preparation")
	}
	if again := h.adapter.Start(context.Background(), prepared, control); again.Status != provider.SubmissionUnknown || h.host.starts != 1 {
		t.Fatal("start repeated")
	}
}

func TestControlledReadinessInvalidatesChangedResources(t *testing.T) {
	for _, kind := range []string{"input", "runner", "manifest", "policy", "image", "missing_container", "missing_marker"} {
		t.Run(kind, func(t *testing.T) {
			h, _ := controlledHarness(t)
			plan, prepared := h.prepare(t)
			c := h.host.container(t)
			switch kind {
			case "input":
				f := c.files["relay/staging/input-000.bin"]
				f.data = []byte("changed input")
				c.files["relay/staging/input-000.bin"] = f
			case "runner":
				delete(c.files, "relay/runner/relay_runner/main.py")
			case "manifest":
				f := c.files["relay/manifest.json"]
				f.data = append(f.data, ' ')
				c.files["relay/manifest.json"] = f
			case "policy":
				i := slices.Index(c.args, "--pids-limit")
				c.args[i+1] = "777"
			case "image":
				h.host.image = false
			case "missing_container":
				delete(h.host.containers, c.name)
			case "missing_marker":
				delete(c.files, "relay/staged.json")
			}
			seen, err := h.adapter.ReconcilePreparation(context.Background(), plan, testOperation)
			if err == nil && seen.Status == provider.ReconciliationFound && seen.Prepared != nil && seen.Prepared.ReadinessRevision() == prepared.ReadinessRevision() {
				t.Fatal("changed resources retained readiness")
			}
			if kind == "missing_marker" && (err != nil || seen.Prepared == nil || seen.Prepared.Ready || seen.Prepared.Resource != prepared.Resource) {
				t.Fatal("partial resource identity lost", err)
			}
			if h.host.starts != 0 || h.host.created != 1 || h.host.builds != 0 {
				t.Fatal("reconciliation created effects")
			}
			for _, cmd := range h.host.calls {
				if slices.Contains(cmd, "pull") {
					t.Fatal("reconciliation installed an image")
				}
			}
		})
	}
}

func TestControlledStartGatesAndLostAcknowledgement(t *testing.T) {
	for _, kind := range []string{"stale_revision", "expired_hold", "changed_policy", "lost_start_ack", "lost_control_ack"} {
		t.Run(kind, func(t *testing.T) {
			h, clock := controlledHarness(t)
			_, prepared := h.prepare(t)
			c := h.host.container(t)
			start := startAuthority(prepared, clock)
			switch kind {
			case "stale_revision":
				start.ReadinessRevision = domain.SHA256Digest(strings.Repeat("b", 64))
			case "expired_hold":
				clock.at = clock.at.Add(8 * time.Second)
				h.host.engineTime = clock.at
			case "changed_policy":
				i := slices.Index(c.args, "--pids-limit")
				c.args[i+1] = "777"
			case "lost_start_ack":
				h.host.fail = func(args []string) (Result, bool) {
					if len(args) > 1 && args[0] == "container" && args[1] == "start" {
						c.status = "running"
						c.started = clock.at
						h.host.starts++
						return Result{ExitCode: 1}, true
					}
					return Result{}, false
				}
			case "lost_control_ack":
				h.host.fail = func(args []string) (Result, bool) {
					if len(args) > 2 && args[0] == "container" && args[1] == "cp" && args[2] == "--archive" {
						c.files["relay/start.json"] = fakeFile{data: []byte(`{"ack":"lost"}`)}
						return Result{ExitCode: 1}, true
					}
					return Result{}, false
				}
			}
			out := h.adapter.Start(context.Background(), prepared, start)
			if kind == "expired_hold" && (out.Problem == nil || out.Problem.Details[domain.ReasonDetail] != "preparation_expired") {
				t.Fatal("hold expiry lost its reason", out)
			}
			if strings.HasPrefix(kind, "lost_") {
				if out.Status != provider.SubmissionUnknown {
					t.Fatal("uncertain control became rejection", out.Status)
				}
				h.host.fail = nil
				again := h.adapter.Start(context.Background(), prepared, start)
				if again.Status != provider.SubmissionUnknown {
					t.Fatal("possible original start became repeatable", again.Status)
				}
				expected := 0
				if kind == "lost_start_ack" {
					expected = 1
				}
				if h.host.starts != expected {
					t.Fatal("start repeated", h.host.starts)
				}
			} else if out.Status != provider.SubmissionRejected || h.host.starts != 0 {
				t.Fatal("invalid start accepted", out.Status)
			}
		})
	}
}

func TestControlledPreparationNeverInstallsMissingImage(t *testing.T) {
	h, _ := controlledHarness(t)
	h.host.image = false
	h.host.registry = true
	plan, err := h.adapter.Validate(context.Background(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.adapter.Prepare(context.Background(), plan, testOperation); err == nil || h.host.image || h.host.created != 0 {
		t.Fatal("prepare installed missing image", err)
	}
}
