package sshhost

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

const sshWorkloadCanary = "synthetic-ssh-workload-material/+?only"

func secretHarness(t *testing.T) (*harness, *deadlineClock, provider.Plan, provider.Prepared, provider.StartAuthorization, workloadsecret.Material) {
	t.Helper()
	h, clock := controlledHarness(t)
	h.job = testJob(t, strings.Replace(testSpec, `"name":"PRIVATE_LABEL"`, `"name":"PRIVATE_LABEL","execution_control":"prepare_then_start","workload_secrets":[{"name":"ingest","exposure":"file_and_local_process_arguments"}]`, 1), h.blobs)
	h.job.Required = append(h.job.Required, domain.CapabilitySeparateStart, domain.CapabilityEphemeralSecrets)
	plan, prepared := h.prepare(t)
	start := startAuthority(prepared, clock)
	start.Secrets = &provider.SecretAuthorization{AuthorizationID: "secret_fixture", ExpiresAt: clock.at.Add(time.Minute).Format(time.RFC3339), Slots: []workloadsecret.Slot{{Name: "ingest", Exposure: "file_and_local_process_arguments"}}}
	m := workloadsecret.Material{AuthorizationID: start.Secrets.AuthorizationID, Key: workloadsecret.Key{WorkspaceID: prepared.Identity.WorkspaceID, JobID: prepared.Identity.JobID, AttemptID: prepared.Identity.AttemptID}, ExpiresAt: start.Secrets.ExpiresAt, Values: map[string][]byte{"ingest": []byte(sshWorkloadCanary)}}
	return h, clock, plan, prepared, start, m
}
func TestWorkloadSecretSSHUsesOnlyFixedStdinAndPrivateTmpfs(t *testing.T) {
	h, _, plan, prepared, start, material := secretHarness(t)
	defer material.Clear()
	container := h.host.container(t)
	if h.host.starts != 0 || h.host.commands("exec") != 0 || !slices.Contains(container.args, secretTmpfsPath+":"+secretTmpfsOptions) {
		t.Fatal("preparation delivered material or omitted private tmpfs")
	}
	if out := h.adapter.Start(context.Background(), prepared, &start, nil); out.Status != provider.SubmissionRejected || h.host.starts != 0 {
		t.Fatal("ordinary Start accepted material-free secret job")
	}
	if _, err := h.adapter.ReconcilePrepare(context.Background(), plan, testOperation); err != nil {
		t.Fatal(err)
	}
	out := h.adapter.Start(context.Background(), prepared, &start, &material)
	if out.Status != provider.SubmissionAccepted || h.host.starts != 1 || len(h.host.secretPackets) != 1 {
		t.Fatal("secret start failed", out.Status, out.Problem)
	}
	var packet struct {
		AuthorizationID domain.OperationID  `json:"authorization_id"`
		Values          map[string][]byte   `json:"values"`
		ManifestSHA256  domain.SHA256Digest `json:"manifest_sha256"`
	}
	if json.Unmarshal(h.host.secretPackets[0], &packet) != nil || string(packet.Values["ingest"]) != sshWorkloadCanary || packet.AuthorizationID != material.AuthorizationID || packet.ManifestSHA256 != prepared.ManifestSHA256 {
		t.Fatal("stdin did not bind exact material and manifest")
	}
	for _, call := range h.host.calls {
		raw := strings.Join(call, " ")
		if strings.Contains(raw, sshWorkloadCanary) || strings.Contains(raw, base64.StdEncoding.EncodeToString([]byte(sshWorkloadCanary))) {
			t.Fatal("secret reached Docker/SSH argv")
		}
	}
	for name, file := range container.files {
		if bytes.Contains(file.data, []byte(sshWorkloadCanary)) || bytes.Contains(file.data, []byte(base64.StdEncoding.EncodeToString([]byte(sshWorkloadCanary)))) {
			t.Fatal("secret reached persistent staging", name)
		}
	}
	if again := h.adapter.Start(context.Background(), prepared, &start, &material); again.Status != provider.SubmissionUnknown || h.host.starts != 1 || len(h.host.secretPackets) != 1 {
		t.Fatal("repeated start reinjected material")
	}
}
func TestWorkloadSecretSSHUncertainDeliveryNeverReinjects(t *testing.T) {
	for _, kind := range []string{"lost_delivery_ack", "lost_start_ack", "wrong_slot", "wrong_attempt", "changed_tmpfs"} {
		t.Run(kind, func(t *testing.T) {
			h, clock, _, prepared, start, material := secretHarness(t)
			defer material.Clear()
			c := h.host.container(t)
			switch kind {
			case "lost_delivery_ack":
				h.host.secretAckLost = true
			case "lost_start_ack":
				h.host.fail = func(args []string) (Result, bool) {
					if len(args) > 1 && args[0] == "container" && args[1] == "start" {
						c.status, c.started = "running", clock.at
						h.host.starts++
						return Result{ExitCode: 1}, true
					}
					return Result{}, false
				}
			case "wrong_slot":
				material.Values = map[string][]byte{"other": []byte(sshWorkloadCanary)}
			case "wrong_attempt":
				material.Key.AttemptID = "another_attempt"
			case "changed_tmpfs":
				c.args[slices.Index(c.args, "--tmpfs")+1] = "/relay/secrets:rw,size=131072,mode=0777"
			}
			out := h.adapter.Start(context.Background(), prepared, &start, &material)
			if strings.HasPrefix(kind, "lost_") {
				if out.Status != provider.SubmissionUnknown {
					t.Fatal("uncertainty became rejection", out.Status)
				}
				delivered := len(h.host.secretPackets)
				h.host.fail = nil
				h.host.secretAckLost = false
				if again := h.adapter.Start(context.Background(), prepared, &start, &material); again.Status != provider.SubmissionUnknown || h.host.starts != 1 || len(h.host.secretPackets) != delivered {
					t.Fatal("uncertain delivery repeated")
				}
			} else if out.Status != provider.SubmissionRejected || h.host.starts != 0 || len(h.host.secretPackets) != 0 {
				t.Fatal("invalid secret policy started compute", out.Status)
			}
		})
	}
}
