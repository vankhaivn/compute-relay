package sshhost

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

const multiDayWall = int64(604800)

func multiDayJob(t *testing.T, h *harness, wall int64) provider.ResolvedJob {
	t.Helper()
	spec := strings.Replace(testSpec, `"name":"PRIVATE_LABEL"`, `"name":"PRIVATE_LABEL","execution_control":"prepare_then_start","workload_secrets":[{"name":"ingest","exposure":"file"}]`, 1)
	spec = strings.Replace(spec, `"remote_wall_seconds":10`, `"remote_wall_seconds":`+jsonInt(wall), 1)
	job := testJob(t, spec, h.blobs)
	job.WallSeconds = wall
	job.Required = append(job.Required, domain.CapabilitySeparateStart, domain.CapabilityEphemeralSecrets)
	if err := job.Validate(); err != nil {
		t.Fatal(err)
	}
	return job
}

func jsonInt(n int64) string {
	raw, _ := json.Marshal(n)
	return string(raw)
}

func TestMultiDayWallIsBoundedByTheConnectionLimit(t *testing.T) {
	if MaxWallSeconds != multiDayWall || Capabilities().MaxWallSeconds != multiDayWall || !Capabilities().Valid() {
		t.Fatal("SSH adapter does not advertise the seven-day maximum")
	}
	h, _ := controlledHarness(t)
	h.adapter.config.MaxWallSeconds = 172800
	if _, err := h.adapter.Validate(context.Background(), multiDayJob(t, h, 172800)); err != nil {
		t.Fatal("job at the connection limit was refused", err)
	}
	if _, err := h.adapter.Validate(context.Background(), multiDayJob(t, h, 172801)); err == nil {
		t.Fatal("job above the connection limit was accepted")
	}
	config := h.adapter.config
	for wall, valid := range map[int64]bool{multiDayWall: true, multiDayWall + 1: false} {
		config.MaxWallSeconds = wall
		_, err := NewAdapter(config, h.adapter.binding, h.vault, h.blobs, h.adapter.clock)
		if valid != (err == nil) || !valid && !errors.Is(err, ErrRequest) {
			t.Fatalf("connection wall %d: %v", wall, err)
		}
	}
}

func TestMultiDayWallStartsWithShortLivedWorkloadSecret(t *testing.T) {
	h, clock := controlledHarness(t)
	h.adapter.config.MaxWallSeconds = MaxWallSeconds
	h.job = multiDayJob(t, h, multiDayWall)
	_, prepared := h.prepare(t)
	expires := clock.at.Add(time.Minute)
	start := provider.StartAuthorization{AuthorizationID: "grant_fixture", ReadinessRevision: prepared.ReadinessRevision(), MaxRemoteWallSeconds: multiDayWall,
		ExecutionDeadline: clock.at.Add(time.Duration(multiDayWall) * time.Second).Format(time.RFC3339), StartBefore: expires.Format(time.RFC3339),
		Secrets: &provider.SecretAuthorization{AuthorizationID: "secret_fixture", ExpiresAt: expires.Format(time.RFC3339), Slots: []workloadsecret.Slot{{Name: "ingest", Exposure: "file"}}}}
	// The secret only has to outlive delivery at start; the attempt may run for seven days.
	if err := start.Validate(prepared); err != nil {
		t.Fatal(err)
	}
	late := start
	late.StartBefore = expires.Add(time.Second).Format(time.RFC3339)
	if late.Validate(prepared) == nil {
		t.Fatal("start window outlived its workload secret")
	}
	material := workloadsecret.Material{AuthorizationID: "secret_fixture", Key: workloadsecret.Key{WorkspaceID: prepared.Identity.WorkspaceID, JobID: prepared.Identity.JobID, AttemptID: prepared.Identity.AttemptID}, ExpiresAt: start.Secrets.ExpiresAt, Values: map[string][]byte{"ingest": []byte(sshWorkloadCanary)}}
	defer material.Clear()
	out := h.adapter.Start(context.Background(), prepared, &start, &material)
	if out.Status != provider.SubmissionAccepted || h.host.starts != 1 || len(h.host.secretPackets) != 1 {
		t.Fatal("multi-day start failed", out.Status, out.Problem)
	}
	var control startControl
	if json.Unmarshal(h.host.container(t).files["relay/start.json"].data, &control) != nil || control.MaxRemoteWallSeconds != multiDayWall || control.ExecutionDeadline != start.ExecutionDeadline {
		t.Fatal("start control lost the multi-day budget")
	}
}
