package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestManagedCPUSelectionAdmitsOnlyCPUAndIgnoresGPUAllowance(t *testing.T) {
	for _, remaining := range []float64{0, 3600} {
		t.Run(fmt.Sprintf("gpu-remaining-%g", remaining), func(t *testing.T) {
			f := newManagedTestFixture(t, true)
			f.adapter.quotaRemaining = &remaining
			gpuConnection := f.connection(t, "gpu-connection-key", "SYNTHETIC_SAME_ACCOUNT")
			f.adapter.config.MachineShape = "cpu"
			if err := f.adapter.config.validate(); err != nil {
				t.Fatal("CPU policy rejected", err)
			}
			c := f.connection(t, "cpu-connection-key", "SYNTHETIC_FIRST")
			if c.Selection.Accelerator != "cpu" {
				t.Fatal("selection lost frozen CPU resource")
			}
			descriptors, err := f.services.connections.Descriptors(context.Background(), f.actor, "app")
			if err != nil || len(descriptors[0].Capabilities.Accelerators) != 1 || descriptors[0].Capabilities.Accelerators[0] != "cpu" {
				t.Fatal("descriptor does not advertise configured CPU", err)
			}
			ctx := context.Background()
			objectService, err := objects.New(f.host.access, f.host.inputs, f.host.store)
			if err != nil {
				t.Fatal(err)
			}
			payload := "synthetic bundle never executed"
			bundle, err := objectService.Upload(ctx, f.actor, "app", int64(len(payload)), provider.Digest([]byte(payload)), strings.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			jobs, err := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			raw := fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"cpu fixture","profile":%q,"bundle":{"object_id":%q},"execution":{"kind":"python","command":["python","MUST_NOT_EXECUTE.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`, c.Selection.Profile, bundle.ID)
			gpuRaw := strings.Replace(raw, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`, 1)
			if _, err := jobs.Submit(ctx, f.actor, "app", "gpu-on-cpu-rejected", []byte(gpuRaw)); !errors.Is(err, admission.ErrRequirements) {
				t.Fatal("CPU profile admitted a GPU job", err)
			}
			job, err := jobs.Submit(ctx, f.actor, "app", "cpu-job-admission", []byte(raw))
			if err != nil {
				t.Fatal("CPU admission failed", err)
			}
			claim, err := f.host.store.ClaimNextManaged(ctx, "cpu_worker", time.Now().UTC())
			if err != nil || claim.Claim != nil {
				t.Fatal("CPU skipped finite consent", err)
			}
			grant := fmt.Sprintf(`{"attempt_id":%q,"max_remote_wall_seconds":10,"authorize_private_staging":true,"authorize_compute":true}`, job.AttemptID)
			if _, err := f.services.authorizations.Authorize(ctx, f.actor, "app", job.JobID, "cpu-grant-key", []byte(grant)); err != nil {
				t.Fatal("exhausted GPU quota blocked CPU grant", err)
			}
			claim, err = f.host.store.ClaimNextManaged(ctx, "cpu_worker", time.Now().UTC())
			if err != nil || claim.Claim == nil || claim.Claim.JobID != job.JobID {
				t.Fatal("CPU dispatch claim blocked", claim, err)
			}
			work, err := f.host.store.LoadDispatch(ctx, *claim.Claim, time.Now().UTC())
			if err != nil || work.Job.Profile.Accelerator != "cpu" {
				t.Fatal("dispatch lost CPU policy", err)
			}
			current, err := f.services.connections.Get(ctx, f.actor, "app", c.ID)
			if err != nil || current.ActiveAttempts != 1 {
				t.Fatal("CPU did not reserve an account slot", current, err)
			}
			cpuUnknown, gpuUnchanged := false, false
			for _, q := range current.Quotas {
				if q.Resource == "cpu" {
					cpuUnknown = q.Status == "unknown" && q.Remaining == nil && q.ObservedAt == nil
				}
				if q.Resource == "gpu" {
					gpuUnchanged = q.Status == "known" && q.Remaining != nil && *q.Remaining == int64(remaining)
				}
			}
			if !cpuUnknown || !gpuUnchanged {
				t.Fatal("CPU capacity invented a balance or rewrote GPU allowance", current.Quotas)
			}
			otherResource := raw
			if remaining > 0 {
				otherResource = strings.Replace(gpuRaw, c.Selection.Profile, gpuConnection.Selection.Profile, 1)
			}
			second, err := jobs.Submit(ctx, f.actor, "app", "same-account-second-job", []byte(otherResource))
			if err != nil {
				t.Fatal(err)
			}
			grant = fmt.Sprintf(`{"attempt_id":%q,"max_remote_wall_seconds":10,"authorize_private_staging":true,"authorize_compute":true}`, second.AttemptID)
			if _, err := f.services.authorizations.Authorize(ctx, f.actor, "app", second.JobID, "same-account-second-grant", []byte(grant)); err != nil {
				t.Fatal(err)
			}
			next, err := f.host.store.ClaimNextManaged(ctx, "second_worker", time.Now().UTC())
			if err != nil || next.Claim != nil {
				t.Fatal("CPU ignored shared account concurrency", next, err)
			}

		})
	}
}

func TestManagedCPURefreshKeepsHistoricalGPUResourcePolicy(t *testing.T) {
	f := newManagedTestFixture(t, true)
	original := f.connection(t, "original-gpu-key", "SYNTHETIC_FIRST")
	old := f.binding(t, original)
	f.adapter.config.MachineShape = "cpu"
	_, err := f.services.connections.Submit(context.Background(), f.actor, "app", original.ID, "check-cpu-key", []byte(fmt.Sprintf(`{"action":"check","expected_revision":%d}`, original.Revision)))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := f.services.connections.RunOnce(context.Background()); err != nil || !worked {
		t.Fatal(err)
	}
	updated, err := f.services.connections.Get(context.Background(), f.actor, "app", original.ID)
	if err != nil || updated.Selection == nil || updated.Selection.Accelerator != "cpu" || updated.Selection.Profile == original.Selection.Profile {
		t.Fatal("CPU refresh did not publish a distinct immutable selection", updated, err)
	}
	for _, snapshot := range []provider.BindingSnapshot{old, f.binding(t, updated), old} {
		saved, err := f.host.store.ReadManagedBinding(context.Background(), snapshot)
		if err != nil {
			t.Fatal(err)
		}
		// Legacy GPU profiles omitted the generic accelerator field. Recovery
		// must continue using their original private runtime configuration.
		if snapshot == old {
			saved.Profile.Accelerator = ""
		}
		p, err := f.host.managedProvider(f.adapter.config, f.services.connections, saved)
		if err != nil {
			t.Fatal("saved resource cannot be reconstructed", err)
		}
		support := p.Describe().Support(domain.CapabilityGPU)
		if snapshot == old && support != domain.CapabilitySupportUnknown || snapshot != old && support != domain.CapabilitySupportUnsupported {
			t.Fatal("current CPU default reinterpreted a historical GPU job", support)
		}
	}
	encoded, _ := json.Marshal(original.Selection)
	if !strings.Contains(string(encoded), `"accelerator":"gpu"`) {
		t.Fatal("GPU selection lacks its resource metadata")
	}
	saved, err := f.host.store.ReadManagedBinding(context.Background(), f.binding(t, updated))
	if err != nil {
		t.Fatal(err)
	}
	saved.Profile.Accelerator = "gpu"
	if _, err := f.host.managedProvider(f.adapter.config, f.services.connections, saved); err == nil {
		t.Fatal("conflicting frozen resources accepted")
	}
}
