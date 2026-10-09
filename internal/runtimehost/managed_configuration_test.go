package runtimehost

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestManagedLiveConfigurationUsesFrozenWallPolicyInWorker(t *testing.T) {
	f := newManagedTestFixture(t, true)
	f.adapter.config.MachineShape = "cpu"
	original := f.connection(t, "wall-policy-create", "SYNTHETIC_FIRST")
	old := f.binding(t, original)
	// Neither current process defaults nor adapter defaults may reinterpret a
	// configure action or an already admitted profile.
	f.adapter.config.MachineShape = "NvidiaTeslaP100"
	f.adapter.config.MaxRemoteWallSeconds = 10
	op, err := f.services.connections.Submit(context.Background(), f.actor, "app", original.ID, "wall-policy-configure",
		[]byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":7200}}`, original.Revision)))
	if err != nil || op.Status != "succeeded" || f.adapter.checks != 1 {
		t.Fatal("configuration needs restart or provider check", err)
	}
	updated, err := f.services.connections.Get(context.Background(), f.actor, "app", original.ID)
	if err != nil || updated.Selection == nil || updated.Selection.Accelerator != "cpu" {
		t.Fatal("configuration changed resource type", err)
	}
	for _, tc := range []struct {
		binding provider.BindingSnapshot
		wall    int64
		valid   bool
	}{{old, 200, true}, {old, 600, false}, {f.binding(t, updated), 600, true}, {f.binding(t, updated), 7201, false}} {
		saved, err := f.host.store.ReadManagedBinding(context.Background(), tc.binding)
		if err != nil {
			t.Fatal(err)
		}
		p, err := f.host.managedProvider(f.config, f.services.connections, saved)
		if err != nil {
			t.Fatal(err)
		}
		job := wallPolicyJob(saved, tc.wall)
		if err := job.Validate(); err != nil {
			t.Fatal("invalid test job", err)
		}
		_, err = p.Validate(context.Background(), job)
		if (err == nil) != tc.valid {
			t.Fatalf("frozen wall policy=%d job=%d accepted=%v", saved.Profile.MaxRemoteWallSeconds, tc.wall, err == nil)
		}
	}
	if f.adapter.checks != 1 {
		t.Fatal("local policy validation contacted a provider")
	}
}

func wallPolicyJob(saved connections.RuntimeBinding, wall int64) provider.ResolvedJob {
	input := provider.InputSnapshot{Bundle: domain.ObjectMetadata{ID: "code", WorkspaceID: "app", Bytes: 4, SHA256: provider.Digest([]byte("code"))}, Inputs: []provider.FrozenInput{}}
	spec := []byte(fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"finite fixture","profile":%q,"bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","MUST_NOT_EXECUTE.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":%d,"setup_seconds":5,"finalization_grace_seconds":2}}`, saved.Profile.Binding.Profile, wall))
	return provider.ResolvedJob{
		Identity: provider.Identity{InstallationID: "installation", WorkspaceID: "app", JobID: "job", AttemptID: "attempt", InstanceID: saved.Profile.Binding.ProviderInstanceID, IntentID: "intent", ResourceKey: "resource", Nonce: strings.Repeat("a", 32), BundleSHA256: input.Bundle.SHA256, InputManifestSHA256: input.InputDigest()},
		Binding:  saved.Profile.Binding, Specification: spec, SpecificationSHA256: provider.Digest(spec), Required: []domain.CapabilityName{domain.CapabilityBatchExecution}, WallSeconds: wall, Inputs: &input,
	}
}
