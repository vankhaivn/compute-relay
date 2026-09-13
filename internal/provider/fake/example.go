package fake

import (
	"encoding/json"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// ExampleJob contains synthetic, non-secret data only. The command is NEVER executed.
func ExampleJob(instance domain.ProviderInstanceID) provider.ResolvedJob {
	spec := json.RawMessage(`{"api_version":"compute-connector/v1alpha1","name":"offline-fixture","profile":"fixture","bundle":{"object_id":"obj_code"},"execution":{"kind":"python","command":["python","never-executed.py"],"working_directory":".","environment":{},"dependencies":{}},"inputs":[],"outputs":[{"path":"result.json","required":true}],"resources":{"accelerator":"cpu","minimum_gpu_count":0},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":60,"setup_seconds":10,"finalization_grace_seconds":5}}`)
	return provider.ResolvedJob{
		Identity: provider.Identity{InstallationID: "install_fixture", WorkspaceID: "ws_fixture", JobID: "job_fixture", AttemptID: "att_fixture",
			InstanceID: instance, IntentID: "intent_fixture", ResourceKey: "resource_fixture", Nonce: "fixture-nonce-00000001",
			BundleSHA256: provider.Digest([]byte("fixture-code")), InputManifestSHA256: provider.Digest([]byte("fixture-inputs"))},
		Binding:       domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: instance, ConfigurationRevision: "fixture-1"},
		Specification: spec, SpecificationSHA256: provider.Digest(spec), WallSeconds: 60,
		Required: []domain.CapabilityName{domain.CapabilityBatchExecution, domain.CapabilityPython, domain.CapabilityPrivateInputStaging, domain.CapabilityExecutionTimeout, domain.CapabilityRemoteNetworkControl},
	}
}
