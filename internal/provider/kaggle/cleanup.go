package kaggle

import (
	"context"
	"encoding/json"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var _ provider.ResourceCleaner = (*RuntimeAdapter)(nil)

// SDK 0.1.35's DeleteDataset/DeleteKernel accept only owner and slug. They cannot
// condition deletion on the recorded numeric ID/version. A preceding read cannot
// close the replacement race, so these resources remain explicitly retained.
func (p *RuntimeAdapter) DescribeCleanup(request provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	if p == nil || !request.Valid() || string(request.Remote.Identity.InstanceID) != p.config.InstanceID {
		return provider.CleanupDescription{}, ErrRuntimeBinding
	}
	id := request.Remote.Identity
	description := provider.CleanupDescription{Version: "1", Support: domain.CapabilitySupportUnsupported,
		Reason: "conditional_delete_unavailable"}
	switch request.Purpose {
	case "staging":
		var ref stagingReference
		seed, _ := json.Marshal([]string{string(id.InstallationID), string(id.WorkspaceID), string(id.InstanceID), string(id.JobID), string(id.AttemptID), string(request.CreationOperationID)})
		name := p.config.AccountName + "/crs-" + string(provider.Digest(seed))[:40]
		if closedObject([]byte(request.Remote.Resource), &ref) != nil || !positiveDecimal(ref.DatasetID) || ref.Version != 1 || !ref.Marker.Valid() || ref.Reference != name || request.Remote.Version != "prepared" {
			return provider.CleanupDescription{}, ErrStagingIdentity
		}
		description.Kind, description.ID, description.Name, description.Scope = "dataset", ref.DatasetID, ref.Reference, "dataset_object"
	case "execution":
		var ref kernelReference
		seed, _ := json.Marshal([]string{string(id.InstallationID), string(id.WorkspaceID), string(id.InstanceID), string(id.JobID), string(id.AttemptID), string(id.IntentID), id.ResourceKey, id.Nonce})
		name := p.config.AccountName + "/cre-" + string(provider.Digest(seed))[:40]
		if closedObject([]byte(request.Remote.Resource), &ref) != nil || !positiveDecimal(ref.KernelID) || !ref.SourceSHA256.Valid() || ref.Reference != name || request.Remote.Version != "1" {
			return provider.CleanupDescription{}, ErrExecutionIdentity
		}
		description.Kind, description.ID, description.Name, description.Scope = "kernel", ref.KernelID, ref.Reference, "kernel_object"
	}
	if !description.Valid() {
		return provider.CleanupDescription{}, ErrRuntimeBinding
	}
	return description, nil
}

func (p *RuntimeAdapter) CleanupResource(_ context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	if _, err := p.DescribeCleanup(request); err != nil {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "recorded Kaggle resource identity is incomplete or changed")
	}
	// No credential resolution or provider RPC occurs, including in dry-run mode.
	return provider.CleanupOutcome{}, ErrRuntimeCleanup
}
