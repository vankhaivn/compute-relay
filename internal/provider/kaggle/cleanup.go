package kaggle

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var _ provider.ResourceCleaner = (*RuntimeAdapter)(nil)

const nameBasedDeleteWarning = "Kaggle deletes by owner/name. Renaming may leave the original undeleted. If someone replaces it after Relay checks it, the replacement may be deleted, including its versions and outputs."

func (p *RuntimeAdapter) DescribeCleanup(request provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	if p == nil || !request.Valid() || string(request.Remote.Identity.InstanceID) != p.config.InstanceID {
		return provider.CleanupDescription{}, ErrRuntimeBinding
	}
	id := request.Remote.Identity
	description := provider.CleanupDescription{Version: "1", Support: domain.CapabilitySupportSupported,
		Reason: "name_based_delete_requires_acceptance", NameBasedDelete: true, Warning: nameBasedDeleteWarning}
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

func (p *RuntimeAdapter) CleanupResource(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	description, err := p.DescribeCleanup(request)
	if err != nil {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "recorded Kaggle resource identity is incomplete or changed")
	}
	if request.Mode == provider.CleanupApply && !request.AcceptNameBasedDelete {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeUnsupportedCapability, domain.FailureStageValidation, nameBasedDeleteWarning+" Set accept_name_based_delete to confirm this risk.")
	}
	if p.load == nil || p.stager == nil || p.runCleanup == nil {
		return provider.CleanupOutcome{}, ErrRuntimeBinding
	}
	plan, prepared, err := p.load(ctx, request.Remote.Identity)
	if err != nil || plan.Job.Identity != request.Remote.Identity || plan.Job.Binding != p.binding.Binding || plan.Digest() != request.PlanSHA256 || prepared.Validate(plan, request.CreationOperationID) != nil {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "original cleanup plan or preparation could not be verified")
	}
	query := cleanupRequest{Protocol: 1, Owner: p.config.AccountName, Slug: strings.SplitN(description.Name, "/", 2)[1], Kind: description.Kind, ID: description.ID, AcceptNameBasedDelete: request.AcceptNameBasedDelete}
	if request.Purpose == "staging" {
		var ref stagingReference
		if prepared.Resource != request.Remote.Resource || closedObject([]byte(prepared.Resource), &ref) != nil {
			return provider.CleanupOutcome{}, ErrStagingIdentity
		}
		query.SHA256 = ref.Marker
	} else {
		var ref kernelReference
		if closedObject([]byte(request.Remote.Resource), &ref) != nil {
			return provider.CleanupOutcome{}, ErrExecutionIdentity
		}
		query.SHA256 = ref.SourceSHA256
	}
	mode := "apply"
	if request.Mode == provider.CleanupDryRun {
		mode = "preview"
	} else if request.RecoverOnly {
		mode = "recover"
	}
	local, err := p.stager.local(ctx, p.config, Local, nil)
	if err != nil || !local.valid(Local) || local.Local != "ready" {
		return provider.CleanupOutcome{}, ErrProcess
	}
	var response cleanupResponse
	called := false
	err = p.stager.credentials.WithCredential(ctx, p.config.CredentialRef, func(secret []byte) error {
		if called || len(secret) == 0 || len(secret) > 8192 {
			return ErrProtocol
		}
		called = true
		for _, b := range secret {
			if b < 33 || b > 126 {
				return ErrProtocol
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		response, err = p.runCleanup(ctx, p.config, mode, secret, query)
		return err
	})
	if err != nil || !called || !response.valid() {
		return provider.CleanupOutcome{}, ErrProcess
	}
	switch response.Status {
	case "absent":
		return provider.CleanupOutcome{AlreadyAbsent: true}, nil
	case "deleted":
		if mode == "apply" {
			return provider.CleanupOutcome{Deleted: true}, nil
		}
	case "would_delete":
		if mode == "preview" {
			return provider.CleanupOutcome{WouldDelete: true}, nil
		}
	case "identity_mismatch":
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "Kaggle resource identity, version, privacy or ownership bytes changed")
	case "active":
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation, "Kaggle execution is active or unresolved")
	}
	return provider.CleanupOutcome{}, provider.Problem(domain.CodeProviderStateUnknown, domain.FailureStageObservation, "Kaggle deletion or absence could not be confirmed; preserve the original cleanup target")
}

type cleanupRequest struct {
	Protocol              int                 `json:"protocol"`
	Owner                 string              `json:"owner"`
	Slug                  string              `json:"slug"`
	Kind                  string              `json:"kind"`
	ID                    string              `json:"id"`
	SHA256                domain.SHA256Digest `json:"sha256"`
	AcceptNameBasedDelete bool                `json:"accept_name_based_delete"`
}

type cleanupResponse struct {
	Protocol int    `json:"protocol"`
	Status   string `json:"status"`
}

func (r cleanupResponse) valid() bool {
	if r.Protocol != 1 {
		return false
	}
	switch r.Status {
	case "absent", "deleted", "would_delete", "identity_mismatch", "active", "unknown":
		return true
	}
	return false
}
