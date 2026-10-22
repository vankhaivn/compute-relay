package kaggle

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (p *RuntimeAdapter) DescribeCleanup(request provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	if p == nil || !request.Valid() || string(request.Remote.Identity.InstanceID) != p.config.InstanceID {
		return provider.CleanupDescription{}, provider.WithReason(ErrRuntimeBinding, "cleanup_binding_invalid")
	}
	id := request.Remote.Identity
	description := provider.CleanupDescription{Version: "1", Support: domain.CapabilitySupportSupported,
		Reason: "recorded_attempt_resource"}
	switch request.Purpose {
	case "staging":
		var ref stagingReference
		seed, _ := json.Marshal([]string{string(id.InstallationID), string(id.WorkspaceID), string(id.InstanceID), string(id.JobID), string(id.AttemptID), string(request.CreationOperationID)})
		name := p.config.AccountName + "/crs-" + string(provider.Digest(seed))[:40]
		if closedObject([]byte(request.Remote.Resource), &ref) != nil || !positiveDecimal(ref.DatasetID) || ref.Version != 1 || !ref.Marker.Valid() || ref.Reference != name || request.Remote.Version != "prepared" {
			return provider.CleanupDescription{}, provider.WithReason(ErrStagingIdentity, "cleanup_staging_identity_invalid")
		}
		description.Kind, description.ID, description.Name, description.Scope = "dataset", ref.DatasetID, ref.Reference, "dataset_object"
	case "execution":
		var ref kernelReference
		seed, _ := json.Marshal([]string{string(id.InstallationID), string(id.WorkspaceID), string(id.InstanceID), string(id.JobID), string(id.AttemptID), string(id.IntentID), id.ResourceKey, id.Nonce})
		name := p.config.AccountName + "/cre-" + string(provider.Digest(seed))[:40]
		if closedObject([]byte(request.Remote.Resource), &ref) != nil || !positiveDecimal(ref.KernelID) || !ref.SourceSHA256.Valid() || ref.Reference != name || request.Remote.Version != "1" {
			return provider.CleanupDescription{}, provider.WithReason(ErrExecutionIdentity, "cleanup_execution_identity_invalid")
		}
		description.Kind, description.ID, description.Name, description.Scope = "kernel", ref.KernelID, ref.Reference, "kernel_object"
	}
	if !description.Valid() {
		return provider.CleanupDescription{}, provider.WithReason(ErrRuntimeBinding, "cleanup_binding_invalid")
	}
	return description, nil
}

func (p *RuntimeAdapter) CleanupResource(ctx context.Context, request provider.CleanupResourceRequest) (out provider.CleanupOutcome, retErr error) {
	reason := "cleanup_binding_invalid"
	defer func() {
		if retErr != nil {
			retErr = provider.WithReason(retErr, reason)
		}
	}()
	description, err := p.DescribeCleanup(request)
	if err != nil {
		reason = provider.Reason(err, reason)
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "recorded Kaggle resource identity is incomplete or changed")
	}
	if p.load == nil || p.stager == nil || p.runCleanup == nil {
		return provider.CleanupOutcome{}, ErrRuntimeBinding
	}
	plan, prepared, err := p.load(ctx, request.Remote.Identity)
	if err != nil {
		return provider.CleanupOutcome{}, provider.LocalStateError{Err: err}
	}
	if plan.Job.Identity != request.Remote.Identity || plan.Job.Binding != p.binding.Binding || plan.Digest() != request.PlanSHA256 || prepared.Validate(plan, request.CreationOperationID) != nil {
		reason = "cleanup_plan_invalid"
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "original cleanup plan or preparation could not be verified")
	}
	query := cleanupRequest{Protocol: 1, Owner: p.config.AccountName, Slug: strings.SplitN(description.Name, "/", 2)[1], Kind: description.Kind, ID: description.ID}
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
	var response cleanupResponse
	called := false
	invoked := false
	violated := false
	err = p.stager.credentials.WithCredential(ctx, p.config.CredentialRef, func(secret []byte) error {
		if called {
			violated = true
			return ErrProtocol
		}
		called = true
		if len(secret) == 0 || len(secret) > 8192 {
			return ErrProtocol
		}
		for _, b := range secret {
			if b < 33 || b > 126 {
				return ErrProtocol
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		invoked = true
		response, err = p.runCleanup(ctx, p.config, mode, secret, query)
		return err
	})
	if provider.IsLocalStateError(err) {
		return provider.CleanupOutcome{}, err
	}
	if err != nil || !called || violated || !response.valid() {
		if ctx.Err() != nil {
			return provider.CleanupOutcome{}, contextStep(ctx.Err(), "cleanup")
		}
		reason = "cleanup_call_failed"
		if !invoked {
			reason = "cleanup_credential_unavailable"
		}
		if violated {
			reason = "cleanup_credential_callback_invalid"
		}
		if err == nil && called && !violated {
			reason = "cleanup_response_invalid"
		}
		return provider.CleanupOutcome{}, provider.WithReason(ErrUnavailable, provider.Reason(err, reason))
	}
	reason = response.Reason
	if reason == "" {
		reason = "cleanup_" + response.Status
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
	if response.Status == "deleted" || response.Status == "would_delete" {
		reason = "cleanup_response_invalid"
	}
	return provider.CleanupOutcome{}, provider.Problem(domain.CodeProviderStateUnknown, domain.FailureStageObservation, "Kaggle deletion or absence could not be confirmed; preserve the original cleanup target")
}

type cleanupRequest struct {
	Protocol int                 `json:"protocol"`
	Owner    string              `json:"owner"`
	Slug     string              `json:"slug"`
	Kind     string              `json:"kind"`
	ID       string              `json:"id"`
	SHA256   domain.SHA256Digest `json:"sha256"`
}

type cleanupResponse struct {
	Protocol int    `json:"protocol"`
	Status   string `json:"status"`
	Reason   string `json:"-"`
}

func (r cleanupResponse) valid() bool {
	if r.Reason != "" && (!cleanupReasons[r.Reason] || r.Status != "unknown" && r.Status != "active" && r.Status != "identity_mismatch") {
		return false
	}
	if r.Protocol != 1 {
		return false
	}
	switch r.Status {
	case "absent", "deleted", "would_delete", "identity_mismatch", "active", "unknown":
		return true
	}
	return false
}

// ObserveStagingCleanup resolves only the original preparation after compute has
// ended. NotFound requires exact HTTP 404; a private-name 403 stays unknown.
func (p *RuntimeAdapter) ObserveStagingCleanup(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	if p == nil || p.stager == nil || plan.Job.Binding != p.binding.Binding {
		return provider.PreparationObservation{}, provider.WithReason(ErrRuntimeBinding, "cleanup_staging_binding_invalid")
	}
	seen, _, err := p.stager.check(ctx, "cleanup_observe", plan, operation)
	return seen, err
}
