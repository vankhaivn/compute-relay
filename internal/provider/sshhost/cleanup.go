package sshhost

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (a *Adapter) DescribeCleanup(request provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	if !request.Valid() || request.Remote.Identity.InstanceID != a.config.InstanceID {
		return provider.CleanupDescription{}, failure(ErrIdentity, "describe_cleanup_identity_unverified")
	}
	ref, err := decodeRef(request.Remote.Resource, request.Remote.Identity)
	if err != nil || request.Remote.Version != ref.ID && request.Remote.Version != "prepared" {
		return provider.CleanupDescription{}, failure(errors.Join(ErrIdentity, err), "describe_cleanup_identity_unverified")
	}
	description := provider.CleanupDescription{Kind: "docker_container", ID: ref.ID, Name: ref.Container, Version: ref.ID,
		Scope: "container_writable_layer", Support: domain.CapabilitySupportSupported, Reason: "immutable_container_id"}
	if ref.EngineID == "" {
		description.Support, description.Reason = domain.CapabilitySupportUnsupported, "engine_identity_not_recorded"
	}
	return description, nil
}

func (a *Adapter) CleanupResource(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	description, err := a.DescribeCleanup(request)
	if err != nil {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "recorded container ownership is incomplete or changed").WithDetail(domain.ReasonDetail, failureReason(err, "cleanup_receipt_invalid")).WithCause(err)
	}
	if description.Support != domain.CapabilitySupportSupported {
		return provider.CleanupOutcome{}, failure(provider.ErrCleanupUnsupported, "cleanup_engine_identity_missing")
	}
	outcome, err := a.cleanup(ctx, request)
	if provider.IsLocalStateError(err) {
		return outcome, err
	}
	if errors.Is(err, ErrIdentity) {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "container ownership no longer matches the original receipt").WithDetail(domain.ReasonDetail, failureReason(err, "cleanup_identity_unverified")).WithCause(err)
	}
	if errors.Is(err, ErrNotDone) {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation, "container activity does not permit cleanup").WithDetail(domain.ReasonDetail, failureReason(err, "cleanup_container_not_finished")).WithCause(err)
	}
	return outcome, err
}
