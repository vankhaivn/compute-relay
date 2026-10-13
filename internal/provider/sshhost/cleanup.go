package sshhost

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var _ provider.ResourceCleaner = (*Adapter)(nil)

func (a *Adapter) DescribeCleanup(request provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	if !request.Valid() || request.Remote.Identity.InstanceID != a.config.InstanceID {
		return provider.CleanupDescription{}, ErrIdentity
	}
	ref, err := decodeRef(request.Remote.Resource, request.Remote.Identity)
	if err != nil || request.Remote.Version != ref.ID && request.Remote.Version != "prepared" {
		return provider.CleanupDescription{}, ErrIdentity
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
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "recorded container ownership is incomplete or changed")
	}
	if description.Support != domain.CapabilitySupportSupported {
		return provider.CleanupOutcome{}, provider.ErrCleanupUnsupported
	}
	outcome, err := a.cleanup(ctx, request.CleanupRequest, request.PlanSHA256)
	if errors.Is(err, ErrIdentity) {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "container ownership no longer matches the original receipt")
	}
	if errors.Is(err, ErrNotDone) {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageObservation, "container activity does not permit cleanup")
	}
	return outcome, err
}
