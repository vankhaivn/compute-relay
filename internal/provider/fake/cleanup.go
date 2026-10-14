package fake

import (
	"context"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (p *Basic) DescribeCleanup(request provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	if !request.Valid() || request.Remote.Identity.InstanceID != p.instance {
		return provider.CleanupDescription{}, identityError()
	}
	return provider.CleanupDescription{Kind: "fake_resource", ID: request.Remote.Resource, Name: request.Remote.Resource,
		Version: "fixture-1", Scope: "fake_attempt", Support: domain.CapabilitySupportSupported, Reason: "offline_fixture"}, nil
}

func (p *Basic) CleanupResource(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	if err := ctx.Err(); err != nil {
		return provider.CleanupOutcome{}, err
	}
	if _, err := p.DescribeCleanup(request); err != nil {
		return provider.CleanupOutcome{}, err
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, ok := p.backend.records[attemptKey(request.Remote.Identity)]
	if !ok || request.CreationOperationID != r.prepared.PreparationID || request.PlanSHA256 != r.prepared.PlanSHA256 ||
		request.Remote.Resource != r.prepared.Resource || request.Remote.Identity != r.prepared.Identity ||
		request.Purpose == "execution" && request.Remote != r.remote {
		return provider.CleanupOutcome{}, identityError()
	}
	if r.submitted && !p.state(r).Terminal() {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation, "fake resource is active")
	}
	p.backend.stats.CleanupCalls++
	if r.deleted {
		return provider.CleanupOutcome{AlreadyAbsent: true}, nil
	}
	if request.RecoverOnly {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeProviderStateUnknown, domain.FailureStageObservation, "the original deletion outcome remains unresolved")
	}
	if request.Mode == provider.CleanupDryRun {
		return provider.CleanupOutcome{WouldDelete: true}, nil
	}
	r.deleted = true
	return provider.CleanupOutcome{Deleted: true}, nil
}
