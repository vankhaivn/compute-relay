package cleanup

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// WithPreviewResolver returns a service configured before the HTTP server starts.
// Admission-only installations still expose the local inventory without provider reads.
func (s *Service) WithPreviewResolver(resolver Resolver) *Service {
	copy := *s
	copy.previewResolver = resolver
	return &copy
}

func (s *Service) Preview(ctx context.Context, principal auth.Principal, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, discard, acceptNameBasedDelete bool) (Preview, error) {
	p, err := s.authorize(ctx, principal, w, auth.Operate)
	if err != nil {
		return Preview{}, err
	}
	repo := s.repo
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	plan, err := repo.PlanCleanup(ctx, w, p.TokenID(), job, attempt, discard)
	if err != nil {
		return Preview{}, err
	}
	if len(plan.Resources) > operations.MaxCleanupResources || plan.WorkspaceID != w || plan.JobID != job || plan.AttemptID != attempt {
		return Preview{}, ErrInvalid
	}
	result := Preview{WorkspaceID: w, JobID: job, AttemptID: attempt, ObservedAt: s.clock.Now().UTC(), Eligible: plan.Eligible, Reason: plan.Reason, Resources: make([]ResourcePreview, 0, len(plan.Resources))}
	var adapter provider.Provider
	if s.previewResolver != nil {
		adapter, _ = s.previewResolver.Resolve(plan.Binding)
	}
	for _, resource := range plan.Resources {
		view := previewResource(ctx, plan, resource, adapter, acceptNameBasedDelete)
		result.Resources = append(result.Resources, view)
		if !view.Eligible {
			if result.Eligible {
				result.Reason = view.Reason
			}
			result.Eligible = false
		}
	}
	// A fresh transaction rechecks scope, token revocation, active/held work and the
	// complete resource inventory after the remote reads. No preview receipt can apply.
	again, err := repo.PlanCleanup(ctx, w, p.TokenID(), job, attempt, discard)
	if err != nil {
		return Preview{}, err
	}
	if again.Fingerprint() != plan.Fingerprint() {
		return Preview{}, operations.ErrChanged
	}
	return result, nil
}

func previewResource(ctx context.Context, plan PreviewPlan, resource Resource, adapter provider.Provider, acceptNameBasedDelete bool) (view ResourcePreview) {
	request := resource.Request
	view = ResourcePreview{ResourceID: request.LedgerID, Purpose: request.Purpose, Fingerprint: resource.Fingerprint(), Observation: "not_checked", Reason: plan.Reason}
	defer func() {
		if recover() != nil {
			view.Eligible, view.Observation, view.Reason = false, "unknown", "provider_inspection_failed"
		}
	}()
	if resource.Reason != "" {
		view.Reason = resource.Reason
	}
	if adapter == nil {
		if resource.Reason == "" {
			view.Reason = "provider_not_configured"
		}
		return view
	}
	description, err := adapter.DescribeCleanup(request)
	if err != nil || !description.Valid() {
		if resource.Reason == "" {
			view.Reason = "identity_unverified"
		}
		return view
	}
	view.Target = &description
	if !plan.Eligible || resource.Reason != "" {
		return view
	}
	if description.Support != domain.CapabilitySupportSupported {
		view.Reason = description.Reason
		return view
	}
	if adapter.Verify(ctx, plan.Binding) != nil {
		view.Observation, view.Reason = "unknown", "binding_unverified"
		return view
	}
	request.Mode = provider.CleanupDryRun
	outcome, err := adapter.CleanupResource(ctx, request)
	if err != nil || outcome.Deleted || outcome.WouldDelete && outcome.AlreadyAbsent {
		view.Observation, view.Reason = "unknown", "remote_state_unverified"
		return view
	}
	switch {
	case outcome.AlreadyAbsent:
		view.Eligible, view.Observation, view.Reason = true, "absent", "absence_observed"
	case outcome.WouldDelete:
		view.Eligible, view.Observation, view.Reason = true, "present", "eligible"
	default:
		view.Observation, view.Reason = "unknown", "remote_state_unverified"
	}
	if view.Eligible && description.NameBasedDelete && !acceptNameBasedDelete {
		view.Eligible, view.Reason = false, "name_based_delete_not_accepted"
	}
	return view
}
