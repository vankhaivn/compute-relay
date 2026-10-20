package cleanup

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// PreviewTimeout bounds one preview including its read-only provider calls. Kaggle needs
// several API reads per resource, so the HTTP route grants this budget instead of the
// short control-request deadline.
const PreviewTimeout = time.Minute

// WithPreviewResolver returns a service configured before the HTTP server starts.
// Admission-only installations still expose the local inventory without provider reads.
func (s *Service) WithPreviewResolver(resolver Resolver) *Service {
	copy := *s
	copy.previewResolver = resolver
	return &copy
}

func (s *Service) Preview(ctx context.Context, principal auth.Principal, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, discard bool) (Preview, error) {
	p, err := s.authorize(ctx, principal, w, auth.Operate)
	if err != nil {
		return Preview{}, err
	}
	repo := s.repo
	ctx, cancel := context.WithTimeout(ctx, PreviewTimeout)
	defer cancel()
	plan, err := repo.PlanCleanup(ctx, w, p.TokenID(), job, attempt, discard)
	if err != nil {
		return Preview{}, err
	}
	if len(plan.Resources) > operations.MaxCleanupResources || plan.WorkspaceID != w || plan.JobID != job || plan.AttemptID != attempt {
		return Preview{}, ErrInvalid
	}
	var adapter provider.Provider
	if s.previewResolver != nil {
		adapter, _ = s.previewResolver.Resolve(plan.Binding)
	}
	// One binding check covers every resource of this read-only preview.
	checked, bindingErr := false, error(nil)
	verified := func() bool {
		if !checked {
			bindingErr = adapter.Verify(ctx, plan.Binding)
			checked = true
		}
		return bindingErr == nil
	}
	// Resolve only a settled attempt's original staging intent, with metadata reads.
	// Exact discovery enriches cleanup evidence; it never reopens dispatch.
	absent := false
	if observer, ok := adapter.(StagingObserver); ok && plan.UnconfirmedPlan != nil && verified() {
		seen, observeErr := observer.ObserveStagingCleanup(ctx, plan.UnconfirmedPlan.Clone(), plan.PreparationID)
		if observeErr == nil && seen.Validate(*plan.UnconfirmedPlan, plan.PreparationID) == nil {
			switch seen.Status {
			case provider.ReconciliationFound:
				if recorder, ok := repo.(PreparationRepository); ok && seen.Prepared.Private {
					if err := recorder.RecordCleanupPreparation(ctx, w, p.TokenID(), job, attempt, plan, *seen.Prepared); err != nil {
						return Preview{}, err
					}
					plan, err = repo.PlanCleanup(ctx, w, p.TokenID(), job, attempt, discard)
					if err != nil {
						return Preview{}, err
					}
				}
			case provider.ReconciliationNotFound:
				absent = true
			}
		}
	}
	result := Preview{WorkspaceID: w, JobID: job, AttemptID: attempt, ObservedAt: s.clock.Now().UTC(), Eligible: plan.Eligible, Reason: plan.Reason, Resources: make([]ResourcePreview, 0, len(plan.Resources))}
	for _, resource := range plan.Resources {
		view := previewResource(ctx, plan, resource, adapter, verified)
		if absent && resource.Request.Purpose == "staging" && resource.Reason == "creation_unresolved" {
			view.Observation, view.Reason = "absent", "absence_observed"
			result.Reason = "absence_observed"
		}
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

func previewResource(ctx context.Context, plan PreviewPlan, resource Resource, adapter provider.Provider, verified func() bool) (view ResourcePreview) {
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
	if !verified() {
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
	return view
}
