package cleanup

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (e *Engine) applyResources(ctx, call context.Context, w Work) (Outcome, error) {
	repo, ok := e.repo.(ResourceRepository)
	if !ok || len(w.Resources) == 0 || len(w.Resources) > MaxResources {
		return Outcome{}, ErrInvalid
	}
	p, resolveErr := func() (p provider.Provider, err error) {
		defer func() {
			if recover() != nil {
				p, err = nil, ErrInvalid
			}
		}()
		return e.resolver.Resolve(w.Binding)
	}()
	cleaner, supported := p.(provider.ResourceCleaner)
	verifier, verifies := p.(provider.BindingVerifier)
	allAbsent, priorUnresolved := true, false
	var firstFailure *Outcome
	for _, item := range w.Resources {
		result := item.Result
		if !result.Valid() || !item.Resource.Request.Valid() || item.Resource.Fingerprint() != result.Fingerprint {
			return Outcome{}, ErrInvalid
		}
		if result.State == ResourcePending || result.State == ResourceRunning {
			var failed Outcome
			switch {
			case priorUnresolved:
				result.State, result.Reason = ResourceRetained, "execution_cleanup_unresolved"
				failed = failure(domain.CodeProviderStateUnknown, "execution cleanup is unresolved; staging resources were retained for recovery", domain.RecommendedActionInspectProvider)
			case call.Err() != nil:
				result.State, result.Reason = ResourceRetained, "cleanup_deadline_reached"
				failed = failure(domain.CodeProviderStateUnknown, "the cleanup deadline was reached before this resource could be checked", domain.RecommendedActionInspectProvider)
			case resolveErr != nil || p == nil:
				result.State, result.Reason = ResourceRetained, "provider_not_configured"
				failed = failure(domain.CodeConfigurationInvalid, "the attempt's frozen provider binding is not configured; resources were retained", domain.RecommendedActionConfigure)
			case !supported || !verifies:
				result.State, result.Reason = ResourceRetained, "resource_cleanup_unsupported"
				failed = failure(domain.CodeUnsupportedCapability, "the adapter cannot clean individual ledger resources safely; resources were retained", domain.RecommendedActionInspectProvider)
			default:
				result, failed = inspectCleanupResource(call, cleaner, verifier, w, item)
				if result.State == ResourceRunning {
					if err := repo.BeginCleanupResource(call, w, item); err != nil {
						// No provider mutation follows a changed target or an expired fence.
						if errors.Is(err, ErrLeaseLost) || ctx.Err() != nil {
							return Outcome{}, err
						}
						result.State, result.Reason = ResourceRetained, "eligibility_changed"
						failed = failure(domain.CodeIllegalStateTransition, "cleanup eligibility changed before deletion; the resource was retained", domain.RecommendedActionRetryRead)
					} else {
						result, failed = callCleanupResource(call, cleaner, item, w.AcceptNameBasedDelete)
					}
				}
			}
			if ctx.Err() != nil {
				return Outcome{}, ctx.Err()
			}
			// A recovered running intent may already have changed the provider. A
			// later refusal cannot turn that missing acknowledgement into retention.
			if item.Result.State == ResourceRunning && !result.State.Confirmed() {
				result.State, result.Reason = ResourceUnknown, "prior_delete_outcome_unknown"
				failed = failure(domain.CodeProviderStateUnknown, "the earlier deletion outcome remains unknown; preserve the original target for recovery", domain.RecommendedActionInspectProvider)
			}
			if err := repo.CompleteCleanupResource(ctx, w, result); err != nil {
				return Outcome{}, err
			}
			if !result.State.Confirmed() && firstFailure == nil {
				firstFailure = &failed
			}
		}
		if !result.State.Confirmed() {
			priorUnresolved = true
			if firstFailure == nil {
				o := failure(domain.CodeProviderStateUnknown, "one or more original cleanup targets remain unresolved; inspect their resource outcomes", domain.RecommendedActionInspectProvider)
				firstFailure = &o
			}
		}
		allAbsent = allAbsent && result.State == ResourceAbsent
	}
	if firstFailure != nil {
		return *firstFailure, nil
	}
	if allAbsent {
		return Outcome{Effect: AlreadyAbsent}, nil
	}
	return Outcome{Effect: RemoteDeleted}, nil
}

func inspectCleanupResource(ctx context.Context, cleaner provider.ResourceCleaner, verifier provider.BindingVerifier, w Work, item ResourceWork) (result ResourceResult, failed Outcome) {
	result = item.Result
	defer func() {
		if recover() != nil {
			result.State, result.Reason = ResourceRetained, "provider_inspection_failed"
			failed = failure(domain.CodeProviderStateUnknown, "the provider could not verify this resource; no new deletion was attempted", domain.RecommendedActionInspectProvider)
		}
	}()
	description, err := cleaner.DescribeCleanup(item.Resource.Request)
	if err != nil || !description.Valid() {
		result.State, result.Reason = ResourceRetained, "identity_unverified"
		return result, failure(domain.CodeRemoteIdentityMismatch, "the recorded cleanup target could not be verified; this resource was retained", domain.RecommendedActionInspectProvider)
	}
	if description.Support != domain.CapabilitySupportSupported {
		result.State, result.Reason = ResourceRetained, description.Reason
		return result, failure(domain.CodeUnsupportedCapability, "exact-target cleanup is unavailable for this resource; inspect its retained reason", domain.RecommendedActionInspectProvider)
	}
	if description.NameBasedDelete && !w.AcceptNameBasedDelete {
		result.State, result.Reason = ResourceRetained, "name_based_delete_not_accepted"
		return result, failure(domain.CodeUnsupportedCapability, description.Warning+" Set accept_name_based_delete to confirm this risk.", domain.RecommendedActionInspectProvider)
	}
	if verifier.VerifyBinding(ctx, w.Binding) != nil {
		result.State, result.Reason = ResourceRetained, "binding_unverified"
		return result, failure(domain.CodeRemoteIdentityMismatch, "the frozen provider binding could not be verified; this resource was retained", domain.RecommendedActionInspectProvider)
	}
	result.State, result.Reason = ResourceRunning, "delete_intent_recorded"
	return result, Outcome{}
}

func callCleanupResource(ctx context.Context, cleaner provider.ResourceCleaner, item ResourceWork, acceptNameBasedDelete bool) (result ResourceResult, failed Outcome) {
	result = item.Result
	result.State, result.Reason = ResourceUnknown, "provider_outcome_unknown"
	failed = failure(domain.CodeProviderStateUnknown, "remote cleanup returned no confirmed outcome; preserve the original targets for recovery", domain.RecommendedActionInspectProvider)
	defer func() {
		if recover() != nil {
			result.State, result.Reason = ResourceUnknown, "provider_outcome_unknown"
		}
	}()
	request := item.Resource.Request
	request.Mode = provider.CleanupApply
	request.AcceptNameBasedDelete = acceptNameBasedDelete
	request.RecoverOnly = item.Result.State == ResourceRunning
	outcome, err := cleaner.CleanupResource(ctx, request)
	if err == nil && outcome.Deleted && !outcome.AlreadyAbsent && !outcome.WouldDelete {
		result.State, result.Reason = ResourceDeleted, "deletion_confirmed"
		return result, Outcome{}
	}
	if err == nil && outcome.AlreadyAbsent && !outcome.Deleted && !outcome.WouldDelete {
		result.State, result.Reason = ResourceAbsent, "absence_confirmed"
		return result, Outcome{}
	}
	if err != nil && (outcome.Deleted || outcome.AlreadyAbsent || outcome.WouldDelete) {
		return result, failed // Contradictory evidence cannot establish a safe refusal.
	}
	if errors.Is(err, provider.ErrCleanupUnsupported) {
		result.State, result.Reason = ResourceRetained, "resource_cleanup_unsupported"
		return result, failure(domain.CodeUnsupportedCapability, "the adapter retained an unsupported cleanup target", domain.RecommendedActionInspectProvider)
	}
	var value domain.Problem
	var pointer *domain.Problem
	var code domain.ErrorCode
	if errors.As(err, &value) {
		code = value.Code
	} else if errors.As(err, &pointer) && pointer != nil {
		code = pointer.Code
	}
	switch code {
	case domain.CodeRemoteIdentityMismatch:
		result.State, result.Reason = ResourceRetained, "identity_mismatch"
		failed = failure(code, "the remote resource did not match the original target; this resource was retained", domain.RecommendedActionInspectProvider)
	case domain.CodeRemoteExecutionUnresolved:
		result.State, result.Reason = ResourceRetained, "execution_unresolved"
		failed = failure(code, "the resource is active or unresolved; this resource was retained", domain.RecommendedActionReconcile)
	case domain.CodeProviderAuthFailed, domain.CodeProviderAccessDenied:
		result.State, result.Reason = ResourceFailed, "provider_access_denied"
		failed = failure(code, "the provider refused cleanup; no deletion was confirmed", domain.RecommendedActionConfigure)
	}
	return result, failed
}
