package cleanup

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (e *Engine) applyResources(ctx, call context.Context, w Work) (Outcome, error) {
	repo, ok := e.repo.(ResourceRepository)
	if !ok || len(w.Resources) == 0 || len(w.Resources) > operations.MaxCleanupResources {
		return Outcome{}, ErrInvalid
	}
	var p provider.Provider
	var resolveErr error
	needsProvider := false
	for _, item := range w.Resources {
		if item.Result.State == operations.ResourcePending || item.Result.State == operations.ResourceRunning {
			needsProvider = true
			break
		}
	}
	if needsProvider {
		p, resolveErr = func() (p provider.Provider, err error) {
			defer func() {
				if recover() != nil {
					p, err = nil, provider.WithReason(ErrInvalid, "cleanup_resolver_panic")
				}
			}()
			return e.resolver.Resolve(w.Binding)
		}()
	}
	if provider.IsLocalStateError(resolveErr) {
		return Outcome{}, resolveErr
	}
	allAbsent, priorUnresolved := true, false
	var firstFailure *Outcome
	for _, item := range w.Resources {
		result := item.Result
		if !result.Valid() || !item.Resource.Request.Valid() || item.Resource.Fingerprint() != result.Fingerprint {
			return Outcome{}, ErrInvalid
		}
		if result.State == operations.ResourcePending || result.State == operations.ResourceRunning {
			var failed Outcome
			switch {
			case priorUnresolved:
				result.State, result.Reason = operations.ResourceRetained, "execution_cleanup_unresolved"
				failed = failure(domain.CodeProviderStateUnknown, "execution cleanup is unresolved; staging resources were retained for recovery", domain.RecommendedActionInspectProvider)
			case call.Err() != nil:
				result.State, result.Reason = operations.ResourceRetained, "cleanup_deadline_reached"
				failed = failure(domain.CodeProviderStateUnknown, "the cleanup deadline was reached before this resource could be checked", domain.RecommendedActionInspectProvider)
			case resolveErr != nil || p == nil:
				result.State, result.Reason = operations.ResourceRetained, cleanupReason(resolveErr, "provider_not_configured")
				failed = failure(domain.CodeConfigurationInvalid, "the attempt's frozen provider binding is not configured; resources were retained", domain.RecommendedActionConfigure)
			default:
				var err error
				result, failed, err = inspectCleanupResource(call, p, w, item)
				if err != nil {
					return Outcome{}, err
				}
				if result.State == operations.ResourceRunning {
					if err := repo.BeginCleanupResource(call, w, item); err != nil {
						if ctx.Err() != nil {
							return Outcome{}, err
						}
						switch err {
						case operations.ErrChanged, operations.ErrBusy, operations.ErrState, operations.ErrUnresolved, operations.ErrUncollected:
							result.State, result.Reason = operations.ResourceRetained, "eligibility_changed"
							failed = failure(domain.CodeIllegalStateTransition, "cleanup eligibility changed before deletion; the resource was retained", domain.RecommendedActionRetryRead)
						default:
							// Storage failures are not evidence that eligibility changed.
							// Preserve the claim for retry/recovery or fatal worker shutdown.
							return Outcome{}, err
						}
					} else {
						result, failed, err = callCleanupResource(call, p, item)
						if err != nil {
							return Outcome{}, err
						}
					}
				}
			}
			if ctx.Err() != nil {
				return Outcome{}, ctx.Err()
			}
			// A recovered running intent may already have changed the provider. A
			// later refusal cannot turn that missing acknowledgement into retention.
			if item.Result.State == operations.ResourceRunning && !result.State.Confirmed() {
				result.State, result.Reason = operations.ResourceUnknown, "prior_delete_outcome_unknown"
				failed = failure(domain.CodeProviderStateUnknown, "the earlier deletion outcome remains unknown; preserve the original target for recovery", domain.RecommendedActionInspectProvider)
			}
			failed = withFailureReason(failed, result.Reason, nil)
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
				o = withFailureReason(o, result.Reason, nil)
				firstFailure = &o
			}
		}
		allAbsent = allAbsent && result.State == operations.ResourceAbsent
	}
	if firstFailure != nil {
		return *firstFailure, nil
	}
	if allAbsent {
		return Outcome{Effect: operations.AlreadyAbsent}, nil
	}
	return Outcome{Effect: operations.RemoteDeleted}, nil
}

func inspectCleanupResource(ctx context.Context, adapter provider.Provider, w Work, item ResourceWork) (result operations.ResourceResult, failed Outcome, localErr error) {
	result = item.Result
	defer func() {
		if recover() != nil {
			result.State, result.Reason = operations.ResourceRetained, "cleanup_inspection_panic"
			failed = failure(domain.CodeProviderStateUnknown, "the provider could not verify this resource; no new deletion was attempted", domain.RecommendedActionInspectProvider)
		}
		failed = withFailureReason(failed, result.Reason, nil)
	}()
	description, err := adapter.DescribeCleanup(item.Resource.Request)
	if provider.IsLocalStateError(err) {
		return result, Outcome{}, err
	}
	if err != nil || !description.Valid() {
		result.State, result.Reason = operations.ResourceRetained, cleanupReason(err, "cleanup_description_failed")
		return result, withFailureReason(failure(domain.CodeRemoteIdentityMismatch, "the recorded cleanup target could not be verified; this resource was retained", domain.RecommendedActionInspectProvider), result.Reason, err), nil
	}
	if description.Support != domain.CapabilitySupportSupported {
		result.State, result.Reason = operations.ResourceRetained, description.Reason
		return result, failure(domain.CodeUnsupportedCapability, "exact-target cleanup is unavailable for this resource; inspect its retained reason", domain.RecommendedActionInspectProvider), nil
	}
	if err := adapter.Verify(ctx, w.Binding); err != nil {
		if provider.IsLocalStateError(err) {
			return result, Outcome{}, err
		}
		result.State, result.Reason = operations.ResourceRetained, cleanupReason(err, "cleanup_binding_unverified")
		outcome := failure(domain.CodeProviderStateUnknown, "the frozen provider binding could not be verified; this resource was retained", domain.RecommendedActionInspectProvider)
		if known, ok := cleanupProblem(err); ok {
			known.Message = outcome.Problem.Message
			known.Details = known.PublicDetails()
			outcome.Problem = &known
		}
		return result, withFailureReason(outcome, result.Reason, err), nil
	}
	result.State, result.Reason = operations.ResourceRunning, "delete_intent_recorded"
	return result, Outcome{}, nil
}

func callCleanupResource(ctx context.Context, adapter provider.Provider, item ResourceWork) (result operations.ResourceResult, failed Outcome, localErr error) {
	result = item.Result
	result.State, result.Reason = operations.ResourceUnknown, "cleanup_delete_unconfirmed"
	failed = failure(domain.CodeProviderStateUnknown, "remote cleanup returned no confirmed outcome; preserve the original targets for recovery", domain.RecommendedActionInspectProvider)
	defer func() {
		if recover() != nil {
			result.State, result.Reason = operations.ResourceUnknown, "cleanup_delete_panic"
		}
		failed = withFailureReason(failed, result.Reason, nil)
	}()
	request := item.Resource.Request
	request.Mode = provider.CleanupApply
	request.RecoverOnly = item.Result.State == operations.ResourceRunning
	outcome, err := adapter.CleanupResource(ctx, request)
	if provider.IsLocalStateError(err) {
		return result, Outcome{}, err
	}
	if err == nil && outcome.Deleted && !outcome.AlreadyAbsent && !outcome.WouldDelete {
		result.State, result.Reason = operations.ResourceDeleted, "deletion_confirmed"
		return result, Outcome{}, nil
	}
	if err == nil && outcome.AlreadyAbsent && !outcome.Deleted && !outcome.WouldDelete {
		result.State, result.Reason = operations.ResourceAbsent, "absence_confirmed"
		return result, Outcome{}, nil
	}
	if err != nil {
		result.Reason = cleanupReason(err, "cleanup_delete_failed")
		failed = withFailureReason(failed, result.Reason, err)
	}
	if err != nil && (outcome.Deleted || outcome.AlreadyAbsent || outcome.WouldDelete) {
		return result, failed, nil // Contradictory evidence cannot establish a safe refusal.
	}
	if errors.Is(err, provider.ErrCleanupUnsupported) {
		result.State, result.Reason = operations.ResourceRetained, "resource_cleanup_unsupported"
		return result, failure(domain.CodeUnsupportedCapability, "the adapter retained an unsupported cleanup target", domain.RecommendedActionInspectProvider), nil
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
		result.State, result.Reason = operations.ResourceRetained, cleanupReason(err, "identity_mismatch")
		failed = failure(code, "the remote resource did not match the original target; this resource was retained", domain.RecommendedActionInspectProvider)
	case domain.CodeRemoteExecutionUnresolved:
		result.State, result.Reason = operations.ResourceRetained, cleanupReason(err, "execution_unresolved")
		failed = failure(code, "the resource is active or unresolved; this resource was retained", domain.RecommendedActionReconcile)
	case domain.CodeProviderAuthFailed, domain.CodeProviderAccessDenied:
		result.State, result.Reason = operations.ResourceFailed, cleanupReason(err, "provider_access_denied")
		failed = failure(code, "the provider refused cleanup; no deletion was confirmed", domain.RecommendedActionConfigure)
	}
	return result, withFailureReason(failed, result.Reason, err), nil
}

func cleanupReason(err error, fallback string) string {
	return provider.Reason(err, fallback)
}

func withFailureReason(outcome Outcome, reason string, cause error) Outcome {
	if outcome.Problem != nil {
		problem := outcome.Problem.WithDetail(domain.ReasonDetail, reason)
		if cause != nil {
			if known, ok := cleanupProblem(cause); ok && known.Code == problem.Code {
				problem = problem.WithRetrySemantics(known.SafeOperationRetry, known.ComputeMayHaveStarted)
				for _, key := range []string{domain.CauseDetail, domain.FirstCauseDetail} {
					if value := known.PublicDetails()[key]; value != "" {
						problem = problem.WithDetail(key, value)
					}
				}
			}
			problem = problem.WithCause(cause)
		}
		outcome.Problem = &problem
	}
	return outcome
}

func cleanupProblem(err error) (domain.Problem, bool) {
	var value domain.Problem
	var pointer *domain.Problem
	if errors.As(err, &value) && value.Validate() == nil {
		return value, true
	}
	if errors.As(err, &pointer) && pointer != nil && pointer.Validate() == nil {
		return *pointer, true
	}
	return domain.Problem{}, false
}
