package dispatch

import (
	"encoding/json"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// Apply is the only journal state machine. It returns facts to commit, not a callback
// that could hold a SQL transaction over I/O. Each begin action is a one-shot gate.
func Apply(old Journal, state domain.AttemptState, a Action, now time.Time) (Journal, domain.AttemptState, domain.EventType, error) {
	if !old.Valid() || state.Validate() != nil || !scheduler.ValidTime(now) || old.Version == math.MaxInt64 {
		return Journal{}, state, "", ErrInvalid
	}
	raw, err := json.Marshal(old)
	if err != nil || len(raw) > MaxJournalBytes {
		return Journal{}, state, "", ErrInvalid
	}
	var j Journal
	if json.Unmarshal(raw, &j) != nil {
		return Journal{}, state, "", ErrInvalid
	}
	next := state
	var event domain.EventType
	switch a.Kind {
	case PreventDispatch:
		if old.SubmitStarted || state.Orchestration.Terminal() || state.Execution != domain.ExecutionNotSubmitted || (state.RemoteActivity != domain.RemoteActivityNotStarted && state.RemoteActivity != domain.RemoteActivityInactive) {
			return Journal{}, state, "", ErrConflict
		}
		j.Phase = Prevented
		next.Orchestration = domain.OrchestrationCancelled
		next.Cancellation = domain.CancellationPrevented
		next.ReleaseEvidence = domain.ReleaseEvidenceNotApplicable
		event = domain.EventCancellationRequested
	case RequestReconciliation:
		if !old.SubmitStarted || state.Execution.Terminal() || state.Orchestration.Terminal() || (old.Phase != Attention && old.Phase != Submitting && old.Phase != Submitted) || old.Problem != nil && (old.Problem.Code == domain.CodeRemoteIdentityMismatch || old.Problem.Code == domain.CodePrivateStagingUnavailable) {
			return Journal{}, state, "", ErrConflict
		}
		j.Phase = Submitting
		if j.Remote != nil {
			j.Phase = Submitted
		}
		j.Failures = 0
		j.Problem = nil
		next.Orchestration = domain.OrchestrationReconciling
		if cancellationPending(state) {
			next.Orchestration = domain.OrchestrationCancelling
		}
		event = domain.EventReconciliationRequested
	case BeginPreparation:
		if old.Phase != Local || old.Plan != nil || !scheduler.LocalOnly(state) || a.Plan == nil || a.Plan.Job.Inputs == nil || a.Plan.Job.Validate() != nil || !a.PreparationID.Valid() {
			return Journal{}, state, "", ErrConflict
		}
		plan := a.Plan.Clone()
		j.Plan = &plan
		j.PreparationID = a.PreparationID
		j.Phase = Staging
		j.Problem = nil
		j.Failures = 0
		next.Orchestration = domain.OrchestrationPreparing
		event = domain.EventPreparationIntended
	case PreparationSeen:
		if old.Phase != Staging || old.SubmitStarted || a.Prepared == nil || a.Prepared.Validate(*old.Plan, old.PreparationID) != nil {
			return Journal{}, state, "", ErrConflict
		}
		if old.Prepared != nil && old.Prepared.Resource != a.Prepared.Resource {
			return Journal{}, state, "", ErrConflict
		}
		copy := *a.Prepared
		j.Prepared = &copy
		j.Problem = nil
		j.Failures = 0
		if copy.Ready {
			j.Phase = Ready
			event = domain.EventProviderStagingReady
		} else {
			event = domain.EventPreparationObserved
		}
		next.Orchestration = domain.OrchestrationPreparing
		if !copy.Private {
			// Preserve the discovered resource for inspection, but never submit it.
			j.Phase = Attention
			j.Problem = problem(domain.CodePrivateStagingUnavailable, false)
			next.Orchestration = domain.OrchestrationNeedsAttention
			event = domain.EventPreparationObserved
		}
	case BeginSubmission:
		if old.Phase != Ready || old.SubmitStarted || old.Prepared == nil || !old.Prepared.Ready || !old.Prepared.Private || !scheduler.LocalOnly(state) || state.Cancellation != domain.CancellationNotRequested {
			return Journal{}, state, "", ErrConflict
		}
		j.SubmitStarted = true
		j.Phase = Submitting
		j.Problem = nil
		j.Failures = 0
		next.Orchestration = domain.OrchestrationDispatching
		next.Execution = domain.ExecutionUnknown
		next.RemoteActivity = domain.RemoteActivityPossible
		event = domain.EventSubmissionIntentRecorded
	case SubmissionSeen:
		if old.Phase != Submitting || !old.SubmitStarted || old.Remote != nil || a.Submission == nil || a.Submission.Validate(old.Plan.Job.Identity) != nil {
			return Journal{}, state, "", ErrConflict
		}
		switch a.Submission.Status {
		case provider.SubmissionAccepted:
			remote := *a.Submission.Remote
			j.Remote = &remote
			j.Phase = Submitted
			j.Problem = nil
			j.Failures = 0
			next.Orchestration = domain.OrchestrationSubmitted
			if cancellationPending(state) {
				next.Orchestration = domain.OrchestrationCancelling
			}
			event = domain.EventSubmissionAccepted
			// Accepted does not imply the provider has started or even reported queued.
		case provider.SubmissionRejected:
			j.Phase = Rejected
			code := domain.CodeProviderRejected
			if a.Submission.Problem != nil {
				switch a.Submission.Problem.Code {
				case domain.CodeProviderRateLimited, domain.CodeQuotaExhausted, domain.CodeProviderAuthFailed, domain.CodeProviderAccessDenied:
					code = a.Submission.Problem.Code
				}
			}
			j.Problem = problem(code, false)
			next.Orchestration = domain.OrchestrationFailed
			next.Execution = domain.ExecutionNotSubmitted
			next.RemoteActivity = domain.RemoteActivityInactive
			next.ReleaseEvidence = domain.ReleaseEvidenceNotApplicable
			if state.Cancellation != domain.CancellationNotRequested {
				// Explicit non-acceptance, not a not-found lookup, proves prevention.
				next.Orchestration = domain.OrchestrationCancelled
				next.Cancellation = domain.CancellationPrevented
			}
			event = domain.EventSubmissionRejected
		case provider.SubmissionUnknown:
			j.Problem = problem(domain.CodeProviderSubmissionUnknown, true)
			next.Orchestration = domain.OrchestrationReconciling
			event = domain.EventSubmissionUnknown
		}
	case ObservationSeen:
		if old.Phase != Submitted || old.Remote == nil || a.Observation == nil || a.Observation.Validate(*old.Remote) != nil {
			return Journal{}, state, "", ErrConflict
		}
		obs := *a.Observation
		if obs.ObservedAt.After(now) || old.Observation != nil && !obs.ObservedAt.After(old.Observation.ObservedAt) {
			return Journal{}, state, "", ErrStaleObservation
		}
		next.Execution = obs.Execution
		next.RemoteActivity = obs.RemoteActivity
		next.ReleaseEvidence = obs.ReleaseEvidence
		if obs.Execution == domain.ExecutionUnknown {
			// A new uncertain poll does not erase earlier confirmed execution/activity evidence.
			next.Execution = state.Execution
			next.RemoteActivity = state.RemoteActivity
			next.ReleaseEvidence = state.ReleaseEvidence
			next.Orchestration = domain.OrchestrationReconciling
		} else if obs.Execution.Terminal() {
			next.Orchestration = domain.OrchestrationCollecting
			j.Phase = Collectible
			if obs.Execution == domain.ExecutionTimedOut {
				next.DeadlineExceeded = true
			}
			if state.Cancellation != domain.CancellationNotRequested {
				if obs.Execution == domain.ExecutionCancelled {
					next.Cancellation = domain.CancellationConfirmed
				} else {
					next.Cancellation = domain.CancellationTooLate
				}
			}
		} else if obs.Execution == domain.ExecutionRunning || obs.Execution == domain.ExecutionStarting {
			next.Orchestration = domain.OrchestrationRunning
		} else {
			next.Orchestration = domain.OrchestrationSubmitted
		}
		if !obs.Execution.Terminal() && cancellationPending(state) {
			next.Orchestration = domain.OrchestrationCancelling
		}
		if !obs.Execution.Terminal() && state.Cancellation == domain.CancellationManual {
			next.Orchestration = domain.OrchestrationNeedsAttention
		}
		if err := domain.ValidateAttemptStateTransition(state, next); err != nil {
			return Journal{}, state, "", ErrStaleObservation
		}
		// Raw provider strings are not persisted by the common runtime. Keep a bounded enum
		// summary instead; adapter-specific diagnostics require a separately redacted export.
		obs.RawState = string(obs.Execution)
		j.Observation = &obs
		j.Problem = nil
		j.Failures = 0
		if obs.Execution == domain.ExecutionUnknown {
			j.Failures = min(old.Failures+1, MaxFailures)
			j.Problem = problem(domain.CodeProviderStateUnknown, true)
			if j.Failures == MaxFailures {
				j.Phase = Attention
				next.Orchestration = domain.OrchestrationNeedsAttention
				if cancellationPending(state) {
					next.Cancellation = domain.CancellationManual
				}
			}
		}
		event = domain.EventExecutionObserved
	case Fault:
		if old.Phase == Rejected || old.Phase == Failed || old.Phase == Collectible || old.Phase == Attention || old.Phase == Prevented {
			return Journal{}, state, "", ErrConflict
		}
		j.Failures++
		if j.Failures > MaxFailures {
			j.Failures = MaxFailures
		}
		may := old.SubmitStarted && old.Phase != Rejected
		j.Problem = problem(a.Code, may)
		if j.Problem == nil {
			return Journal{}, state, "", ErrInvalid
		}
		permanent := a.Code == domain.CodeRemoteIdentityMismatch || a.Code == domain.CodePrivateStagingUnavailable
		if old.Plan == nil {
			next.Orchestration = domain.OrchestrationBlocked
			switch a.Code {
			case domain.CodeInputNotFound, domain.CodeInputDigestMismatch, domain.CodeInputTooLarge, domain.CodeInvalidInputPath, domain.CodeUnsupportedCapability, domain.CodeResourceRequirementUnsatisfied:
				permanent = true
			}
			if permanent || j.Failures == MaxFailures {
				j.Phase = Failed
				next.Orchestration = domain.OrchestrationFailed
				next.ReleaseEvidence = domain.ReleaseEvidenceNotApplicable
			}
		} else {
			if may {
				next.Orchestration = domain.OrchestrationReconciling
			} else {
				next.Orchestration = domain.OrchestrationPreparing
			}
			if permanent || j.Failures == MaxFailures {
				j.Phase = Attention
				next.Orchestration = domain.OrchestrationNeedsAttention
				if cancellationPending(state) {
					next.Cancellation = domain.CancellationManual
				}
			}
		}
		event = domain.EventReconciliationDeferred
	default:
		return Journal{}, state, "", ErrInvalid
	}
	j.Version++
	if !j.Valid() || domain.ValidateAttemptStateTransition(state, next) != nil {
		return Journal{}, state, "", ErrInvalid
	}
	return j, next, event, nil
}

// Problem messages are controlled text, not provider/URL/OS exception strings.
func problem(code domain.ErrorCode, may bool) *domain.Problem {
	stage := domain.FailureStageObservation
	message := "Provider observation is unresolved; no new execution was requested."
	action := domain.RecommendedActionReconcile
	switch code {
	case domain.CodeInputFetchFailed, domain.CodeInputNotFound, domain.CodeInputDigestMismatch, domain.CodeInputTooLarge:
		stage = domain.FailureStageInputPreparation
		message = "Input snapshot could not be prepared or verified."
		action = domain.RecommendedActionFixRequest
	case domain.CodeUnsupportedCapability, domain.CodeResourceRequirementUnsatisfied, domain.CodeInvalidInputPath:
		stage = domain.FailureStageValidation
		message = "The frozen job cannot satisfy required capabilities or runner constraints."
		action = domain.RecommendedActionFixRequest
	case domain.CodeConfigurationInvalid, domain.CodeStateStoreUnavailable:
		stage = domain.FailureStageLocalRuntime
		message = "The frozen configuration or local store is unavailable; no fallback was selected."
		action = domain.RecommendedActionConfigure
	case domain.CodeProviderAuthFailed, domain.CodeProviderAccessDenied:
		stage = domain.FailureStageAuthentication
		message = "The frozen provider account binding could not be verified."
		action = domain.RecommendedActionConfigure
	case domain.CodePrivateStagingUnavailable, domain.CodeStagingFailed, domain.CodeStagingNotReady:
		stage = domain.FailureStageProviderPreparation
		message = "Private staging identity or readiness is unresolved; creation was not repeated."
	case domain.CodeProviderRejected, domain.CodeProviderRateLimited, domain.CodeQuotaExhausted:
		stage = domain.FailureStageSubmission
		message = "Provider proved non-acceptance; another compute request requires explicit operator action."
		action = domain.RecommendedActionInspectProvider
	case domain.CodeProviderSubmissionUnknown:
		stage = domain.FailureStageSubmission
		message = "Submission may have been accepted; reconcile the recorded identity without resubmitting."
	case domain.CodeProviderStateUnknown, domain.CodeProviderUnreachable, domain.CodeRemoteIdentityMismatch:
	default:
		return nil
	}
	p, err := domain.NewProblem(code, message, stage)
	if err != nil {
		return nil
	}
	p = p.WithRetrySemantics(false, may).WithRecommendedAction(action)
	if p.Validate() != nil {
		return nil
	}
	return &p
}
