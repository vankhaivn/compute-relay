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
	case RefuseNewWork:
		if old.SubmitStarted || (old.Phase != Local && old.Phase != Ready) || state.Cancellation != domain.CancellationNotRequested || state.Execution != domain.ExecutionNotSubmitted || state.RemoteActivity != domain.RemoteActivityNotStarted {
			return Journal{}, state, "", ErrConflict
		}
		reason, message := a.RefusalReason, "Host health refuses new preparation or execution."
		switch reason {
		case "", "host_unhealthy":
			reason = "host_unhealthy"
		case "host_storage_limit":
			message = "Host storage budget refuses new preparation or execution."
		case "host_storage_unknown":
			message = "Host storage observation is incomplete or stale."
		default:
			return Journal{}, state, "", ErrInvalid
		}
		p := provider.Problem(domain.CodeResourceRequirementUnsatisfied, domain.FailureStageValidation, message).WithDetail(domain.ReasonDetail, reason).WithRecommendedAction(domain.RecommendedActionWait)
		j.Problem = &p
		if old.Plan == nil {
			next.Orchestration = domain.OrchestrationQueued
		}
		// A prepared hold remains owned and stopped; its unused start grant is not
		// consumed and the machine reservation is retained while health refuses it.
		event = domain.EventReconciliationDeferred
	case PreventDispatch, ExpireDispatch:
		if old.SubmitStarted || state.Orchestration.Terminal() || state.Execution != domain.ExecutionNotSubmitted || (state.RemoteActivity != domain.RemoteActivityNotStarted && state.RemoteActivity != domain.RemoteActivityInactive) {
			return Journal{}, state, "", ErrConflict
		}
		j.Phase = Prevented
		next.Orchestration = domain.OrchestrationCancelled
		next.Cancellation = domain.CancellationPrevented
		next.ReleaseEvidence = domain.ReleaseEvidenceNotApplicable
		event = domain.EventCancellationRequested
		if a.Kind == ExpireDispatch {
			if state.Cancellation != domain.CancellationNotRequested {
				return Journal{}, state, "", ErrConflict
			}
			next.Orchestration = domain.OrchestrationTimedOut
			next.Cancellation = domain.CancellationNotRequested
			next.DeadlineExceeded = true
			j.Problem = problem(domain.CodeRemoteTimeout, false)
			if a.PreparationExpired {
				p := provider.Problem(domain.CodeRemoteTimeout, domain.FailureStageExecution, "Preparation hold has expired; no new execution was requested.").WithDetail(domain.ReasonDetail, "preparation_expired")
				j.Problem = &p
			}
			event = domain.EventReconciliationDeferred
		}
	case RequestReconciliation:
		if a.OwnerPresent {
			if old.Phase != Attention || !old.SubmitStarted || old.Remote != nil || state.Execution != domain.ExecutionUnknown ||
				a.ResourceName == "" || a.ResourceName != old.SubmissionResourceName {
				return Journal{}, state, "", ErrConflict
			}
			j.OwnerSubmissionPresent = true
			j.SubmissionAbsenceObservedAt = nil
		}
		if old.RemoteAbsence() != nil || state.Execution.Terminal() || state.Orchestration.Terminal() || old.Problem != nil && (old.Problem.Code == domain.CodeRemoteIdentityMismatch || old.Problem.Code == domain.CodePrivateStagingUnavailable) {
			return Journal{}, state, "", ErrConflict
		}
		if !old.SubmitStarted {
			if (old.Phase != Attention && old.Phase != Staging && old.Phase != Ready) ||
				state.Execution != domain.ExecutionNotSubmitted || state.RemoteActivity != domain.RemoteActivityNotStarted ||
				state.Cancellation != domain.CancellationNotRequested || state.DeadlineExceeded ||
				(state.Orchestration != domain.OrchestrationNeedsAttention && state.Orchestration != domain.OrchestrationPreparing && state.Orchestration != domain.OrchestrationReconciling) {
				return Journal{}, state, "", ErrConflict
			}
			// Re-enter the observer path with the original preparation intent. Local
			// preparation and its mutation permit are never rearmed by this control.
			j.Phase = Staging
		} else {
			if old.Phase != Attention && old.Phase != Submitting && old.Phase != Submitted {
				return Journal{}, state, "", ErrConflict
			}
			j.Phase = Submitting
			if j.Remote != nil {
				j.Phase = Submitted
			}
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
	case EndUnconfirmedPreparation:
		if old.Phase != Staging || old.Plan == nil || old.Prepared != nil || old.SubmitStarted ||
			state.Orchestration.Terminal() || state.Execution != domain.ExecutionNotSubmitted ||
			state.RemoteActivity != domain.RemoteActivityNotStarted || state.Cancellation != domain.CancellationNotRequested {
			return Journal{}, state, "", ErrConflict
		}
		// Only compute eligibility ends. An unconfirmed staging resource stays
		// unknown in the ledger; this neither proves absence nor permits deletion.
		j.Phase = Prevented
		p := provider.Problem(domain.CodeStagingNotReady, domain.FailureStageProviderPreparation,
			"Private staging could not be confirmed within the observation budget; no execution was submitted.").
			WithDetail(domain.ReasonDetail, "staging_unconfirmed").WithRecommendedAction(domain.RecommendedActionFixRequest)
		// Keep the step that failed first and why the latest read stayed unresolved.
		cause := a.Cause
		if old.Problem != nil {
			details := old.Problem.PublicDetails()
			if first := details[domain.ReasonDetail]; first != "" {
				p = p.WithDetail(domain.FirstCauseDetail, first)
			}
			if cause == "" {
				cause = details[domain.CauseDetail]
			}
		}
		if cause != "" {
			p = p.WithDetail(domain.CauseDetail, cause)
			if p.PublicDetails()[domain.CauseDetail] != cause {
				return Journal{}, state, "", ErrInvalid
			}
		}
		j.Problem = &p
		next.Orchestration = domain.OrchestrationFailed
		next.ReleaseEvidence = domain.ReleaseEvidenceNotApplicable
		event = domain.EventReconciliationDeferred
	case BeginSubmission:
		if old.Phase != Ready || old.SubmitStarted || old.Prepared == nil || !old.Prepared.Ready || !old.Prepared.Private || !scheduler.LocalOnly(state) || state.Cancellation != domain.CancellationNotRequested {
			return Journal{}, state, "", ErrConflict
		}
		j.SubmitStarted = true
		if a.StartAuthorization != nil {
			copy := a.StartAuthorization.Clone()
			j.StartAuthorization = &copy
		}
		j.Phase = Submitting
		j.Problem = nil
		j.Failures = 0
		next.Orchestration = domain.OrchestrationDispatching
		next.Execution = domain.ExecutionUnknown
		next.RemoteActivity = domain.RemoteActivityPossible
		event = domain.EventSubmissionIntentRecorded
	case SubmissionSeen, SubmissionAbsenceSeen, OwnerSubmissionAbsent:
		if a.Kind != SubmissionSeen {
			if (old.Phase != Submitting && old.Phase != Attention) || !old.SubmitStarted || old.Remote != nil ||
				(a.Kind == SubmissionAbsenceSeen && old.OwnerSubmissionPresent) || state.Execution != domain.ExecutionUnknown || state.Orchestration.Terminal() || !domain.ValidResourceName(a.ResourceName) ||
				old.SubmissionResourceName != "" && old.SubmissionResourceName != a.ResourceName {
				return Journal{}, state, "", ErrConflict
			}
			j.SubmissionResourceName = a.ResourceName
			reason := "submission_absent_owned_list"
			if a.Kind == OwnerSubmissionAbsent {
				reason = "submission_absent_owner_confirmed"
			} else if old.SubmissionAbsenceObservedAt == nil || now.Before(old.SubmissionAbsenceObservedAt.Add(SubmissionAbsenceGrace)) {
				if old.SubmissionAbsenceObservedAt == nil {
					at := now.UTC()
					j.SubmissionAbsenceObservedAt = &at
				}
				j.Phase = Submitting
				j.Failures = max(1, j.Failures)
				p := *problem(domain.CodeProviderSubmissionUnknown, true)
				p = p.WithDetail(domain.ReasonDetail, "submission_absence_confirming").WithDetail("resource_name", a.ResourceName)
				j.Problem = &p
				next.Orchestration = domain.OrchestrationReconciling
				event = domain.EventReconciliationDeferred
				break
			}
			p := provider.Problem(domain.CodeProviderRejected, domain.FailureStageSubmission, "Submission resolved absent; a replacement requires a new attempt").
				WithDetail(domain.ReasonDetail, reason).WithDetail("resource_name", a.ResourceName)
			a.Submission = &provider.SubmissionOutcome{Status: provider.SubmissionRejected, Problem: &p}
		}
		if (old.Phase != Submitting && a.Kind != OwnerSubmissionAbsent) || !old.SubmitStarted || old.Remote != nil || a.Submission == nil || a.Submission.Validate(old.Plan.Job.Identity) != nil {
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
				case domain.CodeProviderRateLimited, domain.CodeQuotaExhausted, domain.CodeProviderAuthFailed, domain.CodeProviderAccessDenied, domain.CodeRemoteTimeout:
					code = a.Submission.Problem.Code
				}
			}
			j.Problem = problem(code, false)
			if a.Submission.Problem != nil && j.Problem != nil {
				// Only the adapter's closed reason survives; its message and other details do not.
				j.Problem.Details = a.Submission.Problem.PublicDetails()
			}
			next.Orchestration = domain.OrchestrationFailed
			next.Execution = domain.ExecutionNotSubmitted
			next.RemoteActivity = domain.RemoteActivityInactive
			next.ReleaseEvidence = domain.ReleaseEvidenceNotApplicable
			if code == domain.CodeRemoteTimeout {
				next.Orchestration = domain.OrchestrationTimedOut
				next.DeadlineExceeded = true
			}
			if state.Cancellation != domain.CancellationNotRequested {
				// Explicit non-acceptance, not a not-found lookup, proves prevention.
				next.Orchestration = domain.OrchestrationCancelled
				next.Cancellation = domain.CancellationPrevented
			}
			event = domain.EventSubmissionRejected
		case provider.SubmissionUnknown:
			j.Problem = problem(domain.CodeProviderSubmissionUnknown, true)
			if a.Submission.Problem != nil {
				j.Problem.Details = a.Submission.Problem.PublicDetails()
				if j.SubmissionProblem == nil {
					p := *j.Problem
					j.SubmissionProblem = &p
				}
				j.SubmissionResourceName = j.Problem.Details["resource_name"]
			}
			next.Orchestration = domain.OrchestrationReconciling
			event = domain.EventSubmissionUnknown
		}
	case RemoteAbsenceSeen:
		if old.Phase != Submitted || old.Remote == nil || a.Absence == nil || a.Absence.Remote != *old.Remote ||
			!scheduler.ValidTime(a.Absence.ObservedAt) || a.Absence.ObservedAt.After(now) ||
			old.Observation != nil && !a.Absence.ObservedAt.After(old.Observation.ObservedAt) {
			return Journal{}, state, "", ErrConflict
		}
		j.Phase = Attention
		p := provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation,
			"The exact remote resource is absent; final execution outcome and results remain unknown").
			WithDetail(domain.ReasonDetail, "remote_not_found").
			WithDetail(remoteAbsenceDetail, a.Absence.ObservedAt.UTC().Format(time.RFC3339Nano)).WithRetrySemantics(false, true).
			WithRecommendedAction(domain.RecommendedActionInspectProvider)
		j.Problem = &p
		next.Orchestration = domain.OrchestrationNeedsAttention
		next.Execution = domain.ExecutionUnknown
		next.RemoteActivity = domain.RemoteActivityInactive
		next.ReleaseEvidence = domain.ReleaseEvidenceProviderReported
		if cancellationPending(state) {
			next.Cancellation = domain.CancellationManual
		}
		event = domain.EventReconciliationDeferred
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
		if unconfirmedPreparation(old) && j.PreparationObservationStarted == nil {
			started := now.UTC()
			j.PreparationObservationStarted = &started
		}
		j.Failures++
		if j.Failures > MaxFailures {
			j.Failures = MaxFailures
		}
		may := old.SubmitStarted && old.Phase != Rejected
		j.Problem = problem(a.Code, may)
		if j.Problem != nil && a.Details != nil {
			p := j.Problem.WithDetail(domain.ReasonDetail, a.Reason)
			p.Details = a.Details
			j.Problem.Details = p.PublicDetails()
			if old.SubmitStarted && old.Remote == nil && j.Problem.Details["resource_name"] != "" {
				j.SubmissionResourceName = j.Problem.Details["resource_name"]
			}
		}
		if j.Problem == nil {
			return Journal{}, state, "", ErrInvalid
		}
		// Later deferred reconciliations keep the reason of the step that failed first.
		reason := a.Reason
		if reason == "" && old.Problem != nil && old.Problem.Stage == j.Problem.Stage {
			reason = old.Problem.PublicDetails()[domain.ReasonDetail]
		}
		if reason != "" {
			p := j.Problem.WithDetail(domain.ReasonDetail, reason)
			if p.PublicDetails()[domain.ReasonDetail] != reason {
				return Journal{}, state, "", ErrInvalid
			}
			j.Problem = &p
		}
		if a.Cause != "" {
			p := j.Problem.WithDetail(domain.CauseDetail, a.Cause)
			if p.PublicDetails()[domain.CauseDetail] != a.Cause {
				return Journal{}, state, "", ErrInvalid
			}
			j.Problem = &p
		}
		permanent := a.Code == domain.CodeRemoteIdentityMismatch || a.Code == domain.CodePrivateStagingUnavailable
		if old.Plan == nil {
			started, ok := journalTime(journalDetail(old, localFailureDetail))
			if !ok {
				started = now.UTC()
			}
			p := j.Problem.WithDetail(localFailureDetail, started.Format(time.RFC3339Nano))
			j.Problem = &p
			next.Orchestration = domain.OrchestrationQueued
			switch a.Code {
			case domain.CodeInputNotFound, domain.CodeInputDigestMismatch, domain.CodeInputTooLarge, domain.CodeInvalidInputPath, domain.CodeUnsupportedCapability, domain.CodeResourceRequirementUnsatisfied:
				permanent = true
			}
			if permanent || !now.Before(started.Add(LocalFailureBudget)) {
				j.Phase = Failed
				next.Orchestration = domain.OrchestrationFailed
				next.ReleaseEvidence = domain.ReleaseEvidenceNotApplicable
				if j.Problem.RecommendedAction == domain.RecommendedActionReconcile {
					p := j.Problem.WithRecommendedAction(domain.RecommendedActionFixRequest)
					j.Problem = &p
				}
			} else {
				p := j.Problem.WithRecommendedAction(domain.RecommendedActionWait).WithRetrySemantics(true, false)
				j.Problem = &p
			}
		} else {
			if may {
				next.Orchestration = domain.OrchestrationReconciling
			} else {
				next.Orchestration = domain.OrchestrationPreparing
			}
			// Prolonged unknown/read failures become visible attention even for
			// known remote identities. Explicit recovery still observes only.
			if permanent || j.Failures == MaxFailures && !unconfirmedPreparation(old) {
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
	if j.Problem != nil && j.SubmissionProblem != nil {
		for key, value := range j.SubmissionProblem.PublicDetails() {
			if key != domain.ReasonDetail && j.Problem.Details[key] == "" {
				p := j.Problem.WithDetail(key, value)
				j.Problem = &p
			}
		}
	}
	if j.Problem != nil && j.SubmissionResourceName != "" {
		p := j.Problem.WithDetail("resource_name", j.SubmissionResourceName)
		j.Problem = &p
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
	case domain.CodeRemoteTimeout:
		p := provider.Problem(code, domain.FailureStageExecution, "Execution authorization deadline has expired; no new execution was requested.").WithDetail(domain.ReasonDetail, "absolute_deadline_expired")
		return &p
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
	case domain.CodeRemoteExecutionUnresolved:
		stage = domain.FailureStageOperation
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
