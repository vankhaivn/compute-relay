package api

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// jobs is reached only after the existing server's auth, Host/Origin, rate, body,
// deadline and workspace guards. A nil service never creates a nondurable fallback.
func (h *handler) jobs(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if h.config.Jobs == nil {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "job admission is not configured")
		return
	}
	if r.Method == http.MethodGet {
		record, err := h.config.Jobs.Get(r.Context(), p, workspace, domain.JobID(r.PathValue("job")))
		if err != nil {
			jobError(w, r, err)
			return
		}
		respond(w, 200, jobStatus(record))
		return
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Encoding") != "" || len(params) > 1 || len(params) == 1 && !strings.EqualFold(params["charset"], "utf-8") {
		respondError(w, r, 415, domain.CodeInvalidRequest, domain.FailureStageValidation, "send an unencoded JSON job specification")
		return
	}
	key := ""
	if r.Pattern == "POST /v1/workspaces/{workspace}/jobs" {
		keys := r.Header.Values("Idempotency-Key")
		if len(keys) != 1 {
			jobError(w, r, admission.ErrKey)
			return
		}
		key = keys[0]
		if _, err := admission.KeyDigest(key); err != nil {
			jobError(w, r, err)
			return
		}
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		mapError(w, r, err)
		return
	}
	if r.Pattern == "POST /v1/workspaces/{workspace}/jobs/validate" {
		report, err := h.config.Jobs.Validate(r.Context(), p, workspace, body)
		if err != nil {
			jobError(w, r, err)
			return
		}
		respond(w, 200, report)
		return
	}
	receipt, err := h.config.Jobs.Submit(r.Context(), p, workspace, key, body)
	if err != nil {
		jobError(w, r, err)
		return
	}
	w.Header().Set("Location", receipt.Links.Self)
	respond(w, http.StatusAccepted, receipt)
}

func jobStatus(record admission.Record) map[string]any {
	a := record.Attempt
	status := map[string]any{
		"api_version": record.Job.SpecificationVersion, "job_id": record.Job.ID, "workspace_id": record.Job.WorkspaceID,
		"name": record.Job.Name, "active_attempt_id": a.ID, "revision": a.Revision, "created_at": record.Job.CreatedAt, "updated_at": a.UpdatedAt,
		"state": map[string]any{"orchestration": a.State.Orchestration, "execution": a.State.Execution, "result": a.State.Result, "cancellation": a.State.Cancellation, "remote_activity": a.State.RemoteActivity, "release_evidence": a.State.ReleaseEvidence, "deadline_exceeded": a.State.DeadlineExceeded},
		"links": admission.JobLinks(record.Job.WorkspaceID, record.Job.ID),
	}
	if record.Collection != nil {
		status["collection"] = record.Collection
	}
	if deadline := record.Request.Spec().Timeouts.ExecutionDeadline; deadline != "" {
		status["execution_deadline"] = deadline
	}
	if record.Preparation != nil {
		status["preparation"] = record.Preparation
	}
	if record.Scheduling != nil {
		status["scheduling"] = record.Scheduling
	}
	if p := record.Problem; p != nil {
		// Only a closed reason is public; other details and internal causes stay private.
		problem := map[string]any{"code": p.Code, "message": p.Message, "stage": p.Stage, "safe_operation_retry": p.SafeOperationRetry, "compute_may_have_started": p.ComputeMayHaveStarted, "recommended_action": p.RecommendedAction}
		if details := p.PublicDetails(); details != nil {
			problem["details"] = details
		}
		status["problem"] = problem
	}
	return status
}
func jobError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, admission.ErrUnsupportedInput):
		respondError(w, r, 422, domain.CodeUnsupportedCapability, domain.FailureStageValidation, "HTTPS input sources are unsupported; upload inputs as objects")
	case errors.Is(err, admission.ErrSpec):
		respondError(w, r, 400, domain.CodeInvalidJobSpec, domain.FailureStageValidation, "invalid job specification")
	case errors.Is(err, admission.ErrKey):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "one 8-256 byte printable Idempotency-Key is required")
	case errors.Is(err, admission.ErrSelectionPending):
		w.Header().Set("Retry-After", "1")
		respond(w, 429, ErrorEnvelope{Error: ErrorBody{Code: domain.CodeRequestLimitExceeded,
			Message: "selected connection is being updated; retry the same request", Stage: domain.FailureStageValidation,
			RequestID: RequestID(r.Context()), SafeOperationRetry: true, RecommendedAction: domain.RecommendedActionWait,
			Details: map[string]string{domain.ReasonDetail: "connection_update_pending"}}})
	case errors.Is(err, admission.ErrSelectionUnavailable):
		w.Header().Set("Retry-After", "5")
		respond(w, 503, ErrorEnvelope{Error: ErrorBody{Code: domain.CodeProviderUnreachable,
			Message: "selected connection is temporarily unavailable; retry the same request", Stage: domain.FailureStageObservation,
			RequestID: RequestID(r.Context()), SafeOperationRetry: true, RecommendedAction: domain.RecommendedActionWait,
			Details: map[string]string{domain.ReasonDetail: "connection_unavailable"}}})
	case errors.Is(err, admission.ErrSelection):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageValidation, "selected profile revision is stale; choose the current connection revision explicitly")
	case errors.Is(err, admission.ErrConflict):
		respondError(w, r, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation, "idempotency key was used with a different request")
	case errors.Is(err, admission.ErrInputs):
		respondError(w, r, 404, domain.CodeInputNotFound, domain.FailureStageInputPreparation, "referenced input is missing or not visible")
	case errors.Is(err, admission.ErrNotFound):
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "job not found or not visible")
	case errors.Is(err, hoststorage.ErrHardLimit), errors.Is(err, hoststorage.ErrUnknown), errors.Is(err, hoststorage.ErrIneligible):
		reason, message := "host_storage_limit", "host storage budget does not permit new work; inspect connection storage"
		if errors.Is(err, hoststorage.ErrUnknown) || errors.Is(err, hoststorage.ErrIneligible) {
			reason, message = "host_storage_unknown", "host storage is awaiting a complete observation or pending cleanup; wait and inspect connection storage"
		}
		respond(w, 422, ErrorEnvelope{Error: ErrorBody{Code: domain.CodeResourceRequirementUnsatisfied,
			Message: message, Stage: domain.FailureStageValidation, RequestID: RequestID(r.Context()),
			RecommendedAction: domain.RecommendedActionWait, Details: refusalDetails(err, reason)}})
	case errors.Is(err, admission.ErrHostUnhealthy):
		respond(w, 422, ErrorEnvelope{Error: ErrorBody{Code: domain.CodeResourceRequirementUnsatisfied,
			Message: "host health does not permit new work; inspect connection health", Stage: domain.FailureStageValidation, RequestID: RequestID(r.Context()),
			RecommendedAction: domain.RecommendedActionWait, Details: refusalDetails(err, "host_unhealthy")}})
	case errors.Is(err, admission.ErrRequirements):
		respondError(w, r, 422, domain.CodeResourceRequirementUnsatisfied, domain.FailureStageValidation, "job exceeds the resolved profile policy")
	case errors.Is(err, admission.ErrExpired):
		respondError(w, r, 422, domain.CodeRemoteTimeout, domain.FailureStageExecution, "absolute execution deadline has expired")
	case errors.Is(err, admission.ErrLimit):
		w.Header().Set("Retry-After", "1")
		respondError(w, r, 429, domain.CodeRequestLimitExceeded, domain.FailureStageLocalRuntime, "local outstanding job limit reached")
	default:
		mapError(w, r, err)
	}
}

func refusalDetails(err error, reason string) map[string]string {
	details := map[string]string{domain.ReasonDetail: reason}
	if cause := provider.Reason(err, ""); cause != "" {
		details[domain.CauseDetail] = cause
	}
	return details
}
