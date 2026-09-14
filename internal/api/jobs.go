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
)

// jobs is reached only after the existing server's auth, Host/Origin, rate, body,
// deadline and workspace guards. A nil service never creates a nondurable fallback.
func (h *handler) jobs(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID, segments []string) {
	if h.config.Jobs == nil {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "job admission is not configured")
		return
	}
	if len(segments) == 5 && segments[4] != "validate" && r.Method == http.MethodGet {
		record, err := h.config.Jobs.Get(r.Context(), p, workspace, domain.JobID(segments[4]))
		if err != nil {
			jobError(w, r, err)
			return
		}
		respond(w, 200, jobStatus(record))
		return
	}
	if (len(segments) != 4 && !(len(segments) == 5 && segments[4] == "validate")) || r.Method != http.MethodPost {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "job route not implemented")
		return
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Encoding") != "" || len(params) > 1 || len(params) == 1 && !strings.EqualFold(params["charset"], "utf-8") {
		respondError(w, r, 415, domain.CodeInvalidRequest, domain.FailureStageValidation, "send an unencoded JSON job specification")
		return
	}
	key := ""
	if len(segments) == 4 {
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
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, admission.MaxRequestBytes))
	if err != nil {
		mapError(w, r, err)
		return
	}
	if len(segments) == 5 {
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
	if p := record.Problem; p != nil {
		// Deliberately exclude arbitrary details and internal causes from public status.
		status["problem"] = map[string]any{"code": p.Code, "message": p.Message, "stage": p.Stage, "safe_operation_retry": p.SafeOperationRetry, "compute_may_have_started": p.ComputeMayHaveStarted, "recommended_action": p.RecommendedAction}
	}
	return status
}
func jobError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, admission.ErrSpec):
		respondError(w, r, 400, domain.CodeInvalidJobSpec, domain.FailureStageValidation, "invalid job specification")
	case errors.Is(err, admission.ErrKey):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "one 8-256 byte printable Idempotency-Key is required")
	case errors.Is(err, admission.ErrConflict):
		respondError(w, r, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation, "idempotency key was used with a different request")
	case errors.Is(err, admission.ErrInputs):
		respondError(w, r, 404, domain.CodeInputNotFound, domain.FailureStageInputPreparation, "referenced input is missing or not visible")
	case errors.Is(err, admission.ErrNotFound):
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "job not found or not visible")
	case errors.Is(err, admission.ErrRequirements):
		respondError(w, r, 422, domain.CodeResourceRequirementUnsatisfied, domain.FailureStageValidation, "job exceeds the resolved profile policy")
	case errors.Is(err, admission.ErrLimit):
		w.Header().Set("Retry-After", "1")
		respondError(w, r, 429, domain.CodeRequestLimitExceeded, domain.FailureStageLocalRuntime, "local outstanding job limit reached")
	default:
		mapError(w, r, err)
	}
}
