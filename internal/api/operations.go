package api

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

// Mutation routes only admit durable local records. Cleanup GET may perform a
// bounded provider dry run, never apply. All existing middleware runs first.
func (h *handler) controls(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if h.config.Operations == nil {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "durable controls are not configured")
		return
	}
	if r.PathValue("operation") != "" {
		record, err := h.config.Operations.Get(r.Context(), p, workspace, domain.OperationID(r.PathValue("operation")))
		if errors.Is(err, operations.ErrNotFound) && h.config.Cleanup != nil {
			// Cleanup receipts share the operation identifier namespace.
			cleaned, cleanupErr := h.config.Cleanup.Get(r.Context(), p, workspace, domain.OperationID(r.PathValue("operation")))
			if cleanupErr != nil {
				controlError(w, r, cleanupErr)
				return
			}
			if cleaned.Validate() != nil || cleaned.Operation.WorkspaceID != workspace || string(cleaned.Operation.ID) != r.PathValue("operation") {
				controlError(w, r, errors.New("invalid operation record"))
				return
			}
			respond(w, http.StatusOK, cleanupView(cleaned))
			return
		}
		if err != nil {
			controlError(w, r, err)
			return
		}
		if record.Validate() != nil || record.Operation.WorkspaceID != workspace || string(record.Operation.ID) != r.PathValue("operation") {
			controlError(w, r, errors.New("invalid operation record"))
			return
		}
		respond(w, http.StatusOK, controlView(record))
		return
	}
	kind := domain.OperationKind("")
	switch {
	case strings.HasSuffix(r.Pattern, "/cancel"):
		kind = domain.OperationCancel
	case strings.HasSuffix(r.Pattern, "/retry"):
		kind = domain.OperationRetryCompute
	case strings.HasSuffix(r.Pattern, "/reconcile"):
		kind = domain.OperationReconcile
	case strings.HasSuffix(r.Pattern, "/collect"):
		kind = domain.OperationCollect
	case strings.HasSuffix(r.Pattern, "/cleanup"):
		if h.config.Cleanup == nil {
			managedNotFound(w, r)
			return
		}
		kind = domain.OperationCleanup
	}
	if kind == domain.OperationCleanup && r.Method == http.MethodGet {
		h.cleanupPreview(w, r, p, workspace, domain.JobID(r.PathValue("job")))
		return
	}
	if err := auth.Require(p, workspace, auth.Operate); err != nil {
		controlError(w, r, err)
		return
	}
	job := domain.JobID(r.PathValue("job"))
	if !job.Valid() {
		controlError(w, r, operations.ErrRequest)
		return
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Encoding") != "" || len(params) > 1 || len(params) == 1 && !strings.EqualFold(params["charset"], "utf-8") {
		respondError(w, r, 415, domain.CodeInvalidRequest, domain.FailureStageValidation, "send an unencoded JSON control request")
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 {
		jobError(w, r, admission.ErrKey)
		return
	}
	if _, err := admission.KeyDigest(keys[0]); err != nil {
		jobError(w, r, err)
		return
	}
	if r.Body == nil {
		controlError(w, r, operations.ErrRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		mapError(w, r, err) // No operation was admitted before the body was read.
		return
	}
	if kind == domain.OperationCleanup {
		cleaned, err := h.config.Cleanup.Submit(r.Context(), p, workspace, job, keys[0], body)
		if err != nil {
			controlError(w, r, err)
			return
		}
		if cleaned.Validate() != nil || cleaned.Operation.WorkspaceID != workspace || cleaned.Operation.JobID != job {
			controlError(w, r, errors.New("invalid operation receipt"))
			return
		}
		w.Header().Set("Location", operationLocation(cleaned.Operation))
		respond(w, http.StatusAccepted, cleanupView(cleaned))
		return
	}
	record, err := h.config.Operations.Submit(r.Context(), p, workspace, job, kind, keys[0], body)
	if err != nil {
		controlError(w, r, err)
		return
	}
	if record.Validate() != nil || record.Operation.WorkspaceID != workspace || record.Operation.JobID != job || record.Operation.Kind != kind {
		controlError(w, r, errors.New("invalid operation receipt"))
		return
	}
	w.Header().Set("Location", controlLocation(record))
	respond(w, http.StatusAccepted, controlView(record))
}

func controlLocation(r operations.Record) string { return operationLocation(r.Operation) }

func operationLocation(op domain.Operation) string {
	return "/v1/workspaces/" + url.PathEscape(string(op.WorkspaceID)) + "/operations/" + url.PathEscape(string(op.ID))
}

// Do not serialize the repository record, reason, token identity, request hashes,
// provider references, or arbitrary Problem.Details. Replayed POSTs retain their
// original receipt; clients read Location for the current operation revision.
func controlView(r operations.Record) map[string]any {
	op := r.Operation
	view := map[string]any{
		"operation_id": op.ID, "workspace_id": op.WorkspaceID, "job_id": op.JobID, "attempt_id": op.AttemptID,
		"kind": op.Kind, "status": op.Status, "revision": op.Revision, "created_at": op.CreatedAt, "updated_at": op.UpdatedAt,
		"effect": r.Effect, "remote_termination_confirmed": r.TerminationConfirmed, "replay": r.Replay, "problem": nil,
		"links": map[string]string{"self": controlLocation(r), "job": "/v1/workspaces/" + url.PathEscape(string(op.WorkspaceID)) + "/jobs/" + url.PathEscape(string(op.JobID))},
	}
	if r.NewAttemptID != "" {
		view["new_attempt_id"] = r.NewAttemptID
	}
	view["problem"] = publicOperationProblem(op.Failure)
	return view
}

// cleanupView exposes typed resource outcomes, never the request reason, full ownership
// identity or opaque provider reference.
func cleanupView(r cleanup.Record) map[string]any {
	op := r.Operation
	view := map[string]any{
		"operation_id": op.ID, "workspace_id": op.WorkspaceID, "job_id": op.JobID, "attempt_id": op.AttemptID,
		"kind": op.Kind, "status": op.Status, "revision": op.Revision, "created_at": op.CreatedAt, "updated_at": op.UpdatedAt,
		"effect": r.Effect, "remote_termination_confirmed": false, "replay": r.Replay, "problem": publicOperationProblem(op.Failure),
		"links": map[string]string{"self": operationLocation(op), "job": "/v1/workspaces/" + url.PathEscape(string(op.WorkspaceID)) + "/jobs/" + url.PathEscape(string(op.JobID))},
	}
	if len(r.Resources) > 0 {
		view["resources"] = r.Resources
	}
	return view
}

func publicOperationProblem(p *domain.Problem) any {
	if p == nil {
		return nil
	}
	return map[string]any{"code": p.Code, "message": p.Message, "stage": p.Stage, "safe_operation_retry": p.SafeOperationRetry, "compute_may_have_started": p.ComputeMayHaveStarted, "recommended_action": p.RecommendedAction}
}

func controlError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, admission.ErrExpired):
		jobError(w, r, err)
	case errors.Is(err, auth.ErrUnauthenticated), errors.Is(err, auth.ErrForbidden):
		mapError(w, r, err)
	case errors.Is(err, operations.ErrRequest):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "an explicit attempt_id and valid non-secret reason are required; retry requires a reason")
	case errors.Is(err, operations.ErrNotFound), errors.Is(err, admission.ErrNotFound):
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "operation, job or attempt not found or not visible")
	case errors.Is(err, operations.ErrConflict):
		respondError(w, r, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation, "idempotency key was used with a different control request")
	case errors.Is(err, admission.ErrLimit):
		jobError(w, r, err)
	case errors.Is(err, operations.ErrUnresolved):
		controlProblem(w, r, 409, domain.CodeRemoteExecutionUnresolved, domain.RecommendedActionReconcile, "Remote execution is unresolved; reconcile the existing attempt without submitting another execution.")
	case errors.Is(err, cleanup.ErrUncollected):
		controlProblem(w, r, 409, domain.CodeIllegalStateTransition, domain.RecommendedActionCollect, "Results for this attempt were never collected; collect them first, or repeat the cleanup with discard_uncollected set to true.")
	case errors.Is(err, operations.ErrCollect):
		controlProblem(w, r, 409, domain.CodeIllegalStateTransition, domain.RecommendedActionCollect, "Execution succeeded; recover this attempt's results with collect, not compute retry.")
	case errors.Is(err, operations.ErrInputs):
		controlProblem(w, r, 409, domain.CodeInputNotFound, domain.RecommendedActionFixRequest, "Original frozen inputs cannot be verified; no new attempt was created and no URL was re-fetched.")
	case errors.Is(err, operations.ErrChanged), errors.Is(err, operations.ErrState), errors.Is(err, operations.ErrBusy):
		controlProblem(w, r, 409, domain.CodeIllegalStateTransition, domain.RecommendedActionRetryRead, "The control is not appropriate for the target attempt; read its current state before choosing another action.")
	default:
		// A transaction may have committed even when its acknowledgement was lost.
		// Never turn an arbitrary repository error into proof that compute is absent.
		controlProblem(w, r, 503, domain.CodeStateStoreUnavailable, domain.RecommendedActionContactOperator, "Operation acknowledgement is unavailable; preserve the same request and Idempotency-Key, and do not create a different compute request.")
	}
}

func controlProblem(w http.ResponseWriter, r *http.Request, status int, code domain.ErrorCode, action domain.RecommendedAction, message string) {
	respond(w, status, ErrorEnvelope{Error: ErrorBody{Code: code, Message: message, Stage: domain.FailureStageOperation,
		RequestID: RequestID(r.Context()), ComputeMayHaveStarted: true, RecommendedAction: action}})
}
