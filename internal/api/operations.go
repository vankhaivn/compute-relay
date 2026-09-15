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
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

// controls only admits or reads durable local records. Provider calls and transfer
// work are never executed by an HTTP handler. All existing middleware runs first.
func (h *handler) controls(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID, segments []string) {
	if h.config.Operations == nil {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "durable controls are not configured")
		return
	}
	if segments[3] == "operations" && len(segments) == 5 {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			respondError(w, r, 405, domain.CodeInvalidRequest, domain.FailureStageValidation, "operation records are read-only")
			return
		}
		record, err := h.config.Operations.Get(r.Context(), p, workspace, domain.OperationID(segments[4]))
		if err != nil {
			controlError(w, r, err)
			return
		}
		if record.Validate() != nil || record.Operation.WorkspaceID != workspace || string(record.Operation.ID) != segments[4] {
			controlError(w, r, errors.New("invalid operation record"))
			return
		}
		respond(w, http.StatusOK, controlView(record))
		return
	}
	kind := domain.OperationKind("")
	if segments[3] == "jobs" && len(segments) == 6 {
		switch segments[5] {
		case "cancel":
			kind = domain.OperationCancel
		case "retry":
			kind = domain.OperationRetryCompute
		case "reconcile":
			kind = domain.OperationReconcile
		case "collect":
			kind = domain.OperationCollect
		}
	}
	if !operations.Supported(kind) {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "control route not implemented")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		respondError(w, r, 405, domain.CodeInvalidRequest, domain.FailureStageValidation, "controls require an explicit POST")
		return
	}
	if err := auth.Require(p, workspace, auth.Operate); err != nil {
		controlError(w, r, err)
		return
	}
	job := domain.JobID(segments[4])
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
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, operations.MaxRequestBytes))
	if err != nil {
		mapError(w, r, err) // No operation was admitted before the body was read.
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

func controlLocation(r operations.Record) string {
	return "/v1/workspaces/" + url.PathEscape(string(r.Operation.WorkspaceID)) + "/operations/" + url.PathEscape(string(r.Operation.ID))
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
	if p := op.Failure; p != nil {
		view["problem"] = map[string]any{"code": p.Code, "message": p.Message, "stage": p.Stage, "safe_operation_retry": p.SafeOperationRetry, "compute_may_have_started": p.ComputeMayHaveStarted, "recommended_action": p.RecommendedAction}
	}
	return view
}

func controlError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
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
