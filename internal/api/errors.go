package api

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
)

type ErrorBody struct {
	Code                  domain.ErrorCode         `json:"code"`
	Message               string                   `json:"message"`
	Stage                 domain.FailureStage      `json:"stage"`
	RequestID             string                   `json:"request_id"`
	SafeOperationRetry    bool                     `json:"safe_operation_retry"`
	ComputeMayHaveStarted bool                     `json:"compute_may_have_started"`
	RecommendedAction     domain.RecommendedAction `json:"recommended_action"`
}

type ErrorEnvelope struct {
	Error ErrorBody `json:"error"`
}

func respondError(w http.ResponseWriter, r *http.Request, status int, code domain.ErrorCode, stage domain.FailureStage, message string) {
	action := domain.RecommendedActionFixRequest
	if status == 401 || status == 403 {
		action = domain.RecommendedActionConfigure
	} else if status == 429 {
		action = domain.RecommendedActionWait
	} else if status >= 500 {
		action = domain.RecommendedActionContactOperator
	}
	if status == 401 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="compute-relay"`)
	}
	respond(w, status, ErrorEnvelope{Error: ErrorBody{Code: code, Message: message, Stage: stage,
		RequestID: RequestID(r.Context()), RecommendedAction: action}})
}

func mapError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	var network net.Error
	switch {
	case errors.Is(err, auth.ErrUnauthenticated):
		respondError(w, r, 401, domain.CodeRuntimeAuthRequired, domain.FailureStageAuthentication, "missing or invalid runtime token")
	case errors.Is(err, auth.ErrForbidden):
		respondError(w, r, 403, domain.CodeWorkspaceForbidden, domain.FailureStageAuthentication, "workspace action not permitted")
	case errors.Is(err, objects.ErrNotFound), errors.Is(err, blobfs.ErrNotFound), errors.Is(err, auth.ErrNotFound):
		respondError(w, r, 404, domain.CodeInputNotFound, domain.FailureStageInputPreparation, "object not found")
	case errors.Is(err, blobfs.ErrTooLarge), errors.As(err, &tooLarge):
		respondError(w, r, 413, domain.CodeInputTooLarge, domain.FailureStageInputPreparation, "object exceeds byte limit")
	case errors.Is(err, blobfs.ErrDigestMismatch):
		respondError(w, r, 400, domain.CodeInputDigestMismatch, domain.FailureStageInputPreparation, "object digest does not match declaration")
	case errors.Is(err, objects.ErrInvalid), errors.Is(err, blobfs.ErrInvalid), errors.Is(err, blobfs.ErrLengthMismatch):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid object declaration")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), errors.As(err, &network) && network.Timeout():
		respondError(w, r, 408, domain.CodeInputFetchFailed, domain.FailureStageInputPreparation, "request transfer did not complete")
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.ErrNoProgress):
		respondError(w, r, 400, domain.CodeInputFetchFailed, domain.FailureStageInputPreparation, "request body is incomplete")
	case errors.Is(err, blobfs.ErrStorageFull):
		respondError(w, r, 503, domain.CodeDiskLimitExceeded, domain.FailureStageLocalRuntime, "local storage limit reached")
	default:
		// Deliberately do not serialize arbitrary repository/OS/provider error strings.
		respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime, "local runtime storage or authorization unavailable")
	}
}
