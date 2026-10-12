package api

import (
	"errors"
	"net/http"
	"net/url"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

func (h *handler) workloadSecrets(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID, segments []string) {
	if h.config.Secrets == nil {
		managedNotFound(w, r)
		return
	}
	job := domain.JobID(segments[4])
	if !job.Valid() {
		workloadSecretError(w, r, workloadsecret.ErrRequest)
		return
	}
	supply := len(segments) == 6
	read := len(segments) == 7
	revoke := len(segments) == 8 && segments[7] == "revoke"
	if !supply && !read && !revoke {
		managedNotFound(w, r)
		return
	}
	method := http.MethodPost
	if read {
		method = http.MethodGet
	}
	if !managedMethod(w, r, method) || !managedScope(w, r, p, workspace, auth.Execute) {
		return
	}
	var receipt workloadsecret.Receipt
	var err error
	if read {
		receipt, err = h.config.Secrets.Get(r.Context(), p, workspace, job, domain.OperationID(segments[6]))
	} else {
		limit := workloadsecret.MaxRequestBytes
		if revoke {
			limit = 4096
		}
		body, key, ok := readManagedRequest(w, r, limit)
		if !ok {
			return
		}
		defer clear(body)
		if supply {
			receipt, err = h.config.Secrets.Supply(r.Context(), p, workspace, job, key, body)
		} else {
			receipt, err = h.config.Secrets.Revoke(r.Context(), p, workspace, job, domain.OperationID(segments[6]), key, body)
		}
	}
	if err != nil {
		workloadSecretError(w, r, err)
		return
	}
	if receipt.Validate() != nil || receipt.WorkspaceID != workspace || receipt.JobID != job || !supply && string(receipt.AuthorizationID) != segments[6] {
		workloadSecretError(w, r, workloadsecret.ErrState)
		return
	}
	if read {
		respond(w, http.StatusOK, receipt)
		return
	}
	w.Header().Set("Location", "/v1/workspaces/"+url.PathEscape(string(workspace))+"/jobs/"+url.PathEscape(string(job))+"/workload-secrets/"+url.PathEscape(string(receipt.AuthorizationID)))
	respond(w, http.StatusAccepted, receipt)
}
func workloadSecretError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, workloadsecret.ErrRequest):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "an exact attempt, readiness revision, expiry and bounded declared secret slots are required")
	case errors.Is(err, workloadsecret.ErrNotFound):
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "workload secret authorization not found or not visible")
	case errors.Is(err, workloadsecret.ErrConflict):
		respondError(w, r, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation, "workload secret authorization already exists or the original request does not match")
	case errors.Is(err, workloadsecret.ErrUnavailable):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageOperation, "required workload secrets are missing, expired, revoked or require explicit resupply")
	case errors.Is(err, workloadsecret.ErrState):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageOperation, "the attempt is not eligible for workload secret supply or revocation")
	case errors.Is(err, workloadsecret.ErrLimit):
		respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime, "ephemeral workload secret capacity is unavailable; preserve the original request for explicit replay")
	default:
		authorizationError(w, r, err)
	}
}
