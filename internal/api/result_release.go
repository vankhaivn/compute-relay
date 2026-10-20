package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func (h *handler) releaseResults(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if !managedScope(w, r, p, workspace, auth.Operate) {
		return
	}
	job := domain.JobID(r.PathValue("job"))
	raw, key, ok := readManagedRequest(w, r, 1024)
	if !ok {
		return
	}
	fields, err := jsonwire.Object(raw, 1024)
	var request retention.ReleaseRequest
	if err != nil || jsonwire.Fields(fields, []string{"attempt_id", "artifact_id", "sha256"}, nil) != nil ||
		json.Unmarshal(raw, &request) != nil || !request.Valid() || !job.Valid() {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "an exact attempt and verified output identity are required")
		return
	}
	receipt, err := h.config.ResultReleases.Release(r.Context(), p, workspace, job, key, request)
	switch {
	case errors.Is(err, retention.ErrReleaseConflict):
		respondError(w, r, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation, "result release key was used for another target")
	case errors.Is(err, retention.ErrPinned):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageOperation, "result is still needed by active or unresolved work")
	case errors.Is(err, retention.ErrChanged), errors.Is(err, retention.ErrInvalid):
		respondError(w, r, 409, domain.CodeInvalidRequest, domain.FailureStageValidation, "result release identity does not match the verified output")
	case errors.Is(err, admission.ErrNotFound):
		respondError(w, r, 404, domain.CodeArtifactMissing, domain.FailureStageResults, "verified output not found or not visible")
	case err != nil:
		artifactError(w, r, err)
	default:
		status := http.StatusOK
		if receipt.PendingFiles > 0 {
			status = http.StatusAccepted
		}
		respond(w, status, receipt)
	}
}
