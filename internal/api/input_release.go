package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

const inputReleaseBodyLimit = 65536

func (h *handler) inputRelease(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	preview := strings.HasSuffix(r.URL.Path, "input-release-preview")
	scope := auth.Operate
	if preview {
		scope = auth.Read
	}
	if !managedScope(w, r, p, workspace, scope) {
		return
	}
	var raw []byte
	var key string
	var ok bool
	if preview {
		raw, _, ok = readManagedJSON(w, r, inputReleaseBodyLimit, false)
	} else {
		raw, key, ok = readManagedRequest(w, r, inputReleaseBodyLimit)
	}
	if !ok {
		return
	}
	fields, err := jsonwire.Object(raw, inputReleaseBodyLimit)
	required := []string{"jobs"}
	if !preview {
		required = append(required, "preview_sha256")
	}
	var request retention.InputReleaseRequest
	if err != nil || jsonwire.Fields(fields, required, nil) != nil || json.Unmarshal(raw, &request) != nil || !request.Normalize(!preview) {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "exact collected job identities and a matching preview are required")
		return
	}
	// Validate nested objects too; unknown fields must not silently disappear.
	var jobs []json.RawMessage
	if json.Unmarshal(fields["jobs"], &jobs) != nil {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid jobs")
		return
	}
	for _, j := range jobs {
		f, e := jsonwire.Object(j, inputReleaseBodyLimit)
		if e != nil || jsonwire.Fields(f, []string{"job_id", "attempt_id", "artifact_id", "sha256"}, nil) != nil {
			respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid job identity")
			return
		}
	}
	if preview {
		out, e := h.config.InputReleases.Preview(r.Context(), p, workspace, request)
		if e != nil {
			inputReleaseError(w, r, e)
			return
		}
		respond(w, 200, out)
		return
	}
	out, err := h.config.InputReleases.Release(r.Context(), p, workspace, key, request)
	if err != nil {
		inputReleaseError(w, r, err)
		return
	}
	status := http.StatusOK
	if out.PendingFiles > 0 {
		status = http.StatusAccepted
	}
	respond(w, status, out)
}
func inputReleaseError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, retention.ErrReleaseConflict):
		respondError(w, r, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation, "input release key was used for another target")
	case errors.Is(err, retention.ErrPinned):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageOperation, "inputs are still needed by active or unresolved work")
	case errors.Is(err, retention.ErrChanged), errors.Is(err, retention.ErrInvalid):
		respondError(w, r, 409, domain.CodeInvalidRequest, domain.FailureStageValidation, "input release identity or preview changed")
	case errors.Is(err, admission.ErrNotFound):
		respondError(w, r, 404, domain.CodeArtifactMissing, domain.FailureStageResults, "verified output not found or not visible")
	default:
		artifactError(w, r, err)
	}
}
func (h *handler) localStorage(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if !managedScope(w, r, p, workspace, auth.Read) {
		return
	}
	query := r.URL.Query()
	if len(query) > 1 || len(query) == 1 && (len(query["refresh"]) != 1 || (query.Get("refresh") != "true" && query.Get("refresh") != "false")) {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "refresh must be true or false")
		return
	}
	out, err := h.config.LocalStorage.Measure(r.Context(), query.Get("refresh") == "true")
	if err != nil {
		respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageObservation, "local storage measurement unavailable")
		return
	}
	respond(w, 200, out)
}
