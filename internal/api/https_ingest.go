package api

import (
	"errors"
	"mime"
	"net/http"
	"net/url"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/httpsinput"
)

func (h *handler) ingestObject(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if err := auth.Require(p, workspace, auth.Write); err != nil {
		mapError(w, r, err)
		return
	}
	if h.config.HTTPSInputs == nil {
		mapError(w, r, auth.ErrForbidden)
		return
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(params) != 0 || len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Encoding") != "" {
		respondError(w, r, 415, domain.CodeInvalidRequest, domain.FailureStageValidation, "send an unencoded JSON HTTPS input request")
		return
	}
	var request httpsinput.Request
	if err := DecodeJSON(w, r, h.config.MaxJSONBytes, &request); err != nil {
		var limit *http.MaxBytesError
		if errors.As(err, &limit) {
			mapError(w, r, err)
		} else {
			respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid public HTTPS input request")
		}
		return
	}
	select {
	case h.uploads <- struct{}{}:
		defer func() { <-h.uploads }()
	default:
		h.limited(w, r, 1)
		return
	}
	result, err := h.config.HTTPSInputs.Ingest(r.Context(), p, workspace, request)
	if err != nil {
		h.mapIngestError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/workspaces/"+url.PathEscape(string(workspace))+"/objects/"+string(result.ID))
	respond(w, http.StatusCreated, result)
}

func (h *handler) mapIngestError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, httpsinput.ErrURL):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid public HTTPS input")
	case errors.Is(err, httpsinput.ErrBlocked):
		respondError(w, r, 400, domain.CodeInputFetchFailed, domain.FailureStageInputPreparation, "HTTPS destination is not permitted")
	case errors.Is(err, httpsinput.ErrFetch):
		respondError(w, r, 400, domain.CodeInputFetchFailed, domain.FailureStageInputPreparation, "HTTPS input transfer failed")
	case errors.Is(err, httpsinput.ErrTimeout):
		respondError(w, r, 408, domain.CodeInputFetchFailed, domain.FailureStageInputPreparation, "HTTPS input time budget exceeded")
	case errors.Is(err, httpsinput.ErrLimit):
		mapError(w, r, blobfs.ErrTooLarge)
	case errors.Is(err, httpsinput.ErrDigest):
		mapError(w, r, blobfs.ErrDigestMismatch)
	case errors.Is(err, httpsinput.ErrBusy):
		h.limited(w, r, 1)
	default:
		mapError(w, r, err)
	}
}
