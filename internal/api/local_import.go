package api

import (
	"errors"
	"mime"
	"net/http"
	"net/url"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/localinput"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/safefs"
)

func (h *handler) importObject(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if err := auth.Require(p, workspace, auth.Write); err != nil {
		mapError(w, r, err)
		return
	}
	if h.config.LocalImports == nil {
		mapError(w, r, auth.ErrForbidden)
		return
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(params) != 0 || len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Encoding") != "" {
		respondError(w, r, 415, domain.CodeInvalidRequest, domain.FailureStageValidation, "send an unencoded JSON import request")
		return
	}
	var request localinput.Request
	if err := DecodeJSON(w, r, h.config.MaxJSONBytes, &request); err != nil {
		var limit *http.MaxBytesError
		if errors.As(err, &limit) {
			mapError(w, r, err)
		} else {
			respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid import request")
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
	result, err := h.config.LocalImports.Import(r.Context(), p, workspace, request)
	if err != nil {
		mapImportError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/workspaces/"+url.PathEscape(string(workspace))+"/objects/"+string(result.ID))
	respond(w, 201, result)
}
func mapImportError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, localinput.ErrForbidden):
		mapError(w, r, auth.ErrForbidden)
	case errors.Is(err, safefs.ErrNotFound):
		mapError(w, r, objects.ErrNotFound)
	case errors.Is(err, packaging.ErrChanged):
		respondError(w, r, 400, domain.CodeInputChanged, domain.FailureStageInputPreparation, "local input changed; request a new snapshot")
	case errors.Is(err, localinput.ErrDigest):
		mapError(w, r, blobfs.ErrDigestMismatch)
	case errors.Is(err, packaging.ErrLimit):
		mapError(w, r, blobfs.ErrTooLarge)
	case errors.Is(err, packaging.ErrPath):
		respondError(w, r, 400, domain.CodeInvalidInputPath, domain.FailureStageValidation, "unsafe or excluded import path")
	case errors.Is(err, packaging.ErrSecret):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "selected input may contain credential material")
	case errors.Is(err, packaging.ErrInvalid):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid import selection")
	default:
		mapError(w, r, err)
	}
}
