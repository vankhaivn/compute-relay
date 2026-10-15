package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

func storageCleanupError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, hoststorage.ErrInvalid), errors.Is(err, connections.ErrRequest):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid storage cleanup request")
	case errors.Is(err, hoststorage.ErrUnverified):
		respondError(w, r, 409, domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "storage target or binding cannot be verified")
	case errors.Is(err, hoststorage.ErrIneligible):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageOperation, "storage target is no longer eligible")
	case errors.Is(err, hoststorage.ErrUnknown):
		respondError(w, r, 503, domain.CodeProviderStateUnknown, domain.FailureStageObservation, "storage observation is incomplete or stale")
	case errors.Is(err, admission.ErrConflict):
		respondError(w, r, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation, "idempotency key was used for another storage target")
	default:
		connectionError(w, r, err)
	}
}

func (h *handler) storageCleanup(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if h.config.StorageCleanup == nil {
		managedNotFound(w, r)
		return
	}
	if r.PathValue("storageOperation") != "" {
		if !managedScope(w, r, p, workspace, auth.Read) {
			return
		}
		op, err := h.config.StorageCleanup.Operation(r.Context(), workspace, p.TokenID(), domain.OperationID(r.PathValue("storageOperation")))
		if err != nil {
			storageCleanupError(w, r, err)
			return
		}
		respond(w, 200, op)
		return
	}
	if !managedScope(w, r, p, workspace, auth.Manage) {
		return
	}
	connection, orphan := r.PathValue("connection"), r.PathValue("orphan")
	if !domain.ObjectID(connection).Valid() || len(orphan) != len("orphan_")+64 {
		storageCleanupError(w, r, hoststorage.ErrInvalid)
		return
	}
	if r.Method == http.MethodGet {
		preview, err := h.config.StorageCleanup.Preview(r.Context(), workspace, p.TokenID(), connection, orphan)
		if err != nil {
			storageCleanupError(w, r, err)
			return
		}
		respond(w, 200, preview)
		return
	}
	raw, key, ok := readManagedRequest(w, r, 1024)
	if !ok {
		return
	}
	fields, err := jsonwire.Object(raw, 1024)
	if err != nil || jsonwire.Fields(fields, []string{"fingerprint"}, nil) != nil {
		storageCleanupError(w, r, hoststorage.ErrInvalid)
		return
	}
	var body struct {
		Fingerprint string `json:"fingerprint"`
	}
	if json.Unmarshal(raw, &body) != nil || !domain.SHA256Digest(body.Fingerprint).Valid() {
		storageCleanupError(w, r, hoststorage.ErrInvalid)
		return
	}
	op, err := h.config.StorageCleanup.Apply(r.Context(), workspace, p.TokenID(), connection, orphan, body.Fingerprint, key)
	if err != nil {
		storageCleanupError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/workspaces/"+string(workspace)+"/storage-cleanup-operations/"+string(op.OperationID))
	respond(w, 202, op)
}
