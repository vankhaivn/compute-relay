package api

import (
	"errors"
	"net/http"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

func (h *handler) storage(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if !managedScope(w, r, p, workspace, auth.Read) {
		return
	}
	id := r.PathValue("connection")
	if !domain.ObjectID(id).Valid() {
		connectionError(w, r, connections.ErrRequest)
		return
	}
	view, err := h.config.Storage.ConnectionStorage(r.Context(), workspace, p.TokenID(), id)
	if err != nil {
		switch {
		case errors.Is(err, hoststorage.ErrUnverified):
			respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageObservation, "storage requires a verified SSH machine and Docker engine")
		default:
			connectionError(w, r, err)
		}
		return
	}
	respond(w, http.StatusOK, view)
}
