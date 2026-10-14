package api

import (
	"net/http"
	"net/url"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

func (h *handler) cleanupPreview(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID, job domain.JobID) {
	if h.config.Cleanup == nil || !h.config.Cleanup.PreviewAvailable() {
		controlError(w, r, operations.ErrNotFound)
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || len(query["attempt_id"]) != 1 || len(query["discard_uncollected"]) > 1 || len(query["accept_name_based_delete"]) > 1 || len(query) > 3 {
		controlError(w, r, operations.ErrRequest)
		return
	}
	for key := range query {
		if key != "attempt_id" && key != "discard_uncollected" && key != "accept_name_based_delete" {
			controlError(w, r, operations.ErrRequest)
			return
		}
	}
	discard := false
	if values, ok := query["discard_uncollected"]; ok {
		if values[0] != "true" && values[0] != "false" {
			controlError(w, r, operations.ErrRequest)
			return
		}
		discard = values[0] == "true"
	}
	acceptNameBasedDelete := false
	if values, ok := query["accept_name_based_delete"]; ok {
		if values[0] != "true" && values[0] != "false" {
			controlError(w, r, operations.ErrRequest)
			return
		}
		acceptNameBasedDelete = values[0] == "true"
	}
	preview, err := h.config.Cleanup.Preview(r.Context(), p, workspace, job, domain.AttemptID(query.Get("attempt_id")), discard, acceptNameBasedDelete)
	if err != nil {
		controlError(w, r, err)
		return
	}
	respond(w, http.StatusOK, preview)
}
