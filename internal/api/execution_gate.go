package api

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
)

func (h *handler) executionGate(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if h.config.Authorizations == nil {
		managedNotFound(w, r)
		return
	}
	job := domain.JobID(r.PathValue("job"))
	if !job.Valid() {
		authorizationError(w, r, executionauth.ErrRequest)
		return
	}
	if strings.HasSuffix(r.Pattern, "/readiness") {
		if !managedScope(w, r, p, workspace, auth.Read) {
			return
		}
		state, err := h.config.Authorizations.Readiness(r.Context(), p, workspace, job)
		if err != nil {
			authorizationError(w, r, err)
			return
		}
		respond(w, http.StatusOK, state)
		return
	}
	isPost := r.Method == http.MethodPost
	isGet := !isPost
	if !managedScope(w, r, p, workspace, auth.Execute) {
		return
	}
	var receipt executionauth.GateReceipt
	var err error
	if isGet {
		receipt, err = h.config.Authorizations.GateAuthorization(r.Context(), p, workspace, job, domain.OperationID(r.PathValue("authorization")))
	} else {
		body, key, ok := readManagedRequest(w, r, executionauth.MaxRequestBytes)
		if !ok {
			return
		}
		defer clear(body)
		if strings.HasSuffix(r.Pattern, "/prepare") {
			receipt, err = h.config.Authorizations.Prepare(r.Context(), p, workspace, job, key, body)
		} else {
			receipt, err = h.config.Authorizations.Start(r.Context(), p, workspace, job, key, body)
		}
	}
	if err != nil {
		authorizationError(w, r, err)
		return
	}
	if receipt.Validate() != nil || receipt.WorkspaceID != workspace || receipt.JobID != job || isGet && string(receipt.AuthorizationID) != r.PathValue("authorization") {
		authorizationError(w, r, executionauth.ErrState)
		return
	}
	if isPost {
		w.Header().Set("Location", "/v1/workspaces/"+url.PathEscape(string(workspace))+"/jobs/"+url.PathEscape(string(job))+"/gate-authorizations/"+url.PathEscape(string(receipt.AuthorizationID)))
		respond(w, http.StatusAccepted, receipt)
	} else {
		respond(w, http.StatusOK, receipt)
	}
}
