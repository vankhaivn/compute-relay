package api

import (
	"net/http"
	"net/url"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
)

func (h *handler) executionGate(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID, segments []string) {
	if h.config.Authorizations == nil {
		managedNotFound(w, r)
		return
	}
	job := domain.JobID(segments[4])
	if !job.Valid() {
		authorizationError(w, r, executionauth.ErrRequest)
		return
	}
	if len(segments) == 6 && segments[5] == "readiness" {
		if !managedMethod(w, r, http.MethodGet) || !managedScope(w, r, p, workspace, auth.Read) {
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
	isPost := len(segments) == 6 && (segments[5] == "prepare" || segments[5] == "start")
	isGet := len(segments) == 7 && segments[5] == "gate-authorizations"
	if !isPost && !isGet {
		managedNotFound(w, r)
		return
	}
	method := http.MethodGet
	if isPost {
		method = http.MethodPost
	}
	if !managedMethod(w, r, method) || !managedScope(w, r, p, workspace, auth.Execute) {
		return
	}
	var receipt executionauth.GateReceipt
	var err error
	if isGet {
		receipt, err = h.config.Authorizations.GateAuthorization(r.Context(), p, workspace, job, domain.OperationID(segments[6]))
	} else {
		body, key, ok := readManagedRequest(w, r, executionauth.MaxRequestBytes)
		if !ok {
			return
		}
		defer clear(body)
		if segments[5] == "prepare" {
			receipt, err = h.config.Authorizations.Prepare(r.Context(), p, workspace, job, key, body)
		} else {
			receipt, err = h.config.Authorizations.Start(r.Context(), p, workspace, job, key, body)
		}
	}
	if err != nil {
		authorizationError(w, r, err)
		return
	}
	if receipt.Validate() != nil || receipt.WorkspaceID != workspace || receipt.JobID != job || isGet && string(receipt.AuthorizationID) != segments[6] {
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
