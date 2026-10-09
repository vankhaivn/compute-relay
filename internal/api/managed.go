package api

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
)

// connections admits durable local administration only. Account verification and
// all provider calls belong to the connection worker, never this HTTP boundary.
func (h *handler) connections(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID, segments []string) {
	if h.config.Connections == nil {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "connection administration is not configured")
		return
	}
	if len(segments) < 4 {
		managedNotFound(w, r)
		return
	}
	resource := segments[3]
	if resource == "providers" && len(segments) == 4 {
		if !managedMethod(w, r, http.MethodGet) || !managedScope(w, r, p, workspace, auth.Read) {
			return
		}
		providers, err := h.config.Connections.Descriptors(r.Context(), p, workspace)
		if err != nil {
			connectionError(w, r, err)
			return
		}
		if providers == nil {
			providers = []connections.Descriptor{}
		}
		respond(w, http.StatusOK, struct {
			Providers []connections.Descriptor `json:"providers"`
		}{providers})
		return
	}
	if resource == "connection-operations" && len(segments) == 5 {
		if !managedMethod(w, r, http.MethodGet) || !managedScope(w, r, p, workspace, auth.Manage) {
			return
		}
		operation, err := h.config.Connections.Operation(r.Context(), p, workspace, domain.OperationID(segments[4]))
		if err != nil {
			connectionError(w, r, err)
			return
		}
		if !validConnectionOperation(operation, workspace) || string(operation.ID) != segments[4] {
			connectionError(w, r, connections.ErrUnavailable)
			return
		}
		respond(w, http.StatusOK, operation)
		return
	}
	if resource != "connections" {
		managedNotFound(w, r)
		return
	}
	if len(segments) > 4 && !domain.ObjectID(segments[4]).Valid() {
		connectionError(w, r, connections.ErrRequest)
		return
	}
	if len(segments) == 4 && r.Method == http.MethodGet {
		if !managedScope(w, r, p, workspace, auth.Read) {
			return
		}
		items, err := h.config.Connections.List(r.Context(), p, workspace)
		if err != nil {
			connectionError(w, r, err)
			return
		}
		if items == nil {
			items = []connections.Connection{}
		}
		for _, item := range items {
			if item.Workspace != workspace {
				connectionError(w, r, connections.ErrUnavailable)
				return
			}
		}
		respond(w, http.StatusOK, struct {
			Connections []connections.Connection `json:"connections"`
		}{items})
		return
	}
	if len(segments) == 5 {
		if !managedMethod(w, r, http.MethodGet) || !managedScope(w, r, p, workspace, auth.Read) {
			return
		}
		item, err := h.config.Connections.Get(r.Context(), p, workspace, segments[4])
		if err != nil {
			connectionError(w, r, err)
			return
		}
		if item.Workspace != workspace || item.ID != segments[4] {
			connectionError(w, r, connections.ErrUnavailable)
			return
		}
		respond(w, http.StatusOK, item)
		return
	}
	connection := ""
	if len(segments) == 6 && segments[5] == "actions" {
		connection = segments[4]
	} else if len(segments) != 4 {
		managedNotFound(w, r)
		return
	}
	methods := http.MethodPost
	if len(segments) == 4 {
		methods = "GET, POST"
	}
	if !managedMethod(w, r, methods) || !managedScope(w, r, p, workspace, auth.Manage) {
		return
	}
	body, key, ok := readManagedRequest(w, r, connections.MaxRequestBytes)
	if !ok {
		return
	}
	defer clear(body)
	operation, err := h.config.Connections.Submit(r.Context(), p, workspace, connection, key, body)
	clear(body)
	if err != nil {
		connectionError(w, r, err)
		return
	}
	if !validConnectionOperation(operation, workspace) || connection != "" && operation.ConnectionID != connection {
		connectionError(w, r, connections.ErrUnavailable)
		return
	}
	w.Header().Set("Location", "/v1/workspaces/"+url.PathEscape(string(workspace))+"/connection-operations/"+url.PathEscape(string(operation.ID)))
	respond(w, http.StatusAccepted, operation)
}

func (h *handler) authorizations(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID, segments []string) {
	if h.config.Authorizations == nil {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "execution authorization is not configured")
		return
	}
	isPost := len(segments) == 6 && segments[3] == "jobs" && segments[5] == "authorize"
	isGet := len(segments) == 7 && segments[3] == "jobs" && segments[5] == "authorizations"
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
	job := domain.JobID(segments[4])
	if !job.Valid() {
		authorizationError(w, r, executionauth.ErrRequest)
		return
	}
	var receipt executionauth.Receipt
	var err error
	if isGet {
		receipt, err = h.config.Authorizations.Get(r.Context(), p, workspace, job, domain.OperationID(segments[6]))
	} else {
		body, key, ok := readManagedRequest(w, r, executionauth.MaxRequestBytes)
		if !ok {
			return
		}
		defer clear(body)
		receipt, err = h.config.Authorizations.Authorize(r.Context(), p, workspace, job, key, body)
		clear(body)
	}
	if err != nil {
		authorizationError(w, r, err)
		return
	}
	if receipt.Validate() != nil || receipt.WorkspaceID != workspace || receipt.JobID != job || isGet && string(receipt.AuthorizationID) != segments[6] {
		authorizationError(w, r, errors.New("invalid authorization receipt"))
		return
	}
	if isPost {
		w.Header().Set("Location", "/v1/workspaces/"+url.PathEscape(string(workspace))+"/jobs/"+url.PathEscape(string(job))+"/authorizations/"+url.PathEscape(string(receipt.AuthorizationID)))
		respond(w, http.StatusAccepted, receipt)
		return
	}
	respond(w, http.StatusOK, receipt)
}

func managedNotFound(w http.ResponseWriter, r *http.Request) {
	respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "managed route not implemented")
}

func managedMethod(w http.ResponseWriter, r *http.Request, methods string) bool {
	for _, method := range strings.Split(methods, ", ") {
		if r.Method == method {
			return true
		}
	}
	w.Header().Set("Allow", methods)
	respondError(w, r, 405, domain.CodeInvalidRequest, domain.FailureStageValidation, "method is not permitted for this managed route")
	return false
}

func managedScope(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID, scope auth.Scope) bool {
	if err := auth.Require(p, workspace, scope); err != nil {
		mapError(w, r, err)
		return false
	}
	return true
}

// Read into one bounded allocation so secret-bearing bodies leave no abandoned
// growth buffers. The caller clears successful reads; every failure clears here.
func readManagedRequest(w http.ResponseWriter, r *http.Request, limit int) ([]byte, string, bool) {
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 || len(r.Header.Values("Content-Encoding")) != 0 || len(params) > 1 || len(params) == 1 && !strings.EqualFold(params["charset"], "utf-8") {
		respondError(w, r, 415, domain.CodeInvalidRequest, domain.FailureStageValidation, "send an unencoded JSON request")
		return nil, "", false
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 {
		jobError(w, r, admission.ErrKey)
		return nil, "", false
	}
	if _, err := admission.KeyDigest(keys[0]); err != nil {
		jobError(w, r, err)
		return nil, "", false
	}
	if r.Body == nil {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "a JSON request body is required")
		return nil, "", false
	}
	if r.ContentLength > int64(limit) {
		mapError(w, r, &http.MaxBytesError{Limit: int64(limit)})
		return nil, "", false
	}
	body := make([]byte, limit+1)
	success := false
	defer func() {
		if !success {
			clear(body)
		}
	}()
	used, empty := 0, 0
	for used <= limit {
		if err := r.Context().Err(); err != nil {
			mapError(w, r, err)
			return nil, "", false
		}
		n, err := r.Body.Read(body[used:])
		used += n
		if used > limit {
			err = &http.MaxBytesError{Limit: int64(limit)}
		}
		if err == io.EOF {
			success = true
			return body[:used], keys[0], true
		}
		if n == 0 && err == nil {
			empty++
			if empty >= 100 {
				err = io.ErrNoProgress
			}
		} else {
			empty = 0
		}
		if err != nil {
			mapError(w, r, err)
			return nil, "", false
		}
	}
	panic("unreachable bounded request read")
}

func validConnectionOperation(operation connections.Operation, workspace domain.WorkspaceID) bool {
	if !operation.ID.Valid() || operation.Workspace != workspace || !domain.ObjectID(operation.ConnectionID).Valid() || operation.ConnectionRevision < 1 || operation.CreatedAt.IsZero() || operation.UpdatedAt.Before(operation.CreatedAt) {
		return false
	}
	switch operation.Action {
	case "create", "check", "replace_credential", "disable", "enable", "remove", "configure":
	default:
		return false
	}
	switch operation.Status {
	case "accepted", "running", "succeeded":
		return operation.Problem == nil
	case "failed":
		if operation.Problem != nil {
			switch *operation.Problem {
			case "credential_rejected", "credential_store_unavailable", "provider_unavailable", "account_changed", "active_work", "stale_revision", "unsupported_provider":
				return true
			}
		}
	}
	return false
}

func connectionError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, auth.ErrUnauthenticated), errors.Is(err, auth.ErrForbidden):
		mapError(w, r, err)
	case errors.Is(err, admission.ErrKey):
		jobError(w, r, err)
	case errors.Is(err, connections.ErrRequest), errors.Is(err, connections.ErrUnsupported):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "connection request or provider type is invalid")
	case errors.Is(err, connections.ErrNotFound):
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "connection or operation not found or not visible")
	case errors.Is(err, connections.ErrConflict):
		respondError(w, r, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation, "connection revision or original idempotent request does not match")
	case errors.Is(err, connections.ErrActiveWork):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageOperation, "retained work still requires this connection")
	case errors.Is(err, connections.ErrLimit):
		w.Header().Set("Retry-After", "1")
		respondError(w, r, 429, domain.CodeRequestLimitExceeded, domain.FailureStageLocalRuntime, "saved connection limit reached")
	case errors.Is(err, connections.ErrVault):
		respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime, "protected credential storage is unavailable")
	default:
		respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime, "connection acknowledgement is unavailable; preserve the same request and Idempotency-Key")
	}
}

func authorizationError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, auth.ErrUnauthenticated), errors.Is(err, auth.ErrForbidden):
		mapError(w, r, err)
	case errors.Is(err, admission.ErrKey):
		jobError(w, r, err)
	case errors.Is(err, executionauth.ErrRequest):
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "an exact attempt, frozen wall-time bound and explicit staging and compute authorization are required")
	case errors.Is(err, executionauth.ErrNotFound), errors.Is(err, admission.ErrNotFound):
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "authorization, job or attempt not found or not visible")
	case errors.Is(err, executionauth.ErrConflict):
		respondError(w, r, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation, "authorization already exists or the original request does not match")
	case errors.Is(err, executionauth.ErrState):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageOperation, "the attempt is not eligible for execution authorization")
	case errors.Is(err, executionauth.ErrInputs):
		respondError(w, r, 409, domain.CodeInputNotFound, domain.FailureStageInputPreparation, "execution authorization requires frozen immutable inputs")
	case errors.Is(err, executionauth.ErrQuota):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageOperation, "execution authorization requires sufficient known fresh quota")
	default:
		controlProblem(w, r, 503, domain.CodeStateStoreUnavailable, domain.RecommendedActionContactOperator, "Authorization acknowledgement is unavailable; preserve the same request and Idempotency-Key, then read or replay it.")
	}
}
