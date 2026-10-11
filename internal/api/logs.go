package api

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func logPath(path string) bool {
	s := strings.Split(strings.TrimPrefix(path, "/"), "/")
	return len(s) == 6 && s[0] == "v1" && s[1] == "workspaces" && s[3] == "jobs" && s[5] == "logs"
}
func parseLogQuery(r *http.Request) (domain.AttemptID, provider.PageRequest, error) {
	page := provider.PageRequest{Limit: 100}
	v, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || r.URL.ForceQuery || len(r.URL.RawQuery) > 2048 || r.ContentLength > 0 || len(r.TransferEncoding) != 0 {
		return "", page, joblogs.ErrInvalid
	}
	if r.Body != nil {
		var one [1]byte
		n, err := r.Body.Read(one[:])
		if n != 0 || err != io.EOF {
			return "", page, joblogs.ErrInvalid
		}
	}
	for key, values := range v {
		if len(values) != 1 || (key != "attempt_id" && key != "cursor" && key != "limit") {
			return "", page, joblogs.ErrInvalid
		}
	}
	attempt := domain.AttemptID(v.Get("attempt_id"))
	page.Cursor = v.Get("cursor")
	if values, ok := v["limit"]; ok {
		page.Limit, err = strconv.Atoi(values[0])
		if err != nil || strconv.Itoa(page.Limit) != values[0] {
			return "", page, joblogs.ErrInvalid
		}
	}
	if !attempt.Valid() || page.Validate() != nil {
		return "", page, joblogs.ErrInvalid
	}
	return attempt, page, nil
}
func (h *handler) logs(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID, segments []string) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		respondError(w, r, 405, domain.CodeInvalidRequest, domain.FailureStageValidation, "method not implemented for this route")
		return
	}
	attempt, request, err := parseLogQuery(r)
	job := domain.JobID(segments[4])
	if err != nil || !job.Valid() {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid log request")
		return
	}
	if h.config.Logs == nil {
		respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime, "log reader unavailable")
		return
	}
	page, err := h.config.Logs.Read(r.Context(), p, workspace, job, attempt, request)
	if errors.Is(err, joblogs.ErrNotFound) {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "job attempt not found or not visible")
		return
	}
	if errors.Is(err, provider.ErrLogCursorInvalid) {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid log cursor")
		return
	}
	if errors.Is(err, provider.ErrLogCursorReset) {
		respondError(w, r, 409, domain.CodeLogCursorReset, domain.FailureStageObservation, "log continuity changed; restart from an empty cursor")
		return
	}
	if err != nil {
		mapError(w, r, err)
		return
	}
	respond(w, 200, map[string]any{"api_version": "compute-connector/v1alpha1", "workspace_id": workspace, "job_id": job, "attempt_id": attempt, "source": page.Source, "availability": page.Availability, "lines": page.Lines, "next_cursor": page.NextCursor, "truncated": page.Truncated})
}
