package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/artifactwire"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func artifactPath(path string) bool {
	s := strings.Split(strings.TrimPrefix(path, "/"), "/")
	return len(s) >= 6 && len(s) <= 8 && s[0] == "v1" && s[1] == "workspaces" && s[3] == "jobs" && s[5] == "artifacts" && (len(s) != 8 || s[7] == "content")
}

type artifactQuery struct {
	attempt domain.AttemptID
	cursor  string
	limit   int
}

func parseArtifactQuery(r *http.Request, listing bool) (artifactQuery, error) {
	q := artifactQuery{limit: artifactwire.MaxPage}
	v, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(r.URL.RawQuery) > 512 || r.URL.ForceQuery || r.ContentLength > 0 || len(r.TransferEncoding) != 0 || r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
		return q, artifactwire.ErrInvalid
	}
	for key, values := range v {
		if len(values) != 1 || (key != "attempt_id" && (!listing || key != "limit" && key != "cursor")) {
			return q, artifactwire.ErrInvalid
		}
	}
	q.attempt = domain.AttemptID(v.Get("attempt_id"))
	q.cursor = v.Get("cursor")
	if !q.attempt.Valid() || len(q.cursor) > 80 {
		return q, artifactwire.ErrInvalid
	}
	if values, ok := v["limit"]; ok {
		q.limit, err = strconv.Atoi(values[0])
		if err != nil || q.limit < 1 || q.limit > artifactwire.MaxPage || strconv.Itoa(q.limit) != values[0] {
			return q, artifactwire.ErrInvalid
		}
	}
	return q, nil
}

func publishedView(result collection.Result, target artifactwire.Target) (artifactwire.Page, error) {
	p := artifactwire.Page{Target: target, ResultPhase: result.Phase, VerifiedAt: result.VerifiedAt, Total: len(result.Files), Artifacts: make([]artifactwire.File, 0, len(result.Files))}
	if !target.Valid() || string(result.WorkspaceID) != target.WorkspaceID || string(result.JobID) != target.JobID || string(result.AttemptID) != target.AttemptID || result.VerifiedAt.IsZero() || result.Phase == "" || len(result.Phase) > 64 || p.Total < 1 || p.Total > artifactwire.MaxFiles {
		return artifactwire.Page{}, collection.ErrUnavailable
	}
	seenID, seenPath := map[string]bool{}, map[string]bool{}
	var total int64
	for _, f := range result.Files {
		v := artifactwire.File{ID: string(f.ID), Path: f.Path, Role: f.Role, Bytes: f.Object.Bytes, SHA256: string(f.Object.SHA256)}
		if !v.Valid() || !f.Object.Valid() || string(f.Object.WorkspaceID) != target.WorkspaceID || seenID[v.ID] || seenPath[strings.ToLower(v.Path)] || v.Bytes > artifactwire.MaxBytes-total {
			return artifactwire.Page{}, collection.ErrUnavailable
		}
		seenID[v.ID], seenPath[strings.ToLower(v.Path)] = true, true
		total += v.Bytes
		p.Artifacts = append(p.Artifacts, v)
	}
	sort.Slice(p.Artifacts, func(i, j int) bool { return p.Artifacts[i].Path < p.Artifacts[j].Path })
	p.SnapshotSHA256 = artifactwire.Snapshot(target, p.ResultPhase, p.VerifiedAt, p.Artifacts)
	return p, nil
}

func (h *handler) artifacts(w http.ResponseWriter, r *http.Request, principal auth.Principal, workspace domain.WorkspaceID, segments []string) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		respondError(w, r, 405, domain.CodeInvalidRequest, domain.FailureStageValidation, "artifact routes are read-only")
		return
	}
	if h.config.Results == nil {
		artifactError(w, r, collection.ErrNotFound)
		return
	}
	query, err := parseArtifactQuery(r, len(segments) == 6)
	job := domain.JobID(segments[4])
	if err != nil || !job.Valid() || len(segments) > 6 && !domain.ArtifactID(segments[6]).Valid() {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "explicit attempt and bounded artifact parameters required")
		return
	}
	target := artifactwire.Target{WorkspaceID: string(workspace), JobID: string(job), AttemptID: string(query.attempt)}
	result, err := h.config.Results.Read(r.Context(), principal, workspace, job, query.attempt)
	if err != nil {
		artifactError(w, r, err)
		return
	}
	page, err := publishedView(result, target)
	if err != nil {
		artifactError(w, r, err)
		return
	}
	if len(segments) == 6 {
		offset, err := artifactwire.Offset(query.cursor, page.SnapshotSHA256, page.Total)
		if err != nil {
			respondError(w, r, 409, domain.CodeInvalidRequest, domain.FailureStageValidation, "artifact cursor does not match this publication")
			return
		}
		end := min(offset+query.limit, page.Total)
		page.Artifacts = page.Artifacts[offset:end]
		if end < page.Total {
			page.NextCursor = artifactwire.Cursor(page.SnapshotSHA256, end)
		}
		respond(w, 200, page)
		return
	}
	var selected *artifactwire.File
	for i := range page.Artifacts {
		if page.Artifacts[i].ID == segments[6] {
			selected = &page.Artifacts[i]
			break
		}
	}
	if selected == nil {
		artifactError(w, r, collection.ErrNotFound)
		return
	}
	if len(segments) == 7 {
		respond(w, 200, artifactwire.Metadata{Target: target, ResultPhase: page.ResultPhase, VerifiedAt: page.VerifiedAt, Artifact: *selected})
		return
	}
	// Share the existing finite expensive-transfer pool with uploads. No unbounded
	// worker is spawned, and admission slots do not become remote dispatch permits.
	select {
	case h.uploads <- struct{}{}:
		defer func() { <-h.uploads }()
	default:
		h.limited(w, r, 1)
		return
	}
	meta, stream, err := h.config.Results.Open(r.Context(), principal, workspace, job, query.attempt, domain.ArtifactID(selected.ID))
	if err != nil {
		artifactError(w, r, err)
		return
	}
	closed := false
	defer func() {
		if !closed {
			_ = closeArtifact(stream)
		}
	}()
	actual := artifactwire.File{ID: string(meta.ID), Path: meta.Path, Role: meta.Role, Bytes: meta.Object.Bytes, SHA256: string(meta.Object.SHA256)}
	stillPublished := func() bool {
		current, err := h.config.Results.Read(r.Context(), principal, workspace, job, query.attempt)
		if err != nil {
			return false
		}
		v, err := publishedView(current, target)
		return err == nil && v.SnapshotSHA256 == page.SnapshotSHA256
	}
	if stream == nil || actual != *selected || !stillPublished() {
		artifactError(w, r, collection.ErrUnavailable)
		return
	}
	// A fixed Content-Length could acknowledge the final byte before a late source
	// error. Declare a mandatory trailer instead; incomplete/failed streams lack it.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="`+selected.ID+`.bin"`)
	w.Header().Set("Content-Security-Policy", "sandbox")
	w.Header().Set("Accept-Ranges", "none")
	w.Header().Set("X-Artifact-ID", selected.ID)
	w.Header().Set("X-Workspace-ID", target.WorkspaceID)
	w.Header().Set("X-Job-ID", target.JobID)
	w.Header().Set("X-Attempt-ID", target.AttemptID)
	w.Header().Set("X-Content-Bytes", strconv.FormatInt(selected.Bytes, 10))
	w.Header().Set("X-Content-SHA256", selected.SHA256)
	w.Header().Set("Trailer", artifactwire.VerifiedTrailer)
	// From this boundary, all panics abort the response without diagnostics or JSON
	// appended to private binary bytes. The outer API recovery must preserve this.
	defer func() {
		if recover() != nil {
			panic(http.ErrAbortHandler)
		}
	}()
	w.WriteHeader(http.StatusOK)
	hash := sha256.New()
	n, copyErr := io.CopyN(io.MultiWriter(w, hash), stream, selected.Bytes)
	var extra [1]byte
	extraN, eofErr := stream.Read(extra[:])
	closeErr := closeArtifact(stream)
	closed = true
	if copyErr != nil || n != selected.Bytes || extraN != 0 || eofErr != io.EOF || closeErr != nil || hex.EncodeToString(hash.Sum(nil)) != selected.SHA256 || r.Context().Err() != nil || !stillPublished() {
		panic(http.ErrAbortHandler)
	}
	w.Header().Set(artifactwire.VerifiedTrailer, "true")
}

func closeArtifact(c io.Closer) (err error) {
	defer func() {
		if recover() != nil {
			err = collection.ErrUnavailable
		}
	}()
	if c == nil {
		return collection.ErrUnavailable
	}
	return c.Close()
}
func artifactError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, auth.ErrUnauthenticated), errors.Is(err, auth.ErrForbidden):
		mapError(w, r, err)
	case errors.Is(err, retention.ErrExpired):
		respondError(w, r, 410, domain.CodeArtifactMissing, domain.FailureStageResults, "published result has expired; retained bytes are unavailable")
	case errors.Is(err, collection.ErrNotFound):
		respondError(w, r, 404, domain.CodeArtifactMissing, domain.FailureStageResults, "published artifact not found or not visible")
	default:
		respondError(w, r, 503, domain.CodeArtifactCollectionFailed, domain.FailureStageResults, "published artifact cannot be read safely")
	}
}
