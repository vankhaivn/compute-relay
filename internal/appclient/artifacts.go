package appclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/artifactwire"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

func artifactBase(t artifactwire.Target) string {
	return "/v1/workspaces/" + t.WorkspaceID + "/jobs/" + t.JobID + "/artifacts"
}

// artifactGET never accepts a server-provided URL. path is built only by the
// validated callers below. One fresh transport means no reused-request replay.
func artifactGET(ctx context.Context, endpoint string, token []byte, path string) (*http.Response, func(), error) {
	base, err := Endpoint(endpoint)
	if err != nil || !ValidToken(token) || ctx.Err() != nil {
		return nil, func() {}, ErrRequest
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return nil, func() {}, ErrRequest
	}
	req.Close = true
	req.Header.Set("Authorization", "Bearer "+string(token))
	req.Header.Set("Accept-Encoding", "identity")
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 32 << 10, ResponseHeaderTimeout: 10 * time.Second}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, func() {}, &Failure{Stage: "transport"}
	}
	return resp, transport.CloseIdleConnections, nil
}

func artifactJSON(resp *http.Response, token []byte) ([]byte, error) {
	fail := &Failure{Stage: "response", HTTPStatus: resp.StatusCode}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		_ = resp.Body.Close()
		return nil, fail
	}
	media, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(resp.Header.Values("Content-Type")) != 1 || len(params) > 1 || len(params) == 1 && !strings.EqualFold(params["charset"], "utf-8") || len(resp.Header.Values("Content-Encoding")) > 1 || resp.Header.Get("Content-Encoding") != "" && resp.Header.Get("Content-Encoding") != "identity" || resp.ContentLength > artifactwire.MaxJSON {
		_ = resp.Body.Close()
		return nil, fail
	}
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, artifactwire.MaxJSON+1))
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil || len(raw) > artifactwire.MaxJSON || resp.ContentLength >= 0 && int64(len(raw)) != resp.ContentLength || bytes.Contains(raw, token) || containsToken(raw, token) {
		clear(raw)
		return nil, fail
	}
	m, err := jsonwire.Object(raw, artifactwire.MaxJSON)
	if err != nil {
		clear(raw)
		return nil, fail
	}
	if resp.StatusCode != http.StatusOK {
		fail.Stage = "http"
		var body struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(m["error"], &body) == nil && codePattern.MatchString(body.Code) {
			fail.Code = body.Code
		}
		clear(raw)
		return nil, fail
	}
	if _, exists := m["error"]; exists {
		clear(raw)
		return nil, fail
	}
	return raw, nil
}

func ListArtifacts(parent context.Context, endpoint string, token []byte, target artifactwire.Target, cursor string, limit int) (artifactwire.Page, error) {
	if !target.Valid() || len(cursor) > 80 || limit < 1 || limit > artifactwire.MaxPage {
		return artifactwire.Page{}, ErrRequest
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	query := url.Values{"attempt_id": {target.AttemptID}, "limit": {strconv.Itoa(limit)}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	resp, done, err := artifactGET(ctx, endpoint, token, artifactBase(target)+"?"+query.Encode())
	if err != nil {
		return artifactwire.Page{}, err
	}
	defer done()
	raw, err := artifactJSON(resp, token)
	defer clear(raw)
	if err != nil {
		return artifactwire.Page{}, err
	}
	p, err := artifactwire.DecodePage(raw, target, cursor, limit)
	if err != nil || ctx.Err() != nil {
		return artifactwire.Page{}, &Failure{Stage: "artifact_metadata"}
	}
	return p, nil
}

func ArtifactMetadata(parent context.Context, endpoint string, token []byte, target artifactwire.Target, id string) (artifactwire.Metadata, error) {
	if !target.Valid() || !ValidID(id) || !strings.HasPrefix(id, "art_") {
		return artifactwire.Metadata{}, ErrRequest
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	query := url.Values{"attempt_id": {target.AttemptID}}
	resp, done, err := artifactGET(ctx, endpoint, token, artifactBase(target)+"/"+id+"?"+query.Encode())
	if err != nil {
		return artifactwire.Metadata{}, err
	}
	defer done()
	raw, err := artifactJSON(resp, token)
	defer clear(raw)
	if err != nil {
		return artifactwire.Metadata{}, err
	}
	m, err := artifactwire.DecodeMetadata(raw, target, id)
	if err != nil || ctx.Err() != nil {
		return artifactwire.Metadata{}, &Failure{Stage: "artifact_metadata"}
	}
	return m, nil
}

// DownloadArtifact writes only to a caller-owned unpublished sink. The pin must
// come from metadata for the explicit attempt, not a current/active-attempt alias.
// Failure may follow all bytes; no caller may publish on Content-Length alone.
func DownloadArtifact(parent context.Context, endpoint string, token []byte, pin artifactwire.Metadata, dst io.Writer) error {
	if !pin.Target.Valid() || !pin.Artifact.Valid() || pin.VerifiedAt.IsZero() || dst == nil {
		return ErrRequest
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	query := url.Values{"attempt_id": {pin.AttemptID}}
	resp, done, err := artifactGET(ctx, endpoint, token, artifactBase(pin.Target)+"/"+pin.Artifact.ID+"/content?"+query.Encode())
	if err != nil {
		return err
	}
	defer done()
	closed := false
	defer func() {
		if !closed {
			_ = resp.Body.Close()
		}
	}()
	fail := &Failure{Stage: "artifact_transfer", HTTPStatus: resp.StatusCode}
	if resp.StatusCode != http.StatusOK {
		_, err := artifactJSON(resp, token)
		closed = true
		if err != nil {
			return err
		}
		return fail
	}
	for key, value := range map[string]string{
		"Content-Type": "application/octet-stream", "X-Artifact-ID": pin.Artifact.ID,
		"X-Workspace-ID": pin.WorkspaceID, "X-Job-ID": pin.JobID, "X-Attempt-ID": pin.AttemptID,
		"X-Content-SHA256": pin.Artifact.SHA256, "X-Content-Bytes": strconv.FormatInt(pin.Artifact.Bytes, 10),
	} {
		if len(resp.Header.Values(key)) != 1 || resp.Header.Get(key) != value {
			return fail
		}
	}
	_, declared := resp.Trailer[http.CanonicalHeaderKey(artifactwire.VerifiedTrailer)]
	if !declared || len(resp.Header.Values(artifactwire.VerifiedTrailer)) != 0 || resp.ContentLength != -1 || len(resp.TransferEncoding) != 1 || resp.TransferEncoding[0] != "chunked" || len(resp.Header.Values("Content-Encoding")) > 1 || resp.Header.Get("Content-Encoding") != "" && resp.Header.Get("Content-Encoding") != "identity" {
		return fail
	}
	copyErr := artifactwire.Copy(ctx, dst, resp.Body, pin.Artifact)
	closeErr := resp.Body.Close()
	closed = true
	// Trailer is read only after Copy has consumed EOF, never concurrently with Read.
	if copyErr != nil || closeErr != nil || ctx.Err() != nil || len(resp.Trailer.Values(artifactwire.VerifiedTrailer)) != 1 || resp.Trailer.Get(artifactwire.VerifiedTrailer) != "true" {
		return fail
	}
	return nil
}
