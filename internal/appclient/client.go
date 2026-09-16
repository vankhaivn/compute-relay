// Package appclient implements finite, non-retrying calls to the local application API.
// It has no provider, credential discovery, storage, scheduler or shell dependency.
package appclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

const MaxResponseBytes = 1 << 20

var ErrRequest = errors.New("invalid local API request")
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var codePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

// Request is an explicit action, not an arbitrary URL or header map. Body is owned
// by the caller and must be a bounded, cooperative reader until Exchange returns.
type Request struct {
	Action, Workspace, ID, Attempt, Key, Reason string
	Body                                        io.Reader
	Bytes                                       int64
	SHA256                                      string
}

type Failure struct {
	Stage                   string `json:"stage"`
	HTTPStatus              int    `json:"http_status,omitempty"`
	Code                    string `json:"code,omitempty"`
	RequestMayHaveCommitted bool   `json:"request_may_have_committed"`
}

func (*Failure) Error() string { return "local API call failed; no automatic retry was attempted" }

// Endpoint permits exactly one literal-loopback HTTP authority, without DNS,
// proxy, credentials, path prefix, fragment or query. TLS/remote API is separate work.
func Endpoint(raw string) (string, error) {
	if len(raw) > 256 {
		return "", ErrRequest
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Opaque != "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" {
		return "", ErrRequest
	}
	host, port, err := net.SplitHostPort(u.Host)
	ip, ipErr := netip.ParseAddr(host)
	n, portErr := strconv.Atoi(port)
	if err != nil || ipErr != nil || !ip.IsLoopback() || ip.Is4In6() || ip.Zone() != "" || portErr != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port || net.JoinHostPort(ip.String(), port) != u.Host {
		return "", ErrRequest
	}
	base := "http://" + u.Host
	if raw != base && raw != base+"/" {
		return "", ErrRequest
	}
	return base, nil
}

func ValidToken(token []byte) bool {
	if len(token) != 47 || !bytes.HasPrefix(token, []byte("cr1_")) {
		return false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(string(token[4:]))
	defer clear(raw)
	return err == nil && len(raw) == 32
}
func ValidID(id string) bool { return idPattern.MatchString(id) }
func ValidKey(key string) bool {
	if len(key) < 8 || len(key) > 256 {
		return false
	}
	for _, c := range key {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

func route(r Request) (method, path string, status int, body io.Reader, size int64, err error) {
	fail := func() (string, string, int, io.Reader, int64, error) { return "", "", 0, nil, 0, ErrRequest }
	if !ValidID(r.Workspace) {
		return fail()
	}
	base := "/v1/workspaces/" + r.Workspace
	method, status, body, size = http.MethodPost, http.StatusAccepted, r.Body, r.Bytes
	control := false
	switch r.Action {
	case "upload":
		if r.Body == nil || r.Bytes < 0 || r.Bytes > 2<<30 || !digestPattern.MatchString(r.SHA256) {
			return fail()
		}
		path, status = base+"/objects", http.StatusCreated
	case "validate", "submit":
		if r.Body == nil || r.Bytes < 1 || r.Bytes > 1<<20 {
			return fail()
		}
		path = base + "/jobs"
		if r.Action == "validate" {
			path += "/validate"
			status = http.StatusOK
		}
	case "status", "operation":
		if !ValidID(r.ID) {
			return fail()
		}
		method, status = http.MethodGet, http.StatusOK
		path = base + "/jobs/" + r.ID
		if r.Action == "operation" {
			path = base + "/operations/" + r.ID
		}
	case "cancel", "retry", "reconcile", "collect":
		control = true
		if !ValidID(r.ID) || !ValidID(r.Attempt) || !utf8.ValidString(r.Reason) || len(r.Reason) > 512 || strings.ContainsAny(r.Reason, "\x00\r") || r.Action == "retry" && strings.TrimSpace(r.Reason) == "" {
			return fail()
		}
		raw, e := json.Marshal(struct {
			Attempt string `json:"attempt_id"`
			Reason  string `json:"reason,omitempty"`
		}{r.Attempt, r.Reason})
		if e != nil {
			return fail()
		}
		body, size = bytes.NewReader(raw), int64(len(raw))
		path = base + "/jobs/" + r.ID + "/" + r.Action
	default:
		return fail()
	}
	if (r.Action == "submit" || control) != (r.Key != "") || r.Key != "" && !ValidKey(r.Key) {
		return fail()
	}
	if r.Action != "upload" && r.SHA256 != "" {
		return fail()
	}
	if !control && (r.Attempt != "" || r.Reason != "") {
		return fail()
	}
	if (r.Action == "upload" || r.Action == "validate" || r.Action == "submit") && r.ID != "" {
		return fail()
	}
	if (control || method == http.MethodGet) && (r.Body != nil || r.Bytes != 0) {
		return fail()
	}
	return method, path, status, body, size, nil
}

// Validate checks action arguments without reading a token, source or network.
func Validate(r Request) error { _, _, _, _, _, err := route(r); return err }

// Exchange creates one fresh HTTP/1 transport for one request. No redirect handler,
// proxy, cookies, keepalive reuse or GetBody replay is supplied. A lost acknowledgement
// remains uncertainty about local admission/control, not permission for new compute.
func Exchange(parent context.Context, endpoint string, token []byte, r Request) ([]byte, error) {
	base, err := Endpoint(endpoint)
	if err != nil || !ValidToken(token) {
		return nil, ErrRequest
	}
	method, path, expected, body, size, err := route(r)
	if err != nil {
		return nil, err
	}
	budget := 30 * time.Second
	if r.Action == "upload" {
		budget = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	if ctx.Err() != nil {
		return nil, &Failure{Stage: "before_request"}
	}
	var tracked *requestBody
	if body != nil {
		tracked = &requestBody{reader: body, closed: make(chan struct{})}
		body = tracked
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, body)
	if err != nil {
		return nil, ErrRequest
	}
	req.ContentLength, req.GetBody, req.Close = size, nil, true
	req.Header.Set("Authorization", "Bearer "+string(token))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
		if r.Action == "upload" {
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("X-Content-SHA256", r.SHA256)
		}
	}
	if r.Key != "" {
		req.Header.Set("Idempotency-Key", r.Key)
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 32 << 10, ResponseHeaderTimeout: 10 * time.Second}
	defer transport.CloseIdleConnections()
	resp, err := transport.RoundTrip(req)
	// RoundTrip may return before request-body closure. Do not release the caller's
	// file until the transport has relinquished it. Cooperative readers are required.
	if tracked != nil {
		if err != nil {
			cancel()
		}
		select {
		case <-tracked.closed:
		case <-ctx.Done():
			cancel()
			if resp != nil {
				_ = resp.Body.Close()
			}
			<-tracked.closed
			return nil, &Failure{Stage: "transport", RequestMayHaveCommitted: r.Action != "validate"}
		}
	}
	mutating := method == http.MethodPost && r.Action != "validate"
	if err != nil {
		return nil, &Failure{Stage: "transport", RequestMayHaveCommitted: mutating}
	}
	return receive(resp, expected, r, token, mutating)
}

// requestBody does not own the source. Close waits for active Reads before
// releasing it to the caller; a reader ignoring cancellation can delay the join.
type requestBody struct {
	reader  io.Reader
	closed  chan struct{}
	life    sync.RWMutex
	stopped bool
}

func (b *requestBody) Read(p []byte) (int, error) {
	b.life.RLock()
	defer b.life.RUnlock()
	if b.stopped {
		return 0, io.ErrClosedPipe
	}
	return b.reader.Read(p)
}
func (b *requestBody) Close() error {
	b.life.Lock()
	defer b.life.Unlock()
	if !b.stopped {
		b.stopped = true
		close(b.closed)
	}
	return nil
}

func receive(resp *http.Response, expected int, r Request, token []byte, mutating bool) ([]byte, error) {
	fail := &Failure{Stage: "response", HTTPStatus: resp.StatusCode, RequestMayHaveCommitted: mutating}
	// Never read a redirect body or use its destination as new credential authority.
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		_ = resp.Body.Close()
		return nil, fail
	}
	media, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(resp.Header.Values("Content-Type")) != 1 || len(params) > 1 || len(params) == 1 && !strings.EqualFold(params["charset"], "utf-8") || resp.Header.Get("Content-Encoding") != "" && resp.Header.Get("Content-Encoding") != "identity" || resp.ContentLength > MaxResponseBytes {
		_ = resp.Body.Close()
		return nil, fail
	}
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil || len(raw) > MaxResponseBytes || resp.ContentLength >= 0 && int64(len(raw)) != resp.ContentLength || bytes.Contains(raw, token) {
		clear(raw)
		return nil, fail
	}
	object, err := jsonwire.Object(raw, MaxResponseBytes)
	if err != nil {
		clear(raw)
		return nil, fail
	}
	if resp.StatusCode != expected {
		fail.Stage = "http"
		var envelope struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(object["error"], &envelope) == nil && codePattern.MatchString(envelope.Code) {
			fail.Code = envelope.Code
		}
		clear(raw)
		return nil, fail
	}
	if _, hasError := object["error"]; hasError || containsToken(raw, token) || !responseIdentity(object, r) {
		clear(raw)
		return nil, fail
	}
	return raw, nil
}

func text(m map[string]json.RawMessage, k string) string {
	var s string
	_ = json.Unmarshal(m[k], &s)
	return s
}
func boolean(m map[string]json.RawMessage, k string) bool {
	var b *bool
	return json.Unmarshal(m[k], &b) == nil && b != nil
}
func timestamp(m map[string]json.RawMessage, k string) bool {
	_, err := time.Parse(time.RFC3339Nano, text(m, k))
	return err == nil
}
func positive(m map[string]json.RawMessage, k string) bool {
	var n int64
	return json.Unmarshal(m[k], &n) == nil && n > 0
}
func responseIdentity(m map[string]json.RawMessage, r Request) bool {
	switch r.Action {
	case "upload":
		var n int64
		return ValidID(text(m, "object_id")) && text(m, "workspace_id") == r.Workspace && json.Unmarshal(m["bytes"], &n) == nil && string(m["bytes"]) != "null" && n == r.Bytes && text(m, "sha256") == r.SHA256
	case "validate":
		var warnings, requirements []json.RawMessage
		return string(bytes.TrimSpace(m["valid"])) == "true" && json.Unmarshal(m["warnings"], &warnings) == nil && warnings != nil && json.Unmarshal(m["requirements"], &requirements) == nil && requirements != nil
	case "submit":
		var links map[string]json.RawMessage
		if json.Unmarshal(m["links"], &links) != nil || text(links, "self") != "/v1/workspaces/"+r.Workspace+"/jobs/"+text(m, "job_id") {
			return false
		}
		return ValidID(text(m, "job_id")) && ValidID(text(m, "attempt_id")) && text(m, "status") == "queued" && timestamp(m, "created_at") && boolean(m, "idempotency_replay")
	case "status":
		var state map[string]json.RawMessage
		if text(m, "api_version") != "compute-connector/v1alpha1" || text(m, "workspace_id") != r.Workspace || text(m, "job_id") != r.ID || !ValidID(text(m, "active_attempt_id")) || !positive(m, "revision") || !timestamp(m, "updated_at") || json.Unmarshal(m["state"], &state) != nil {
			return false
		}
		for _, k := range []string{"orchestration", "execution", "result", "cancellation", "remote_activity", "release_evidence"} {
			if text(state, k) == "" {
				return false
			}
		}
		return boolean(state, "deadline_exceeded")
	default:
		if text(m, "workspace_id") != r.Workspace || !ValidID(text(m, "operation_id")) || !ValidID(text(m, "job_id")) || !ValidID(text(m, "attempt_id")) || !positive(m, "revision") || !timestamp(m, "created_at") || !timestamp(m, "updated_at") || text(m, "status") == "" || text(m, "effect") == "" || !boolean(m, "replay") || !boolean(m, "remote_termination_confirmed") {
			return false
		}
		if r.Action == "operation" {
			return text(m, "operation_id") == r.ID
		}
		return controlIdentity(m, r)
	}
}

// Normalize string escapes for literal-token rejection without changing output bytes.
func containsToken(raw, token []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var decoded any
	if d.Decode(&decoded) != nil {
		return true
	}
	normalized, err := json.Marshal(decoded)
	defer clear(normalized)
	return err != nil || bytes.Contains(normalized, token)
}
