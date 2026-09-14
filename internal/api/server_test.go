package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/testsupport"
)

type fixture struct {
	server   *http.Server
	access   *auth.Service
	catalog  *testsupport.Memory
	blobs    *blobfs.Store
	tokens   map[string]string
	tokenIDs map[string]string
	url      string
}

func setupHTTP(t *testing.T, mutate func(*Config), ready func(context.Context) error) *fixture {
	t.Helper()
	cfg := DefaultConfig()
	cfg.MaxUploadBytes = 1 << 20
	cfg.MaxJSONBytes = 128
	cfg.GlobalRate = Rate{10000, 10000}
	cfg.WorkspaceRate = Rate{10000, 10000}
	m := testsupport.NewMemory(auth.Workspace{ID: "a", Enabled: true}, auth.Workspace{ID: "b", Enabled: true})
	a, err := auth.New(m, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s, err := objects.New(a, b, m)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listen = ln.Addr().String()
	if mutate != nil {
		mutate(&cfg)
	}
	server, err := NewServer(cfg, a, s, ready)
	if err != nil {
		ln.Close()
		b.Close()
		t.Fatal(err)
	}
	f := &fixture{server: server, access: a, catalog: m, blobs: b, url: "http://" + ln.Addr().String(), tokens: map[string]string{}, tokenIDs: map[string]string{}}
	for _, name := range []string{"a", "b", "read"} {
		w := domain.WorkspaceID(name)
		scopes := []auth.Scope{auth.Read, auth.Write}
		if name == "read" {
			w = "a"
			scopes = []auth.Scope{auth.Read}
		}
		sec, record, err := a.Issue(context.Background(), w, scopes, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[name] = sec.Reveal()
		f.tokenIDs[name] = record.ID
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(ln) }()
	t.Cleanup(func() { _ = server.Close(); <-done; _ = b.Close() })
	return f
}

func (f *fixture) request(t *testing.T, method, path, who string, body io.Reader, change func(*http.Request)) (*http.Response, []byte) {
	t.Helper()
	r, err := http.NewRequest(method, f.url+path, body)
	if err != nil {
		t.Fatal(err)
	}
	if who != "" {
		r.Header.Set("Authorization", "Bearer "+f.tokens[who])
	}
	if method == http.MethodPost {
		r.Header.Set("Content-Type", "application/octet-stream")
	}
	if change != nil {
		change(r)
	}
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range f.tokens {
		if bytes.Contains(data, []byte(token)) {
			t.Fatal("token leaked into response")
		}
	}
	if response.Header.Get("X-Request-ID") == "" || response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unsafe response headers")
	}
	return response, data
}
func assertStatus(t *testing.T, r *http.Response, body []byte, want int) {
	t.Helper()
	if r.StatusCode != want {
		t.Fatalf("status=%d want=%d body=%s", r.StatusCode, want, body)
	}
	if want >= 400 {
		var e ErrorEnvelope
		if err := json.Unmarshal(body, &e); err != nil {
			t.Fatal(err)
		}
		p, err := domain.NewProblem(e.Error.Code, e.Error.Message, e.Error.Stage)
		if err != nil {
			t.Fatal("invalid error taxonomy", err)
		}
		_ = p
		if e.Error.RequestID != r.Header.Get("X-Request-ID") || e.Error.ComputeMayHaveStarted || e.Error.SafeOperationRetry {
			t.Fatal("invalid error envelope")
		}
	}
}

func TestLoopbackUploadMetadataIsolationAndRevocation(t *testing.T) {
	f := setupHTTP(t, nil, nil)
	payload := strings.Repeat("stream", 10000)
	sum := sha256.Sum256([]byte(payload))
	response, body := f.request(t, "POST", "/v1/workspaces/a/objects", "a", strings.NewReader(payload), func(r *http.Request) {
		r.Header.Set("X-Content-SHA256", hex.EncodeToString(sum[:]))
		r.Header.Set("X-Request-ID", "attacker-supplied")
	})
	assertStatus(t, response, body, 201)
	if response.Header.Get("X-Request-ID") == "attacker-supplied" {
		t.Fatal("untrusted request ID reflected")
	}
	var m domain.ObjectMetadata
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	if !m.Valid() || m.Bytes != int64(len(payload)) || string(m.SHA256) != hex.EncodeToString(sum[:]) {
		t.Fatal("incorrect upload receipt")
	}
	path := response.Header.Get("Location")
	response, body = f.request(t, "GET", path, "a", nil, nil)
	assertStatus(t, response, body, 200)
	response, body = f.request(t, "GET", path, "b", nil, nil)
	assertStatus(t, response, body, 403)
	response, body = f.request(t, "GET", "/v1/workspaces/b/objects/"+string(m.ID), "b", nil, nil)
	assertStatus(t, response, body, 404)
	content, err := f.blobs.Open(context.Background(), "a", m.ID)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(content)
	content.Close()
	if err != nil || string(actual) != payload {
		t.Fatal("stored bytes changed")
	}
	if err := f.access.Revoke(context.Background(), f.tokenIDs["a"]); err != nil {
		t.Fatal(err)
	}
	response, body = f.request(t, "GET", path, "a", nil, nil)
	assertStatus(t, response, body, 401)
	if response.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("missing bearer challenge")
	}
}

func TestRequestGuardsAndNoFalseAdmission(t *testing.T) {
	f := setupHTTP(t, nil, nil)
	cases := []struct {
		name, method, path, who string
		want                    int
		change                  func(*http.Request)
	}{
		{"health", "GET", "/healthz", "", 200, nil},
		{"no auth", "GET", "/v1/info", "", 401, nil},
		{"bad auth", "GET", "/v1/info", "a", 401, func(r *http.Request) { r.Header.Set("Authorization", "Bearer invalid") }},
		{"duplicate auth", "GET", "/v1/info", "a", 401, func(r *http.Request) { r.Header.Add("Authorization", "Bearer invalid") }},
		{"unready", "GET", "/readyz", "a", 503, nil},
		{"host", "GET", "/v1/info", "a", 403, func(r *http.Request) { r.Host = "attacker.invalid:7331" }},
		{"host port", "GET", "/v1/info", "a", 403, func(r *http.Request) { r.Host = "127.0.0.1:1" }},
		{"origin", "GET", "/v1/info", "a", 403, func(r *http.Request) { r.Header.Set("Origin", "https://attacker.invalid") }},
		{"null origin", "GET", "/v1/info", "a", 403, func(r *http.Request) { r.Header.Set("Origin", "null") }},
		{"same origin", "GET", "/v1/info", "a", 200, func(r *http.Request) { r.Header.Set("Origin", f.url) }},
		{"fetch metadata", "GET", "/v1/info", "a", 403, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }},
		{"query token", "GET", "/v1/info?token=x", "", 400, nil},
		{"encoded path", "GET", "/v1/workspaces/a/objects/obj%2fother", "a", 400, nil},
		{"wrong workspace", "POST", "/v1/workspaces/b/objects", "a", 403, nil},
		{"read only", "POST", "/v1/workspaces/a/objects", "read", 403, nil},
		{"unsupported media", "POST", "/v1/workspaces/a/objects", "a", 415, func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }},
		{"encoded body", "POST", "/v1/workspaces/a/objects", "a", 415, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }},
		{"invalid digest", "POST", "/v1/workspaces/a/objects", "a", 400, func(r *http.Request) { r.Header.Set("X-Content-SHA256", "oops") }},
		{"wrong digest", "POST", "/v1/workspaces/a/objects", "a", 400, func(r *http.Request) { r.Header.Set("X-Content-SHA256", strings.Repeat("a", 64)) }},
		{"jobs not admitted", "POST", "/v1/workspaces/a/jobs", "a", 404, nil},
		{"no admin route", "POST", "/v1/tokens", "a", 404, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, b := f.request(t, tc.method, tc.path, tc.who, strings.NewReader("payload"), tc.change)
			assertStatus(t, r, b, tc.want)
		})
	}
	if f.catalog.ObjectCount() != 0 {
		t.Fatal("rejected requests admitted an object")
	}
}

func TestDeclaredAndChunkedUploadLimits(t *testing.T) {
	f := setupHTTP(t, func(c *Config) { c.MaxUploadBytes = 8 }, nil)
	for _, chunked := range []bool{false, true} {
		r, b := f.request(t, "POST", "/v1/workspaces/a/objects", "a", strings.NewReader("012345678"), func(r *http.Request) {
			if chunked {
				r.ContentLength = -1
			}
		})
		assertStatus(t, r, b, 413)
	}
	if f.catalog.ObjectCount() != 0 {
		t.Fatal("oversized object admitted")
	}
}

func TestHTTPDeadlineInterruptsBlockedBody(t *testing.T) {
	f := setupHTTP(t, func(c *Config) { c.RequestTimeout = 50 * time.Millisecond; c.UploadTimeout = 200 * time.Millisecond }, nil)
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(f.url, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = fmt.Fprintf(connection, "POST /v1/workspaces/a/objects HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/octet-stream\r\nContent-Length: 10\r\n\r\nx", strings.TrimPrefix(f.url, "http://"), f.tokens["a"])
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		t.Fatal("blocked body was not interrupted with HTTP response", err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	assertStatus(t, response, body, 408)
	if f.catalog.ObjectCount() != 0 {
		t.Fatal("incomplete timeout upload admitted")
	}
}

func TestWorkspaceAndGlobalRateLimits(t *testing.T) {
	now := time.Now()
	f := setupHTTP(t, func(c *Config) { c.Now = func() time.Time { return now }; c.WorkspaceRate = Rate{1, 1} }, nil)
	r, b := f.request(t, "GET", "/v1/info", "a", nil, nil)
	assertStatus(t, r, b, 200)
	r, b = f.request(t, "GET", "/v1/info", "a", nil, nil)
	assertStatus(t, r, b, 429)
	if r.Header.Get("Retry-After") != "1" {
		t.Fatal("missing retry hint")
	}
	r, b = f.request(t, "GET", "/v1/info", "b", nil, nil)
	assertStatus(t, r, b, 200)
	g := setupHTTP(t, func(c *Config) { c.Now = func() time.Time { return now }; c.GlobalRate = Rate{1, 1} }, nil)
	r, b = g.request(t, "GET", "/v1/info", "", nil, nil)
	assertStatus(t, r, b, 401)
	r, b = g.request(t, "GET", "/v1/info", "", nil, nil)
	assertStatus(t, r, b, 429)
}

func TestConcurrentRequestLimitAndSanitizedPanic(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	f := setupHTTP(t, func(c *Config) { c.MaxInFlight = 1; c.MaxUploads = 1 }, func(context.Context) error { close(entered); <-release; return nil })
	done := make(chan struct{})
	go func() {
		defer close(done)
		r, _ := http.NewRequest("GET", f.url+"/readyz", nil)
		r.Header.Set("Authorization", "Bearer "+f.tokens["a"])
		resp, err := http.DefaultClient.Do(r)
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-entered
	r, b := f.request(t, "GET", "/v1/info", "a", nil, nil)
	assertStatus(t, r, b, 429)
	close(release)
	<-done
	p := setupHTTP(t, nil, func(context.Context) error { panic("secret-path-canary") })
	r, b = p.request(t, "GET", "/readyz", "a", nil, nil)
	assertStatus(t, r, b, 503)
	if strings.Contains(string(b), "secret-path-canary") {
		t.Fatal("panic leaked")
	}
}

func TestDecodeJSONRejectsUnknownTrailingAndOversize(t *testing.T) {
	for _, body := range []string{`{"name":"ok","extra":true}`, `{"name":"ok"} {}`, `{"name":"` + strings.Repeat("x", 100) + `"}`, `invalid`} {
		r := httptest.NewRequest("POST", "http://127.0.0.1:7331/test", strings.NewReader(body))
		var value struct {
			Name string `json:"name"`
		}
		if err := DecodeJSON(httptest.NewRecorder(), r, 64, &value); err == nil {
			t.Fatalf("invalid JSON accepted: %s", body)
		}
	}
	r := httptest.NewRequest("POST", "http://127.0.0.1:7331/test", strings.NewReader(`{"name":"ok"}`))
	var value struct {
		Name string `json:"name"`
	}
	if err := DecodeJSON(httptest.NewRecorder(), r, 64, &value); err != nil || value.Name != "ok" {
		t.Fatal(err)
	}
}

func TestUnsafeServerConfigurationRejected(t *testing.T) {
	f := setupHTTP(t, nil, nil)
	s, _ := objects.New(f.access, f.blobs, f.catalog)
	for _, address := range []string{"0.0.0.0:7331", "localhost:7331", "[::]:7331", "192.168.1.2:7331", "127.0.0.1:99999"} {
		cfg := DefaultConfig()
		cfg.Listen = address
		if _, err := NewServer(cfg, f.access, s, nil); err == nil {
			t.Fatal("unsafe bind", address)
		}
	}
	for _, rate := range []float64{math.NaN(), math.Inf(1), 0, -1} {
		cfg := DefaultConfig()
		cfg.GlobalRate.PerSecond = rate
		if _, err := NewServer(cfg, f.access, s, nil); err == nil {
			t.Fatal("unsafe rate accepted")
		}
	}
}

func TestLimiterBoundsKeysWithoutResettingLiveAllowance(t *testing.T) {
	now := time.Now()
	l := newLimiter(Rate{1, 1}, 1)
	if ok, _ := l.allow("a", now); !ok {
		t.Fatal("first request denied")
	}
	if ok, _ := l.allow("b", now); ok {
		t.Fatal("live bucket evicted")
	}
	if ok, _ := l.allow("a", now.Add(-time.Second)); ok {
		t.Fatal("backward clock restored allowance")
	}
	if ok, _ := l.allow("a", now.Add(time.Second)); !ok {
		t.Fatal("bucket failed to refill")
	}
	if ok, _ := l.allow("b", now.Add(time.Minute*2)); !ok {
		t.Fatal("idle bucket not reclaimed")
	}
}

func TestConcurrentUploadCapAndDisconnectedBody(t *testing.T) {
	f := setupHTTP(t, func(c *Config) { c.MaxUploads = 1 }, nil)
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(f.url, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = fmt.Fprintf(connection, "POST /v1/workspaces/a/objects HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/octet-stream\r\nContent-Length: 10\r\n\r\nx", strings.TrimPrefix(f.url, "http://"), f.tokens["a"])
	if err != nil {
		t.Fatal(err)
	}
	uploads := f.server.Handler.(*handler).uploads
	deadline := time.Now().Add(3 * time.Second)
	for len(uploads) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("first upload did not start")
		}
		time.Sleep(time.Millisecond)
	}
	response, body := f.request(t, "POST", "/v1/workspaces/a/objects", "a", strings.NewReader("payload"), nil)
	assertStatus(t, response, body, 429)
	if err = connection.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	response, err = http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, response, body, 400)
	if f.catalog.ObjectCount() != 0 {
		t.Fatal("disconnected or throttled upload was committed")
	}
}
