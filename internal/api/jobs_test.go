package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// Admission tests deliberately use synthetic METADATA, not an input-ready or executed
// workload claim. The object writer fails if a handler unexpectedly performs a transfer.
type noUpload struct{}

func (noUpload) Put(context.Context, domain.ObjectMetadata, io.Reader) (domain.ObjectMetadata, error) {
	return domain.ObjectMetadata{}, errors.New("unexpected upload")
}

type jobHTTP struct {
	url      string
	handler  http.Handler
	store    *sqlite.Store
	tokens   map[string]string
	tokenIDs map[string]string
}

func setupJobHTTP(t *testing.T) *jobHTTP {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "state"), sqlite.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	access, err := auth.New(store, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := objects.New(access, noUpload{}, store)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	f := &jobHTTP{store: store, tokens: map[string]string{}, tokenIDs: map[string]string{}}
	for _, name := range []string{"a", "b", "read"} {
		w := domain.WorkspaceID(name)
		scopes := []auth.Scope{auth.Read, auth.Write}
		if name == "read" {
			w = "a"
			scopes = []auth.Scope{auth.Read}
		}
		if err = store.PutWorkspace(ctx, auth.Workspace{ID: w, Enabled: true, AllowedProfiles: []string{"default-gpu"}}); err != nil {
			t.Fatal(err)
		}
		sec, record, err := access.Issue(ctx, w, scopes, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[name], f.tokenIDs[name] = sec.Reveal(), record.ID
	}
	if err = store.PutProfile(ctx, admission.DefaultProfile(domain.ProviderBinding{Profile: "default-gpu", ProviderInstanceID: "fake", ConfigurationRevision: "rev1"}, "account"), true); err != nil {
		t.Fatal(err)
	}
	if err = store.CommitObject(ctx, domain.ObjectMetadata{ID: "code", WorkspaceID: "a", Bytes: 0, SHA256: domain.SHA256Digest(strings.Repeat("a", 64))}); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := api.DefaultConfig()
	cfg.Listen = ln.Addr().String()
	cfg.Jobs = svc
	cfg.WorkspaceRate = api.Rate{PerSecond: 1000, Burst: 1000}
	cfg.GlobalRate = cfg.WorkspaceRate
	server, err := api.NewServer(cfg, access, obj, store.Ready)
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	f.url = "http://" + ln.Addr().String()
	f.handler = server.Handler
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(ln) }()
	t.Cleanup(func() { _ = server.Close(); <-done })
	return f
}

const jobBody = `{"api_version":"compute-connector/v1alpha1","name":"test","profile":"default-gpu","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","not-executed.py"]},"inputs":[],"outputs":[{"path":"result.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":120,"setup_seconds":30,"finalization_grace_seconds":15}}`

func (f *jobHTTP) call(t *testing.T, method, path, who, key, body string, want int) (http.Header, []byte) {
	t.Helper()
	r, err := http.NewRequest(method, f.url+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if who != "" {
		r.Header.Set("Authorization", "Bearer "+f.tokens[who])
	}
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != want {
		t.Fatalf("%s %s status=%d want=%d: %s", method, path, resp.StatusCode, want, data)
	}
	for _, token := range f.tokens {
		if bytes.Contains(data, []byte(token)) {
			t.Fatal("token leaked")
		}
	}
	if resp.Header.Get("X-Request-ID") == "" || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("missing safe headers")
	}
	if want >= 400 {
		wireSchema(t, "error", data)
	}
	return resp.Header, data
}
func wireSchema(t *testing.T, name string, data []byte) {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	for _, file := range []string{"common", name} {
		b, err := schemas.Files.ReadFile(file + ".v1alpha1.schema.json")
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err = json.Unmarshal(b, &value); err != nil {
			t.Fatal(err)
		}
		if err = c.AddResource("https://schema.example/"+file+".v1alpha1.schema.json", value); err != nil {
			t.Fatal(err)
		}
	}
	schema, err := c.Compile("https://schema.example/" + name + ".v1alpha1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var value any
	if err = json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	if err = schema.Validate(value); err != nil {
		t.Fatal("actual HTTP wire shape violates schema", err)
	}
}
func TestDurableAdmissionHTTPContractAndAuthorization(t *testing.T) {
	f := setupJobHTTP(t)
	path := "/v1/workspaces/a/jobs"
	_, v := f.call(t, "POST", path+"/validate", "a", "", jobBody, 200)
	wireSchema(t, "job-validation", v)
	headers, b := f.call(t, "POST", path, "a", "request-0001", jobBody, 202)
	wireSchema(t, "job-admission", b)
	var first admission.Receipt
	if err := json.Unmarshal(b, &first); err != nil {
		t.Fatal(err)
	}
	if first.Replay || headers.Get("Location") != first.Links.Self {
		t.Fatal("invalid new receipt")
	}
	_, b = f.call(t, "POST", path, "a", "request-0001", jobBody, 202)
	var replay admission.Receipt
	_ = json.Unmarshal(b, &replay)
	if !replay.Replay || replay.JobID != first.JobID || replay.AttemptID != first.AttemptID {
		t.Fatal("duplicate admission")
	}
	_, b = f.call(t, "GET", first.Links.Self, "a", "", "", 200)
	wireSchema(t, "job-status", b)
	if bytes.Contains(b, []byte("not-executed.py")) || bytes.Contains(b, []byte("account")) {
		t.Fatal("internal request/profile exposed")
	}
	f.call(t, "POST", path, "a", "request-0001", strings.Replace(jobBody, `"test"`, `"changed"`, 1), 409)
	f.call(t, "POST", path, "a", "", jobBody, 400)
	f.call(t, "POST", path, "read", "read-key-01", jobBody, 403)
	f.call(t, "POST", path, "", "missing-token", jobBody, 401)
	f.call(t, "GET", first.Links.Self, "b", "", "", 403)
	f.call(t, "GET", "/v1/workspaces/b/jobs/"+string(first.JobID), "b", "", "", 404)
	f.call(t, "POST", "/v1/workspaces/b/jobs", "b", "foreign-code", jobBody, 404)
	f.call(t, "POST", path, "a", "invalid-json", `{"name":"a","name":"b"}`, 400)
	f.call(t, "POST", path, "a", "oversized-01", strings.Repeat(" ", admission.MaxRequestBytes+1), 413)
	f.call(t, "POST", first.Links.Self+"/retry", "a", "retry-key", "{}", 404)
	if err := f.store.RevokeToken(context.Background(), f.tokenIDs["a"]); err != nil {
		t.Fatal(err)
	}
	f.call(t, "POST", path, "a", "request-0001", jobBody, 401)
}

type lostAdmissionResponse struct {
	header http.Header
	status int
}

func (w *lostAdmissionResponse) Header() http.Header     { return w.header }
func (w *lostAdmissionResponse) WriteHeader(status int)  { w.status = status }
func (*lostAdmissionResponse) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestLostHTTPReceiptReplaysCommittedIDs(t *testing.T) {
	f := setupJobHTTP(t)
	path := "/v1/workspaces/a/jobs"
	r := httptest.NewRequest("POST", f.url+path, strings.NewReader(jobBody))
	r.Header.Set("Authorization", "Bearer "+f.tokens["a"])
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "lost-http-receipt")
	broken := &lostAdmissionResponse{header: make(http.Header)}
	f.handler.ServeHTTP(broken, r)
	if broken.status != 202 {
		t.Fatalf("did not commit before response failure: %d", broken.status)
	}
	headers, b := f.call(t, "POST", path, "a", "lost-http-receipt", jobBody, 202)
	var receipt admission.Receipt
	_ = json.Unmarshal(b, &receipt)
	if !receipt.Replay || headers.Get("Location") != broken.header.Get("Location") {
		t.Fatal("response failure created a second job")
	}
}
