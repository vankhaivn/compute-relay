package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
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
	profiles map[string]string
	url      string
	handler  http.Handler
	store    *sqlite.Store
	tokens   map[string]string
	tokenIDs map[string]string
}

func decodeHTTPJSON[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal("invalid HTTP JSON", err)
	}
	return value
}

func setupJobHTTP(t *testing.T) *jobHTTP {
	return setupJobHTTPWith(t, jobHTTPOptions{profiles: true})
}

type jobHTTPOptions struct {
	code        []byte
	extraScopes map[string][]auth.Scope
	profiles    bool
	configure   func(*api.Config, *auth.Service, *sqlite.Store)
}

func setupJobHTTPWith(t *testing.T, options jobHTTPOptions) *jobHTTP {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "state"), sqlite.DefaultOptions())
	requireNoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	access, err := auth.New(store, store)
	requireNoError(t, err)
	obj, err := objects.New(access, noUpload{}, store)
	requireNoError(t, err)
	svc, err := admission.New(access, store, admission.DefaultLimits())
	requireNoError(t, err)
	f := &jobHTTP{store: store, tokens: map[string]string{}, tokenIDs: map[string]string{}}
	for _, workspace := range []domain.WorkspaceID{"a", "b"} {
		if err = store.PutWorkspace(ctx, auth.Workspace{ID: workspace, Enabled: true, AllowedProfiles: []string{"default-gpu"}}); err != nil {
			t.Fatal(err)
		}
	}
	actors := map[string][]auth.Scope{
		"a": {auth.Read, auth.Write}, "b": {auth.Read, auth.Write}, "read": {auth.Read},
	}
	for name, scopes := range options.extraScopes {
		actors[name] = scopes
	}
	for name, scopes := range actors {
		w := domain.WorkspaceID("a")
		if name == "b" {
			w = "b"
		}
		sec, record, err := access.Issue(ctx, w, scopes, time.Time{})
		requireNoError(t, err)
		f.tokens[name], f.tokenIDs[name] = sec.Reveal(), record.ID
	}
	if options.profiles {
		f.profiles = configureJobProfiles(t, store, access)
	}
	digest := domain.SHA256Digest(strings.Repeat("a", 64))
	if options.code != nil {
		hash := sha256.Sum256(options.code)
		digest = domain.SHA256Digest(hex.EncodeToString(hash[:]))
	}
	if err = store.CommitObject(ctx, domain.ObjectMetadata{ID: "code", WorkspaceID: "a", Bytes: int64(len(options.code)), SHA256: digest}); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	requireNoError(t, err)
	cfg := api.DefaultConfig()
	cfg.Listen = ln.Addr().String()
	cfg.Jobs = svc
	if options.configure != nil {
		options.configure(&cfg, access, store)
	}
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

// Configure the same managed selection used in production; these tests grant no compute.
func configureJobProfiles(t *testing.T, store *sqlite.Store, access *auth.Service) map[string]string {
	t.Helper()
	ctx := context.Background()
	service, err := connections.New(access, store, &managedVault{values: map[string][]byte{}}, []connections.Adapter{&managedAdapter{}})
	requireNoError(t, err)
	profiles := map[string]string{}
	for _, workspace := range []domain.WorkspaceID{"a", "b"} {
		secret, token, err := access.Issue(ctx, workspace, []auth.Scope{auth.Read, auth.Manage}, time.Time{})
		requireNoError(t, err)
		principal, err := access.Authenticate(ctx, secret.Reveal())
		requireNoError(t, err)
		op, err := service.Submit(ctx, principal, workspace, "", "job-fixture-connection", []byte(managedBody))
		requireNoError(t, err)
		if worked, err := service.RunOnce(ctx); err != nil || !worked {
			t.Fatal("managed fixture verification", worked, err)
		}
		connection, err := service.Get(ctx, principal, workspace, op.ConnectionID)
		if err != nil || connection.Selection == nil {
			t.Fatal("managed fixture selection", err)
		}
		profiles[string(workspace)] = connection.Selection.Profile
		if err := store.RevokeToken(ctx, token.ID); err != nil {
			t.Fatal(err)
		}
	}
	return profiles
}

func (f *jobHTTP) job(workspace, body string) string {
	if profile := f.profiles[workspace]; profile != "" {
		return strings.ReplaceAll(body, `"profile":"default-gpu"`, `"profile":"`+profile+`"`)
	}
	return body
}

const jobBody = `{"api_version":"compute-connector/v1alpha1","name":"test","profile":"default-gpu","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","not-executed.py"]},"inputs":[],"outputs":[{"path":"result.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":120,"setup_seconds":30,"finalization_grace_seconds":15}}`

func (f *jobHTTP) call(t *testing.T, method, path, who, key, body string, want int) (http.Header, []byte) {
	t.Helper()
	parts := strings.Split(path, "/")
	if len(parts) > 3 {
		body = f.job(parts[3], body)
	}
	r, err := http.NewRequest(method, f.url+path, strings.NewReader(body))
	requireNoError(t, err)
	if who != "" {
		r.Header.Set("Authorization", "Bearer "+f.tokens[who])
	}
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(r)
	requireNoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	requireNoError(t, err)
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
	for _, file := range []string{"common", "workload-secrets", name} {
		b, err := schemas.Files.ReadFile(file + ".v1alpha1.schema.json")
		requireNoError(t, err)
		var value any
		if err = json.Unmarshal(b, &value); err != nil {
			t.Fatal(err)
		}
		if err = c.AddResource("https://schema.example/"+file+".v1alpha1.schema.json", value); err != nil {
			t.Fatal(err)
		}
	}
	schema, err := c.Compile("https://schema.example/" + name + ".v1alpha1.schema.json")
	requireNoError(t, err)
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
	first := decodeHTTPJSON[admission.Receipt](t, b)
	if first.Replay || headers.Get("Location") != first.Links.Self {
		t.Fatal("invalid new receipt")
	}
	_, b = f.call(t, "POST", path, "a", "request-0001", jobBody, 202)
	wireSchema(t, "job-admission", b) // A replay still reports status queued.
	replay := decodeHTTPJSON[admission.Receipt](t, b)
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
	r := httptest.NewRequest("POST", f.url+path, strings.NewReader(f.job("a", jobBody)))
	r.Header.Set("Authorization", "Bearer "+f.tokens["a"])
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "lost-http-receipt")
	broken := &lostAdmissionResponse{header: make(http.Header)}
	f.handler.ServeHTTP(broken, r)
	if broken.status != 202 {
		t.Fatalf("did not commit before response failure: %d", broken.status)
	}
	headers, b := f.call(t, "POST", path, "a", "lost-http-receipt", jobBody, 202)
	receipt := decodeHTTPJSON[admission.Receipt](t, b)
	if !receipt.Replay || headers.Get("Location") != broken.header.Get("Location") {
		t.Fatal("response failure created a second job")
	}
}

func TestHTTPSInputsFailAdmissionWithoutCreatingJobs(t *testing.T) {
	f := setupJobHTTP(t)
	var requests atomic.Int32
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Redirect(w, r, "https://127.0.0.1/private", http.StatusFound)
	}))
	defer redirect.Close()
	for _, source := range []string{"https://example.org/input", "https://127.0.0.1/private", redirect.URL} {
		input := `[{"name":"source","source":{"kind":"https","url":"` + source + `"},"target":"input"}]`
		body := strings.Replace(jobBody, `"inputs":[]`, `"inputs":`+input, 1)
		for _, path := range []string{"/v1/workspaces/a/jobs", "/v1/workspaces/a/jobs/validate"} {
			_, raw := f.call(t, "POST", path, "a", "https-rejected-1", body, 422)
			if !bytes.Contains(raw, []byte(domain.CodeUnsupportedCapability)) || !bytes.Contains(raw, []byte("upload inputs as objects")) {
				t.Fatalf("unclear rejection: %s", raw)
			}
		}
	}
	if requests.Load() != 0 {
		t.Fatal("rejected URL was dereferenced")
	}
	_, receipt := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "https-rejected-1", jobBody, 202)
	if bytes.Contains(receipt, []byte(`"idempotency_replay":true`)) {
		t.Fatal("rejected input created an admission receipt")
	}
}

func TestRemovedInputRoutesAreUnavailable(t *testing.T) {
	f := setupJobHTTP(t)
	for _, route := range []string{"import", "ingest"} {
		f.call(t, "POST", "/v1/workspaces/a/objects/"+route, "a", "", `{}`, 405)
	}
}

func requireNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
