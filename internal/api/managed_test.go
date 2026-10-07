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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

const managedSecret = "synthetic-managed-secret-canary"
const managedBody = `{"provider_type":"fixture","label":"Saved fixture","credentials":{"api_token":"` + managedSecret + `"}}`

type managedVault struct {
	mu     sync.Mutex
	values map[string][]byte
	fail   error
}

func (v *managedVault) Create(_ context.Context, key string, value []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.fail != nil {
		return v.fail
	}
	if prior, ok := v.values[key]; ok {
		if bytes.Equal(prior, value) {
			return nil
		}
		return credentials.ErrConflict
	}
	v.values[key] = append([]byte(nil), value...)
	return nil
}

func (v *managedVault) WithSecret(_ context.Context, key string, use func([]byte) error) error {
	v.mu.Lock()
	if v.fail != nil {
		failure := v.fail
		v.mu.Unlock()
		return failure
	}
	value, ok := v.values[key]
	owned := append([]byte(nil), value...)
	v.mu.Unlock()
	defer clear(owned)
	if !ok {
		return credentials.ErrUnconfigured
	}
	return use(owned)
}

func (v *managedVault) Delete(_ context.Context, key string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	clear(v.values[key])
	delete(v.values, key)
	return nil
}

type managedAdapter struct{ verifies atomic.Int32 }

func (*managedAdapter) Descriptor() connections.Descriptor {
	return connections.Descriptor{
		Type: "fixture", Label: "Fixture provider",
		Fields:       []connections.Field{{Name: "api_token", Label: "API token", Required: true, WriteOnly: true, MaxBytes: 8192}},
		Capabilities: connections.Capabilities{Accelerators: []string{"cpu", "gpu"}, RemoteCancel: "unsupported", Quota: "supported"},
	}
}
func (*managedAdapter) Encode(fields map[string]string) ([]byte, error) {
	return []byte(fields["api_token"]), nil
}
func (a *managedAdapter) Verify(context.Context, []byte) (connections.Verification, error) {
	a.verifies.Add(1)
	limit, used, remaining := float64(1800), float64(600), float64(1200)
	return connections.Verification{CanonicalAccount: "private-account-canary", Quota: provider.QuotaObservation{
		Status: provider.QuotaKnown, Resource: "gpu", Unit: "seconds", Limit: &limit, Used: &used, Remaining: &remaining,
		ObservedAt: time.Now().UTC(), Source: "fixture", Precision: "lower_bound",
	}}, nil
}
func (*managedAdapter) Profile(binding domain.ProviderBinding, account string) admission.Profile {
	return admission.DefaultProfile(binding, account)
}

type managedHTTP struct {
	*jobHTTP
	service *connections.Service
	access  *auth.Service
	vault   *managedVault
	adapter *managedAdapter
}

func setupManagedHTTP(t *testing.T, configured bool) *managedHTTP {
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
	jobs, err := admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	f := &managedHTTP{jobHTTP: &jobHTTP{store: store, tokens: map[string]string{}, tokenIDs: map[string]string{}}, access: access,
		vault: &managedVault{values: map[string][]byte{}}, adapter: &managedAdapter{}}
	for _, workspace := range []domain.WorkspaceID{"a", "b"} {
		if err = store.PutWorkspace(ctx, auth.Workspace{ID: workspace, Enabled: true, AllowedProfiles: []string{"default-gpu"}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, who := range []string{"a", "b", "read", "write", "operate", "manage", "execute"} {
		workspace := domain.WorkspaceID("a")
		scopes := []auth.Scope{auth.Read, auth.Write, auth.Operate, auth.Manage, auth.Execute}
		if who == "b" {
			workspace = "b"
		} else if who != "a" {
			scopes = []auth.Scope{auth.Scope(who)}
		}
		secret, record, err := access.Issue(ctx, workspace, scopes, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[who], f.tokenIDs[who] = secret.Reveal(), record.ID
	}
	if err = store.CommitObject(ctx, domain.ObjectMetadata{ID: "code", WorkspaceID: "a", Bytes: 0, SHA256: domain.SHA256Digest(strings.Repeat("a", 64))}); err != nil {
		t.Fatal(err)
	}
	f.service, err = connections.New(access, store, f.vault, []connections.Adapter{f.adapter}, nil)
	if err != nil {
		t.Fatal(err)
	}
	authorizations, err := executionauth.New(access, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := api.DefaultConfig()
	cfg.Listen, cfg.Jobs = ln.Addr().String(), jobs
	if configured {
		cfg.Connections, cfg.Authorizations = f.service, authorizations
	}
	cfg.WorkspaceRate = api.Rate{PerSecond: 10000, Burst: 10000}
	cfg.GlobalRate = cfg.WorkspaceRate
	server, err := api.NewServer(cfg, access, obj, store.Ready)
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}
	f.url, f.handler = "http://"+ln.Addr().String(), server.Handler
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(ln) }()
	t.Cleanup(func() { _ = server.Close(); <-done })
	return f
}

func managedSafeResponse(t *testing.T, raw []byte) {
	t.Helper()
	for _, canary := range []string{managedSecret, "private-account-canary", "vault:", "credential_ref", "key_sha256", "fingerprint", "actor_token_id"} {
		if bytes.Contains(raw, []byte(canary)) {
			t.Fatal("managed HTTP response exposed private state")
		}
	}
}

func readManagedOperation(t *testing.T, raw []byte) connections.Operation {
	t.Helper()
	wireSchema(t, "connection-operation", raw)
	managedSafeResponse(t, raw)
	var operation connections.Operation
	if err := json.Unmarshal(raw, &operation); err != nil {
		t.Fatal(err)
	}
	return operation
}

func TestManagedHTTPConnectionContractsReplayAndAuthority(t *testing.T) {
	f := setupManagedHTTP(t, true)
	base := "/v1/workspaces/a"
	_, raw := f.call(t, "GET", base+"/providers", "read", "", "", 200)
	wireSchema(t, "provider-descriptors", raw)
	managedSafeResponse(t, raw)
	_, raw = f.call(t, "GET", base+"/connections", "read", "", "", 200)
	wireSchema(t, "connection", raw)
	if string(bytes.TrimSpace(raw)) != `{"connections":[]}` {
		t.Fatal("empty connections response is not a schema array")
	}
	headers, raw := f.call(t, "POST", base+"/connections", "manage", "managed-create-01", managedBody, 202)
	first := readManagedOperation(t, raw)
	location := base + "/connection-operations/" + string(first.ID)
	if first.Replay || first.Action != "create" || headers.Get("Location") != location || f.adapter.verifies.Load() != 0 {
		t.Fatal("creation did not return a local durable receipt")
	}
	_, raw = f.call(t, "POST", base+"/connections", "manage", "managed-create-01", managedBody, 202)
	replay := readManagedOperation(t, raw)
	if !replay.Replay || replay.ID != first.ID || replay.ConnectionID != first.ConnectionID {
		t.Fatal("connection replay changed receipt identity")
	}
	_, raw = f.call(t, "GET", location, "manage", "", "", 200)
	readManagedOperation(t, raw)
	connectionPath := base + "/connections/" + first.ConnectionID
	_, raw = f.call(t, "GET", connectionPath, "read", "", "", 200)
	wireSchema(t, "connection", raw)
	managedSafeResponse(t, raw)
	_, raw = f.call(t, "GET", base+"/connections", "read", "", "", 200)
	wireSchema(t, "connection", raw)
	managedSafeResponse(t, raw)
	if f.adapter.verifies.Load() != 0 {
		t.Fatal("HTTP administration invoked provider verification")
	}
	for _, who := range []string{"read", "write", "operate", "execute"} {
		f.call(t, "POST", base+"/connections", who, "blocked-create-01", managedBody, 403)
		f.call(t, "POST", connectionPath+"/actions", who, "blocked-action-01", `{"action":"check","expected_revision":1}`, 403)
		f.call(t, "GET", location, who, "", "", 403)
	}
	for _, path := range []string{base + "/providers", base + "/connections", connectionPath} {
		f.call(t, "GET", path, "manage", "", "", 403)
		f.call(t, "GET", path, "execute", "", "", 403)
	}
	f.call(t, "POST", base+"/connections", "a", "managed-create-01", strings.Replace(managedBody, "Saved fixture", "Changed fixture", 1), 409)
	f.call(t, "GET", connectionPath, "b", "", "", 403)
	f.call(t, "GET", strings.Replace(connectionPath, "/a/", "/b/", 1), "b", "", "", 404)
	f.call(t, "GET", strings.Replace(location, "/a/", "/b/", 1), "b", "", "", 404)
	f.call(t, "GET", base+"/providers", "", "", "", 401)
	if err := f.store.RevokeToken(context.Background(), f.tokenIDs["manage"]); err != nil {
		t.Fatal(err)
	}
	f.call(t, "POST", base+"/connections", "manage", "managed-create-01", managedBody, 401)
}

func TestManagedHTTPGuardsAndUnavailableVault(t *testing.T) {
	f := setupManagedHTTP(t, true)
	path := "/v1/workspaces/a/connections"
	for _, tc := range []struct {
		name, body string
		mutate     func(*http.Request)
		want       int
	}{
		{"missing key", managedBody, func(r *http.Request) { r.Header.Del("Idempotency-Key") }, 400},
		{"duplicate key", managedBody, func(r *http.Request) { r.Header.Add("Idempotency-Key", "another-key") }, 400},
		{"duplicate type", managedBody, func(r *http.Request) { r.Header.Add("Content-Type", "application/json") }, 415},
		{"encoded", managedBody, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, 415},
		{"wrong type", managedBody, func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, 415},
		{"wrong charset", managedBody, func(r *http.Request) { r.Header.Set("Content-Type", "application/json; charset=latin1") }, 415},
		{"duplicate JSON", `{"provider_type":"fixture","provider_type":"fixture","label":"Saved","credentials":{"api_token":"` + managedSecret + `"}}`, nil, 400},
		{"case alias", strings.Replace(managedBody, "provider_type", "Provider_Type", 1), nil, 400},
		{"unknown field", strings.Replace(managedBody, `"api_token"`, `"undeclared_token"`, 1), nil, 400},
		{"null credential", `{"provider_type":"fixture","label":"Saved","credentials":{"api_token":null}}`, nil, 400},
		{"declared limit", strings.Repeat(" ", connections.MaxRequestBytes+1), nil, 413},
		{"chunked limit", strings.Repeat(" ", connections.MaxRequestBytes+1), func(r *http.Request) { r.ContentLength = -1 }, 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", f.url+path, strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+f.tokens["manage"])
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "guarded-create")
			if tc.mutate != nil {
				tc.mutate(r)
			}
			response := httptest.NewRecorder()
			f.handler.ServeHTTP(response, r)
			if response.Code != tc.want {
				t.Fatalf("request guard returned %d, want %d", response.Code, tc.want)
			}
			wireSchema(t, "error", response.Body.Bytes())
			managedSafeResponse(t, response.Body.Bytes())
		})
	}
	_, raw := f.call(t, "GET", path, "read", "", "", 200)
	if string(bytes.TrimSpace(raw)) != `{"connections":[]}` || f.adapter.verifies.Load() != 0 {
		t.Fatal("rejected request produced connection work")
	}
	f.vault.mu.Lock()
	f.vault.fail = errors.New("synthetic-vault-diagnostic-canary")
	f.vault.mu.Unlock()
	_, raw = f.call(t, "POST", path, "manage", "unavailable-vault", managedBody, 503)
	if bytes.Contains(raw, []byte("synthetic-vault-diagnostic-canary")) {
		t.Fatal("raw credential-store error escaped")
	}
	managedSafeResponse(t, raw)
}

func TestManagedHTTPMethodAndCompositionBoundaries(t *testing.T) {
	for _, configured := range []bool{false, true} {
		f := setupManagedHTTP(t, configured)
		for _, tc := range []struct{ method, path, allow string }{
			{"POST", "/providers", "GET"},
			{"DELETE", "/connections", "GET, POST"},
			{"POST", "/connections/con_fixture", "GET"},
			{"GET", "/connections/con_fixture/actions", "POST"},
			{"POST", "/connection-operations/op_fixture", "GET"},
			{"GET", "/jobs/job_fixture/authorize", "POST"},
			{"POST", "/jobs/job_fixture/authorizations/grant_fixture", "GET"},
		} {
			want := 404
			if configured {
				want = 405
			}
			headers, raw := f.call(t, tc.method, "/v1/workspaces/a"+tc.path, "a", "method-test-key", "{}", want)
			managedSafeResponse(t, raw)
			if configured && headers.Get("Allow") != tc.allow {
				t.Fatal("managed route omitted its allowed method")
			}
		}
		_, raw := f.call(t, "GET", "/v1/info", "a", "", "", 200)
		for _, feature := range []string{"managed_connections", "attempt_authorization"} {
			if bytes.Contains(raw, []byte(feature)) != configured {
				t.Fatal("feature advertisement does not match runtime composition")
			}
		}
	}
}

type observedManagedBody struct {
	value    []byte
	observed []byte
	err      error
}

func (b *observedManagedBody) Read(target []byte) (int, error) {
	if len(b.value) == 0 {
		return 0, io.EOF
	}
	n := copy(target, b.value)
	b.observed = target[:n]
	b.value = b.value[n:]
	return n, b.err
}
func (*observedManagedBody) Close() error { return nil }

func TestManagedHTTPClearsOwnedCredentialRequestBuffers(t *testing.T) {
	f := setupManagedHTTP(t, true)
	for _, readError := range []error{nil, io.ErrUnexpectedEOF} {
		body := &observedManagedBody{value: []byte(managedBody), err: readError}
		r := httptest.NewRequest("POST", f.url+"/v1/workspaces/a/connections", nil)
		r.Body = body
		r.ContentLength = -1
		r.Header.Set("Authorization", "Bearer "+f.tokens["manage"])
		r.Header.Set("Content-Type", "application/json; charset=UTF-8")
		r.Header.Set("Idempotency-Key", "buffer-clear-request")
		response := httptest.NewRecorder()
		f.handler.ServeHTTP(response, r)
		want := 202
		if readError != nil {
			want = 400
		}
		if response.Code != want || len(body.observed) == 0 || !bytes.Equal(body.observed, make([]byte, len(body.observed))) {
			t.Fatal("credential request buffer survived handler return")
		}
		managedSafeResponse(t, response.Body.Bytes())
	}
}

func verifiedManagedConnection(t *testing.T, f *managedHTTP) connections.Connection {
	t.Helper()
	_, raw := f.call(t, "POST", "/v1/workspaces/a/connections", "manage", "verified-create-01", managedBody, 202)
	operation := readManagedOperation(t, raw)
	if handled, err := f.service.RunOnce(context.Background()); err != nil || !handled {
		t.Fatal("fixture connection worker did not finish verification", err)
	}
	_, raw = f.call(t, "GET", "/v1/workspaces/a/connections/"+operation.ConnectionID, "read", "", "", 200)
	wireSchema(t, "connection", raw)
	managedSafeResponse(t, raw)
	var connection connections.Connection
	if err := json.Unmarshal(raw, &connection); err != nil {
		t.Fatal(err)
	}
	if connection.Authentication != "verified" || connection.Selection == nil || !connection.CredentialPresent {
		t.Fatal("fixture connection has no verified immutable selection")
	}
	return connection
}

func TestManagedHTTPConnectionActionContracts(t *testing.T) {
	f := setupManagedHTTP(t, true)
	connection := verifiedManagedConnection(t, f)
	path := "/v1/workspaces/a/connections/" + connection.ID
	action, err := json.Marshal(map[string]any{"action": "disable", "expected_revision": connection.Revision})
	if err != nil {
		t.Fatal(err)
	}
	before := f.adapter.verifies.Load()
	headers, raw := f.call(t, "POST", path+"/actions", "manage", "disable-connection-01", string(action), 202)
	first := readManagedOperation(t, raw)
	if first.Action != "disable" || first.ConnectionID != connection.ID || first.ConnectionRevision <= connection.Revision || headers.Get("Location") == "" {
		t.Fatal("connection action lost target revision or Location")
	}
	if handled, err := f.service.RunOnce(context.Background()); err != nil || !handled {
		t.Fatal("fixture connection worker did not apply disable", err)
	}
	_, raw = f.call(t, "GET", headers.Get("Location"), "manage", "", "", 200)
	completed := readManagedOperation(t, raw)
	if completed.Status != "succeeded" {
		t.Fatal("action status did not report completion")
	}
	_, raw = f.call(t, "GET", path, "read", "", "", 200)
	wireSchema(t, "connection", raw)
	managedSafeResponse(t, raw)
	var disabled connections.Connection
	if err := json.Unmarshal(raw, &disabled); err != nil || disabled.NewWork != "disabled" || disabled.Selection != nil || !disabled.CredentialPresent {
		t.Fatal("disabled connection projection violates its contract")
	}
	_, raw = f.call(t, "POST", path+"/actions", "manage", "disable-connection-01", string(action), 202)
	replay := readManagedOperation(t, raw)
	if !replay.Replay || replay.ID != first.ID || replay.Status != first.Status {
		t.Fatal("action replay did not preserve the original receipt")
	}
	f.call(t, "POST", path+"/actions", "manage", "stale-action-01", string(action), 409)
	if f.adapter.verifies.Load() != before {
		t.Fatal("disable or HTTP reads unexpectedly verified the provider")
	}
}

func TestManagedHTTPAuthorizationContractsAndIndependentScope(t *testing.T) {
	f := setupManagedHTTP(t, true)
	connection := verifiedManagedConnection(t, f)
	body := strings.Replace(jobBody, "default-gpu", connection.Selection.Profile, 1)
	_, raw := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "managed-job-01", body, 202)
	var job admission.Receipt
	if err := json.Unmarshal(raw, &job); err != nil {
		t.Fatal(err)
	}
	request := `{"attempt_id":"` + string(job.AttemptID) + `","max_remote_wall_seconds":120,"authorize_private_staging":true,"authorize_compute":true}`
	path := job.Links.Self + "/authorize"
	for _, who := range []string{"read", "write", "operate", "manage"} {
		f.call(t, "POST", path, who, "managed-grant-01", request, 403)
	}
	before := f.adapter.verifies.Load()
	headers, raw := f.call(t, "POST", path, "execute", "managed-grant-01", request, 202)
	wireSchema(t, "execution-authorization", raw)
	managedSafeResponse(t, raw)
	var first executionauth.Receipt
	if err := json.Unmarshal(raw, &first); err != nil {
		t.Fatal(err)
	}
	location := job.Links.Self + "/authorizations/" + string(first.AuthorizationID)
	if first.Status != executionauth.Granted || first.Replay || first.AttemptID != job.AttemptID || headers.Get("Location") != location || first.ConsumedAt != nil {
		t.Fatal("authorization receipt lost its finite frozen target")
	}
	_, raw = f.call(t, "GET", location, "execute", "", "", 200)
	wireSchema(t, "execution-authorization", raw)
	managedSafeResponse(t, raw)
	_, raw = f.call(t, "POST", path, "execute", "managed-grant-01", request, 202)
	var replay executionauth.Receipt
	if err := json.Unmarshal(raw, &replay); err != nil || !replay.Replay || replay.AuthorizationID != first.AuthorizationID {
		t.Fatal("authorization replay created another grant")
	}
	wireSchema(t, "execution-authorization", raw)
	for _, who := range []string{"read", "write", "operate", "manage"} {
		f.call(t, "GET", location, who, "", "", 403)
	}
	f.call(t, "GET", location, "b", "", "", 403)
	f.call(t, "GET", strings.Replace(location, "/a/", "/b/", 1), "b", "", "", 404)
	f.call(t, "POST", strings.Replace(path, "/a/", "/b/", 1), "b", "cross-workspace-grant", request, 404)
	f.call(t, "POST", path, "execute", "new-key-same-attempt", request, 409)
	f.call(t, "POST", path, "execute", "managed-grant-01", strings.Replace(request, ":120", ":121", 1), 409)
	for _, invalid := range []string{
		strings.Replace(request, ":120", ":121", 1),
		strings.Replace(request, `"authorize_compute":true`, `"authorize_compute":false`, 1),
		strings.Replace(request, `"authorize_compute":true`, `"authorize_compute":null`, 1),
		strings.Replace(request, `"attempt_id":`, `"Attempt_ID":`, 1),
		strings.Replace(request, `"authorize_compute":true`, `"authorize_compute":true,"authorize_compute":true`, 1),
	} {
		f.call(t, "POST", path, "execute", "invalid-authorization", invalid, 400)
	}
	f.call(t, "POST", path, "execute", "oversize-authorization", strings.Repeat(" ", executionauth.MaxRequestBytes+1), 413)
	if f.adapter.verifies.Load() != before {
		t.Fatal("authorization HTTP work called a provider")
	}
	if err := f.store.RevokeToken(context.Background(), f.tokenIDs["execute"]); err != nil {
		t.Fatal(err)
	}
	f.call(t, "POST", path, "execute", "managed-grant-01", request, 401)
}
