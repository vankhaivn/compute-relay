package sqlite

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

func controlOperationSchema(t *testing.T) *jsonschema.Schema {
	return cleanupWireSchema(t, "control-operation.v1alpha1.schema.json")
}

func cleanupWireSchema(t *testing.T, name string) *jsonschema.Schema {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("source path unavailable")
	}
	path := filepath.ToSlash(filepath.Join(filepath.Dir(source), "..", "..", "..", "api", "schemas", name))
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	schema, err := c.Compile((&url.URL{Scheme: "file", Path: path}).String())
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

// serveCleanupFixture composes the real controls and cleanup admission over the fixture store.
func serveCleanupFixture(t *testing.T, f *dispatchFixture) (string, string) {
	t.Helper()
	access, err := auth.New(f.s, f.s)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := access.Issue(t.Context(), "a", []auth.Scope{auth.Read, auth.Operate}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	controls, err := operations.New(access, f.s, f.blobs, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	cleanups, err := cleanup.NewService(access, f.s, nil)
	if err != nil {
		t.Fatal(err)
	}
	cleanups = cleanups.WithPreviewResolver(cleanupResolver{f.adapter})
	inputs, err := objects.New(access, f.blobs, f.s)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := api.DefaultConfig()
	cfg.Listen, cfg.Operations, cfg.Cleanup = listener.Addr().String(), controls, cleanups
	server, err := api.NewServer(cfg, access, inputs, f.s.Ready)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	server.ErrorLog = log.New(io.Discard, "", 0)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return "http://" + listener.Addr().String(), secret.Reveal()
}

func TestCleanupHTTPPreviewUsesBoundedPublishedContract(t *testing.T) {
	f, id := collectedFixture(t)
	endpoint, token := serveCleanupFixture(t, f)
	path := endpoint + "/v1/workspaces/a/jobs/" + string(id.JobID) + "/cleanup"
	query := "?attempt_id=" + string(id.AttemptID)
	_, raw := cleanupHTTP(t, "GET", path+query, token, "", "", 200)
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	if err := cleanupWireSchema(t, "cleanup-preview.v1alpha1.schema.json").Validate(value); err != nil {
		t.Fatal("preview violates published contract", err, string(raw))
	}
	resources, ok := value["resources"].([]any)
	if !ok || len(resources) != 2 || value["eligible"] != true || controlCount(t, f.s, "SELECT count(*) FROM cleanup_operations") != 0 {
		t.Fatal("preview changed state or omitted candidates", string(raw))
	}
	for _, invalid := range []string{
		"", "?attempt_id=", "?attempt_id=a&attempt_id=b", query + "&discard_uncollected=1", query + "&discard_uncollected=true&discard_uncollected=false",
		query + "&remote_ref=foreign", query + "&discard_uncollected=", query + "&attempt_id=x", query + "&x=y;z",
	} {
		cleanupHTTP(t, "GET", path+invalid, token, "", "", 400)
	}
	cleanupHTTP(t, "POST", path+query, token, "query-not-permit", string(cleanupBody(id.AttemptID, false, "")), 400)
	cleanupHTTP(t, "GET", path+query, token, "", "{}", 400)
}

func cleanupHTTP(t *testing.T, method, endpoint, token, key, body string, want int) (http.Header, []byte) {
	t.Helper()
	r, err := http.NewRequest(method, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != want {
		t.Fatalf("%s %s status=%d want=%d: %s %v", method, endpoint, resp.StatusCode, want, raw, err)
	}
	return resp.Header, raw
}

func TestCleanupHTTPAdmitsAppliesAndReplaysWithStore(t *testing.T) {
	f, id := collectedFixture(t)
	endpoint, token := serveCleanupFixture(t, f)
	schema := controlOperationSchema(t)
	decode := func(raw []byte) map[string]any {
		t.Helper()
		var value map[string]any
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(any(value)); err != nil {
			t.Fatal("cleanup receipt violates the published contract", err, string(raw))
		}
		return value
	}
	path := endpoint + "/v1/workspaces/a/jobs/" + string(id.JobID) + "/cleanup"
	body := `{"attempt_id":"` + string(id.AttemptID) + `","reason":"private-reason-canary"}`
	headers, raw := cleanupHTTP(t, "POST", path, token, "cleanup-http", body, 202)
	accepted := decode(raw)
	if accepted["kind"] != "cleanup" || accepted["status"] != "accepted" || accepted["effect"] != "pending" || strings.Contains(string(raw), "private-reason-canary") {
		t.Fatal("unexpected cleanup acceptance", string(raw))
	}
	engine, _ := cleanupWorker(t, f)
	f.clock.advance(time.Second)
	runCleanup(t, engine)
	_, raw = cleanupHTTP(t, "GET", endpoint+headers.Get("Location"), token, "", "", 200)
	if current := decode(raw); current["status"] != "succeeded" || current["effect"] != "remote_deleted" || current["replay"] != false {
		t.Fatal("applied cleanup not readable", string(raw))
	}
	_, raw = cleanupHTTP(t, "POST", path, token, "cleanup-http", body, 202)
	if replay := decode(raw); replay["replay"] != true || replay["status"] != "accepted" || replay["operation_id"] != accepted["operation_id"] {
		t.Fatal("replay did not return the original receipt", string(raw))
	}
	_, raw = cleanupHTTP(t, "POST", path, token, "cleanup-http", `{"attempt_id":"`+string(id.AttemptID)+`"}`, 409)
	if !strings.Contains(string(raw), string(domain.CodeIdempotencyConflict)) {
		t.Fatal("changed payload did not conflict", string(raw))
	}
}
