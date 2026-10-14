package api_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// Frozen consumer projections from Studio's compute/{client,contracts}.py and
// projects/remote_render/contracts.py. Extra fields are accepted like Projection;
// required fields and literal values are checked independently of Relay schemas.
func studioFields(t *testing.T, m map[string]any, fields map[string]string) {
	t.Helper()
	for key, rule := range fields {
		value, exists := m[key]
		if !exists {
			t.Fatalf("Studio requires field %q: %v", key, m)
		}
		if strings.HasPrefix(rule, "?") {
			if value == nil {
				continue
			}
			rule = rule[1:]
		}
		valid := false
		switch rule {
		case "id":
			s, ok := value.(string)
			valid = ok && regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`).MatchString(s)
		case "profile":
			s, ok := value.(string)
			valid = ok && regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,127}$`).MatchString(s)
		case "provider":
			s, ok := value.(string)
			valid = ok && regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`).MatchString(s)
		case "label":
			s, ok := value.(string)
			valid = ok && len(s) > 0 && len(s) <= 128
		case "str":
			_, valid = value.(string)
		case "bool":
			_, valid = value.(bool)
		case "int", "revision", "wall", "bytes":
			n, ok := value.(float64)
			valid = ok && n >= 0 && n == float64(int64(n))
			if rule == "revision" {
				valid = valid && n >= 1 && n <= 2147483647
			}
			if rule == "wall" {
				valid = valid && n >= 1 && n <= 86400
			}
			if rule == "bytes" {
				valid = valid && n <= 4*(1<<30)
			}
		case "sha":
			s, ok := value.(string)
			valid = ok && regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(s)
		case "date":
			s, ok := value.(string)
			_, err := time.Parse(time.RFC3339Nano, s)
			valid = ok && err == nil
		default:
			s, ok := value.(string)
			valid = ok && strings.Contains("|"+rule+"|", "|"+s+"|")
		}
		if !valid {
			t.Fatalf("Studio field %q=%v does not satisfy %s", key, value, rule)
		}
	}
}
func studioMap(t *testing.T, value any) map[string]any {
	t.Helper()
	m, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected object, got %T", value)
	}
	return m
}
func studioArray(t *testing.T, value any, max int) []any {
	t.Helper()
	a, ok := value.([]any)
	if !ok || len(a) > max {
		t.Fatalf("expected array bounded by %d: %v", max, value)
	}
	return a
}
func studioConnection(t *testing.T, m map[string]any) {
	t.Helper()
	studioFields(t, m, map[string]string{"connection_id": "id", "workspace_id": "id", "revision": "revision", "provider_type": "label", "label": "label", "authentication": "pending|verified|rejected|unavailable", "credential_present": "bool", "new_work": "enabled|disabled|removed", "account_id": "?id", "active_attempts": "int", "updated_at": "date"})
	selection, ok := m["selection"]
	if !ok {
		t.Fatal("missing nullable selection")
	}
	if selection != nil {
		s := studioMap(t, selection)
		studioFields(t, s, map[string]string{"profile": "profile", "configuration_revision": "id", "max_remote_wall_seconds": "wall", "allow_remote_internet": "bool"})
		if a, ok := s["accelerator"]; ok && a != nil {
			studioFields(t, s, map[string]string{"accelerator": "cpu|gpu|tpu"})
		}
	}
	if c := m["configuration"]; c != nil {
		studioFields(t, studioMap(t, c), map[string]string{"max_remote_wall_seconds": "wall"})
	}
	for _, v := range studioArray(t, m["quotas"], 16) {
		studioFields(t, studioMap(t, v), map[string]string{"status": "known|unknown|stale", "resource": "cpu|gpu|tpu", "unit": "seconds", "remaining": "?int", "observed_at": "?date", "precision": "lower_bound|unknown"})
		q := studioMap(t, v)
		for _, key := range []string{"limit", "used", "local_reserved"} {
			if v, ok := q[key]; ok && v != nil {
				studioFields(t, q, map[string]string{key: "int"})
			}
		}
		if v, ok := q["reset_at"]; ok && v != nil {
			studioFields(t, q, map[string]string{"reset_at": "date"})
		}
	}
}
func studioConnectionOperation(t *testing.T, m map[string]any) {
	t.Helper()
	studioFields(t, m, map[string]string{"operation_id": "id", "workspace_id": "id", "connection_id": "id", "action": "create|check|replace_credential|disable|enable|remove|configure", "status": "accepted|running|succeeded|failed", "connection_revision": "revision", "created_at": "date", "updated_at": "date", "problem": "?credential_rejected|credential_store_unavailable|provider_unavailable|account_changed|active_work|stale_revision|unsupported_provider", "replay": "bool"})
}
func studioControl(t *testing.T, m map[string]any) {
	t.Helper()
	studioFields(t, m, map[string]string{"operation_id": "id", "workspace_id": "id", "job_id": "id", "attempt_id": "id", "kind": "cancel|collect|reconcile|retry_compute", "status": "accepted|running|succeeded|failed|manual_required", "effect": "str", "remote_termination_confirmed": "bool"})
	effect := m["effect"].(string)
	if len(effect) == 0 || len(effect) > 64 {
		t.Fatal("unbounded control effect")
	}
}
func studioStatus(t *testing.T, m map[string]any) {
	t.Helper()
	studioFields(t, m, map[string]string{"api_version": "compute-connector/v1alpha1", "workspace_id": "id", "job_id": "id", "active_attempt_id": "id", "revision": "revision"})
	studioFields(t, studioMap(t, m["state"]), map[string]string{"orchestration": "queued|preparing|dispatching|submitted|running|collecting|blocked|reconciling|cancelling|needs_attention|succeeded|failed|cancelled|timed_out", "execution": "not_submitted|unknown|queued|starting|running|succeeded|failed|cancelled|timed_out", "result": "not_available|collecting|available|incomplete|invalid|expired", "cancellation": "not_requested|requested|requesting|accepted|prevented|confirmed|too_late|manual_required", "remote_activity": "not_started|unknown|possible|active|inactive", "release_evidence": "unknown|not_applicable|not_observable|provider_reported|confirmed", "deadline_exceeded": "bool"})
	if c := m["collection"]; c != nil {
		c := studioMap(t, c)
		studioFields(t, c, map[string]string{"mode": "automatic|manual", "state": "not_ready|awaiting_request|pending|discovering|transferring|verifying|available|failed|expired|unknown", "attempt_id": "id"})
		if v := c["operation_id"]; v != nil {
			studioFields(t, c, map[string]string{"operation_id": "id"})
		}
		if p := c["progress"]; p != nil {
			studioProgress(t, studioMap(t, p), "selected_output_bytes", false)
		}
	}
	if p := m["preparation"]; p != nil {
		if progress := studioMap(t, p)["progress"]; progress != nil {
			studioProgress(t, studioMap(t, progress), "staged_input_bytes", true)
		}
	}
	if p := m["problem"]; p != nil {
		if d := studioMap(t, p)["details"]; d != nil {
			if r := studioMap(t, d)["reason"]; r != nil {
				s, ok := r.(string)
				if !ok || !regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`).MatchString(s) {
					t.Fatal("invalid Studio problem reason")
				}
			}
		}
	}
}
func studioProgress(t *testing.T, p map[string]any, scope string, requiredTotal bool) {
	t.Helper()
	studioFields(t, p, map[string]string{"scope": scope, "generation": "revision", "bytes_completed": "int", "observed_at": "date"})
	max := float64(4 * (1 << 30))
	if requiredTotal {
		max = 5 * (1 << 30)
		studioFields(t, p, map[string]string{"bytes_total": "int"})
	}
	if p["bytes_completed"].(float64) > max {
		t.Fatal("unbounded progress")
	}
	for _, key := range []string{"bytes_total", "bytes_received"} {
		if v := p[key]; v != nil {
			studioFields(t, p, map[string]string{key: "int"})
			if v.(float64) > max {
				t.Fatal("unbounded progress")
			}
		}
	}
	if total := p["bytes_total"]; total != nil && p["bytes_completed"].(float64) > total.(float64) {
		t.Fatal("completed progress exceeds total")
	}
}
func studioArtifact(t *testing.T, m map[string]any) {
	t.Helper()
	studioFields(t, m, map[string]string{"artifact_id": "id", "path": "str", "role": "manifest|log|provenance|output", "bytes": "bytes", "sha256": "sha"})
	if !regexp.MustCompile(`^art_[A-Za-z0-9_.-]{1,123}$`).MatchString(m["artifact_id"].(string)) || len(m["path"].(string)) < 1 || len(m["path"].(string)) > 1024 {
		t.Fatal("invalid Studio artifact identity/path")
	}
}

type studioClock struct{}

func (studioClock) Now() time.Time { return time.Now().UTC() }

// Adapt only the fake's fixture artifact namespace to the runner namespace. The
// manifest, identity, digests and bytes still come from the fake provider.
type studioProvider struct {
	*fake.Complete
	binding provider.BindingSnapshot
	remote  *provider.RemoteReference
}

func (p *studioProvider) VerifyBinding(_ context.Context, b provider.BindingSnapshot) error {
	if b != p.binding {
		return fmt.Errorf("fixture binding changed")
	}
	return nil
}
func (p *studioProvider) Submit(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
	out := p.Complete.Submit(ctx, prepared)
	p.remote = out.Remote
	return out
}
func (p *studioProvider) ListArtifacts(ctx context.Context, r provider.RemoteReference, q provider.PageRequest) (provider.ArtifactPage, error) {
	page, err := p.Complete.ListArtifacts(ctx, r, q)
	for i := range page.Artifacts {
		if page.Artifacts[i].Path == "execution-result.json" {
			page.Artifacts[i].Path = collection.ManifestPath
		} else {
			page.Artifacts[i].Path = "outputs/" + page.Artifacts[i].Path
		}
	}
	return page, err
}
func (p *studioProvider) FetchArtifact(ctx context.Context, r provider.RemoteReference, a provider.Artifact, w io.Writer, max int64) (provider.TransferResult, error) {
	if a.Path == collection.ManifestPath {
		a.Path = "execution-result.json"
	} else {
		a.Path = strings.TrimPrefix(a.Path, "outputs/")
	}
	return p.Complete.FetchArtifact(ctx, r, a, w, max)
}

type studioHTTP struct {
	url, token string
	store      *sqlite.Store
	service    *connections.Service
	registry   *provider.SnapshotRegistry
	blobs      *blobfs.Store
}

func newStudioHTTP(t *testing.T, override func(*sqlite.Store) admission.Repository) *studioHTTP {
	t.Helper()
	ctx := context.Background()
	s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "state"), sqlite.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	access, err := auth.New(s, s)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.PutWorkspace(ctx, auth.Workspace{ID: "studio", Enabled: true, AllowedProfiles: []string{"default-gpu"}}); err != nil {
		t.Fatal(err)
	}
	sec, _, err := access.Issue(ctx, "studio", []auth.Scope{auth.Read, auth.Write, auth.Operate, auth.Manage, auth.Execute}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	obj, err := objects.New(access, blobs, s)
	if err != nil {
		t.Fatal(err)
	}
	var repo admission.Repository = s
	if override != nil {
		repo = override(s)
	}
	jobs, err := admission.New(access, repo, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	service, err := connections.New(access, s, &managedVault{values: map[string][]byte{}}, []connections.Adapter{&managedAdapter{}})
	if err != nil {
		t.Fatal(err)
	}
	permits, err := executionauth.New(access, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	controls, err := operations.New(access, s, blobs, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	registry := provider.NewSnapshotRegistry()
	logs, err := joblogs.NewReader(access, s, registry)
	if err != nil {
		t.Fatal(err)
	}
	results, err := collection.NewReader(access, s, blobs)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := api.DefaultConfig()
	cfg.Listen = ln.Addr().String()
	cfg.Jobs = jobs
	cfg.Connections = service
	cfg.Authorizations = permits
	cfg.Operations = controls
	cfg.Logs = logs
	cfg.Results = results
	cfg.GlobalRate = api.Rate{PerSecond: 10000, Burst: 10000}
	cfg.WorkspaceRate = cfg.GlobalRate
	server, err := api.NewServer(cfg, access, obj, s.Ready)
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(ln) }()
	t.Cleanup(func() { _ = server.Close(); <-done })
	return &studioHTTP{url: "http://" + ln.Addr().String(), token: sec.Reveal(), store: s, service: service, registry: registry, blobs: blobs}
}
func (f *studioHTTP) request(t *testing.T, method, path, key string, body []byte, contentType string, want int) (*http.Response, []byte) {
	t.Helper()
	r, err := http.NewRequest(method, f.url+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+f.token)
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	if contentType == "application/octet-stream" {
		r.Header.Set("X-Content-SHA256", string(provider.Digest(body)))
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != want {
		t.Fatalf("%s %s: status %d want %d: %s", method, path, resp.StatusCode, want, raw)
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("missing no-store")
	}
	if bytes.Contains(raw, []byte(f.token)) || bytes.Contains(raw, []byte(managedSecret)) {
		t.Fatal("credential in wire response")
	}
	return resp, raw
}
func (f *studioHTTP) json(t *testing.T, method, path, key string, body any, want int) map[string]any {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	resp, b := f.request(t, method, path, key, raw, "application/json", want)
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatal("Studio rejects non-JSON response")
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil || m == nil {
		t.Fatalf("invalid JSON: %s", b)
	}
	return m
}
func studioBundle(t *testing.T) []byte {
	t.Helper()
	data := []byte("raise SystemExit('offline fixture must never execute')\n")
	m, err := json.Marshal(packaging.Manifest{Version: packaging.Version, Files: []packaging.File{{Path: "never-executed.py", Bytes: int64(len(data)), SHA256: string(provider.Digest(data))}}})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, entry := range []struct {
		name string
		data []byte
	}{{packaging.ManifestPath, m}, {"code/never-executed.py", data}} {
		if err := tw.WriteHeader(&tar.Header{Name: entry.name, Mode: 0644, Size: int64(len(entry.data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestStudioHTTPContract(t *testing.T) {
	f := newStudioHTTP(t, nil)
	ctx := context.Background()
	base := "/v1/workspaces/studio"
	ready := f.json(t, "GET", "/readyz", "", nil, 200)
	if len(ready) != 1 || ready["status"] != "ready" {
		t.Fatal("Studio readiness requires exactly ready", ready)
	}
	info := f.json(t, "GET", "/v1/info", "", nil, 200)
	studioFields(t, info, map[string]string{"api_version": "compute-connector/v1alpha1", "runtime_version": "label"})
	features := studioArray(t, info["features"], 32)
	seen := map[string]bool{}
	for _, v := range features {
		s, ok := v.(string)
		if !ok {
			t.Fatal("non-string feature")
		}
		seen[s] = true
	}
	for _, feature := range []string{"workspace_auth", "job_admission", "managed_connections", "attempt_authorization"} {
		if !seen[feature] {
			t.Fatal("missing required Studio feature", feature)
		}
	}
	descriptors := f.json(t, "GET", base+"/providers", "", nil, 200)
	ps := studioArray(t, descriptors["providers"], 32)
	if len(ps) != 1 {
		t.Fatal("missing fixture descriptor")
	}
	for _, value := range ps {
		p := studioMap(t, value)
		studioFields(t, p, map[string]string{"type": "provider", "label": "label", "credential_storage": "available|unavailable|unsupported"})
		fields := studioArray(t, p["credential_fields"], 16)
		if len(fields) == 0 {
			t.Fatal("missing credential fields")
		}
		for _, value := range fields {
			field := studioMap(t, value)
			studioFields(t, field, map[string]string{"name": "str", "label": "label", "required": "bool", "write_only": "bool", "max_bytes": "int"})
			if !regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`).MatchString(field["name"].(string)) || field["write_only"] != true || field["max_bytes"].(float64) < 1 || field["max_bytes"].(float64) > 16384 {
				t.Fatal("invalid credential field")
			}
		}
		cap := studioMap(t, p["capabilities"])
		studioFields(t, cap, map[string]string{"remote_cancel": "supported|unsupported|unknown", "quota": "supported|unsupported|unknown"})
		for _, a := range studioArray(t, cap["accelerators"], 8) {
			studioFields(t, map[string]any{"accelerator": a}, map[string]string{"accelerator": "cpu|gpu|tpu"})
		}
	}
	if len(studioArray(t, f.json(t, "GET", base+"/connections", "", nil, 200)["connections"], 100)) != 0 {
		t.Fatal("unexpected initial connections")
	}
	create := map[string]any{"provider_type": "fixture", "label": "Studio fixture", "credentials": map[string]string{"api_token": managedSecret}}
	op := f.json(t, "POST", base+"/connections", "studio-create", create, 202)
	studioConnectionOperation(t, op)
	if op["status"] != "accepted" || op["replay"] != false {
		t.Fatal("creation receipt changed")
	}
	id := op["connection_id"].(string)
	opPath := base + "/connection-operations/" + op["operation_id"].(string)
	replay := f.json(t, "POST", base+"/connections", "studio-create", create, 202)
	studioConnectionOperation(t, replay)
	if replay["replay"] != true || replay["operation_id"] != op["operation_id"] {
		t.Fatal("connection replay lost identity")
	}
	if worked, err := f.service.RunOnce(ctx); err != nil || !worked {
		t.Fatal("connection verification", worked, err)
	}
	completed := f.json(t, "GET", opPath, "", nil, 200)
	studioConnectionOperation(t, completed)
	if completed["status"] != "succeeded" {
		t.Fatal("verification did not complete")
	}
	path := base + "/connections/" + id
	c := f.json(t, "GET", path, "", nil, 200)
	studioConnection(t, c)
	if c["authentication"] != "verified" || c["credential_present"] != true {
		t.Fatal("not verified")
	}
	for _, v := range studioArray(t, f.json(t, "GET", base+"/connections", "", nil, 200)["connections"], 100) {
		studioConnection(t, studioMap(t, v))
	}
	action := map[string]any{"action": "check", "expected_revision": c["revision"]}
	op = f.json(t, "POST", path+"/actions", "studio-check", action, 202)
	studioConnectionOperation(t, op)
	if worked, err := f.service.RunOnce(ctx); err != nil || !worked {
		t.Fatal("connection check", worked, err)
	}
	studioConnectionOperation(t, f.json(t, "GET", base+"/connection-operations/"+op["operation_id"].(string), "", nil, 200))
	c = f.json(t, "GET", path, "", nil, 200)
	studioConnection(t, c)
	payload := studioBundle(t)
	_, raw := f.request(t, "POST", base+"/objects", "", payload, "application/octet-stream", 201)
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	studioFields(t, object, map[string]string{"object_id": "id", "workspace_id": "id", "bytes": "int", "sha256": "sha"})
	if object["workspace_id"] != "studio" || !regexp.MustCompile(`^obj_[A-Za-z0-9_.:-]{1,124}$`).MatchString(object["object_id"].(string)) || object["sha256"] != string(provider.Digest(payload)) || object["bytes"] != float64(len(payload)) {
		t.Fatal("upload receipt lost verified bytes")
	}
	selection := studioMap(t, c["selection"])
	profile, enabled, err := f.store.ReadProfile(ctx, selection["profile"].(string))
	if err != nil || !enabled {
		t.Fatal("frozen profile", err)
	}
	binding := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
	backend, err := fake.NewBackend(fake.DefaultScenario())
	if err != nil {
		t.Fatal(err)
	}
	complete, err := fake.NewComplete(backend, studioClock{}, profile.Binding.ProviderInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	p := &studioProvider{Complete: complete, binding: binding}
	if err = f.registry.Register(binding, p); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
		t.Fatal(err)
	}
	dispatchConfig := dispatch.DefaultConfig()
	dispatchConfig.PollDelay = time.Millisecond
	engine, err := dispatch.New(f.store, f.registry, f.blobs, studioClock{}, dispatchConfig)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := collection.New(f.store, f.registry, f.blobs, studioClock{}, collection.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	spec := map[string]any{"api_version": "compute-connector/v1alpha1", "name": "Studio contract", "profile": selection["profile"], "result_collection": "manual", "bundle": map[string]any{"object_id": object["object_id"]}, "execution": map[string]any{"kind": "python", "command": []string{"python", "never-executed.py"}}, "inputs": []any{}, "outputs": []any{map[string]any{"path": "result.json", "required": true}}, "resources": map[string]any{"accelerator": "cpu"}, "network": map[string]any{"remote_internet": "disabled"}, "timeouts": map[string]any{"remote_wall_seconds": 120, "setup_seconds": 30, "finalization_grace_seconds": 15}}
	job := f.json(t, "POST", base+"/jobs", "studio-admit", spec, 202)
	studioFields(t, job, map[string]string{"job_id": "id", "attempt_id": "id", "status": "queued", "idempotency_replay": "bool"})
	if job["idempotency_replay"] != false {
		t.Fatal("first admission is replay")
	}
	same := f.json(t, "POST", base+"/jobs", "studio-admit", spec, 202)
	if same["idempotency_replay"] != true || same["job_id"] != job["job_id"] || same["attempt_id"] != job["attempt_id"] {
		t.Fatal("admission replay changed target")
	}
	jobPath := base + "/jobs/" + job["job_id"].(string)
	attempt := job["attempt_id"].(string)
	state := f.json(t, "GET", jobPath, "", nil, 200)
	studioStatus(t, state)
	if studioMap(t, state["state"])["execution"] != "not_submitted" {
		t.Fatal("admission caused execution")
	}
	permit := f.json(t, "POST", jobPath+"/authorize", "studio-authorize", map[string]any{"attempt_id": attempt, "max_remote_wall_seconds": 120, "authorize_private_staging": true, "authorize_compute": true}, 202)
	studioFields(t, permit, map[string]string{"authorization_id": "id", "workspace_id": "id", "job_id": "id", "attempt_id": "id", "status": "granted|consumed", "max_remote_wall_seconds": "wall"})
	if permit["status"] != "granted" || permit["job_id"] != job["job_id"] || permit["attempt_id"] != attempt {
		t.Fatal("grant not frozen to admitted attempt")
	}
	step := func() {
		t.Helper()
		if worked, err := engine.RunOnce(ctx, "studio_contract_worker"); err != nil || !worked {
			t.Fatal("dispatch did not progress", worked, err)
		}
	}
	step()
	time.Sleep(2 * time.Millisecond)
	step()
	if p.remote == nil {
		t.Fatal("fake submission not reached")
	}
	control := map[string]any{"attempt_id": attempt, "reason": "explicit Studio contract action"}
	for i := 0; i < 2; i++ {
		if err := p.Advance(*p.remote); err != nil {
			t.Fatal(err)
		}
		op := f.json(t, "POST", jobPath+"/reconcile", fmt.Sprintf("studio-observe-%d", i), control, 202)
		studioControl(t, op)
		step()
		state = f.json(t, "GET", jobPath, "", nil, 200)
		studioStatus(t, state)
	}
	if studioMap(t, state["state"])["execution"] != "succeeded" || studioMap(t, state["state"])["release_evidence"] != "not_observable" {
		t.Fatal("terminal execution changed release evidence", state)
	}
	logs := f.json(t, "GET", jobPath+"/logs?attempt_id="+attempt+"&limit=1", "", nil, 200)
	studioFields(t, logs, map[string]string{"api_version": "compute-connector/v1alpha1", "workspace_id": "id", "job_id": "id", "attempt_id": "id", "source": "provider", "availability": "after_completion", "next_cursor": "str", "truncated": "bool"})
	if len(studioArray(t, logs["lines"], 1)) != 1 || logs["next_cursor"] == "" {
		t.Fatal("bounded logs/cursor missing")
	}
	for _, line := range studioArray(t, logs["lines"], 1) {
		if _, ok := line.(string); !ok {
			t.Fatal("invalid log line")
		}
	}
	lastLogs := f.json(t, "GET", jobPath+"/logs?attempt_id="+attempt+"&limit=1&cursor="+logs["next_cursor"].(string), "", nil, 200)
	if lastLogs["next_cursor"] != "" || len(studioArray(t, lastLogs["lines"], 1)) != 1 {
		t.Fatal("log cursor failed")
	}
	op = f.json(t, "POST", jobPath+"/collect", "studio-collect", control, 202)
	studioControl(t, op)
	if worked, err := collector.RunOnce(ctx); err != nil || !worked {
		t.Fatal("collection", worked, err)
	}
	done := f.json(t, "GET", base+"/operations/"+op["operation_id"].(string), "", nil, 200)
	studioControl(t, done)
	if done["kind"] != "collect" || done["status"] != "succeeded" || done["remote_termination_confirmed"] != false {
		t.Fatal("collection receipt changed evidence")
	}
	state = f.json(t, "GET", jobPath, "", nil, 200)
	studioStatus(t, state)
	if studioMap(t, state["state"])["result"] != "available" || studioMap(t, state["collection"])["state"] != "available" {
		t.Fatal("verified result not available")
	}
	artifactPath := jobPath + "/artifacts"
	cursor := ""
	count := 0
	var output map[string]any
	for {
		url := artifactPath + "?attempt_id=" + attempt + "&limit=1"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		page := f.json(t, "GET", url, "", nil, 200)
		studioFields(t, page, map[string]string{"workspace_id": "id", "job_id": "id", "attempt_id": "id", "result_phase": "str", "snapshot_sha256": "sha", "total_artifacts": "int", "next_cursor": "str"})
		if page["total_artifacts"].(float64) < 1 || page["total_artifacts"].(float64) > 10004 || page["result_phase"] != "completed" {
			t.Fatal("invalid artifact page")
		}
		a := studioArray(t, page["artifacts"], 1)
		if len(a) != 1 {
			t.Fatal("empty artifact page")
		}
		for _, v := range a {
			m := studioMap(t, v)
			studioArtifact(t, m)
			if m["role"] == "output" {
				output = m
			}
			count++
		}
		cursor = page["next_cursor"].(string)
		if cursor == "" {
			if count != int(page["total_artifacts"].(float64)) {
				t.Fatal("pagination lost artifacts")
			}
			break
		}
		if count > 10004 || !regexp.MustCompile(`^ar1_[0-9a-f]{64}_[1-9][0-9]{0,4}$`).MatchString(cursor) {
			t.Fatal("invalid/unbounded artifact cursor")
		}
	}
	if output == nil {
		t.Fatal("missing output")
	}
	meta := f.json(t, "GET", artifactPath+"/"+output["artifact_id"].(string)+"?attempt_id="+attempt, "", nil, 200)
	meta = studioMap(t, meta["artifact"])
	studioArtifact(t, meta)
	if meta["sha256"] != output["sha256"] || meta["bytes"] != output["bytes"] {
		t.Fatal("metadata differs from snapshot")
	}
	resp, data := f.request(t, "GET", artifactPath+"/"+output["artifact_id"].(string)+"/content?attempt_id="+attempt, "", nil, "", 200)
	expected := []byte("{\"fixture\":true}\n")
	if !bytes.Equal(data, expected) || string(provider.Digest(data)) != output["sha256"] || resp.Trailer.Get("X-Compute-Relay-Verified") != "true" {
		t.Fatal("artifact download not verified")
	}
	for key, want := range map[string]string{"Content-Type": "application/octet-stream", "X-Artifact-ID": output["artifact_id"].(string), "X-Workspace-ID": "studio", "X-Job-ID": job["job_id"].(string), "X-Attempt-ID": attempt, "X-Content-Bytes": strconv.Itoa(len(expected)), "X-Content-SHA256": string(provider.Digest(expected)), "X-Content-Type-Options": "nosniff"} {
		if resp.Header.Get(key) != want {
			t.Fatal("Studio download header changed", key, resp.Header.Get(key))
		}
	}
	// A fresh queued job gives cancel and retry their real successful Studio flow.
	second := f.json(t, "POST", base+"/jobs", "studio-second", spec, 202)
	secondPath := base + "/jobs/" + second["job_id"].(string)
	body := map[string]any{"attempt_id": second["attempt_id"], "reason": "owner requested another attempt"}
	for _, action := range []string{"cancel", "reconcile", "retry"} {
		op := f.json(t, "POST", secondPath+"/"+action, "studio-control-"+action, body, 202)
		studioControl(t, op)
		expectedKind := action
		if action == "retry" {
			expectedKind = "retry_compute"
		}
		expectedEffect := map[string]string{"cancel": "dispatch_prevented", "reconcile": "observation_refreshed", "retry": "new_attempt_created"}[action]
		if op["kind"] != expectedKind || op["effect"] != expectedEffect || op["status"] != "succeeded" || op["workspace_id"] != "studio" || op["job_id"] != second["job_id"] || op["attempt_id"] != second["attempt_id"] {
			t.Fatal("Studio control target/effect changed", op)
		}
		if action == "retry" {
			studioFields(t, op, map[string]string{"new_attempt_id": "id"})
			if op["new_attempt_id"] == second["attempt_id"] {
				t.Fatal("retry reused original attempt")
			}
		}
		if op["remote_termination_confirmed"] != false {
			t.Fatal("local control claimed remote termination")
		}
		read := f.json(t, "GET", base+"/operations/"+op["operation_id"].(string), "", nil, 200)
		studioControl(t, read)
		if read["operation_id"] != op["operation_id"] || read["kind"] != op["kind"] {
			t.Fatal("operation identity changed")
		}
	}
	studioStatus(t, f.json(t, "GET", secondPath, "", nil, 200))
	for _, tc := range []struct {
		method, path, key string
		body              any
		status            int
		code              string
	}{{"GET", jobPath + "/logs", "", nil, 400, "INVALID_REQUEST"}, {"GET", jobPath + "/artifacts", "", nil, 400, "INVALID_REQUEST"}, {"POST", base + "/jobs", "studio-admit", map[string]any{"invalid": true}, 400, "INVALID_JOB_SPEC"}, {"POST", secondPath + "/collect", "studio-bad-collect", body, 409, "REMOTE_EXECUTION_UNRESOLVED"}, {"GET", base + "/jobs/job_missing", "", nil, 404, "INVALID_REQUEST"}, {"GET", "/v1/workspaces/foreign/providers", "", nil, 403, "WORKSPACE_FORBIDDEN"}, {"POST", base + "/connections", "studio-create", map[string]any{"provider_type": "fixture", "label": "Changed", "credentials": map[string]string{"api_token": managedSecret}}, 409, "IDEMPOTENCY_CONFLICT"}} {
		m := f.json(t, tc.method, tc.path, tc.key, tc.body, tc.status)
		e := studioMap(t, m["error"])
		studioFields(t, e, map[string]string{"code": tc.code, "message": "str", "request_id": "str", "stage": "str", "safe_operation_retry": "bool", "compute_may_have_started": "bool", "recommended_action": "str"})
	}
	validToken := f.token
	f.token = "cr1_" + strings.Repeat("A", 43)
	unauthorized := f.json(t, "GET", jobPath, "", nil, 401)
	studioFields(t, studioMap(t, unauthorized["error"]), map[string]string{"code": "RUNTIME_AUTH_REQUIRED"})
	f.token = validToken
	stats := backend.Stats()
	if stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 || stats.CleanupCalls != 0 {
		t.Fatal("HTTP reads/controls repeated fake execution", stats)
	}
}

// Exercise every public release value through the actual HTTP projection. This
// repository only overrides reads; the main test above uses SQLite and fake evidence.
type studioReleaseRead struct {
	*sqlite.Store
	release atomic.Value
}

func (r *studioReleaseRead) ReadJob(ctx context.Context, w domain.WorkspaceID, token string, id domain.JobID) (admission.Record, error) {
	record, err := r.Store.ReadJob(ctx, w, token, id)
	record.Attempt.State.ReleaseEvidence = r.release.Load().(domain.ReleaseEvidence)
	return record, err
}
func TestStudioHTTPReleaseEvidenceValues(t *testing.T) {
	var repo *studioReleaseRead
	f := newStudioHTTP(t, func(s *sqlite.Store) admission.Repository { repo = &studioReleaseRead{Store: s}; return repo })
	ctx := context.Background()
	base := "/v1/workspaces/studio"
	op := f.json(t, "POST", base+"/connections", "studio-release-connection", map[string]any{
		"provider_type": "fixture", "label": "Release fixture", "credentials": map[string]string{"api_token": managedSecret},
	}, 202)
	if worked, err := f.service.RunOnce(ctx); err != nil || !worked {
		t.Fatal("connection verification", worked, err)
	}
	connection := f.json(t, "GET", base+"/connections/"+op["connection_id"].(string), "", nil, 200)
	if err := f.store.CommitObject(ctx, domain.ObjectMetadata{ID: "code", WorkspaceID: "studio", Bytes: 0, SHA256: provider.Digest(nil)}); err != nil {
		t.Fatal(err)
	}
	var spec map[string]any
	if err := json.Unmarshal([]byte(jobBody), &spec); err != nil {
		t.Fatal(err)
	}
	spec["profile"] = studioMap(t, connection["selection"])["profile"]
	job := f.json(t, "POST", "/v1/workspaces/studio/jobs", "studio-release-admit", spec, 202)
	for _, value := range []string{"unknown", "not_applicable", "not_observable", "provider_reported", "confirmed"} {
		t.Run(value, func(t *testing.T) {
			repo.release.Store(domain.ReleaseEvidence(value))
			m := f.json(t, "GET", "/v1/workspaces/studio/jobs/"+job["job_id"].(string), "", nil, 200)
			studioStatus(t, m)
			if studioMap(t, m["state"])["release_evidence"] != value {
				t.Fatal("release evidence collapsed", m)
			}
		})
	}
}
