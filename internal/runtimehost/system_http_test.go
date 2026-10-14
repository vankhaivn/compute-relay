package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

// Add runner control files to the real fake's catalog without replacing its backend.
type systemArtifactSource struct {
	provider.Provider
	mu       sync.Mutex
	extra    map[string][]byte
	manifest []byte
	lists    int
	fetches  int
}

func (s *systemArtifactSource) ListArtifacts(ctx context.Context, remote provider.RemoteReference) ([]provider.Artifact, error) {
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	files, err := s.Provider.ListArtifacts(ctx, remote)
	if err != nil {
		return nil, err
	}
	for path, content := range s.extra {
		files = append(files, provider.Artifact{Remote: remote, Path: path, Bytes: int64(len(content)), SHA256: provider.Digest(content)})
	}
	return files, nil
}
func (s *systemArtifactSource) Fetch(ctx context.Context, remote provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	s.mu.Lock()
	s.fetches++
	s.mu.Unlock()
	if content, ok := s.extra[file.Path]; ok {
		return provider.CopyVerified(ctx, bytes.NewReader(content), provider.WithTransferProgress(dst, progress), file, limit)
	}
	var manifest bytes.Buffer
	if file.Path == collection.ManifestPath {
		dst = io.MultiWriter(dst, &manifest)
	}
	result, err := s.Provider.Fetch(ctx, remote, file, dst, limit, progress)
	if err == nil && file.Path == collection.ManifestPath {
		s.mu.Lock()
		s.manifest = append([]byte(nil), manifest.Bytes()...)
		s.mu.Unlock()
	}
	return result, err
}
func (s *systemArtifactSource) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists, s.fetches
}

func systemWireSchema(t *testing.T, name string) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	entries, err := schemas.Files.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		raw, err := schemas.Files.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		if err := compiler.AddResource("https://contracts.compute-relay.invalid/"+entry.Name(), value); err != nil {
			t.Fatal(err)
		}
	}
	schema, err := compiler.Compile("https://contracts.compute-relay.invalid/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestPublishedArtifactHTTPReusesImmutablePublication(t *testing.T) {
	ctx := t.Context()
	scenario := fake.DefaultScenario()
	scenario.Files[0].Content = bytes.Repeat([]byte("fixture bytes\n"), 4096)
	f := newSystemFixture(t, scenario)
	job := f.collectibleJob(t, "artifact-http-job", "manual")
	source := &systemArtifactSource{extra: map[string][]byte{
		"control/stdout.log": []byte("fixture output\n"), "control/stderr.log": {},
		"control/environment.json": []byte(`{"fixture":true}`), "scratch/not-selected": []byte("do not download"),
	}}
	f.wrapProvider = func(base provider.Provider) provider.Provider { source.Provider = base; return source }
	body := systemControlBody(t, job.AttemptID)
	original, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "artifact-http-original", body)
	if err != nil {
		t.Fatal(err)
	}
	f.startHTTP(t)
	path, query := job.Links.Artifacts, "?attempt_id="+string(job.AttemptID)
	f.request(t, "GET", path+query, "", "", nil, http.StatusNotFound)
	f.collectOnce(t)
	before, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	lists, fetches := source.counts()
	if lists != 1 || fetches != 5 {
		t.Fatal("collection did not select exactly the published files", lists, fetches)
	}
	type file struct {
		ID           string `json:"artifact_id"`
		Path, SHA256 string
		Bytes        int64
	}
	type page struct {
		SnapshotSHA256 string `json:"snapshot_sha256"`
		NextCursor     string `json:"next_cursor"`
		Total          int    `json:"total_artifacts"`
		Artifacts      []file
	}
	readPage := func(cursor string) page {
		t.Helper()
		_, raw := f.request(t, "GET", path+query+"&limit=2&cursor="+url.QueryEscape(cursor), "", "", nil, http.StatusOK)
		var result page
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := readPage("")
	if first.Total != 5 || len(first.Artifacts) != 2 || first.NextCursor == "" {
		t.Fatal("incomplete first page", first)
	}
	f.reopen(t)
	f.startHTTP(t)
	all := first.Artifacts
	for cursor := first.NextCursor; cursor != ""; {
		page := readPage(cursor)
		if page.SnapshotSHA256 != first.SnapshotSHA256 {
			t.Fatal("restart changed publication")
		}
		all = append(all, page.Artifacts...)
		cursor = page.NextCursor
	}
	if len(all) != 5 {
		t.Fatal("incomplete publication")
	}
	for _, file := range all {
		_, content := f.request(t, "GET", path+"/"+file.ID+"/content"+query, "", "", nil, http.StatusOK)
		expected, ok := source.extra[file.Path]
		if file.Path == collection.ManifestPath {
			expected, ok = source.manifest, true
		} else if file.Path == "outputs/"+scenario.Files[0].Path {
			expected, ok = scenario.Files[0].Content, true
		}
		if !ok || !bytes.Equal(content, expected) || int64(len(content)) != file.Bytes || string(provider.Digest(content)) != file.SHA256 {
			t.Fatal("incorrect published bytes", file.Path)
		}
	}
	f.request(t, "GET", path+"/"+all[0].ID+"?attempt_id=att_foreign", "", "", nil, http.StatusNotFound)
	after, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	if err != nil || !reflect.DeepEqual(after.Attempt, before.Attempt) {
		t.Fatal("delivery rewrote durable outcome", err)
	}
	if l, n := source.counts(); l != lists || n != fetches {
		t.Fatal("delivery repeated provider collection")
	}
	replay, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "artifact-http-original", body)
	if err != nil || !replay.Replay || !reflect.DeepEqual(replay.Operation, original.Operation) || replay.Effect != original.Effect {
		t.Fatal("delivery changed receipt", err)
	}
	f.advance(t, 8*24*time.Hour)
	if _, err := f.host.store.ExpireRetention(ctx, retention.DefaultPolicy(), 100); err != nil {
		t.Fatal(err)
	}
	f.request(t, "GET", path+query, "", "", nil, http.StatusGone)
	if err := f.host.access.Revoke(ctx, f.actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	f.request(t, "GET", path+query, "", "", nil, http.StatusUnauthorized)
	stats := f.backend.Stats()
	if stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 {
		t.Fatal("delivery repeated compute", stats)
	}
}

func TestCleanupHTTPPreviewUsesBoundedPublishedContract(t *testing.T) {
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "cleanup-preview-job", "automatic")
	f.collectOnce(t)
	f.startHTTP(t)
	path, query := job.Links.Self+"/cleanup", "?attempt_id="+string(job.AttemptID)
	before := f.backend.Stats()
	_, raw := f.request(t, "GET", path+query, "", "", nil, http.StatusOK)
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	if err := systemWireSchema(t, "cleanup-preview.v1alpha1.schema.json").Validate(value); err != nil {
		t.Fatal("preview violates published contract", err)
	}
	resources, ok := value["resources"].([]any)
	after := f.backend.Stats()
	if !ok || len(resources) != 2 || value["eligible"] != true || after.PrepareCalls != before.PrepareCalls || after.SubmitCalls != before.SubmitCalls || after.Executions != before.Executions || after.FetchCalls != before.FetchCalls {
		t.Fatal("preview changed provider state or omitted candidates", value)
	}
	attempt, err := f.host.store.ReadLogAttempt(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	if err != nil || attempt.Remote == nil {
		t.Fatal("missing original remote", err)
	}
	adapter, err := f.services.resolver.Resolve(attempt.Binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Observe(t.Context(), *attempt.Remote); err != nil {
		t.Fatal("preview deleted the original resource", err)
	}
	for _, invalid := range []string{
		"", "?attempt_id=", "?attempt_id=a&attempt_id=b", query + "&discard_uncollected=1", query + "&discard_uncollected=true&discard_uncollected=false",
		query + "&remote_ref=foreign", query + "&discard_uncollected=", query + "&attempt_id=x", query + "&x=y;z",
	} {
		f.request(t, "GET", path+invalid, "", "", nil, http.StatusBadRequest)
	}
	body := systemJSON(t, operations.Request{AttemptID: job.AttemptID})
	f.request(t, "POST", path+query, "query-not-permit", "application/json", body, http.StatusBadRequest)
	f.request(t, "GET", path+query, "", "application/json", []byte("{}"), http.StatusBadRequest)
}

func TestCleanupHTTPAdmitsAppliesAndReplaysWithStore(t *testing.T) {
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "cleanup-http-job", "automatic")
	f.collectOnce(t)
	f.startHTTP(t)
	schema := systemWireSchema(t, "control-operation.v1alpha1.schema.json")
	decode := func(raw []byte) map[string]any {
		t.Helper()
		var value map[string]any
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(value); err != nil {
			t.Fatal("cleanup receipt violates published contract", err)
		}
		return value
	}
	path := job.Links.Self + "/cleanup"
	body := systemJSON(t, operations.Request{AttemptID: job.AttemptID, Reason: "private-reason-canary"})
	response, raw := f.request(t, "POST", path, "cleanup-http", "application/json", body, http.StatusAccepted)
	accepted := decode(raw)
	if accepted["kind"] != "cleanup" || accepted["status"] != "accepted" || accepted["effect"] != "pending" || accepted["resources"] != nil || bytes.Contains(raw, []byte("private-reason-canary")) {
		t.Fatal("unexpected cleanup acceptance", string(raw))
	}
	if worked, err := f.cleanupEngine(t, nil).RunOnce(t.Context()); !worked || err != nil {
		t.Fatal("cleanup", worked, err)
	}
	_, raw = f.request(t, "GET", response.Header.Get("Location"), "", "", nil, http.StatusOK)
	if current := decode(raw); current["status"] != "succeeded" || current["effect"] != "remote_deleted" || current["replay"] != false || len(current["resources"].([]any)) != 2 {
		t.Fatal("applied cleanup not readable", string(raw))
	}
	_, raw = f.request(t, "POST", path, "cleanup-http", "application/json", body, http.StatusAccepted)
	if replay := decode(raw); replay["replay"] != true || replay["status"] != "accepted" || replay["operation_id"] != accepted["operation_id"] || replay["resources"] != nil {
		t.Fatal("replay did not return the original receipt", string(raw))
	}
	_, raw = f.request(t, "POST", path, "cleanup-coalesced", "application/json", body, http.StatusAccepted)
	if coalesced := decode(raw); coalesced["status"] != "succeeded" || coalesced["resources"] != nil || coalesced["replay"] != true {
		t.Fatal("coalesced receipt hydrated resource reads", string(raw))
	}
	_, raw = f.request(t, "POST", path, "cleanup-http", "application/json", systemJSON(t, operations.Request{AttemptID: job.AttemptID}), http.StatusConflict)
	if !bytes.Contains(raw, []byte(domain.CodeIdempotencyConflict)) {
		t.Fatal("changed payload did not conflict", string(raw))
	}
}
