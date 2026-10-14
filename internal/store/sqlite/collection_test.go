package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// This adapter exposes synthetic result bytes; it NEVER runs the admitted fixture
// command. The real metadata/blob stores and existing dispatch fixture are composed.
type collectionProvider struct {
	*probeProvider
	mu      sync.Mutex
	files   map[string][]byte
	mode    string
	lists   int
	fetches map[string]int
	hook    func() error
}

func (p *collectionProvider) ListArtifacts(ctx context.Context, remote provider.RemoteReference) ([]provider.Artifact, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lists++
	// The store has only one connection. A provider callback inside a SQL
	// transaction would fail this bounded query instead of deadlocking the suite.
	check, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var n int
	if err := p.store.db.QueryRowContext(check, "SELECT count(*) FROM work_leases WHERE kind='collection' AND held=1").Scan(&n); err != nil || n == 0 {
		return nil, errors.New("transfer called without committed collection ownership")
	}
	if p.mode == "overbound" {
		return make([]provider.Artifact, provider.MaxArtifactFiles+1), nil
	}
	keys := make([]string, 0, len(p.files))
	for key := range p.files {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]provider.Artifact, 0, len(keys))
	for _, path := range keys {
		data := p.files[path]
		artifact := provider.Artifact{Remote: remote, Path: path, Bytes: int64(len(data)), SHA256: provider.Digest(data)}
		if p.mode == "foreign" {
			artifact.Remote.Version = "another-run"
		}
		result = append(result, artifact)
	}
	return result, nil
}
func (p *collectionProvider) Fetch(ctx context.Context, remote provider.RemoteReference, a provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	dst = provider.WithTransferProgress(dst, progress)
	p.mu.Lock()
	p.fetches[a.Path]++
	data, ok := p.files[a.Path]
	data = append([]byte(nil), data...)
	mode := p.mode
	var hook func() error
	if strings.HasPrefix(a.Path, "outputs/") {
		hook, p.hook = p.hook, nil
	}
	p.mu.Unlock()
	if hook != nil {
		if err := hook(); err != nil {
			return provider.TransferResult{}, err
		}
	}
	if !ok || a.Remote != remote || a.Bytes > limit {
		return provider.TransferResult{}, errors.New("invalid fixture transfer")
	}
	if strings.HasPrefix(a.Path, "outputs/") {
		check, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		var pins int
		if err := p.store.db.QueryRowContext(check, "SELECT count(*) FROM collection_snapshots").Scan(&pins); err != nil || pins != 1 {
			return provider.TransferResult{}, errors.New("payload transfer without committed pin or outside-SQL boundary")
		}
		switch mode {
		case "panic":
			panic("private-provider-canary")
		case "partial":
			_, _ = dst.Write(data[:len(data)/2])
			return provider.TransferResult{}, errors.New("partial response canary")
		case "corrupt":
			data[0] ^= 1
		case "long":
			_, _ = dst.Write(append(data, 'x')) // Deliberately ignore the writer error.
			return provider.TransferResult{Bytes: a.Bytes, SHA256: a.SHA256}, nil
		}
	}
	if _, err := io.Copy(dst, bytes.NewReader(data)); err != nil {
		return provider.TransferResult{}, err
	}
	if mode == "ack" && strings.HasPrefix(a.Path, "outputs/") {
		return provider.TransferResult{}, errors.New("error after last byte")
	}
	return provider.TransferResult{Bytes: int64(len(data)), SHA256: a.SHA256}, ctx.Err()
}
func (p *collectionProvider) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, n := range p.fetches {
		total += n
	}
	return p.lists, total
}
func collectionFixture(t *testing.T, collectionMode ...string) (*dispatchFixture, scheduler.Identity, *collectionProvider, *blobfs.Store) {
	return collectionScenarioFixture(t, fake.DefaultScenario(), collectionMode...)
}

func collectionScenarioFixture(t *testing.T, scenario fake.Scenario, collectionMode ...string) (*dispatchFixture, scheduler.Identity, *collectionProvider, *blobfs.Store) {
	t.Helper()
	f := newDispatchFixture(t, scenario)
	id := f.seed(t, "a", 1, false, collectionMode...)
	submittedControl(t, f, id)
	for i := 0; i < 2; i++ {
		if err := f.adapter.Advance(*f.journal(t, id).Remote); err != nil {
			t.Fatal(err)
		}
		if err := f.step(t); err != nil {
			t.Fatal(err)
		}
	}
	j := f.journal(t, id)
	if !j.Observation.Execution.Terminal() {
		t.Fatal("fixture needs terminal provider evidence")
	}
	p := &collectionProvider{probeProvider: f.adapter, fetches: map[string]int{}, files: completedOutputs(t, f, j.Remote.Identity)}
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "results"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	return f, id, p, blobs
}

// completedOutputs is the remote file set of a successful runner for one attempt identity.
func completedOutputs(t testing.TB, f *dispatchFixture, identity provider.Identity) map[string][]byte {
	t.Helper()
	data := bytes.Repeat([]byte("fixture bytes\n"), 4096)
	m := map[string]any{
		"manifest_version": "1", "runner_version": "offline-fixture", "job_id": identity.JobID,
		"attempt_id": identity.AttemptID, "attempt_nonce": identity.Nonce,
		"bundle_sha256": identity.BundleSHA256, "input_manifest_sha256": identity.InputManifestSHA256,
		"started_at": f.clock.Now().Add(-time.Second).Format(time.RFC3339), "finished_at": f.clock.Now().Format(time.RFC3339),
		"phase": "completed", "exit_code": 0, "timed_out": false, "error": nil,
		"resource_check": map[string]bool{"gpu_required": false, "gpu_verified": false},
		"artifacts":      []any{map[string]any{"path": "answer.json", "bytes": len(data), "sha256": provider.Digest(data)}},
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return map[string][]byte{
		collection.ManifestPath: raw, "outputs/answer.json": data, "control/stdout.log": []byte("fixture output\n"),
		"control/stderr.log": {}, "control/environment.json": []byte(`{"fixture":true}`),
		"scratch/not-selected": []byte("do not download"),
	}
}
func collectionEngine(t *testing.T, f *dispatchFixture, p *collectionProvider, blobs collection.BlobStore, repo collection.Repository) *collection.Engine {
	t.Helper()
	p.probeProvider = f.adapter
	registry := provider.NewSnapshotRegistry()
	binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
	if err := registry.Register(binding, p); err != nil {
		t.Fatal(err)
	}
	engine, err := collection.New(repo, registry, blobs, f.clock, collection.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return engine
}
func runCollection(t *testing.T, engine *collection.Engine) {
	t.Helper()
	worked, err := engine.RunOnce(context.Background())
	if err != nil || !worked {
		t.Fatalf("collection: worked=%v err=%v", worked, err)
	}
}
func assertNoNewCompute(t *testing.T, f *dispatchFixture) {
	t.Helper()
	s := f.backend.Stats()
	if s.PrepareCalls != 1 || s.SubmitCalls != 1 || s.Executions != 1 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 {
		t.Fatal("collection repeated compute or changed attempt identity", s)
	}
}
func TestCollectionPublicationCommitsAtomicRows(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t)
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	original, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCollect, "collect-original", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 5 || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect' AND status='succeeded'") != 1 || f.state(t, id).Result != domain.ResultAvailable {
		t.Fatal("publication did not commit metadata, ticket and result atomically")
	}
	var raw string
	if err = f.s.db.QueryRow("SELECT receipt FROM receipts WHERE namespace='operation' AND subject_id=?", original.Operation.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var receipt operations.Record
	if json.Unmarshal([]byte(raw), &receipt) != nil || receipt.Operation.Status != domain.OperationAccepted || receipt.Operation.ID != original.Operation.ID {
		t.Fatal("publication rewrote immutable acceptance")
	}
}

func TestCollectionFailuresDoNotPublishSQLRows(t *testing.T) {
	for _, mode := range []string{"partial", "ack", "corrupt", "long", "panic", "overbound", "foreign", "missing-output", "missing-manifest", "wrong-nonce"} {
		t.Run(mode, func(t *testing.T) {
			f, id, p, blobs := collectionFixture(t)
			p.mode = mode
			switch mode {
			case "missing-output":
				delete(p.files, "outputs/answer.json")
			case "missing-manifest":
				delete(p.files, collection.ManifestPath)
			case "wrong-nonce":
				var m map[string]any
				_ = json.Unmarshal(p.files[collection.ManifestPath], &m)
				m["attempt_nonce"] = strings.Repeat("z", 64)
				p.files[collection.ManifestPath], _ = json.Marshal(m)
			}
			if worked, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(context.Background()); !worked || err == nil {
				t.Fatal("fault did not execute", err)
			}
			if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 0 || f.state(t, id).Result == domain.ResultAvailable {
				t.Fatal("partial metadata escaped SQL publication boundary")
			}
		})
	}
}

func TestCollectionRevisionChangeRejectsPublicationRows(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t)
	service, actor, _ := controlService(t, f, "a", auth.Operate)
	p.hook = func() error {
		_, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCancel, "cancel-during-transfer", controlRequest(id.AttemptID))
		return err
	}
	if _, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(ctx); !errors.Is(err, collection.ErrLeaseLost) {
		t.Fatal("changed revision accepted", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 0 {
		t.Fatal("stale revision published rows")
	}
}
