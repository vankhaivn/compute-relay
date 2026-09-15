package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"strconv"
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

func (p *collectionProvider) ListArtifacts(ctx context.Context, remote provider.RemoteReference, page provider.PageRequest) (provider.ArtifactPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lists++
	// The store has only one connection. A provider callback inside a SQL
	// transaction would fail this bounded query instead of deadlocking the suite.
	check, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var n int
	if err := p.store.db.QueryRowContext(check, "SELECT count(*) FROM collection_leases WHERE held=1").Scan(&n); err != nil || n == 0 {
		return provider.ArtifactPage{}, errors.New("transfer called without committed collection ownership")
	}
	if p.mode == "cycle" {
		return provider.ArtifactPage{NextCursor: "repeat"}, nil
	}
	keys := make([]string, 0, len(p.files))
	for path := range p.files {
		keys = append(keys, path)
	}
	sort.Strings(keys)
	start := 0
	if page.Cursor != "" {
		var err error
		start, err = strconv.Atoi(page.Cursor)
		if err != nil || start < 0 || start > len(keys) {
			return provider.ArtifactPage{}, errors.New("bad cursor")
		}
	}
	result := provider.ArtifactPage{}
	end := start + 2 // Force pagination even when the caller requests 100.
	if end > len(keys) {
		end = len(keys)
	}
	for _, path := range keys[start:end] {
		data := p.files[path]
		a := provider.Artifact{Remote: remote, Path: path, Bytes: int64(len(data)), SHA256: provider.Digest(data)}
		if p.mode == "foreign" {
			a.Remote.Version = "another-run"
		}
		result.Artifacts = append(result.Artifacts, a)
	}
	if end < len(keys) {
		result.NextCursor = strconv.Itoa(end)
	}
	return result, nil
}
func (p *collectionProvider) FetchArtifact(ctx context.Context, remote provider.RemoteReference, a provider.Artifact, dst io.Writer, limit int64) (provider.TransferResult, error) {
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
func collectionFixture(t *testing.T) (*dispatchFixture, scheduler.Identity, *collectionProvider, *blobfs.Store) {
	t.Helper()
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
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
	identity := j.Remote.Identity
	data := bytes.Repeat([]byte("fixture bytes\n"), 4096)
	m := map[string]any{
		"manifest_version": "1", "runner_version": "offline-fixture", "job_id": identity.JobID,
		"attempt_id": identity.AttemptID, "attempt_nonce": identity.Nonce,
		"bundle_sha256": identity.BundleSHA256, "input_manifest_sha256": identity.InputManifestSHA256,
		"started_at": f.clock.Now().Add(-time.Second).Format(time.RFC3339), "finished_at": f.clock.Now().Format(time.RFC3339),
		"phase": "completed", "exit_code": 0, "timed_out": false, "error": nil,
		"resource_check": map[string]bool{"gpu_required": false, "gpu_verified": false},
		"artifacts": []any{map[string]any{"path": "answer.json", "bytes": len(data), "sha256": provider.Digest(data)}},
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	p := &collectionProvider{probeProvider: f.adapter, fetches: map[string]int{}, files: map[string][]byte{
		collection.ManifestPath: raw, "outputs/answer.json": data, "control/stdout.log": []byte("fixture output\n"),
		"control/stderr.log": {}, "control/environment.json": []byte(`{"fixture":true}`),
		"scratch/not-selected": []byte("do not download"),
	}}
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "results"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	return f, id, p, blobs
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
func TestCollectionPublicationReceiptAndScopedReads(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t)
	service, principal, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	original, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "collect-original", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.ReadCollection(ctx, "a", principal.TokenID(), id.JobID, id.AttemptID); !errors.Is(err, collection.ErrNotFound) {
		t.Fatal("unverified artifacts visible", err)
	}
	remapped := f.profile
	remapped.Binding.ConfigurationRevision = "new-revision"
	remapped.Binding.ProviderInstanceID = "replacement"
	remapped.AccountScope = "another-account"
	if err = f.s.PutProfile(ctx, remapped, true); err != nil {
		t.Fatal(err)
	}
	beforeState := f.state(t, id)
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	state := f.state(t, id)
	if state.Orchestration != domain.OrchestrationSucceeded || state.Result != domain.ResultAvailable || state.ReleaseEvidence != beforeState.ReleaseEvidence {
		t.Fatal("final result changed provider release evidence", state)
	}
	current, err := service.Get(ctx, principal, "a", original.Operation.ID)
	if err != nil || current.Effect != operations.ResultsAvailable || current.Operation.Status != domain.OperationSucceeded {
		t.Fatal("control did not complete atomically", err)
	}
	replay, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "collect-original", controlRequest(id.AttemptID))
	if err != nil || replay.Operation.Status != domain.OperationAccepted || !replay.Replay || replay.Operation.ID != original.Operation.ID {
		t.Fatal("original receipt was rewritten", err)
	}
	access, _ := auth.New(f.s, f.s, nil)
	reader, err := collection.NewReader(access, f.s, blobs)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Read(ctx, principal, "a", id.JobID, id.AttemptID)
	if err != nil || len(result.Files) != 5 || result.Phase != "completed" {
		t.Fatal("missing verified result", err)
	}
	for _, file := range result.Files {
		meta, stream, err := reader.Open(ctx, principal, "a", id.JobID, id.AttemptID, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(stream)
		closeErr := stream.Close()
		if err != nil || closeErr != nil || int64(len(data)) != meta.Object.Bytes || provider.Digest(data) != meta.Object.SHA256 {
			t.Fatal("committed content did not match identity")
		}
	}
	if _, _, err := reader.Open(ctx, principal, "a", id.JobID, "foreign", result.Files[0].ID); err == nil {
		t.Fatal("cross-attempt artifact read allowed")
	}
	if _, err := reader.Read(ctx, principal, "b", id.JobID, id.AttemptID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("workspace authorization absent", err)
	}
	if _, _, err := reader.Open(ctx, principal, "a", id.JobID, id.AttemptID, "foreign"); !errors.Is(err, collection.ErrNotFound) {
		t.Fatal("arbitrary artifact ID accepted", err)
	}
	if err = f.s.RevokeToken(ctx, principal.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Read(ctx, principal, "a", id.JobID, id.AttemptID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revoked token read results", err)
	}
	lists, _ := p.counts()
	if lists < 3 || p.fetches["scratch/not-selected"] != 0 || controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 5 {
		t.Fatal("pagination or selected-output boundary failed")
	}
	assertNoNewCompute(t, f)
}

func TestCollectionBadTransfersRequireExplicitRecovery(t *testing.T) {
	for _, mode := range []string{"partial", "ack", "corrupt", "long", "panic", "cycle", "foreign", "missing-output", "missing-manifest", "wrong-nonce"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
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
			engine := collectionEngine(t, f, p, blobs, f.s)
			worked, err := engine.RunOnce(ctx)
			var failure collection.Failure
			if !worked || !errors.As(err, &failure) {
				t.Fatal("transfer failure not recorded", err)
			}
			state := f.state(t, id)
			if state.Orchestration != domain.OrchestrationNeedsAttention || state.Result == domain.ResultAvailable || f.journal(t, id).Problem == nil {
				t.Fatal("false success or missing cached condition")
			}
			if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 0 {
				t.Fatal("partial metadata was published")
			}
			if worked, err = engine.RunOnce(ctx); worked || err != nil {
				t.Fatal("failed operation automatically repeated", err)
			}
			assertNoNewCompute(t, f)
			if mode != "partial" && mode != "ack" {
				return
			}
			// An explicit transfer retry keeps the first snapshot and reuses verified
			// control blobs; it does not list a newer provider run or rerun compute.
			lists, _ := p.counts()
			p.mode = ""
			f.restart(t)
			f.clock.advance(time.Minute)
			service, principal, _ := controlService(t, f, "a", auth.Read, auth.Operate)
			if _, err = service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "explicit-collection-retry", controlRequest(id.AttemptID)); err != nil {
				t.Fatal(err)
			}
			runCollection(t, collectionEngine(t, f, p, blobs, f.s))
			after, _ := p.counts()
			if after != lists || f.state(t, id).Result != domain.ResultAvailable || f.journal(t, id).Problem != nil {
				t.Fatal("recovery refreshed the pin or retained an obsolete condition")
			}
			assertNoNewCompute(t, f)
		})
	}
}

func TestCollectionCancellationRaceKeepsTerminalEvidence(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t)
	service, principal, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	p.hook = func() error {
		op, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCancel, "cancel-during-transfer", controlRequest(id.AttemptID))
		if err == nil && op.Effect != operations.TooLate {
			return errors.New("terminal cancellation was not too late")
		}
		return err
	}
	engine := collectionEngine(t, f, p, blobs, f.s)
	if _, err := engine.RunOnce(ctx); !errors.Is(err, collection.ErrLeaseLost) {
		t.Fatal("changed attempt revision accepted", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 {
		t.Fatal("stale worker published")
	}
	f.clock.advance(11 * time.Minute)
	runCollection(t, engine)
	state := f.state(t, id)
	if state.Orchestration != domain.OrchestrationSucceeded || state.Cancellation != domain.CancellationTooLate {
		t.Fatal("race fabricated cancellation", state)
	}
	assertNoNewCompute(t, f)
}
