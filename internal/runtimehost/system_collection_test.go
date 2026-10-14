package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

// The real fake still owns preparation, execution and exact binding verification.
// This overlay supplies runner files and faults at the public catalog/transfer port.
type systemCollectionSource struct {
	provider.Provider
	mu      sync.Mutex
	files   map[string][]byte
	mode    string
	phase   string
	now     time.Time
	lists   int
	fetches map[string]int
	hook    func() error
	chunk   func(int64) error
	advance func(time.Duration)
}

func (f *managedTestFixture) collectionSource(t *testing.T) *systemCollectionSource {
	t.Helper()
	s := &systemCollectionSource{now: f.fixtureClock().Now(), phase: "completed", fetches: map[string]int{}, files: map[string][]byte{
		"outputs/result.json": bytes.Repeat([]byte("fixture bytes\n"), 4096),
		"control/stdout.log":  []byte("fixture output\n"), "control/stderr.log": {},
		"control/environment.json": []byte(`{"fixture":true}`), "scratch/not-selected": []byte("do not download"),
	}}
	f.wrapProvider = func(base provider.Provider) provider.Provider { s.Provider = base; return s }
	return s
}

func (s *systemCollectionSource) manifest(remote provider.RemoteReference) []byte {
	if raw, ok := s.files[collection.ManifestPath]; ok {
		return raw
	}
	id := remote.Identity
	artifacts := []any{}
	for path, data := range s.files {
		if strings.HasPrefix(path, "outputs/") {
			artifacts = append(artifacts, map[string]any{"path": strings.TrimPrefix(path, "outputs/"), "bytes": len(data), "sha256": provider.Digest(data)})
		}
	}
	var failure any
	exit := 0
	if s.phase != "completed" {
		failure = map[string]any{"code": "COMMAND_FAILED", "message": "synthetic runner failure", "stage": "execution"}
		exit = 1
	}
	m := map[string]any{"manifest_version": "1", "runner_version": "system-fixture", "job_id": id.JobID, "attempt_id": id.AttemptID,
		"attempt_nonce": id.Nonce, "bundle_sha256": id.BundleSHA256, "input_manifest_sha256": id.InputManifestSHA256,
		"started_at": s.now.Add(-time.Second).Format(time.RFC3339), "finished_at": s.now.Format(time.RFC3339),
		"phase": s.phase, "exit_code": exit, "timed_out": false, "error": failure,
		"resource_check": map[string]bool{"gpu_required": false, "gpu_verified": false}, "artifacts": artifacts}
	if s.mode == "wrong-nonce" {
		m["attempt_nonce"] = strings.Repeat("z", 64)
	}
	raw, _ := json.Marshal(m)
	s.files[collection.ManifestPath] = raw
	return raw
}

func (s *systemCollectionSource) ListArtifacts(ctx context.Context, remote provider.RemoteReference) ([]provider.Artifact, error) {
	if _, err := s.Provider.ListArtifacts(ctx, remote); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	s.manifest(remote)
	if s.mode == "overbound" {
		return make([]provider.Artifact, provider.MaxArtifactFiles+1), nil
	}
	paths := make([]string, 0, len(s.files))
	for path := range s.files {
		if s.mode == "missing-manifest" && path == collection.ManifestPath || s.mode == "missing-output" && strings.HasPrefix(path, "outputs/") {
			continue
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	files := make([]provider.Artifact, 0, len(paths))
	for _, path := range paths {
		data := s.files[path]
		a := provider.Artifact{Remote: remote, Path: path, Bytes: int64(len(data)), SHA256: provider.Digest(data)}
		if s.mode == "foreign" {
			a.Remote.Version = "another-run"
		}
		files = append(files, a)
	}
	if s.mode == "duplicate" {
		files = append(files, files[0])
	}
	return files, nil
}

func (s *systemCollectionSource) Fetch(ctx context.Context, remote provider.RemoteReference, a provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	s.mu.Lock()
	s.fetches[a.Path]++
	data, ok := s.files[a.Path]
	data = append([]byte(nil), data...)
	mode, chunk, advance := s.mode, s.chunk, s.advance
	var hook func() error
	if strings.HasPrefix(a.Path, "outputs/") {
		hook, s.hook = s.hook, nil
	}
	s.mu.Unlock()
	if !ok || a.Validate(remote) != nil || a.Bytes > limit {
		return provider.TransferResult{}, collection.ErrInvalid
	}
	if hook != nil {
		if err := hook(); err != nil {
			return provider.TransferResult{}, err
		}
	}
	output := strings.HasPrefix(a.Path, "outputs/")
	if output {
		switch mode {
		case "panic":
			panic("synthetic private provider detail")
		case "partial":
			_, _ = provider.WithTransferProgress(dst, progress).Write(data[:len(data)/2])
			return provider.TransferResult{}, errors.New("partial synthetic response")
		case "corrupt":
			data[0] ^= 1
		case "long":
			_, _ = provider.WithTransferProgress(dst, progress).Write(append(data, 'x'))
			return provider.TransferResult{Bytes: a.Bytes, SHA256: a.SHA256}, nil
		}
	}
	var written int64
	for len(data) > 0 {
		part := data
		if output && advance != nil {
			part = part[:min(len(part), 1024)]
		}
		n, err := dst.Write(part)
		written += int64(n)
		if err != nil {
			return provider.TransferResult{}, err
		}
		if n != len(part) {
			return provider.TransferResult{}, io.ErrShortWrite
		}
		if output && advance != nil {
			advance(50 * time.Millisecond)
		}
		if progress != nil {
			if err := progress(written); err != nil {
				return provider.TransferResult{}, err
			}
		}
		if output && chunk != nil {
			if err := chunk(written); err != nil {
				return provider.TransferResult{}, err
			}
		}
		data = data[n:]
	}
	if mode == "ack" && output {
		return provider.TransferResult{}, errors.New("synthetic lost transfer acknowledgement")
	}
	return provider.TransferResult{Bytes: written, SHA256: a.SHA256}, ctx.Err()
}

func (s *systemCollectionSource) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, count := range s.fetches {
		n += count
	}
	return s.lists, n
}

func (f *managedTestFixture) collectionEngine(t *testing.T, repo collection.Repository) *collection.Engine {
	t.Helper()
	if repo == nil {
		repo = f.host.store
	}
	e, err := collection.New(repo, f.services.resolver, f.host.results, f.fixtureClock(), collection.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (f *managedTestFixture) collectionStatus(t *testing.T, job admission.Receipt) domain.CollectionStatus {
	t.Helper()
	r, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
	if err != nil || r.Collection == nil {
		t.Fatal("collection status", err)
	}
	return *r.Collection
}

func (f *managedTestFixture) assertSingleCompute(t *testing.T, job admission.Receipt) {
	t.Helper()
	stats := f.backend.Stats()
	r, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
	if err != nil || stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 || r.Attempt.ID != job.AttemptID {
		t.Fatal("collection or cleanup created compute", stats, err)
	}
}

func TestCollectionPublicationReceiptAndScopedReads(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	connection := f.connection(t, "publication-connection", "SYNTHETIC_FIRST")
	spec := f.jobSpec(t, connection)
	spec.ResultCollection = "manual"
	job := f.submitJob(t, "collection-publication", spec)
	f.authorizeJob(t, job)
	f.runToCollection(t, job)
	source := f.collectionSource(t)
	body := systemControlBody(t, job.AttemptID)
	original, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "collect-original", body)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := collection.NewReader(f.host.access, f.host.store, f.host.results)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Read(ctx, f.actor, "app", job.JobID, job.AttemptID); !errors.Is(err, collection.ErrNotFound) {
		t.Fatal("unverified result readable", err)
	}
	// A new current selection cannot reinterpret this attempt's frozen profile.
	oldBinding := f.binding(t, connection)
	configuration := map[string]any{"action": "configure", "expected_revision": connection.Revision, "configuration": map[string]any{"max_remote_wall_seconds": 7200}}
	if _, err = f.services.connections.Submit(ctx, f.actor, "app", connection.ID, "publication-configure", systemJSON(t, configuration)); err != nil {
		t.Fatal(err)
	}
	currentConnection, err := f.services.connections.Get(ctx, f.actor, "app", connection.ID)
	if err != nil || currentConnection.Selection == nil || currentConnection.Selection.ConfigurationRevision == string(oldBinding.Binding.ConfigurationRevision) {
		t.Fatal("current selection did not change", err)
	}
	before, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	f.collectOnce(t)
	after, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	if err != nil || after.Attempt.State.Orchestration != domain.OrchestrationSucceeded || after.Attempt.State.Result != domain.ResultAvailable || after.Attempt.State.ReleaseEvidence != before.Attempt.State.ReleaseEvidence {
		t.Fatal("publication changed remote evidence", err)
	}
	attempt, err := f.host.store.ReadLogAttempt(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	if err != nil || attempt.Binding != oldBinding {
		t.Fatal("collection reinterpreted binding", attempt.Binding, err)
	}
	current, err := f.controls(t).Get(ctx, f.actor, "app", original.Operation.ID)
	if err != nil || current.Operation.Status != domain.OperationSucceeded || current.Effect != operations.ResultsAvailable {
		t.Fatal(current, err)
	}
	replay, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "collect-original", body)
	if err != nil || !replay.Replay || replay.Operation != original.Operation {
		t.Fatal("original receipt changed", replay, err)
	}
	result, err := reader.Read(ctx, f.actor, "app", job.JobID, job.AttemptID)
	if err != nil || len(result.Files) != 5 || result.Phase != "completed" {
		t.Fatal("publication", err)
	}
	for _, file := range result.Files {
		meta, stream, err := reader.Open(ctx, f.actor, "app", job.JobID, job.AttemptID, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(stream)
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(data, source.files[file.Path]) || int64(len(data)) != meta.Object.Bytes || provider.Digest(data) != meta.Object.SHA256 {
			t.Fatal("published content differs", file.Path)
		}
	}
	if _, _, err = reader.Open(ctx, f.actor, "app", job.JobID, "att_foreign", result.Files[0].ID); err == nil {
		t.Fatal("cross-attempt read")
	}
	if _, err = reader.Read(ctx, f.actor, "other", job.JobID, job.AttemptID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("workspace authorization", err)
	}
	if _, _, err = reader.Open(ctx, f.actor, "app", job.JobID, job.AttemptID, "art_foreign"); !errors.Is(err, collection.ErrNotFound) {
		t.Fatal("foreign artifact", err)
	}
	if lists, _ := source.counts(); lists != 1 || source.fetches["scratch/not-selected"] != 0 {
		t.Fatal("selected-output/catalog boundary")
	}
	f.assertSingleCompute(t, job)
	if err = f.host.access.Revoke(ctx, f.actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Read(ctx, f.actor, "app", job.JobID, job.AttemptID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revoked token read", err)
	}
}

func TestCollectionBadTransfersRequireExplicitRecovery(t *testing.T) {
	for _, mode := range []string{"partial", "ack", "corrupt", "long", "panic", "overbound", "duplicate", "foreign", "missing-output", "missing-manifest", "wrong-nonce"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			f := newSystemFixture(t, fake.DefaultScenario())
			job := f.collectibleJob(t, "bad-transfer", "automatic")
			source := f.collectionSource(t)
			source.mode = mode
			e := f.collectionEngine(t, nil)
			worked, err := e.RunOnce(ctx)
			var failure collection.Failure
			if !worked || !errors.As(err, &failure) {
				t.Fatal("transfer fault not recorded", err)
			}
			if mode == "overbound" || mode == "duplicate" || mode == "foreign" {
				if _, fetches := source.counts(); fetches != 0 {
					t.Fatal("invalid complete catalog requested bytes", fetches)
				}
			}
			r, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
			if err != nil || r.Attempt.State.Orchestration != domain.OrchestrationNeedsAttention || r.Attempt.State.Result == domain.ResultAvailable || r.Problem == nil {
				t.Fatal("false success or missing problem", err)
			}
			if _, err = f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, collection.ErrNotFound) {
				t.Fatal("partial result visible", err)
			}
			if worked, err = e.RunOnce(ctx); worked || err != nil {
				t.Fatal("automatic transfer retry", worked, err)
			}
			f.assertSingleCompute(t, job)
			if mode != "partial" && mode != "ack" {
				return
			}
			lists, _ := source.counts()
			source.mode = ""
			f.reopen(t)
			f.advance(t, time.Minute)
			if _, err = f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "explicit-transfer-retry", systemControlBody(t, job.AttemptID)); err != nil {
				t.Fatal(err)
			}
			f.collectOnce(t)
			after, _ := source.counts()
			r, err = f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
			if after != lists || err != nil || r.Attempt.State.Result != domain.ResultAvailable || r.Problem != nil {
				t.Fatal("recovery changed pin or kept old problem", err)
			}
			f.assertSingleCompute(t, job)
		})
	}
}

func TestCollectionCancellationRaceKeepsTerminalEvidence(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "cancel-transfer", "automatic")
	source := f.collectionSource(t)
	source.hook = func() error {
		op, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCancel, "cancel-during-transfer", systemControlBody(t, job.AttemptID))
		if err == nil && op.Effect != operations.TooLate {
			return errors.New("terminal cancellation was not too late")
		}
		return err
	}
	if _, err := f.collectionEngine(t, nil).RunOnce(ctx); !errors.Is(err, collection.ErrLeaseLost) {
		t.Fatal("revision change accepted", err)
	}
	if _, err := f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, collection.ErrNotFound) {
		t.Fatal("stale publication", err)
	}
	f.advance(t, 11*time.Minute)
	f.collectOnce(t)
	r, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	if err != nil || r.Attempt.State.Orchestration != domain.OrchestrationSucceeded || r.Attempt.State.Cancellation != domain.CancellationTooLate {
		t.Fatal("race fabricated cancellation", err)
	}
	f.assertSingleCompute(t, job)
}
