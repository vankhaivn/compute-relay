package collection

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type batchSource struct {
	provider.Provider
	data             map[string][]byte
	mode             string
	batches, fetches int
	targetCounts     []int
	joined           chan struct{}
}

func (s *batchSource) Verify(context.Context, provider.BindingSnapshot) error { return nil }
func (s *batchSource) Fetch(context.Context, provider.RemoteReference, provider.Artifact, io.Writer, int64, func(int64) error) (provider.TransferResult, error) {
	s.fetches++
	return provider.TransferResult{}, errors.New("batch must never fall back")
}
func (s *batchSource) FetchBatch(ctx context.Context, _ provider.RemoteReference, targets []provider.Artifact, visit func(provider.Artifact, io.Reader) error) error {
	s.batches++
	s.targetCounts = append(s.targetCounts, len(targets))
	for i, artifact := range targets {
		if s.mode == "missing" && i == 0 {
			continue
		}
		if s.mode == "changed_target" && i == 0 {
			artifact.Bytes++
		}
		raw := s.data[artifact.Path]
		if s.mode == "changed_bytes" && i == 0 {
			raw = []byte("changed")
		}
		if s.mode == "cancel" {
			reader, writer := io.Pipe()
			done := make(chan struct{})
			go func() { defer close(done); <-ctx.Done(); _ = writer.CloseWithError(ctx.Err()) }()
			err := visit(artifact, reader)
			_ = reader.Close()
			<-done
			close(s.joined)
			return err
		}
		if err := visit(artifact, bytes.NewReader(raw)); err != nil {
			return err
		}
		if s.mode == "ignore_callback" && i == 0 {
			_ = visit(artifact, bytes.NewReader(raw))
		}
		if s.mode == "duplicate" && i == 0 {
			return visit(artifact, bytes.NewReader(raw))
		}
	}
	if s.mode == "late_failure" {
		return provider.WithReason(errors.New("private canary"), "artifact_remote_identity_mismatch")
	}
	return nil
}

type batchRepository struct {
	reliabilityRepository
	publications int
}

func (r *batchRepository) CompleteCollection(_ context.Context, w Work, proof Verified) error {
	if !proof.Matches(w) {
		return ErrInvalid
	}
	r.publications++
	return nil
}

func batchFixture(t *testing.T, count int) (*Engine, *batchRepository, *batchSource) {
	t.Helper()
	w, m, _ := manifestFixture(t)
	w.Plan.Job.Specification = []byte(`{"outputs":[{"path":"items","kind":"directory","required":true}],"resources":{"accelerator":"cpu"}}`)
	w.Plan.Job.SpecificationSHA256 = provider.Digest(w.Plan.Job.Specification)
	files := map[string][]byte{}
	artifacts := make([]any, 0, count)
	for i := range count {
		name := fmt.Sprintf("items/%04d", i)
		raw := []byte("x")
		files["outputs/"+name] = raw
		artifacts = append(artifacts, map[string]any{"path": name, "bytes": len(raw), "sha256": provider.Digest(raw)})
	}
	m["artifacts"] = artifacts
	raw, catalog := snapshotFixture(t, w, m, files)
	snapshot, err := BuildSnapshot(w, raw, catalog, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	w.Snapshot = &snapshot
	source := &batchSource{data: files}
	repo := &batchRepository{reliabilityRepository: reliabilityRepository{w: w}}
	e := &Engine{repo: repo, resolver: reliabilityResolver{source}, blobs: &slowCollectionBlobs{data: map[domain.ObjectID][]byte{}}, clock: wallClock{}, config: DefaultConfig(), slots: make(chan struct{}, 1)}
	return e, repo, source
}
func TestCollectionBatchesFiveHundredTinyArtifacts(t *testing.T) {
	e, repo, source := batchFixture(t, 500)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	worked, err := e.RunOnce(ctx)
	if err != nil || !worked || repo.publications != 1 || source.batches != 1 || source.fetches != 0 || len(source.targetCounts) != 1 || source.targetCounts[0] != 500 {
		t.Fatal("collection did not use one exact batch", worked, err, repo.publications, source.batches, source.fetches, source.targetCounts)
	}
}
func TestCollectionBatchFailureNeverPublishesOrFallsBack(t *testing.T) {
	for _, mode := range []string{"duplicate", "missing", "changed_target", "changed_bytes", "late_failure", "ignore_callback"} {
		t.Run(mode, func(t *testing.T) {
			e, repo, source := batchFixture(t, 3)
			source.mode = mode
			_, err := e.RunOnce(context.Background())
			if err == nil || repo.failed == nil || repo.publications != 0 || source.fetches != 0 || source.batches != 1 {
				t.Fatal("failed batch escaped", err, repo.publications, source.fetches)
			}
		})
	}
}
func TestCollectionCachedBatchStillRequiresFinalAcknowledgement(t *testing.T) {
	e, repo, source := batchFixture(t, 3)
	source.mode = "late_failure"
	if _, err := e.RunOnce(context.Background()); err == nil {
		t.Fatal("late failure accepted")
	}
	// Every blob now exists, but final remote identity is still unconfirmed.
	if _, err := e.RunOnce(context.Background()); err == nil || repo.publications != 0 || source.targetCounts[1] != 0 {
		t.Fatal("cached bytes bypassed late failure", err, repo.publications, source.targetCounts)
	}
	source.mode = ""
	if _, err := e.RunOnce(context.Background()); err != nil || repo.publications != 1 || source.targetCounts[2] != 0 {
		t.Fatal("original batch did not recover through fresh observation", err, repo.publications, source.targetCounts)
	}
}
func TestCollectionBatchCancellationJoinsBeforeReturn(t *testing.T) {
	e, repo, source := batchFixture(t, 3)
	source.mode = "cancel"
	source.joined = make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := e.RunOnce(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || repo.publications != 0 || repo.failed != nil {
		t.Fatal("shutdown changed collection", err, repo.publications, repo.failed)
	}
	select {
	case <-source.joined:
	default:
		t.Fatal("batch producer still running")
	}
}

type individualSource struct {
	provider.Provider
	source *batchSource
}

func (s individualSource) Verify(context.Context, provider.BindingSnapshot) error { return nil }
func (s individualSource) Fetch(ctx context.Context, _ provider.RemoteReference, artifact provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	s.source.fetches++
	return provider.CopyVerified(ctx, bytes.NewReader(s.source.data[artifact.Path]), provider.WithTransferProgress(dst, progress), artifact, limit)
}
func TestCollectionRetainsIndividualProviderFallback(t *testing.T) {
	e, repo, source := batchFixture(t, 3)
	e.resolver = reliabilityResolver{individualSource{source: source}}
	if _, err := e.RunOnce(context.Background()); err != nil || repo.publications != 1 || source.batches != 0 || source.fetches != 3 {
		t.Fatal("individual provider behavior changed", err, repo.publications, source.batches, source.fetches)
	}
}
func TestCollectionBudgetAccountsForDeclaredFilesAndDirectoryCeiling(t *testing.T) {
	e, _, _ := batchFixture(t, 1)
	w, _, _ := manifestFixture(t)
	if got, want := e.collectionTimeout(w), provider.ArtifactReadBudget(e.config.Timeout, e.config.MaxBytes, 5); got != want {
		t.Fatal("file declarations lost", got, want)
	}
	w.Plan.Job.Specification = []byte(`{"outputs":[{"path":"dir","kind":"directory"}]}`)
	if got := e.collectionTimeout(w); got != 2*time.Hour {
		t.Fatal("unbounded directory count lost finite ceiling", got)
	}
	if got := provider.ArtifactReadBudget(time.Minute, 500, 500); got != time.Minute+501*time.Second {
		t.Fatal("tiny-file overhead missing", got)
	}
	for _, bytes := range []int64{-1, 1<<63 - 1} {
		if got := provider.ArtifactReadBudget(time.Minute, bytes, 500); got != 2*time.Hour {
			t.Fatal("budget overflow", got)
		}
	}
}
