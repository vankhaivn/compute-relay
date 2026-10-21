package collection

import (
	"bytes"
	"context"
	"errors"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"io"
	"testing"
	"time"
)

type reliabilitySource struct {
	provider.Provider
	verify func(context.Context) error
}

func (p reliabilitySource) Verify(ctx context.Context, _ provider.BindingSnapshot) error {
	return p.verify(ctx)
}

type reliabilityResolver struct{ p provider.Provider }

func (r reliabilityResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	return r.p, nil
}

type reliabilityRepository struct {
	Repository
	w      Work
	failed *Failure
}

func (r *reliabilityRepository) ClaimCollection(context.Context, time.Duration, int) (*Work, error) {
	return &r.w, nil
}
func (r *reliabilityRepository) RenewCollection(context.Context, Work, time.Duration) (time.Time, error) {
	return time.Now().Add(time.Minute), nil
}
func (r *reliabilityRepository) FailCollection(ctx context.Context, _ Work, f Failure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.failed = &f
	return nil
}

func TestVerifyFaultDoesNotInventIdentityMismatch(t *testing.T) {
	for _, identity := range []bool{false, true} {
		w, _, _ := manifestFixture(t)
		cause := error(errors.New("private transport fault"))
		if identity {
			cause = provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "frozen identity changed")
		}
		e := &Engine{resolver: reliabilityResolver{reliabilitySource{verify: func(context.Context) error { return cause }}}}
		_, err := e.resolve(context.Background(), w)
		var f Failure
		if !errors.As(err, &f) || f.Invalid != identity {
			t.Fatal("verify failure misclassified", f, err)
		}
		if !identity && (f.Code != domain.CodeArtifactCollectionFailed || f.Reason != "artifact_account_read_failed") {
			t.Fatal("transient verify is permanent", f)
		}
	}
}
func TestCollectionDeadlineRecordsRetryableEvidence(t *testing.T) {
	w, _, _ := manifestFixture(t)
	w.Snapshot = &Snapshot{}
	repo := &reliabilityRepository{w: w}
	cfg := DefaultConfig()
	cfg.Timeout = 20 * time.Millisecond
	e := &Engine{repo: repo, resolver: reliabilityResolver{reliabilitySource{verify: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}}, clock: wallClock{}, config: cfg, slots: make(chan struct{}, 1)}
	worked, err := e.RunOnce(context.Background())
	var f Failure
	if !worked || !errors.As(err, &f) || repo.failed == nil || repo.failed.Invalid || repo.failed.Reason != "artifact_call_timeout" {
		t.Fatal("deadline disappeared instead of persisting", worked, err, repo.failed)
	}
}
func TestCollectionTimeoutScalesWithPinnedBytes(t *testing.T) {
	cfg := DefaultConfig()
	e := &Engine{config: cfg}
	w := Work{Snapshot: &Snapshot{Files: []File{{Object: domain.ObjectMetadata{Bytes: 2 << 30}}}}}
	if got := e.collectionTimeout(w); got != cfg.Timeout+2048*time.Second {
		t.Fatal("size budget missing", got)
	}
	if got := e.collectionTimeout(Work{}); got != cfg.Timeout+4096*time.Second {
		t.Fatal("discovery ceiling missing", got)
	}
}

type slowCollectionSource struct{ reliabilitySource }

func (p slowCollectionSource) Fetch(ctx context.Context, _ provider.RemoteReference, _ provider.Artifact, _ io.Writer, _ int64, _ func(int64) error) (provider.TransferResult, error) {
	<-ctx.Done()
	return provider.TransferResult{}, ctx.Err()
}

type slowCollectionBlobs struct {
	BlobStore
	data map[domain.ObjectID][]byte
}

func (b *slowCollectionBlobs) Open(_ context.Context, _ domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	if raw, ok := b.data[id]; ok {
		return io.NopCloser(bytes.NewReader(raw)), nil
	}
	return nil, errors.New("not present")
}
func (b *slowCollectionBlobs) Put(_ context.Context, m domain.ObjectMetadata, r io.Reader) (domain.ObjectMetadata, error) {
	raw, err := io.ReadAll(r)
	if err == nil {
		b.data[m.ID] = raw
	}
	return m, err
}
func (r *reliabilityRepository) RecordCollectionProgress(context.Context, Work, string, domain.CollectionProgress) error {
	return nil
}
func TestSlowCollectionDeadlineCommitsFailedReceipt(t *testing.T) {
	w, m, files := manifestFixture(t)
	raw, catalog := snapshotFixture(t, w, m, files)
	snapshot, err := BuildSnapshot(w, raw, catalog, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	w.Snapshot = &snapshot
	repo := &reliabilityRepository{w: w}
	cfg := DefaultConfig()
	cfg.Timeout = time.Millisecond
	src := slowCollectionSource{reliabilitySource{verify: func(context.Context) error { return nil }}}
	e := &Engine{repo: repo, resolver: reliabilityResolver{src}, blobs: &slowCollectionBlobs{data: map[domain.ObjectID][]byte{}}, clock: wallClock{}, config: cfg, slots: make(chan struct{}, 1)}
	_, err = e.RunOnce(context.Background())
	var f Failure
	if !errors.As(err, &f) || repo.failed == nil || repo.failed.Reason != "artifact_call_timeout" || repo.failed.Invalid {
		t.Fatal("slow transfer lost failure receipt", err, repo.failed)
	}
}
