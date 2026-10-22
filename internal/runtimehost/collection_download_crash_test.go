package runtimehost

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

const collectionCrashChunkBytes = 4096

// Unlike TestCollectionProcessKillAfterDurablePin, this child enters the real
// transfer pipe and records received bytes before the parent kills the process.
func TestCollectionProcessKillDuringResultDownload(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "collection-download-kill", "automatic")
	source := f.collectionSource(t)
	data := bytes.Repeat([]byte("download crash fixture\n"), 8192)
	source.files["outputs/result.json"] = data
	w, err := f.host.store.ClaimCollection(ctx, time.Second, 1)
	if err != nil || w == nil {
		t.Fatal("original collection claim", err)
	}
	p, err := f.services.resolver.Resolve(w.Binding)
	requireNoError(t, err)
	catalog, err := p.ListArtifacts(ctx, w.Observation.Remote)
	requireNoError(t, err)
	pin, err := collection.BuildSnapshot(*w, source.files[collection.ManifestPath], catalog, collection.DefaultConfig())
	requireNoError(t, err)
	requireNoError(t, f.host.store.PinCollection(ctx, *w, pin))
	fixture := collectionDownloadCrashFixture{Binding: w.Binding, Remote: w.Observation.Remote, Files: source.files}
	path := filepath.Join(t.TempDir(), "download.json")
	requireNoError(t, os.WriteFile(path, systemJSON(t, fixture), 0o600))
	root := f.host.root.Path
	f.stopHTTP(t)
	requireNoError(t, f.host.Close())
	f.advance(t, 2*time.Second)
	call, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(call, os.Args[0], "-test.run=^TestCollectionDownloadCrashChild$")
	cmd.Env = append(os.Environ(), "SYSTEM_COLLECTION_DOWNLOAD_ROOT="+root, "SYSTEM_COLLECTION_DOWNLOAD_FIXTURE="+path,
		"SYSTEM_COLLECTION_DOWNLOAD_TIME="+f.fixtureClock().Now().Format(time.RFC3339Nano))
	stdout, err := cmd.StdoutPipe()
	requireNoError(t, err)
	cmd.Stderr = os.Stderr
	requireNoError(t, cmd.Start())
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != fmt.Sprintf("collection-download-chunk:%d\n", collectionCrashChunkBytes) {
		t.Fatal("child did not reach actual payload transfer", line, err)
	}
	requireNoError(t, cmd.Process.Kill())
	if err = cmd.Wait(); err == nil {
		t.Fatal("download child was not killed")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("download child did not die by SIGKILL", cmd.ProcessState)
	}
	lists, _ := source.counts()
	f.reopen(t)
	interrupted := f.collectionStatus(t, job)
	if interrupted.State != "transferring" || interrupted.OperationID == nil || *interrupted.OperationID != w.Lease.OperationID ||
		interrupted.Progress == nil || interrupted.Progress.BytesReceived == nil || *interrupted.Progress.BytesReceived != collectionCrashChunkBytes ||
		interrupted.Progress.BytesCompleted != collectionCrashChunkBytes || interrupted.Progress.BytesTotal == nil || *interrupted.Progress.BytesTotal != int64(len(data)) {
		t.Fatal("kill boundary was not an incomplete durable payload read", interrupted)
	}
	if _, err = f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, collection.ErrNotFound) {
		t.Fatal("killed download published partial collection", err)
	}
	var output collection.File
	for _, file := range pin.Files {
		if file.Path == "outputs/result.json" {
			output = file
		}
	}
	if !output.Object.ID.Valid() {
		t.Fatal("fixture has no pinned output")
	}
	stream, err := f.host.results.Open(ctx, "app", output.Object.ID)
	if stream != nil {
		_ = stream.Close()
	}
	if !errors.Is(err, blobfs.ErrNotFound) {
		t.Fatal("killed download published partial blob", err)
	}
	// The transfer child cannot write a failure after SIGKILL. Retain uncertainty
	// and the accepted ticket rather than fabricating a new provider diagnostic.
	op, err := f.controls(t).Get(ctx, f.actor, "app", w.Lease.OperationID)
	requireNoError(t, err)
	if op.Operation.Status != domain.OperationAccepted || op.Operation.Failure != nil {
		t.Fatal("process death fabricated a failed collection", op)
	}
	f.advance(t, 11*time.Minute)
	repo := &collectionDownloadResumeRepository{Store: f.host.store}
	if worked, err := f.collectionEngine(t, repo).RunOnce(ctx); !worked || err != nil {
		t.Fatal("resume interrupted download", worked, err)
	}
	if repo.work == nil || repo.work.Lease.OperationID != w.Lease.OperationID || repo.work.Observation.Remote != w.Observation.Remote ||
		repo.work.Snapshot == nil || repo.work.Snapshot.Digest() != pin.Digest() {
		t.Fatal("download recovery discarded original pin, ticket or remote identity")
	}
	if after, _ := source.counts(); after != lists || source.fetches[output.Path] != 1 {
		t.Fatal("download recovery refreshed catalog or repeated recovery transfer")
	}
	status := f.collectionStatus(t, job)
	if status.State != "available" || status.OperationID == nil || *status.OperationID != w.Lease.OperationID {
		t.Fatal("download recovery did not finish original ticket", status)
	}
	op, err = f.controls(t).Get(ctx, f.actor, "app", w.Lease.OperationID)
	if err != nil || op.Effect != operations.ResultsAvailable || op.Operation.Status != domain.OperationSucceeded {
		t.Fatal("download recovery lost publication receipt", op, err)
	}
	reader, err := collection.NewReader(f.host.access, f.host.store, f.host.results)
	requireNoError(t, err)
	_, stream, err = reader.Open(ctx, f.actor, "app", job.JobID, job.AttemptID, output.ID)
	requireNoError(t, err)
	actual, readErr := io.ReadAll(io.LimitReader(stream, int64(len(data))+1))
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(actual, data) || provider.Digest(actual) != output.Object.SHA256 {
		t.Fatal("download recovery changed pinned bytes or digest", readErr, closeErr)
	}
	f.assertSingleCompute(t, job)
}

type collectionDownloadCrashFixture struct {
	Binding provider.BindingSnapshot
	Remote  provider.RemoteReference
	Files   map[string][]byte
}

type collectionDownloadResumeRepository struct {
	*sqlite.Store
	work *collection.Work
}

func (r *collectionDownloadResumeRepository) ClaimCollection(ctx context.Context, duration time.Duration, workers int) (*collection.Work, error) {
	w, err := r.Store.ClaimCollection(ctx, duration, workers)
	r.work = w
	return w, err
}

// Only the read-only collection boundary is implemented. No simulated remote
// backend or compute mutation is available inside the killed process.
type collectionDownloadCrashSource struct {
	provider.Provider
	fixture collectionDownloadCrashFixture
	clock   *fixedClock
}

func (s *collectionDownloadCrashSource) Resolve(binding provider.BindingSnapshot) (provider.Provider, error) {
	if binding != s.fixture.Binding {
		return nil, collection.ErrInvalid
	}
	return s, nil
}

func (s *collectionDownloadCrashSource) Verify(ctx context.Context, binding provider.BindingSnapshot) error {
	if binding != s.fixture.Binding {
		return collection.ErrInvalid
	}
	return ctx.Err()
}

func (s *collectionDownloadCrashSource) ListArtifacts(context.Context, provider.RemoteReference) ([]provider.Artifact, error) {
	return nil, errors.New("crash recovery must use the original pinned catalog")
}

func (s *collectionDownloadCrashSource) Fetch(ctx context.Context, remote provider.RemoteReference, artifact provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	data, ok := s.fixture.Files[artifact.Path]
	if !ok || remote != s.fixture.Remote || artifact.Validate(remote) != nil || artifact.Bytes > limit ||
		artifact.Bytes != int64(len(data)) || artifact.SHA256 != provider.Digest(data) {
		return provider.TransferResult{}, collection.ErrInvalid
	}
	if !strings.HasPrefix(artifact.Path, "outputs/") {
		return provider.CopyVerified(ctx, bytes.NewReader(data), provider.WithTransferProgress(dst, progress), artifact, limit)
	}
	if len(data) <= collectionCrashChunkBytes || progress == nil {
		return provider.TransferResult{}, collection.ErrInvalid
	}
	n, err := dst.Write(data[:collectionCrashChunkBytes])
	if err != nil || n != collectionCrashChunkBytes {
		return provider.TransferResult{}, errors.Join(err, io.ErrShortWrite)
	}
	// Move the controlled clock beyond the progress checkpoint interval. The
	// marker is emitted only after that real repository write acknowledges bytes.
	s.clock.advance(2 * time.Second)
	if err := progress(int64(n)); err != nil {
		return provider.TransferResult{}, err
	}
	fmt.Printf("collection-download-chunk:%d\n", n)
	<-ctx.Done()
	return provider.TransferResult{}, ctx.Err()
}

func TestCollectionDownloadCrashChild(t *testing.T) {
	root := os.Getenv("SYSTEM_COLLECTION_DOWNLOAD_ROOT")
	if root == "" {
		return
	}
	at, err := time.Parse(time.RFC3339Nano, os.Getenv("SYSTEM_COLLECTION_DOWNLOAD_TIME"))
	requireNoError(t, err)
	raw, err := os.ReadFile(os.Getenv("SYSTEM_COLLECTION_DOWNLOAD_FIXTURE"))
	requireNoError(t, err)
	var fixture collectionDownloadCrashFixture
	requireNoError(t, json.Unmarshal(raw, &fixture))
	h, err := Open(t.Context(), root)
	requireNoError(t, err)
	defer h.Close()
	clock := &fixedClock{now: at}
	injectHostClock(t, h, clock.Now)
	source := &collectionDownloadCrashSource{fixture: fixture, clock: clock}
	engine, err := collection.New(h.store, source, h.results, clock, collection.DefaultConfig())
	requireNoError(t, err)
	worked, err := engine.RunOnce(t.Context())
	t.Fatal("download child returned before SIGKILL", worked, err)
}
