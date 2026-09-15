package collection

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// The worker sees only observational/transfer methods after exact binding resolution.
type source interface {
	provider.BindingVerifier
	ListArtifacts(context.Context, provider.RemoteReference, provider.PageRequest) (provider.ArtifactPage, error)
	FetchArtifact(context.Context, provider.RemoteReference, provider.Artifact, io.Writer, int64) (provider.TransferResult, error)
}
type Engine struct {
	repo     Repository
	resolver Resolver
	blobs    BlobStore
	clock    Clock
	config   Config
	slots    chan struct{}
}

func New(repo Repository, resolver Resolver, blobs BlobStore, clock Clock, cfg Config) (*Engine, error) {
	if repo == nil || resolver == nil || blobs == nil || !cfg.Valid() {
		return nil, ErrInvalid
	}
	if clock == nil {
		clock = wallClock{}
	}
	return &Engine{repo: repo, resolver: resolver, blobs: blobs, clock: clock, config: cfg, slots: make(chan struct{}, cfg.Workers)}, nil
}

// RunOnce is finite. An uncertain SQL acknowledgement is never turned into a second
// mutation or a failed job. The durable lease expires; recovery reloads the same pin.
func (e *Engine) RunOnce(ctx context.Context) (bool, error) {
	select {
	case e.slots <- struct{}{}:
		defer func() { <-e.slots }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	w, err := e.repo.ClaimCollection(ctx, e.clock.Now().UTC(), e.config.Timeout+30*time.Second, e.config.Workers)
	if err != nil || w == nil {
		return false, err
	}
	if !w.Valid() {
		return true, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, e.config.Timeout)
	defer cancel()
	src, err := e.resolve(ctx, *w)
	if err != nil {
		return true, e.recordFailure(ctx, *w, err)
	}
	if w.Snapshot == nil {
		snapshot, err := e.discover(ctx, src, *w)
		if err != nil {
			return true, e.recordFailure(ctx, *w, err)
		}
		if err = e.repo.PinCollection(ctx, *w, snapshot, e.clock.Now().UTC()); err != nil {
			return true, err
		}
		w.Snapshot = &snapshot
	}
	proof, err := e.verifyFiles(ctx, src, *w)
	if err != nil {
		return true, e.recordFailure(ctx, *w, err)
	}
	// No failure write follows a possibly committed publication acknowledgement.
	return true, e.repo.CompleteCollection(ctx, *w, proof, e.clock.Now().UTC())
}
func (e *Engine) recordFailure(ctx context.Context, w Work, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	} // Restart/cancellation recovers the existing accepted ticket.
	f := Failure{Code: domain.CodeArtifactCollectionFailed}
	var known Failure
	if errors.As(err, &known) {
		f = known
	}
	if commitErr := e.repo.FailCollection(ctx, w, f, e.clock.Now().UTC()); commitErr != nil {
		return commitErr
	}
	return f
}
func (e *Engine) resolve(ctx context.Context, w Work) (src source, err error) {
	defer func() {
		if recover() != nil {
			src = nil
			err = ErrUnavailable
		}
	}()
	p, err := e.resolver.Resolve(w.Binding)
	if err != nil {
		return nil, ErrUnavailable
	}
	src, ok := p.(source)
	if !ok || src.VerifyBinding(ctx, w.Binding) != nil {
		return nil, failure(domain.CodeRemoteIdentityMismatch, true)
	}
	return src, nil
}
func (e *Engine) discover(ctx context.Context, src source, w Work) (snapshot Snapshot, err error) {
	defer func() {
		if recover() != nil {
			snapshot = Snapshot{}
			err = ErrUnavailable
		}
	}()
	cursor := ""
	seen := map[string]bool{}
	catalog := []provider.Artifact{}
	for pages := 0; ; pages++ {
		if ctx.Err() != nil {
			return Snapshot{}, ctx.Err()
		}
		if pages >= MaxFiles || seen[cursor] {
			return Snapshot{}, failure(domain.CodeArtifactCollectionFailed, true)
		}
		seen[cursor] = true
		page, err := src.ListArtifacts(ctx, w.Observation.Remote, provider.PageRequest{Cursor: cursor, Limit: 100})
		if err != nil {
			return Snapshot{}, ErrUnavailable
		}
		if len(page.Artifacts) > 100 || len(page.NextCursor) > 512 || len(catalog)+len(page.Artifacts) > e.config.MaxFiles {
			return Snapshot{}, failure(domain.CodeArtifactCollectionFailed, true)
		}
		catalog = append(catalog, page.Artifacts...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	// Check all identities and collisions before the first byte request.
	paths := make([]string, 0, len(catalog))
	var manifestFile *provider.Artifact
	for i := range catalog {
		a := &catalog[i]
		if a.Validate(w.Observation.Remote) != nil {
			return Snapshot{}, failure(domain.CodeRemoteIdentityMismatch, true)
		}
		paths = append(paths, a.Path)
		if a.Path == ManifestPath {
			manifestFile = a
		}
	}
	if !collisionFree(paths) {
		return Snapshot{}, failure(domain.CodeArtifactCollectionFailed, true)
	}
	if manifestFile == nil {
		return Snapshot{}, failure(domain.CodeResultManifestMissing, false)
	}
	if manifestFile.Bytes > MaxManifestBytes {
		return Snapshot{}, failure(domain.CodeArtifactCollectionFailed, true)
	}
	var raw bytes.Buffer
	writer := newCheckedWriter(ctx, &raw, manifestFile.Bytes)
	result, err := src.FetchArtifact(ctx, w.Observation.Remote, *manifestFile, writer, manifestFile.Bytes)
	if err != nil {
		return Snapshot{}, ErrUnavailable
	}
	if !writer.matches(*manifestFile, result) {
		return Snapshot{}, failure(domain.CodeArtifactDigestMismatch, true)
	}
	return BuildSnapshot(w, raw.Bytes(), catalog, e.config)
}
func (e *Engine) verifyFiles(ctx context.Context, src source, w Work) (proof Verified, err error) {
	defer func() {
		if recover() != nil {
			proof = Verified{}
			err = ErrUnavailable
		}
	}()
	if w.Snapshot == nil || ValidateSnapshot(w, *w.Snapshot) != nil {
		return proof, ErrInvalid
	}
	snapshot := w.Snapshot.Clone()
	if len(snapshot.Files) > e.config.MaxFiles {
		return proof, ErrInvalid
	}
	var total int64
	for _, file := range snapshot.Files {
		if file.Object.Bytes > e.config.MaxBytes-total {
			return proof, ErrInvalid
		}
		total += file.Object.Bytes
	}
	for _, file := range snapshot.Files {
		if ctx.Err() != nil {
			return proof, ctx.Err()
		}
		present, err := e.existing(ctx, file.Object)
		if err != nil {
			return proof, err
		}
		if present {
			continue
		}
		if file.Path == ManifestPath {
			meta, err := e.blobs.Put(ctx, file.Object, bytes.NewBufferString(snapshot.Manifest))
			if err != nil || meta != file.Object {
				return proof, ErrUnavailable
			}
		} else if err = e.transfer(ctx, src, w.Observation.Remote, file); err != nil {
			return proof, err
		}
		present, err = e.existing(ctx, file.Object)
		if err != nil {
			return proof, err
		}
		if !present {
			return proof, ErrUnavailable
		}
	}
	m, _, err := parseManifest(w, []byte(snapshot.Manifest))
	if err != nil {
		return proof, err
	}
	return Verified{lease: w.Lease, snapshot: snapshot, phase: m.Phase}, nil
}
func (e *Engine) existing(ctx context.Context, m domain.ObjectMetadata) (bool, error) {
	r, err := e.blobs.Open(ctx, m.WorkspaceID, m.ID)
	if err != nil {
		return false, nil
	} // Put is create-only; unreadable existing files cannot be overwritten.
	if r == nil {
		return false, ErrUnavailable
	}
	h := sha256.New()
	n, readErr := io.Copy(h, io.LimitReader(contextReader{ctx, r}, m.Bytes+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil {
		return false, ErrUnavailable
	}
	if n != m.Bytes || hex.EncodeToString(h.Sum(nil)) != string(m.SHA256) {
		return false, failure(domain.CodeArtifactDigestMismatch, true)
	}
	return true, nil
}
func (e *Engine) transfer(ctx context.Context, src source, remote provider.RemoteReference, file File) (err error) {
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	artifact := provider.Artifact{Remote: remote, Path: file.Path, Bytes: file.Object.Bytes, SHA256: file.Object.SHA256}
	go func() {
		var final error
		defer func() {
			if recover() != nil {
				final = ErrUnavailable
			}
			_ = writer.CloseWithError(final)
			done <- final
		}()
		checked := newCheckedWriter(ctx, writer, artifact.Bytes)
		result, err := src.FetchArtifact(ctx, remote, artifact, checked, artifact.Bytes)
		if err != nil {
			final = ErrUnavailable
			return
		}
		if !checked.matches(artifact, result) {
			final = failure(domain.CodeArtifactDigestMismatch, true)
		}
		// EOF is sent only AFTER the provider return and our independent checksum check.
	}()
	// Closing the reader unblocks a writer after disk failure. Wait for the callback;
	// a fixed worker is not replaced while a misbehaving adapter ignores cancellation.
	joined := false
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
		_ = reader.CloseWithError(ErrUnavailable)
		if !joined {
			<-done
		}
	}()
	meta, putErr := e.blobs.Put(ctx, file.Object, reader)
	_ = reader.CloseWithError(putErr)
	transferErr := <-done
	joined = true
	if transferErr != nil {
		return transferErr
	}
	if putErr != nil || meta != file.Object {
		return ErrUnavailable
	}
	return nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type checkedWriter struct {
	ctx      context.Context
	dst      io.Writer
	hash     hash.Hash
	n, limit int64
	failed   bool
}

func newCheckedWriter(ctx context.Context, w io.Writer, limit int64) *checkedWriter {
	return &checkedWriter{ctx: ctx, dst: w, hash: sha256.New(), limit: limit}
}
func (w *checkedWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		w.failed = true
		return 0, err
	}
	if w.failed || int64(len(p)) > w.limit-w.n {
		w.failed = true
		return 0, ErrInvalid
	}
	n, err := w.dst.Write(p)
	if n < 0 || n > len(p) {
		w.failed = true
		return 0, ErrInvalid
	}
	_, _ = w.hash.Write(p[:n])
	w.n += int64(n)
	if err != nil || n != len(p) {
		w.failed = true
		if err == nil {
			err = io.ErrShortWrite
		}
	}
	return n, err
}
func (w *checkedWriter) matches(a provider.Artifact, r provider.TransferResult) bool {
	return !w.failed && w.n == a.Bytes && r.Bytes == a.Bytes && r.SHA256 == a.SHA256 && hex.EncodeToString(w.hash.Sum(nil)) == string(a.SHA256)
}

// Run supplies an explicitly composed, fixed-size transfer pool. It is never
// started by admission, GET, migration or a production CLI fallback.
func (e *Engine) Run(ctx context.Context) error {
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, e.config.Workers)
	for i := 0; i < e.config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, err := e.RunOnce(run)
				var failedAttempt Failure
				if err != nil && !errors.As(err, &failedAttempt) {
					select {
					case errs <- err:
					default:
					}
					cancel()
					return
				}
				timer := time.NewTimer(e.config.PollDelay)
				select {
				case <-run.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return ctx.Err()
	}
}
