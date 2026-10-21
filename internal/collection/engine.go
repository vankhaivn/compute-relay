package collection

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

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
func (e *Engine) RunOnce(ctx context.Context) (worked bool, resultErr error) {
	select {
	case e.slots <- struct{}{}:
		defer func() { <-e.slots }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	w, err := e.repo.ClaimCollection(ctx, worker.LeaseDuration, e.config.Workers)
	if err != nil || w == nil {
		return false, err
	}
	if !w.Valid() {
		return true, ErrInvalid
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, e.collectionTimeout(*w))
	defer cancel()
	renewWork := *w
	call, stop := worker.Heartbeat(ctx, w.Lease.Until, e.clock.Now, func(c context.Context) (time.Time, error) {
		return e.repo.RenewCollection(c, renewWork, worker.LeaseDuration)
	})
	defer func() {
		resultErr = worker.HeartbeatError(resultErr, stop())
	}()
	recordFailure := func(err error) error {
		var progressErr progressWriteError
		if provider.IsLocalStateError(err) || errors.As(err, &progressErr) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return err
		}
		if renewalErr := stop(); renewalErr != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return worker.HeartbeatError(err, renewalErr)
			}
			// Provider failures normally become collection evidence, not fatal store errors.
			return worker.HeartbeatError(collectionFailure(err), renewalErr)
		}
		if parent.Err() != nil {
			return parent.Err()
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = Failure{Code: domain.CodeArtifactCollectionFailed, Reason: "artifact_call_timeout"}
		}
		record, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return e.recordFailure(record, *w, err)
	}
	src, err := e.resolve(call, *w)
	if err != nil {
		return true, recordFailure(err)
	}
	if w.Snapshot == nil {
		snapshot, err := e.discover(call, src, *w)
		if err != nil {
			return true, recordFailure(err)
		}
		if err = e.repo.PinCollection(call, *w, snapshot); err != nil {
			return true, err
		}
		w.Snapshot = &snapshot
	}
	proof, err := e.verifyFiles(call, src, *w)
	if err != nil {
		return true, recordFailure(err)
	}
	if err := stop(); err != nil {
		return true, err
	}
	// No failure write follows a possibly committed publication acknowledgement.
	return true, e.repo.CompleteCollection(ctx, *w, proof)
}
func (e *Engine) recordFailure(ctx context.Context, w Work, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	} // Restart/cancellation recovers the existing accepted ticket.
	f := collectionFailure(err)
	if commitErr := e.repo.FailCollection(ctx, w, f); commitErr != nil {
		return commitErr
	}
	return f
}

func collectionFailure(err error) Failure {
	f := Failure{Code: domain.CodeArtifactCollectionFailed}
	var known Failure
	if errors.As(err, &known) {
		f = known
	} else {
		var problem domain.Problem
		if errors.As(err, &problem) && problem.Code == domain.CodeArtifactCollectionFailed {
			f.Reason = problem.Details[domain.ReasonDetail]
		}
	}
	if f.Code != domain.CodeArtifactCollectionFailed || !domain.ValidCollectionReason(f.Reason) {
		f.Reason = ""
	}
	return f
}
func (e *Engine) resolve(ctx context.Context, w Work) (src provider.Provider, err error) {
	defer func() {
		if recover() != nil {
			src = nil
			err = ErrUnavailable
		}
	}()
	p, err := e.resolver.Resolve(w.Binding)
	if err != nil {
		if provider.IsLocalStateError(err) {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, Failure{Code: domain.CodeArtifactCollectionFailed, Reason: "artifact_binding_unavailable"}
	}
	src = p
	if src == nil {
		return nil, Failure{Code: domain.CodeArtifactCollectionFailed, Reason: "artifact_binding_unavailable"}
	}
	if err := src.Verify(ctx, w.Binding); err != nil {
		if provider.IsLocalStateError(err) {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var value domain.Problem
		var pointer *domain.Problem
		if errors.As(err, &value) && value.Code == domain.CodeRemoteIdentityMismatch || errors.As(err, &pointer) && pointer != nil && pointer.Code == domain.CodeRemoteIdentityMismatch {
			return nil, failure(domain.CodeRemoteIdentityMismatch, true)
		}
		return nil, Failure{Code: domain.CodeArtifactCollectionFailed, Reason: "artifact_account_read_failed"}
	}
	return src, nil
}
func (e *Engine) discover(ctx context.Context, src provider.Provider, w Work) (snapshot Snapshot, err error) {
	defer func() {
		if recover() != nil {
			snapshot = Snapshot{}
			err = ErrUnavailable
		}
	}()
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	catalog, err := src.ListArtifacts(ctx, w.Observation.Remote)
	if err != nil {
		if provider.IsLocalStateError(err) {
			return Snapshot{}, err
		}
		return Snapshot{}, collectionFailure(err)
	}
	if len(catalog) > e.config.MaxFiles {
		return Snapshot{}, failure(domain.CodeArtifactCollectionFailed, true)
	}
	var total int64
	for _, artifact := range catalog {
		if artifact.Bytes < 0 || artifact.Bytes > provider.MaxArtifactBytes-total {
			return Snapshot{}, failure(domain.CodeArtifactCollectionFailed, true)
		}
		total += artifact.Bytes
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
	result, err := src.Fetch(ctx, w.Observation.Remote, *manifestFile, writer, manifestFile.Bytes, nil)
	if err != nil {
		if provider.IsLocalStateError(err) {
			return Snapshot{}, err
		}
		return Snapshot{}, collectionFailure(err)
	}
	if !writer.matches(*manifestFile, result) {
		return Snapshot{}, failure(domain.CodeArtifactDigestMismatch, true)
	}
	return BuildSnapshot(w, raw.Bytes(), catalog, e.config)
}
func (e *Engine) verifyFiles(ctx context.Context, src provider.Provider, w Work) (proof Verified, err error) {
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
	progress := newProgressTracker(e, w)
	defer func() {
		var progressErr progressWriteError
		if err != nil && ctx.Err() == nil && !errors.As(err, &progressErr) {
			if sampleErr := progress.checkpoint(ctx, "transferring", true); sampleErr != nil {
				if provider.IsLocalStateError(err) {
					err = errors.Join(err, sampleErr)
				} else {
					err = sampleErr
				}
			}
		}
	}()
	if err := progress.checkpoint(ctx, "transferring", true); err != nil {
		return proof, err
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
			if file.Role == "output" {
				progress.completed += file.Object.Bytes
			}
			if err := progress.checkpoint(ctx, "transferring", false); err != nil {
				return proof, err
			}
			continue
		}
		if file.Path == ManifestPath {
			meta, err := e.blobs.Put(ctx, file.Object, bytes.NewBufferString(snapshot.Manifest))
			if err != nil || meta != file.Object {
				return proof, ErrUnavailable
			}
		} else {
			var callback func(int64) error
			if file.Role == "output" {
				callback = progress.stream(ctx, file.Object.Bytes)
			}
			if err = e.transfer(ctx, src, w.Observation.Remote, file, callback); err != nil {
				return proof, err
			}
			if callback != nil && progress.current != file.Object.Bytes {
				return proof, ErrInvalid
			}
		}
		present, err = e.existing(ctx, file.Object)
		if err != nil {
			return proof, err
		}
		if !present {
			return proof, ErrUnavailable
		}
		if file.Role == "output" {
			progress.completed += file.Object.Bytes
			progress.current = 0
		}
		if err := progress.checkpoint(ctx, "transferring", false); err != nil {
			return proof, err
		}
	}
	if err := progress.checkpoint(ctx, "verifying", true); err != nil {
		return proof, err
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
func (e *Engine) transfer(ctx context.Context, src provider.Provider, remote provider.RemoteReference, file File, progress func(int64) error) (err error) {
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
		result, err := src.Fetch(ctx, remote, artifact, checked, artifact.Bytes, progress)
		if err != nil {
			var progressErr progressWriteError
			final = collectionFailure(err)
			if provider.IsLocalStateError(err) || errors.As(err, &progressErr) {
				final = err
			}
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
	return worker.Run(ctx, worker.Config{Workers: e.config.Workers, PollDelay: e.config.PollDelay}, func(ctx context.Context, _ int) (bool, error) {
		return e.RunOnce(ctx)
	}, func(err error) worker.Decision {
		if provider.IsLocalStateError(err) {
			return worker.Classify(err)
		}
		var failedAttempt Failure
		if errors.As(err, &failedAttempt) || errors.Is(err, ErrLeaseLost) {
			return worker.Continue
		}
		return worker.Classify(err)
	})
}

// Allow at least one MiB/s plus the control/verification budget. Before discovery,
// use the admitted byte ceiling; pinned retries use the exact known byte count.
func (e *Engine) collectionTimeout(w Work) time.Duration {
	bytes := e.config.MaxBytes
	if w.Snapshot != nil {
		bytes = 0
		for _, file := range w.Snapshot.Files {
			bytes += file.Object.Bytes
		}
	}
	return min(2*time.Hour, e.config.Timeout+time.Duration((bytes+(1<<20)-1)/(1<<20))*time.Second)
}
