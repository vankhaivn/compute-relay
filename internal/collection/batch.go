package collection

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (e *Engine) discoveryFileLimit(w Work) int {
	var spec admission.Spec
	if json.Unmarshal(w.Plan.Job.Specification, &spec) != nil {
		return e.config.MaxFiles
	}
	files := 4 // Manifest, stdout, stderr and environment.
	for _, output := range spec.Outputs {
		if output.Kind == "directory" {
			return e.config.MaxFiles
		}
		files++
	}
	return min(files, e.config.MaxFiles)
}

func (e *Engine) verifyBatch(ctx context.Context, source provider.ArtifactBatchFetcher, w Work, snapshot Snapshot, progress *progressTracker) error {
	targets := make([]provider.Artifact, 0, len(snapshot.Files))
	files := make(map[string]File, len(snapshot.Files))
	for _, file := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		present, err := e.existing(ctx, file.Object)
		if err != nil {
			return err
		}
		if !present && file.Path == ManifestPath {
			meta, err := e.blobs.Put(ctx, file.Object, bytes.NewBufferString(snapshot.Manifest))
			if err != nil || meta != file.Object {
				return collectionFailureAt(err, "artifact_blob_write_failed")
			}
			present, err = e.existing(ctx, file.Object)
			if err != nil {
				return err
			}
			if !present {
				return collectionFailureAt(ErrUnavailable, "artifact_blob_write_failed")
			}
		}
		if present {
			if file.Role == "output" {
				progress.completed += file.Object.Bytes
			}
			continue
		}
		files[file.Path] = file
		targets = append(targets, provider.Artifact{Remote: w.Observation.Remote, Path: file.Path, Bytes: file.Object.Bytes, SHA256: file.Object.SHA256})
	}
	if err := progress.checkpoint(ctx, "transferring", false); err != nil {
		return err
	}
	visited := make(map[string]bool, len(targets))
	var visitErr error
	err := source.FetchBatch(ctx, w.Observation.Remote, append([]provider.Artifact(nil), targets...), func(artifact provider.Artifact, reader io.Reader) (err error) {
		if visitErr != nil {
			return visitErr
		}
		defer func() {
			if err != nil {
				visitErr = err
			}
		}()
		if err := ctx.Err(); err != nil {
			return err
		}
		file, ok := files[artifact.Path]
		if !ok || visited[artifact.Path] || artifact.Validate(w.Observation.Remote) != nil || artifact.Bytes != file.Object.Bytes || artifact.SHA256 != file.Object.SHA256 || reader == nil {
			return failureAt(domain.CodeArtifactCollectionFailed, true, "artifact_catalog_invalid")
		}
		visited[artifact.Path] = true
		checked := &batchReader{ctx: ctx, reader: io.LimitReader(reader, artifact.Bytes+1), hash: sha256.New()}
		if file.Role == "output" {
			checked.progress = progress.stream(ctx, file.Object.Bytes)
			checked.limit = file.Object.Bytes
		}
		meta, putErr := e.blobs.Put(ctx, file.Object, checked)
		if putErr != nil {
			var progressErr progressWriteError
			if provider.IsLocalStateError(putErr) || errors.As(putErr, &progressErr) {
				return putErr
			}
			reason := "artifact_blob_write_failed"
			if checked.readErr != nil {
				reason = "artifact_download_failed"
			}
			return collectionFailureAt(putErr, reason)
		}
		// Read through the provider's final EOF, even if a store stopped at the declared length.
		if _, err := io.Copy(io.Discard, checked); err != nil {
			return collectionFailureAt(err, "artifact_download_failed")
		}
		if meta != file.Object || checked.n != file.Object.Bytes || hex.EncodeToString(checked.hash.Sum(nil)) != string(file.Object.SHA256) {
			return failure(domain.CodeArtifactDigestMismatch, true)
		}
		if file.Role == "output" {
			progress.completed += file.Object.Bytes
			progress.current = 0
		}
		return progress.checkpoint(ctx, "transferring", false)
	})
	var progressErr progressWriteError
	if provider.IsLocalStateError(err) || errors.As(err, &progressErr) {
		return err
	}
	if visitErr != nil {
		return visitErr
	}
	if err != nil {
		return collectionFailureAt(err, "artifact_download_failed")
	}
	if len(visited) != len(targets) {
		return failure(domain.CodeArtifactMissing, false)
	}
	// The adapter's final identity/terminal acknowledgement precedes this proof.
	for _, file := range snapshot.Files {
		present, err := e.existing(ctx, file.Object)
		if err != nil {
			return err
		}
		if !present {
			return collectionFailureAt(ErrUnavailable, "artifact_blob_read_failed")
		}
	}
	return nil
}

type batchReader struct {
	ctx      context.Context
	reader   io.Reader
	hash     hash.Hash
	n        int64
	readErr  error
	progress func(int64) error
	limit    int64
}

func (r *batchReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		r.n += int64(n)
		_, _ = r.hash.Write(p[:n])
		if r.progress != nil {
			if progressErr := r.progress(min(r.n, r.limit)); progressErr != nil {
				r.readErr = progressErr
				return n, progressErr
			}
		}
	}
	if err != nil && err != io.EOF {
		r.readErr = err
	}
	return n, err
}
