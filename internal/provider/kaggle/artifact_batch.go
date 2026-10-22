package kaggle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

type artifactBatchInvocation func(context.Context, Config, []byte, artifactRequest, []artifactEntry, func(artifactEntry, io.Reader) error) error

var _ provider.ArtifactBatchFetcher = (*RuntimeAdapter)(nil)

func (p *RuntimeAdapter) FetchBatch(ctx context.Context, ref provider.RemoteReference, files []provider.Artifact, visit func(provider.Artifact, io.Reader) error) error {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		if provider.IsLocalStateError(err) {
			return err
		}
		return artifactFailure("artifact_binding_unavailable")
	}
	return c.artifacts.FetchBatch(ctx, ref, files, visit)
}

// FetchBatch shares a finite authenticated read envelope across exact pinned
// targets. Empty batches still verify the remote resource before publication.
func (a *ArtifactReader) FetchBatch(parent context.Context, ref provider.RemoteReference, files []provider.Artifact, visit func(provider.Artifact, io.Reader) error) (retErr error) {
	defer func() {
		if recover() != nil {
			retErr = artifactFailure("artifact_read_failed")
		}
	}()
	if a == nil || visit == nil || len(files) > a.policy.MaxFiles {
		return artifactFailure("artifact_request_invalid")
	}
	if err := a.executor.checkOperationalReference(ref); err != nil {
		return artifactFailure("artifact_remote_identity_mismatch")
	}
	targets := make([]artifactEntry, 0, len(files))
	pinned := make(map[string]provider.Artifact, len(files))
	paths := make(map[string]bool, len(files))
	var total int64
	for _, file := range files {
		if file.Validate(ref) != nil || !a.allowed(file.Path) {
			return artifactFailure("artifact_request_invalid")
		}
		if file.Bytes > a.policy.MaxBytes-total {
			return artifactFailure("artifact_byte_limit")
		}
		if limit, control := artifactControlLimit(file.Path); control && file.Bytes > limit {
			return artifactFailure("artifact_byte_limit")
		}
		key := strings.ToLower(file.Path)
		if paths[key] {
			return artifactFailure("artifact_request_invalid")
		}
		paths[key], pinned[file.Path] = true, file
		total += file.Bytes
		targets = append(targets, artifactEntry{file.Path, file.Bytes, file.SHA256})
	}
	for path := range paths {
		parts := strings.Split(path, "/")
		for i := 1; i < len(parts); i++ {
			if paths[strings.Join(parts[:i], "/")] {
				return artifactFailure("artifact_request_invalid")
			}
		}
	}
	budget := provider.ArtifactReadBudget(a.policy.Timeout, total, len(files))
	ctx, leave, err := a.enterFor(parent, budget)
	if err != nil {
		return artifactContextFailure(err)
	}
	defer leave()
	request := a.request
	request.Outputs = append([]artifactDeclaration(nil), request.Outputs...)
	var recorded kernelReference
	if closedObject([]byte(ref.Resource), &recorded) != nil {
		return artifactFailure("artifact_remote_identity_mismatch")
	}
	request.Execution.KernelID = recorded.KernelID
	seen := make(map[string]bool, len(files))
	var visitErr error
	visitor := func(entry artifactEntry, reader io.Reader) (err error) {
		defer func() {
			if recover() != nil {
				err = artifactFailure("artifact_write_failed")
			}
			if err != nil {
				visitErr = artifactFailure(provider.Reason(err, "artifact_write_failed")).WithCause(errors.Join(ErrArtifactTransfer, err))
			}
		}()
		file, ok := pinned[entry.Path]
		if !ok || seen[entry.Path] || entry != (artifactEntry{file.Path, file.Bytes, file.SHA256}) || reader == nil {
			return artifactFailure("artifact_target_mismatch")
		}
		checked := &artifactBatchReader{r: reader, hash: sha256.New()}
		if err := visit(file, checked); err != nil {
			return err
		}
		if checked.failed || checked.n != file.Bytes || hex.EncodeToString(checked.hash.Sum(nil)) != string(file.SHA256) {
			return artifactFailure("artifact_digest_mismatch")
		}
		seen[file.Path] = true
		return nil
	}
	called, invoked, violated := false, false, false
	err = a.executor.stager.credentials.WithCredential(ctx, a.executor.stager.config.CredentialRef, func(token []byte) error {
		if called {
			violated = true
			return ErrProtocol
		}
		called = true
		if !validArtifactToken(token) {
			return artifactFailure("artifact_credential_unavailable")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		invoked = true
		return a.runBatch(ctx, a.executor.stager.config, token, request, targets, visitor)
	})
	if provider.IsLocalStateError(err) {
		return err
	}
	if visitErr != nil {
		return visitErr
	}
	if ctx.Err() != nil {
		return artifactContextFailure(ctx.Err())
	}
	if !invoked {
		return artifactFailure("artifact_credential_unavailable")
	}
	if violated {
		return artifactFailure("artifact_request_invalid")
	}
	if err != nil {
		return boundedArtifactFailure(err)
	}
	if len(seen) != len(pinned) {
		return artifactFailure("artifact_target_mismatch")
	}
	return nil
}

func validArtifactToken(token []byte) bool {
	if len(token) == 0 || len(token) > credentials.MaxBytes {
		return false
	}
	for _, b := range token {
		if b < 33 || b > 126 {
			return false
		}
	}
	return true
}

type artifactBatchReader struct {
	r      io.Reader
	n      int64
	hash   hash.Hash
	failed bool
}

func (r *artifactBatchReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n < 0 || n > len(p) {
		r.failed = true
		return 0, ErrArtifactTransfer
	}
	if n > 0 {
		r.n += int64(n)
		_, _ = r.hash.Write(p[:n])
	}
	if err != nil && err != io.EOF {
		r.failed = true
	}
	return n, err
}

func runArtifactBatch(ctx context.Context, config Config, token []byte, request artifactRequest, targets []artifactEntry, visit func(artifactEntry, io.Reader) error) error {
	client, err := nativeClient(token, config)
	if err != nil {
		return artifactFailure("artifact_credential_unavailable")
	}
	defer client.Close()
	return artifactBatch(ctx, client, request, targets, visit)
}

func artifactBatch(ctx context.Context, client *kaggleapi.Client, request artifactRequest, targets []artifactEntry, visit func(artifactEntry, io.Reader) error) error {
	remote, err := openArtifactRemote(ctx, client, request)
	if err != nil {
		return err
	}
	selected := make(map[string]artifactEntry, len(remote.selected))
	for _, file := range remote.selected {
		selected[file.Path] = file
	}
	for _, target := range targets {
		_, control := artifactControlLimit(target.Path)
		if !control && selected[target.Path] != target || target.Path == artifactManifest && remote.manifestFile != target {
			return artifactFailure("artifact_target_mismatch")
		}
	}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return artifactContextFailure(err)
		}
		if target.Path == artifactManifest {
			if err := visit(target, bytes.NewReader(remote.manifest)); err != nil {
				return err
			}
			continue
		}
		if err := remote.visit(ctx, target, visit); err != nil {
			return err
		}
	}
	return remote.finish(ctx)
}

// visit joins the sole producer before returning, even when the consumer stops
// early. A complete stream is not success until download EOF/Close verifies.
func (r *artifactRemote) visit(parent context.Context, target artifactEntry, visit func(artifactEntry, io.Reader) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		var result error
		defer func() {
			if recover() != nil {
				result = artifactFailure("artifact_read_failed")
			}
			_ = writer.CloseWithError(result)
			done <- result
		}()
		actual, err := r.download(ctx, target.Path, target.Bytes, writer)
		if err != nil {
			result = artifactDownloadFailure("artifact_download_failed", err)
		} else if actual != target {
			result = artifactFailure("artifact_digest_mismatch")
		}
	}()
	visitErr := visit(target, reader)
	if visitErr != nil {
		_ = reader.CloseWithError(ErrArtifactTransfer)
		cancel()
	} else {
		_ = reader.Close()
	}
	transferErr := <-done
	if visitErr != nil {
		return visitErr
	}
	return transferErr
}
