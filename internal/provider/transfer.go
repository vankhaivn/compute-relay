package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// CopyVerified streams into a TEMPORARY destination, enforcing expected length/digest and
// a policy bound. It never publishes files. On error the caller must discard the output.
// Reader/writer implementations must themselves honor cancellation for blocking I/O.
func CopyVerified(ctx context.Context, source io.Reader, destination io.Writer, artifact Artifact, maxBytes int64) (TransferResult, error) {
	if err := ctx.Err(); err != nil {
		return TransferResult{}, err
	}
	if err := artifact.Validate(artifact.Remote); err != nil {
		return TransferResult{}, err
	}
	if source == nil || destination == nil || maxBytes <= 0 || artifact.Bytes > maxBytes {
		return TransferResult{}, errors.New("invalid transfer endpoint or byte budget")
	}
	hash := sha256.New()
	reader := &contextReader{ctx: ctx, source: source}
	n, err := io.Copy(io.MultiWriter(destination, hash), io.LimitReader(reader, artifact.Bytes))
	if err != nil {
		return TransferResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return TransferResult{}, err
	}
	if n != artifact.Bytes {
		return TransferResult{}, Problem(domain.CodeArtifactCollectionFailed, domain.FailureStageResults, "artifact byte count mismatch")
	}
	var extra [1]byte
	tail, tailErr := io.ReadFull(reader, extra[:])
	if tail != 0 {
		return TransferResult{}, Problem(domain.CodeArtifactCollectionFailed, domain.FailureStageResults, "artifact exceeds declared byte count")
	}
	if tailErr != io.EOF {
		return TransferResult{}, tailErr
	}
	digest := domain.SHA256Digest(hex.EncodeToString(hash.Sum(nil)))
	if digest != artifact.SHA256 {
		return TransferResult{}, Problem(domain.CodeArtifactDigestMismatch, domain.FailureStageResults, "artifact digest mismatch")
	}
	return TransferResult{Bytes: n, SHA256: digest}, nil
}

type contextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(p)
}
