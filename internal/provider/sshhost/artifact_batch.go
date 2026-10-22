package sshhost

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

var _ provider.ArtifactBatchFetcher = (*Adapter)(nil)

// FetchBatch shares authentication and identity checks while copying only the
// exact pinned files. Even an empty batch revalidates staged result identities.
func (a *Adapter) FetchBatch(ctx context.Context, remote provider.RemoteReference, artifacts []provider.Artifact, visit func(provider.Artifact, io.Reader) error) error {
	if a == nil || visit == nil || len(artifacts) > provider.MaxArtifactFiles {
		return failure(ErrRequest, "artifact_batch_request_invalid")
	}
	if remote.Validate() != nil || remote.Identity.InstanceID != a.config.InstanceID {
		return failure(ErrIdentity, "artifact_batch_identity_unverified")
	}
	var total int64
	seen := make(map[string]bool, len(artifacts))
	for _, artifact := range artifacts {
		key := strings.ToLower(artifact.Path)
		if artifact.Validate(remote) != nil || artifact.Bytes > provider.MaxArtifactBytes-total || seen[key] || !strings.HasPrefix(artifact.Path, "outputs/") && !strings.HasPrefix(artifact.Path, "control/") {
			return failure(ErrArtifact, "artifact_batch_request_invalid")
		}
		seen[key] = true
		total += artifact.Bytes
	}
	// Check collisions before opening the connection or requesting any data.
	for key := range seen {
		parts := strings.Split(key, "/")
		for i := 1; i < len(parts); i++ {
			if seen[strings.Join(parts[:i], "/")] {
				return failure(ErrArtifact, "artifact_batch_request_invalid")
			}
		}
	}
	call, cancel := context.WithCancel(ctx)
	if _, bounded := ctx.Deadline(); !bounded {
		cancel()
		call, cancel = context.WithTimeout(ctx, provider.ArtifactReadBudget(10*time.Minute, total, len(artifacts)))
	}
	defer cancel()
	return a.withDocker(call, func(d docker, _ Endpoint) error {
		info, err := inspectRemote(call, d, remote)
		if err != nil {
			return err
		}
		if !exited(info) {
			return failure(ErrNotDone, "artifact_batch_container_not_finished")
		}
		for _, artifact := range artifacts {
			if err := visitArtifact(call, cancel, d, info.ID, artifact, visit); err != nil {
				return err
			}
		}
		final, err := inspectRemote(call, d, remote)
		if err != nil {
			return err
		}
		if !exited(final) {
			return failure(ErrNotDone, "artifact_batch_container_not_finished")
		}
		return nil
	})
}

type artifactBatchReader struct {
	io.Reader
	complete bool
}

func (r *artifactBatchReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.complete = true
	}
	return n, err
}

func visitArtifact(ctx context.Context, cancel context.CancelFunc, d docker, container string, artifact provider.Artifact, visit func(provider.Artifact, io.Reader) error) (err error) {
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		var transferErr error
		defer func() {
			if recover() != nil {
				transferErr = failure(ErrArtifact, "artifact_batch_transfer_panic")
			}
			_ = writer.CloseWithError(transferErr)
			done <- transferErr
		}()
		_, transferErr = fetchArtifact(ctx, d, container, artifact, writer)
	}()
	joined := false
	defer func() {
		if recover() != nil {
			err = failure(ErrArtifact, "artifact_batch_visitor_panic")
		}
		if !joined {
			cancel()
			_ = reader.CloseWithError(err)
			<-done
		}
		_ = reader.Close()
	}()
	stream := &artifactBatchReader{Reader: reader}
	visitorErr := visit(artifact, stream)
	if visitorErr != nil || !stream.complete {
		cancel()
		closeErr := visitorErr
		if closeErr == nil {
			closeErr = failure(ErrArtifact, "artifact_batch_visitor_incomplete")
		}
		_ = reader.CloseWithError(closeErr)
	}
	transferErr := <-done
	joined = true
	if visitorErr != nil {
		return failure(visitorErr, "artifact_batch_visitor_failed")
	}
	if transferErr != nil {
		return transferErr
	}
	if !stream.complete {
		return failure(ErrArtifact, "artifact_batch_visitor_incomplete")
	}
	if err := ctx.Err(); err != nil {
		return failure(err, "artifact_batch_interrupted")
	}
	return nil
}
