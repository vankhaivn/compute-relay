package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// copyArtifact verifies actual bytes and explicit EOF. The caller must additionally check
// source Close and protocol acknowledgement before publishing its destination.
// A destination may already contain bytes on failure; it must be unpublished.
func copyArtifact(ctx context.Context, dst io.Writer, src io.Reader, file artifactFile) (err error) {
	defer func() {
		if recover() != nil {
			err = errArtifactEvidence
		}
	}()
	if !file.Valid() || dst == nil || src == nil {
		return errArtifactEvidence
	}
	hash := sha256.New()
	remaining := file.Bytes
	buffer := make([]byte, 64<<10)
	defer clear(buffer)
	for {
		if ctx.Err() != nil {
			return errArtifactEvidence
		}
		// One extra byte detects overflow without ever writing it to the sink.
		part := buffer[:min(int64(len(buffer)), remaining+1)]
		n, readErr := src.Read(part)
		if n < 0 || n > len(part) || int64(n) > remaining {
			return errArtifactEvidence
		}
		if n > 0 {
			written, writeErr := dst.Write(part[:n])
			if written != n || writeErr != nil {
				return errArtifactEvidence
			}
			_, _ = hash.Write(part[:n])
			remaining -= int64(n)
		}
		if readErr != nil {
			if readErr != io.EOF || remaining != 0 || hex.EncodeToString(hash.Sum(nil)) != file.SHA256 || ctx.Err() != nil {
				return errArtifactEvidence
			}
			return nil
		}
		if n == 0 {
			return errArtifactEvidence // Do not spin indefinitely on an unproductive reader.
		}
	}
}
