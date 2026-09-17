package artifactwire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// Copy verifies actual bytes and explicit EOF. The caller must additionally check
// source Close and protocol acknowledgement before publishing its destination.
// A destination may already contain bytes on failure; it must be unpublished.
func Copy(ctx context.Context, dst io.Writer, src io.Reader, file File) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrInvalid
		}
	}()
	if !file.Valid() || dst == nil || src == nil {
		return ErrInvalid
	}
	hash := sha256.New()
	remaining := file.Bytes
	buffer := make([]byte, 64<<10)
	defer clear(buffer)
	for {
		if ctx.Err() != nil {
			return ErrInvalid
		}
		// One extra byte detects overflow without ever writing it to the sink.
		part := buffer[:min(int64(len(buffer)), remaining+1)]
		n, readErr := src.Read(part)
		if n < 0 || n > len(part) || int64(n) > remaining {
			return ErrInvalid
		}
		if n > 0 {
			written, writeErr := dst.Write(part[:n])
			if written != n || writeErr != nil {
				return ErrInvalid
			}
			_, _ = hash.Write(part[:n])
			remaining -= int64(n)
		}
		if readErr != nil {
			if readErr != io.EOF || remaining != 0 || hex.EncodeToString(hash.Sum(nil)) != file.SHA256 || ctx.Err() != nil {
				return ErrInvalid
			}
			return nil
		}
		if n == 0 {
			return ErrInvalid // Do not spin indefinitely on an unproductive reader.
		}
	}
}
