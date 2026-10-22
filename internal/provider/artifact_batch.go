package provider

import (
	"context"
	"io"
	"time"
)

// ArtifactBatchFetcher shares one read-only verification envelope across exact
// pinned targets. It is optional; providers without it retain individual Fetch.
//
// Each target is visited exactly once, synchronously, with only its bytes. The
// adapter must verify each complete stream and the frozen terminal resource
// before returning success. It must cancel and join every transfer producer
// before returning, including after visitor failure or cancellation. The reader
// is valid only during its callback; callers must not retain it.
//
// Empty targets still require a fresh terminal/identity verification. Earlier
// verified temporary blobs never replace this invocation's final acknowledgement.
// On any failure callers retain the original attempt and never fall back to Fetch.
// Callbacks may stage verified blobs, but cannot publish a partial result set.
type ArtifactBatchFetcher interface {
	FetchBatch(context.Context, RemoteReference, []Artifact, func(Artifact, io.Reader) error) error
}

// ArtifactReadBudget bounds the control envelope, transfer throughput and per-file
// request overhead. Unknown counts/sizes get the finite ceiling, never overflow.
func ArtifactReadBudget(base time.Duration, bytes int64, files int) time.Duration {
	const ceiling = 2 * time.Hour
	if base >= ceiling || bytes < 0 || files < 0 {
		return ceiling
	}
	if base < 0 {
		base = 0
	}
	seconds := bytes / (1 << 20)
	if bytes%(1<<20) != 0 {
		seconds++
	}
	remaining := int64((ceiling - base) / time.Second)
	if seconds > remaining || int64(files) > remaining-seconds {
		return ceiling
	}
	return base + time.Duration(seconds+int64(files))*time.Second
}
