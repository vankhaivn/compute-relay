// Package ports defines small infrastructure seams. Implementations are supplied by
// the composition root; transactional runtime metadata persistence belongs to M3.
package ports

import (
	"context"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// Clock permits deterministic time without sleeping in contract tests.
type Clock interface{ Now() time.Time }
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// CredentialRef is an operator-configured reference, NEVER the credential value.
type CredentialRef string

// WithCredential scopes transient bytes to a callback. Implementations must not log them,
// must reject unconfigured sources, and must erase owned buffers on all return paths.
// Callbacks must not retain bytes. This is hygiene, not protection against the host owner.
type CredentialResolver interface {
	WithCredential(context.Context, CredentialRef, func([]byte) error) error
}

// ObjectMetadata aliases the common value type to preserve the original BlobStore seam.
// Put declarations use Bytes=-1 for unknown length and SHA256="" to calculate a digest.
// Successful results always have nonnegative Bytes and a canonical SHA256.
type ObjectMetadata = domain.ObjectMetadata

// BlobStore owns temporary writes, size/digest verification and atomic publication.
// Every lookup is workspace-scoped; no arbitrary host path is accepted.
// Published bytes are not API-visible until a separate ownership metadata commit.
type BlobStore interface {
	Put(context.Context, ObjectMetadata, io.Reader) (ObjectMetadata, error)
	Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error)
}

// EventSink is optional post-commit delivery, NOT authoritative event persistence.
// Delivery may repeat and consumers must deduplicate by event ID/sequence. Failure never
// rolls back committed state or triggers compute. Store owns the transactional event log.
type EventSink interface {
	Publish(context.Context, domain.Event) error
}
