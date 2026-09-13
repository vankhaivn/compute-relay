// Package ports defines small infrastructure seams. There are deliberately no database,
// host credential, or production blob implementations in the M2-04 foundation.
package ports

import (
	"context"
	"errors"
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

type ObjectMetadata struct {
	ID          domain.ObjectID
	WorkspaceID domain.WorkspaceID
	Bytes       int64
	SHA256      domain.SHA256Digest
}

// BlobStore owns temporary writes, size/digest verification and atomic publication.
// Every lookup is workspace-scoped; no arbitrary host path is accepted.
type BlobStore interface {
	Put(context.Context, ObjectMetadata, io.Reader) (ObjectMetadata, error)
	Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error)
}

// Store is the first consumer-sized persistence seam, not a generic row/database API.
// CommitAttempt must atomically compare the stored revision/identity, validate the domain
// transition, write After and append Event with the next per-job sequence. A mismatch
// changes nothing. No transaction may span a provider call or a large transfer.
// Admission/submission-intent/resource-ledger ports will be added by their M3 consumers.
type Store interface {
	LoadAttempt(context.Context, domain.WorkspaceID, domain.JobID, domain.AttemptID) (domain.Attempt, error)
	CommitAttempt(context.Context, AttemptChange) error
}
type AttemptChange struct {
	WorkspaceID domain.WorkspaceID
	Before      domain.Attempt
	After       domain.Attempt
	Event       domain.Event
}

func (change AttemptChange) Validate() error {
	if !change.WorkspaceID.Valid() {
		return errors.New("invalid workspace")
	}
	if err := change.Before.Validate(); err != nil {
		return err
	}
	if err := change.After.Validate(); err != nil {
		return err
	}
	if err := change.Event.Validate(); err != nil {
		return err
	}
	expected, err := change.Before.Transition(change.After.State, change.After.UpdatedAt)
	if err != nil {
		return err
	}
	if expected != change.After || expected.Revision == change.Before.Revision {
		return errors.New("attempt change must preserve identity and advance one revision")
	}
	if change.Event.WorkspaceID != change.WorkspaceID || change.Event.JobID != change.After.JobID ||
		change.Event.AttemptID != change.After.ID || !change.Event.OccurredAt.Equal(change.After.UpdatedAt) {
		return errors.New("event does not match attempt change")
	}
	return nil
}

// EventSink is optional post-commit delivery, NOT authoritative event persistence.
// Delivery may repeat and consumers must deduplicate by event ID/sequence. Failure never
// rolls back committed state or triggers compute. Store owns the transactional event log.
type EventSink interface {
	Publish(context.Context, domain.Event) error
}
