// Package collection retrieves and verifies one already-terminal attempt. Its
// execution boundary has no Prepare, Submit, Cancel or Cleanup operation.
package collection

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const (
	ManifestPath     = "control/execution-result.json"
	MaxManifestBytes = 1 << 20
	MaxSnapshotBytes = 16 << 20
	MaxFiles         = 10004 // 10,000 payload files and four explicitly selected control files.
)

var (
	ErrInvalid     = errors.New("invalid collection evidence")
	ErrLeaseLost   = errors.New("collection ownership expired or changed")
	ErrUnavailable = errors.New("collection unavailable; retain the existing attempt")
	ErrNotFound    = errors.New("verified result not found or not visible")
)

// Failure contains no provider message, path, response, or credential material.
type Failure struct {
	Code    domain.ErrorCode
	Invalid bool
}

func (f Failure) Error() string                         { return "collection failed: " + string(f.Code) }
func failure(code domain.ErrorCode, invalid bool) error { return Failure{code, invalid} }

// Lease is an internal capability. Wall-clock expiry never changes remote state.
type Lease struct {
	WorkspaceID     domain.WorkspaceID
	JobID           domain.JobID
	AttemptID       domain.AttemptID
	OperationID     domain.OperationID
	Generation      int64
	Fence           string
	Until           time.Time
	AttemptRevision uint64
}

func (l Lease) Valid() bool {
	return l.WorkspaceID.Valid() && l.JobID.Valid() && l.AttemptID.Valid() && l.OperationID.Valid() &&
		l.Generation > 0 && domain.ObjectID(l.Fence).Valid() && len(l.Fence) >= 32 && !l.Until.IsZero() && l.AttemptRevision > 0
}

type Work struct {
	Lease       Lease
	Plan        provider.Plan
	Binding     provider.BindingSnapshot
	Observation provider.Observation
	Snapshot    *Snapshot
}

func (w Work) Valid() bool {
	id := w.Plan.Job.Identity
	return w.Lease.Valid() && w.Plan.Job.Validate() == nil && w.Binding.Valid() &&
		w.Plan.Job.Binding == w.Binding.Binding && w.Observation.Validate(w.Observation.Remote) == nil &&
		w.Observation.Remote.Identity == id && w.Observation.Execution.Terminal() &&
		id.WorkspaceID == w.Lease.WorkspaceID && id.JobID == w.Lease.JobID && id.AttemptID == w.Lease.AttemptID
}

// Paths describe logical remote files only. They never become local filesystem paths.
// Object IDs occupy a dedicated collector-owned blob store; no objects API rows are made.
type File struct {
	ID        domain.ArtifactID     `json:"artifact_id"`
	Path      string                `json:"path"`
	Role      string                `json:"role"`
	Object    domain.ObjectMetadata `json:"object"`
	MediaType string                `json:"media_type,omitempty"`
}
type Snapshot struct {
	Manifest string `json:"manifest"`
	Files    []File `json:"files"`
}

func (s Snapshot) Clone() Snapshot { s.Files = append([]File(nil), s.Files...); return s }
func (s Snapshot) Digest() domain.SHA256Digest {
	raw, err := json.Marshal(s)
	if err != nil || len(raw) > MaxSnapshotBytes {
		return ""
	}
	return provider.Digest(raw)
}
func FileIdentity(remote provider.RemoteReference, path string, size int64, digest domain.SHA256Digest) domain.ArtifactID {
	raw, _ := json.Marshal(struct {
		Version string
		Remote  provider.RemoteReference
		Path    string
		Bytes   int64
		Digest  domain.SHA256Digest
	}{"collection-file/v1", remote, path, size, digest})
	return domain.ArtifactID("art_" + string(provider.Digest(raw)))
}

// Verified cannot be fabricated by adapters or callers through exported fields. It
// is constructed only after every pinned blob has been reopened and hashed by Engine.
type Verified struct {
	lease    Lease
	snapshot Snapshot
	phase    string
}

func (v Verified) Snapshot() Snapshot { return v.snapshot.Clone() }
func (v Verified) Phase() string      { return v.phase }
func (v Verified) Matches(w Work) bool {
	return w.Valid() && v.lease == w.Lease && w.Snapshot != nil && v.snapshot.Digest().Valid() && v.snapshot.Digest() == w.Snapshot.Digest()
}

type Repository interface {
	ClaimCollection(context.Context, time.Time, time.Duration, int) (*Work, error)
	PinCollection(context.Context, Work, Snapshot, time.Time) error
	CompleteCollection(context.Context, Work, Verified, time.Time) error
	FailCollection(context.Context, Work, Failure, time.Time) error
}
type BlobStore interface {
	Put(context.Context, domain.ObjectMetadata, io.Reader) (domain.ObjectMetadata, error)
	Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error)
}
type Resolver interface {
	Resolve(provider.BindingSnapshot) (provider.Provider, error)
}
type Clock interface{ Now() time.Time }
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

type Config struct {
	MaxBytes  int64
	MaxFiles  int
	Workers   int
	Timeout   time.Duration
	PollDelay time.Duration
}

func DefaultConfig() Config {
	return Config{MaxBytes: 4 << 30, MaxFiles: MaxFiles, Workers: 2, Timeout: 10 * time.Minute, PollDelay: time.Second}
}
func (c Config) Valid() bool {
	return c.MaxBytes > 0 && c.MaxBytes <= 4<<30 && c.MaxFiles >= 1 && c.MaxFiles <= MaxFiles && c.Workers >= 1 && c.Workers <= 16 && c.Timeout >= time.Second && c.Timeout <= 30*time.Minute && c.PollDelay >= time.Millisecond && c.PollDelay <= time.Minute
}

// FinalState never upgrades provider execution, cancellation, or release evidence.
// A failed runner with a successful provider wrapper is failed orchestration, not success.
func FinalState(s domain.AttemptState, phase string) (domain.AttemptState, error) {
	if !s.Execution.Terminal() || s.Orchestration.Terminal() || s.Result != domain.ResultCollecting {
		return s, ErrInvalid
	}
	s.Result = domain.ResultAvailable
	s.Orchestration = domain.OrchestrationNeedsAttention
	ordinaryCancel := s.Cancellation == domain.CancellationNotRequested || s.Cancellation == domain.CancellationTooLate
	switch s.Execution {
	case domain.ExecutionSucceeded:
		if ordinaryCancel {
			s.Orchestration = domain.OrchestrationFailed
			if phase == "completed" {
				s.Orchestration = domain.OrchestrationSucceeded
			}
		}
	case domain.ExecutionFailed:
		if ordinaryCancel {
			s.Orchestration = domain.OrchestrationFailed
		}
	case domain.ExecutionTimedOut:
		if ordinaryCancel {
			s.Orchestration = domain.OrchestrationTimedOut
			s.DeadlineExceeded = true
		}
	case domain.ExecutionCancelled:
		if s.Cancellation == domain.CancellationConfirmed {
			s.Orchestration = domain.OrchestrationCancelled
		}
	}
	return s, s.Validate()
}
