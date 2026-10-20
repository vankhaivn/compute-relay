package retention

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

const ReleasePageSize = 100

var ErrReleaseConflict = errors.New("result release key was used for another target")

// ReleaseRequest names an immutable publication through one verified output.
// Releasing it expires the whole local result set, including logs and metadata files.
type ReleaseRequest struct {
	AttemptID  domain.AttemptID    `json:"attempt_id"`
	ArtifactID domain.ArtifactID   `json:"artifact_id"`
	SHA256     domain.SHA256Digest `json:"sha256"`
}

func (r ReleaseRequest) Valid() bool {
	return r.AttemptID.Valid() && r.ArtifactID.Valid() && r.SHA256.Valid()
}

type ReleaseReceipt struct {
	WorkspaceID domain.WorkspaceID `json:"workspace_id"`
	JobID       domain.JobID       `json:"job_id"`
	ReleaseRequest
	ReleasedAt     time.Time `json:"released_at"`
	Files          int       `json:"files"`
	Bytes          int64     `json:"bytes"`
	PendingFiles   int       `json:"pending_files"`
	ReclaimedBytes int64     `json:"reclaimed_bytes"`
}

type ReleaseRepository interface {
	AdmitResultRelease(context.Context, domain.WorkspaceID, string, domain.JobID, string, ReleaseRequest) (ReleaseReceipt, []Deletion, error)
	ReadResultRelease(context.Context, domain.WorkspaceID, string, domain.JobID, string, ReleaseRequest) (ReleaseReceipt, error)
}

type Releaser struct {
	access  *auth.Service
	repo    ReleaseRepository
	sweeper *Sweeper
}

func NewReleaser(access *auth.Service, repo ReleaseRepository, sweeper *Sweeper) (*Releaser, error) {
	if access == nil || nilDependency(repo) || sweeper == nil {
		return nil, ErrInvalid
	}
	return &Releaser{access: access, repo: repo, sweeper: sweeper}, nil
}

// Release commits exact tombstones before I/O. Each replay drains at most one page;
// the ordinary retention worker also recovers pending deletion after a restart.
func (r *Releaser) Release(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, key string, request ReleaseRequest) (ReleaseReceipt, error) {
	fresh, err := r.access.Revalidate(ctx, p)
	if err != nil {
		return ReleaseReceipt{}, err
	}
	if err = auth.Require(fresh, w, auth.Operate); err != nil {
		return ReleaseReceipt{}, err
	}
	keyHash, err := admission.KeyDigest(key)
	if err != nil || !job.Valid() || !request.Valid() {
		return ReleaseReceipt{}, ErrInvalid
	}
	s := r.sweeper
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.bind(ctx); err != nil {
		return ReleaseReceipt{}, err
	}
	receipt, pending, err := r.repo.AdmitResultRelease(ctx, w, fresh.TokenID(), job, keyHash, request)
	if err != nil {
		return ReleaseReceipt{}, err
	}
	if len(pending) > ReleasePageSize {
		return ReleaseReceipt{}, ErrInvalid
	}
	for _, d := range pending {
		if d.Kind != Result || !d.Valid() || d.Object.WorkspaceID != w {
			return ReleaseReceipt{}, ErrInvalid
		}
		// A failed/uncertain delete remains pending. Never claim physical reclamation
		// until the exact deletion and its durable completion have both succeeded.
		if _, err := s.results.Delete(ctx, d.Object); err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if err := s.repo.CompleteRetention(ctx, d); err != nil {
			break
		}
	}
	if ctx.Err() != nil {
		return receipt, ctx.Err()
	}
	return r.repo.ReadResultRelease(ctx, w, fresh.TokenID(), job, keyHash, request)
}
