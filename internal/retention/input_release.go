package retention

import (
	"context"
	"sort"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

const MaxInputReleaseJobs = 100

type InputReleaseJob struct {
	JobID domain.JobID `json:"job_id"`
	ReleaseRequest
}

type InputReleaseRequest struct {
	Jobs          []InputReleaseJob   `json:"jobs"`
	PreviewSHA256 domain.SHA256Digest `json:"preview_sha256,omitempty"`
}

// Normalize makes request order immaterial while refusing duplicate targets.
func (r *InputReleaseRequest) Normalize(confirm bool) bool {
	if len(r.Jobs) < 1 || len(r.Jobs) > MaxInputReleaseJobs || confirm && !r.PreviewSHA256.Valid() || !confirm && r.PreviewSHA256 != "" {
		return false
	}
	r.Jobs = append([]InputReleaseJob(nil), r.Jobs...)
	sort.Slice(r.Jobs, func(i, j int) bool {
		if r.Jobs[i].JobID == r.Jobs[j].JobID {
			return r.Jobs[i].AttemptID < r.Jobs[j].AttemptID
		}
		return r.Jobs[i].JobID < r.Jobs[j].JobID
	})
	for i, j := range r.Jobs {
		if !j.JobID.Valid() || !j.ReleaseRequest.Valid() || i > 0 && j.JobID == r.Jobs[i-1].JobID && j.AttemptID == r.Jobs[i-1].AttemptID {
			return false
		}
	}
	return true
}

type InputReleasePreview struct {
	WorkspaceID     domain.WorkspaceID  `json:"workspace_id"`
	Jobs            []InputReleaseJob   `json:"jobs"`
	Files           int                 `json:"files"`
	ReleasableBytes int64               `json:"releasable_bytes"`
	PreviewSHA256   domain.SHA256Digest `json:"preview_sha256"`
	MeasuredAt      time.Time           `json:"measured_at"`
}

type InputReleaseReceipt struct {
	WorkspaceID domain.WorkspaceID `json:"workspace_id"`
	InputReleaseRequest
	ReleasedAt     time.Time `json:"released_at"`
	Files          int       `json:"files"`
	Bytes          int64     `json:"bytes"`
	PendingFiles   int       `json:"pending_files"`
	ReclaimedBytes int64     `json:"reclaimed_bytes"`
}

type InputReleaseRepository interface {
	PreviewInputRelease(context.Context, domain.WorkspaceID, string, InputReleaseRequest) (InputReleasePreview, error)
	AdmitInputRelease(context.Context, domain.WorkspaceID, string, string, InputReleaseRequest) (InputReleaseReceipt, []Deletion, error)
	ReadInputRelease(context.Context, domain.WorkspaceID, string, string, InputReleaseRequest) (InputReleaseReceipt, error)
}

type InputReleaser struct {
	access  *auth.Service
	repo    InputReleaseRepository
	sweeper *Sweeper
}

func NewInputReleaser(access *auth.Service, repo InputReleaseRepository, sweeper *Sweeper) (*InputReleaser, error) {
	if access == nil || nilDependency(repo) || sweeper == nil {
		return nil, ErrInvalid
	}
	return &InputReleaser{access, repo, sweeper}, nil
}
func (r *InputReleaser) Preview(ctx context.Context, p auth.Principal, w domain.WorkspaceID, request InputReleaseRequest) (InputReleasePreview, error) {
	fresh, err := r.access.Revalidate(ctx, p)
	if err != nil {
		return InputReleasePreview{}, err
	}
	if err = auth.Require(fresh, w, auth.Read); err != nil {
		return InputReleasePreview{}, err
	}
	if !request.Normalize(false) {
		return InputReleasePreview{}, ErrInvalid
	}
	return r.repo.PreviewInputRelease(ctx, w, fresh.TokenID(), request)
}
func (r *InputReleaser) Release(ctx context.Context, p auth.Principal, w domain.WorkspaceID, key string, request InputReleaseRequest) (InputReleaseReceipt, error) {
	fresh, err := r.access.Revalidate(ctx, p)
	if err != nil {
		return InputReleaseReceipt{}, err
	}
	if err = auth.Require(fresh, w, auth.Operate); err != nil {
		return InputReleaseReceipt{}, err
	}
	digest, err := admission.KeyDigest(key)
	if err != nil || !request.Normalize(true) {
		return InputReleaseReceipt{}, ErrInvalid
	}
	s := r.sweeper
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.bind(ctx); err != nil {
		return InputReleaseReceipt{}, err
	}
	receipt, pending, err := r.repo.AdmitInputRelease(ctx, w, fresh.TokenID(), digest, request)
	if err != nil {
		return InputReleaseReceipt{}, err
	}
	if len(pending) > ReleasePageSize {
		return InputReleaseReceipt{}, ErrInvalid
	}
	for _, d := range pending {
		if d.Kind != Input || !d.Valid() || d.Object.WorkspaceID != w {
			return InputReleaseReceipt{}, ErrInvalid
		}
		if _, err = s.inputs.Delete(ctx, d.Object); err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if err = s.repo.CompleteRetention(ctx, d); err != nil {
			break
		}
	}
	if ctx.Err() != nil {
		return receipt, ctx.Err()
	}
	return r.repo.ReadInputRelease(ctx, w, fresh.TokenID(), digest, request)
}
