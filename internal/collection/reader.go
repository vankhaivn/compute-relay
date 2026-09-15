package collection

import (
	"context"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

// Result is an internal application-service response, not a newly advertised HTTP
// endpoint. The source manifest/logs are downloadable files, not trusted instructions.
type Result struct {
	WorkspaceID domain.WorkspaceID
	JobID       domain.JobID
	AttemptID   domain.AttemptID
	Phase       string
	VerifiedAt  time.Time
	Files       []File
}
type ResultRepository interface {
	ReadCollection(context.Context, domain.WorkspaceID, string, domain.JobID, domain.AttemptID) (Result, error)
}
type Reader struct {
	access *auth.Service
	repo   ResultRepository
	blobs  BlobStore
}

func NewReader(access *auth.Service, repo ResultRepository, blobs BlobStore) (*Reader, error) {
	if access == nil || repo == nil || blobs == nil {
		return nil, ErrInvalid
	}
	return &Reader{access: access, repo: repo, blobs: blobs}, nil
}
func (r *Reader) Read(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID) (Result, error) {
	fresh, err := r.access.Revalidate(ctx, p)
	if err != nil {
		return Result{}, err
	}
	if err = auth.Require(fresh, w, auth.Read); err != nil {
		return Result{}, err
	}
	if !job.Valid() || !attempt.Valid() {
		return Result{}, ErrNotFound
	}
	return r.repo.ReadCollection(ctx, w, fresh.TokenID(), job, attempt)
}

// Open requires the explicit attempt and one of its committed artifact IDs. Readers
// receive expected bytes/digest alongside the stream; no physical/provider path escapes.
func (r *Reader) Open(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, id domain.ArtifactID) (File, io.ReadCloser, error) {
	result, err := r.Read(ctx, p, w, job, attempt)
	if err != nil {
		return File{}, nil, err
	}
	for _, f := range result.Files {
		if f.ID == id {
			stream, err := r.blobs.Open(ctx, w, f.Object.ID)
			if err != nil || stream == nil {
				return File{}, nil, ErrUnavailable
			}
			return f, stream, nil
		}
	}
	return File{}, nil, ErrNotFound
}
