// Package objects coordinates authenticated uploads and committed ownership metadata.
package objects

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

var (
	ErrInvalid     = errors.New("invalid object metadata")
	ErrNotFound    = errors.New("object not found")
	ErrUnavailable = errors.New("object metadata repository unavailable")
)

// BlobWriter is the consumer-sized write side of ports.BlobStore. ObjectMetadata is
// shared with that port; no provider transport or alternate byte store is introduced.
type BlobWriter interface {
	Put(context.Context, domain.ObjectMetadata, io.Reader) (domain.ObjectMetadata, error)
}

// Repository owns API visibility. CommitObject is insert-only and must acknowledge only
// after durable commit. A failed/uncertain commit MUST NOT trigger blob deletion: the
// record may already exist. GetObject scopes its query by workspace and ID and returns
// ErrNotFound for absent/invisible objects. SQLite implementation belongs to M3.
type Repository interface {
	CommitObject(context.Context, domain.ObjectMetadata) error
	GetObject(context.Context, domain.WorkspaceID, domain.ObjectID) (domain.ObjectMetadata, error)
}

type Service struct {
	auth  *auth.Service
	blobs BlobWriter
	repo  Repository
}

func New(access *auth.Service, blobs BlobWriter, repo Repository) (*Service, error) {
	if access == nil || blobs == nil || repo == nil {
		return nil, ErrUnavailable
	}
	return &Service{auth: access, blobs: blobs, repo: repo}, nil
}

func (s *Service) Upload(ctx context.Context, principal auth.Principal, workspace domain.WorkspaceID, length int64, digest domain.SHA256Digest, body io.Reader) (domain.ObjectMetadata, error) {
	freshStart, err := s.auth.Revalidate(ctx, principal)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	if err := auth.Require(freshStart, workspace, auth.Write); err != nil {
		return domain.ObjectMetadata{}, err
	}
	if length < -1 || (digest != "" && !digest.Valid()) || body == nil {
		return domain.ObjectMetadata{}, ErrInvalid
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	declaration := domain.ObjectMetadata{
		ID: domain.ObjectID("obj_" + hex.EncodeToString(id[:])), WorkspaceID: workspace,
		Bytes: length, SHA256: digest,
	}
	result, err := s.blobs.Put(ctx, declaration, body)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	if !result.Valid() || result.ID != declaration.ID || result.WorkspaceID != workspace ||
		(length >= 0 && length != result.Bytes) || (digest != "" && digest != result.SHA256) {
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	fresh, err := s.auth.Revalidate(ctx, principal)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	if err := auth.Require(fresh, workspace, auth.Write); err != nil {
		return domain.ObjectMetadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.ObjectMetadata{}, err
	}
	if err := s.repo.CommitObject(ctx, result); err != nil {
		// Preserve the complete unpublished/orphaned blob for M3 reconciliation. Never
		// remove it here: CommitObject might have committed before its response failed.
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	return result, nil
}

func (s *Service) Get(ctx context.Context, principal auth.Principal, workspace domain.WorkspaceID, id domain.ObjectID) (domain.ObjectMetadata, error) {
	fresh, err := s.auth.Revalidate(ctx, principal)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	if err := auth.Require(fresh, workspace, auth.Read); err != nil {
		return domain.ObjectMetadata{}, err
	}
	if !id.Valid() {
		return domain.ObjectMetadata{}, ErrNotFound
	}
	result, err := s.repo.GetObject(ctx, workspace, id)
	if errors.Is(err, ErrNotFound) {
		return domain.ObjectMetadata{}, ErrNotFound
	}
	if err != nil {
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	// Defense in depth even against an accidentally unscoped repository query.
	if result.WorkspaceID != workspace || result.ID != id {
		return domain.ObjectMetadata{}, ErrNotFound
	}
	if !result.Valid() {
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	return result, nil
}
