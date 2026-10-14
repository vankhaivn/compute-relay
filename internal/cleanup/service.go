package cleanup

import (
	"context"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

type Service struct {
	access          *auth.Service
	repo            PreviewRepository
	clock           ports.Clock
	previewResolver Resolver
}

func NewService(access *auth.Service, repo PreviewRepository, clock ports.Clock) (*Service, error) {
	if access == nil || repo == nil {
		return nil, operations.ErrRequest
	}
	if clock == nil {
		clock = ports.SystemClock{}
	}
	return &Service{access: access, repo: repo, clock: clock}, nil
}

func (s *Service) authorize(ctx context.Context, p auth.Principal, w domain.WorkspaceID, scope auth.Scope) (auth.Principal, error) {
	fresh, err := s.access.Revalidate(ctx, p)
	if err != nil {
		return auth.Principal{}, err
	}
	if err = auth.Require(fresh, w, scope); err != nil {
		return auth.Principal{}, err
	}
	return fresh, nil
}
