package hoststorage

import (
	"context"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type ProviderResolver interface {
	Resolve(provider.BindingSnapshot) (provider.Provider, error)
}

type targetObserver interface {
	ObserveStorageTarget(context.Context, Target) (TargetObservation, error)
}

type Service struct {
	repo     CleanupRepository
	resolver ProviderResolver
}

func NewService(repo CleanupRepository, resolver ProviderResolver) (*Service, error) {
	if repo == nil || resolver == nil {
		return nil, ErrInvalid
	}
	return &Service{repo: repo, resolver: resolver}, nil
}

func (s *Service) Preview(ctx context.Context, w domain.WorkspaceID, token, connection, orphan string) (Preview, error) {
	candidate, err := s.repo.FindStorageOrphan(ctx, w, token, connection, orphan)
	if err != nil {
		return Preview{}, err
	}
	p, err := s.resolver.Resolve(candidate.Binding)
	if err != nil {
		return Preview{}, err
	}
	if err := p.Verify(ctx, candidate.Binding); err != nil {
		return Preview{}, err
	}
	observer, ok := p.(targetObserver)
	if !ok {
		return Preview{}, ErrUnverified
	}
	observation, err := observer.ObserveStorageTarget(ctx, candidate.Target)
	if err != nil {
		return Preview{}, err
	}
	return s.repo.RecordStoragePreview(ctx, w, token, candidate, observation)
}

func (s *Service) Apply(ctx context.Context, w domain.WorkspaceID, token, connection, orphan, fingerprint, key string) (Operation, error) {
	return s.repo.AdmitStorageCleanup(ctx, w, token, connection, orphan, fingerprint, key)
}

func (s *Service) Operation(ctx context.Context, w domain.WorkspaceID, token string, id domain.OperationID) (Operation, error) {
	if !id.Valid() {
		return Operation{}, ErrInvalid
	}
	return s.repo.ReadStorageCleanup(ctx, w, token, id)
}

var _ Controller = (*Service)(nil)
