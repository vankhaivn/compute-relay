package objects

import (
	"context"
	"io"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/httpsinput"
)

// HTTPSFetcher is the consumer-sized input transport port. Production composition uses
// httpsinput.Client; implementations must deliver verified EOF within the total budget.
// This is trusted infrastructure, never an application-supplied client or policy bypass.
type HTTPSFetcher interface {
	Fetch(context.Context, httpsinput.Request, func(context.Context, int64, io.Reader) error) error
}

// Ingestor snapshots an HTTPS input through the existing authenticated upload/commit
// boundary. It never refreshes an existing object or supplies a URL to remote compute.
type Ingestor struct {
	objects *Service
	fetcher HTTPSFetcher
}

func NewIngestor(service *Service, fetcher HTTPSFetcher) (*Ingestor, error) {
	if service == nil || fetcher == nil {
		return nil, ErrUnavailable
	}
	return &Ingestor{objects: service, fetcher: fetcher}, nil
}

func (i *Ingestor) Ingest(ctx context.Context, p auth.Principal, workspace domain.WorkspaceID, request httpsinput.Request) (domain.ObjectMetadata, error) {
	fresh, err := i.objects.auth.Revalidate(ctx, p)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	if err := auth.Require(fresh, workspace, auth.Write); err != nil {
		return domain.ObjectMetadata{}, err
	}
	if err := request.Validate(); err != nil {
		return domain.ObjectMetadata{}, err
	}
	var result domain.ObjectMetadata
	called := false
	err = i.fetcher.Fetch(ctx, request, func(ctx context.Context, length int64, body io.Reader) error {
		if called {
			return ErrUnavailable // A fetch must not silently create multiple snapshots.
		}
		called = true
		var err error
		// Upload consumes verified EOF before publication, then refreshes revocation
		// and commits ownership. An uncertain commit must never cause blob deletion.
		result, err = i.objects.Upload(ctx, fresh, workspace, length, domain.SHA256Digest(request.SHA256), body)
		return err
	})
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	if !called || !result.Valid() || result.WorkspaceID != workspace {
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	return result, nil
}
