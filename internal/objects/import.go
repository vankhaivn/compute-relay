package objects

import (
	"context"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/localinput"
)

// Importer composes allowlisted sources with the SAME verified upload/ownership path.
// It cannot admit an object using only filesystem state or caller-supplied workspace IDs.
type Importer struct {
	objects *Service
	inputs  *localinput.Manager
}

func NewImporter(service *Service, inputs *localinput.Manager) (*Importer, error) {
	if service == nil || inputs == nil {
		return nil, ErrUnavailable
	}
	return &Importer{service, inputs}, nil
}
func (i *Importer) Import(ctx context.Context, p auth.Principal, workspace domain.WorkspaceID, request localinput.Request) (domain.ObjectMetadata, error) {
	fresh, err := i.objects.auth.Revalidate(ctx, p)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	if err := auth.Require(fresh, workspace, auth.Write); err != nil {
		return domain.ObjectMetadata{}, err
	}
	source, err := i.inputs.Open(ctx, string(workspace), request)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	defer source.Close()
	// Producer errors reach Upload before successful EOF. Upload revalidates authority
	// before metadata commit and retains complete blobs after uncertain commit outcomes.
	return i.objects.Upload(ctx, fresh, workspace, source.Bytes, domain.SHA256Digest(source.SHA256), source)
}
