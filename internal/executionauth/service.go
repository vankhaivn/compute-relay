package executionauth

import (
	"context"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

type Command struct {
	WorkspaceID domain.WorkspaceID
	TokenID     string
	JobID       domain.JobID
	KeyDigest   string
	Request     Request
}

func (c Command) Validate() error {
	if !c.WorkspaceID.Valid() || !domain.ObjectID(c.TokenID).Valid() || !c.JobID.Valid() || !domain.SHA256Digest(c.KeyDigest).Valid() || c.Request.Validate() != nil {
		return ErrRequest
	}
	return nil
}

type Repository interface {
	AuthorizeExecution(context.Context, Command) (Receipt, error)
	ReadExecutionAuthorization(context.Context, domain.WorkspaceID, string, domain.JobID, domain.OperationID) (Receipt, error)
	PrepareExecution(context.Context, GateCommand, PrepareRequest) (GateReceipt, error)
	StartExecution(context.Context, GateCommand, StartRequest) (GateReceipt, error)
	ReadGateAuthorization(context.Context, domain.WorkspaceID, string, domain.JobID, domain.OperationID) (GateReceipt, error)
	ReadReadiness(context.Context, domain.WorkspaceID, string, domain.JobID) (Readiness, error)
}
type Service struct {
	access  *auth.Service
	repo    Repository
	clock   ports.Clock
	secrets *workloadsecret.Pool
}

func New(access *auth.Service, repo Repository, clock ports.Clock) (*Service, error) {
	if access == nil || repo == nil {
		return nil, ErrRequest
	}
	if clock == nil {
		clock = ports.SystemClock{}
	}
	return &Service{access: access, repo: repo, clock: clock}, nil
}

// WithSecrets connects the process-local material gate before serving requests.
func (s *Service) WithSecrets(pool *workloadsecret.Pool) *Service { s.secrets = pool; return s }
func (s *Service) Authorize(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, key string, raw []byte) (Receipt, error) {
	p, err := s.authorize(ctx, p, w)
	if err != nil {
		return Receipt{}, err
	}
	req, err := Parse(raw)
	if err != nil {
		return Receipt{}, err
	}
	digest, err := admission.KeyDigest(key)
	if err != nil {
		return Receipt{}, err
	}
	return s.repo.AuthorizeExecution(ctx, Command{WorkspaceID: w, TokenID: p.TokenID(), JobID: job, KeyDigest: digest, Request: req})
}
func (s *Service) Get(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, id domain.OperationID) (Receipt, error) {
	p, err := s.authorize(ctx, p, w)
	if err != nil {
		return Receipt{}, err
	}
	if !job.Valid() || !id.Valid() {
		return Receipt{}, ErrNotFound
	}
	return s.repo.ReadExecutionAuthorization(ctx, w, p.TokenID(), job, id)
}
func (s *Service) authorize(ctx context.Context, p auth.Principal, w domain.WorkspaceID) (auth.Principal, error) {
	fresh, err := s.access.Revalidate(ctx, p)
	if err != nil {
		return auth.Principal{}, err
	}
	if err = auth.Require(fresh, w, auth.Execute); err != nil {
		return auth.Principal{}, err
	}
	return fresh, nil
}
