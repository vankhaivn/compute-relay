package cleanup

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

type Command struct {
	WorkspaceID domain.WorkspaceID
	TokenID     string
	JobID       domain.JobID
	Request     Request
	KeyDigest   string
	RequestHash string
	Now         time.Time
}

func (c Command) Validate() error {
	digest, err := c.Request.Digest(c.JobID)
	if err != nil || !c.WorkspaceID.Valid() || c.TokenID == "" || !domain.SHA256Digest(c.KeyDigest).Valid() || digest != c.RequestHash || c.Now.IsZero() {
		return operations.ErrRequest
	}
	return nil
}

// Repository commits admission in one transaction that rechecks current authority and
// every eligibility rule. It never calls a provider.
type Repository interface {
	ReplayCleanup(context.Context, domain.WorkspaceID, string, string, string) (*Record, error)
	AdmitCleanup(context.Context, Command) (Record, error)
	ReadCleanup(context.Context, domain.WorkspaceID, string, domain.OperationID) (Record, error)
}

type Service struct {
	access *auth.Service
	repo   Repository
	clock  ports.Clock
}

func NewService(access *auth.Service, repo Repository, clock ports.Clock) (*Service, error) {
	if access == nil || repo == nil {
		return nil, operations.ErrRequest
	}
	if clock == nil {
		clock = ports.SystemClock{}
	}
	return &Service{access: access, repo: repo, clock: clock}, nil
}

func (s *Service) Submit(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, key string, raw []byte) (Record, error) {
	p, err := s.authorize(ctx, p, w, auth.Operate)
	if err != nil {
		return Record{}, err
	}
	req, err := Parse(raw)
	if err != nil {
		return Record{}, err
	}
	keyHash, err := admission.KeyDigest(key)
	if err != nil {
		return Record{}, err
	}
	hash, err := req.Digest(job)
	if err != nil {
		return Record{}, err
	}
	// A committed matching receipt is returned before any eligibility check, so a lost
	// response stays recoverable after the cleanup completes or the attempt changes.
	prior, err := s.repo.ReplayCleanup(ctx, w, p.TokenID(), keyHash, hash)
	if err != nil {
		return Record{}, err
	}
	if prior != nil {
		return *prior, nil
	}
	return s.repo.AdmitCleanup(ctx, Command{WorkspaceID: w, TokenID: p.TokenID(), JobID: job, Request: req, KeyDigest: keyHash, RequestHash: hash, Now: s.clock.Now().UTC()})
}

func (s *Service) Get(ctx context.Context, p auth.Principal, w domain.WorkspaceID, id domain.OperationID) (Record, error) {
	p, err := s.authorize(ctx, p, w, auth.Read)
	if err != nil {
		return Record{}, err
	}
	if !id.Valid() {
		return Record{}, operations.ErrNotFound
	}
	return s.repo.ReadCleanup(ctx, w, p.TokenID(), id)
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
