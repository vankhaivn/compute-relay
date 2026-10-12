package executionauth

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

type GateCommand struct {
	WorkspaceID domain.WorkspaceID
	TokenID     string
	JobID       domain.JobID
	KeyDigest   string
	Now         time.Time
}

func (c GateCommand) Validate() error {
	if !c.WorkspaceID.Valid() || !domain.ObjectID(c.TokenID).Valid() || !c.JobID.Valid() || !domain.SHA256Digest(c.KeyDigest).Valid() || c.Now.IsZero() || c.Now.Year() < 1970 || c.Now.Year() >= 9999 {
		return ErrRequest
	}
	return nil
}
func (s *Service) gateCommand(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, key string) (GateCommand, error) {
	fresh, err := s.authorize(ctx, p, w)
	if err != nil {
		return GateCommand{}, err
	}
	digest, err := admission.KeyDigest(key)
	if err != nil {
		return GateCommand{}, err
	}
	c := GateCommand{w, fresh.TokenID(), job, digest, s.clock.Now().UTC()}
	return c, c.Validate()
}
func (s *Service) Prepare(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, key string, raw []byte) (GateReceipt, error) {
	c, err := s.gateCommand(ctx, p, w, job, key)
	if err != nil {
		return GateReceipt{}, err
	}
	req, err := ParsePrepare(raw)
	if err != nil {
		return GateReceipt{}, err
	}
	return s.repo.PrepareExecution(ctx, c, req)
}
func (s *Service) Start(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, key string, raw []byte) (GateReceipt, error) {
	c, err := s.gateCommand(ctx, p, w, job, key)
	if err != nil {
		return GateReceipt{}, err
	}
	req, err := ParseStart(raw)
	if err != nil {
		return GateReceipt{}, err
	}
	return s.repo.StartExecution(ctx, c, req)
}
func (s *Service) GateAuthorization(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, id domain.OperationID) (GateReceipt, error) {
	fresh, err := s.authorize(ctx, p, w)
	if err != nil {
		return GateReceipt{}, err
	}
	if !job.Valid() || !id.Valid() {
		return GateReceipt{}, ErrNotFound
	}
	return s.repo.ReadGateAuthorization(ctx, w, fresh.TokenID(), job, id)
}
func (s *Service) Readiness(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID) (Readiness, error) {
	fresh, err := s.access.Revalidate(ctx, p)
	if err != nil {
		return Readiness{}, err
	}
	if err = auth.Require(fresh, w, auth.Read); err != nil {
		return Readiness{}, err
	}
	if !job.Valid() {
		return Readiness{}, ErrNotFound
	}
	return s.repo.ReadReadiness(ctx, w, fresh.TokenID(), job, s.clock.Now().UTC())
}
