package executionauth

import (
	"context"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

type GateCommand struct {
	SecretAvailable bool
	WorkspaceID     domain.WorkspaceID
	TokenID         string
	JobID           domain.JobID
	KeyDigest       string
}

func (c GateCommand) Validate() error {
	if !c.WorkspaceID.Valid() || !domain.ObjectID(c.TokenID).Valid() || !c.JobID.Valid() || !domain.SHA256Digest(c.KeyDigest).Valid() {
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
	c := GateCommand{WorkspaceID: w, TokenID: fresh.TokenID(), JobID: job, KeyDigest: digest}
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
	var receipt GateReceipt
	err = s.secrets.WithAvailability(workloadsecret.Key{WorkspaceID: w, JobID: job, AttemptID: req.AttemptID}, req.SecretAuthorizationID, s.clock.Now().UTC(), func(available bool) error {
		c.SecretAvailable = available
		var err error
		receipt, err = s.repo.StartExecution(ctx, c, req)
		return err
	})
	return receipt, err
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
	now := s.clock.Now().UTC()
	result, err := s.repo.ReadReadiness(ctx, w, fresh.TokenID(), job)
	if err == nil && result.Secrets != nil && result.Secrets.Status == "granted" {
		key := workloadsecret.Key{WorkspaceID: w, JobID: job, AttemptID: result.AttemptID}
		result.Secrets.Status = "available"
		if !s.secrets.Available(key, result.Secrets.AuthorizationID, now) {
			result.Secrets.Status = "material_lost"
			if result.State == "ready" || result.State == "start_authorized" {
				result.RequiredActions = append(result.RequiredActions, "resupply_secrets")
			}
		}
	}
	return result, err
}
