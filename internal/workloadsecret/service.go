package workloadsecret

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

type Command struct {
	WorkspaceID domain.WorkspaceID
	TokenID     string
	JobID       domain.JobID
	KeyDigest   string
}

func (c Command) Validate() error {
	if !c.WorkspaceID.Valid() || !domain.ObjectID(c.TokenID).Valid() || !c.JobID.Valid() || !domain.SHA256Digest(c.KeyDigest).Valid() {
		return ErrRequest
	}
	return nil
}

type Repository interface {
	SupplyWorkloadSecrets(context.Context, Command, Metadata, string) (Receipt, bool, error)
	RevokeWorkloadSecrets(context.Context, Command, domain.AttemptID, domain.OperationID) (Receipt, error)
	ReadWorkloadSecrets(context.Context, domain.WorkspaceID, string, domain.JobID, domain.OperationID) (Receipt, error)
	WorkloadSecretsUsable(context.Context, Key, domain.OperationID) (bool, error)
}
type Service struct {
	access *auth.Service
	repo   Repository
	pool   *Pool
	clock  ports.Clock
}

func New(access *auth.Service, repo Repository, pool *Pool, clock ports.Clock) (*Service, error) {
	if access == nil || repo == nil || pool == nil {
		return nil, ErrRequest
	}
	if clock == nil {
		clock = ports.SystemClock{}
	}
	return &Service{access, repo, pool, clock}, nil
}
func (s *Service) command(ctx context.Context, p auth.Principal, workspace domain.WorkspaceID, job domain.JobID, key string) (Command, error) {
	fresh, err := s.access.Revalidate(ctx, p)
	if err != nil {
		return Command{}, err
	}
	if err = auth.Require(fresh, workspace, auth.Execute); err != nil {
		return Command{}, err
	}
	if len(key) < 8 || len(key) > 256 {
		return Command{}, ErrRequest
	}
	for _, ch := range []byte(key) {
		if ch < 33 || ch > 126 {
			return Command{}, ErrRequest
		}
	}
	hash := sha256.Sum256([]byte(key))
	c := Command{workspace, fresh.TokenID(), job, hex.EncodeToString(hash[:])}
	return c, c.Validate()
}
func (s *Service) Supply(ctx context.Context, p auth.Principal, workspace domain.WorkspaceID, job domain.JobID, key string, raw []byte) (Receipt, error) {
	c, err := s.command(ctx, p, workspace, job, key)
	if err != nil {
		return Receipt{}, err
	}
	req, err := ParseSupply(raw)
	if err != nil {
		return Receipt{}, err
	}
	defer req.Clear()
	return s.pool.Supply(Key{workspace, job, req.AttemptID}, req, s.clock.Now().UTC(), func(fingerprint string, metadata Metadata) (Receipt, bool, error) {
		return s.repo.SupplyWorkloadSecrets(ctx, c, metadata, fingerprint)
	})
}
func (s *Service) Revoke(ctx context.Context, p auth.Principal, workspace domain.WorkspaceID, job domain.JobID, id domain.OperationID, key string, raw []byte) (Receipt, error) {
	c, err := s.command(ctx, p, workspace, job, key)
	if err != nil {
		return Receipt{}, err
	}
	var request struct {
		AttemptID domain.AttemptID `json:"attempt_id"`
	}
	if !id.Valid() || StrictDecode(raw, 4096, &request) != nil || !request.AttemptID.Valid() {
		return Receipt{}, ErrRequest
	}
	return s.pool.Revoke(Key{workspace, job, request.AttemptID}, id, func() (Receipt, error) {
		return s.repo.RevokeWorkloadSecrets(ctx, c, request.AttemptID, id)
	})
}
func (s *Service) Get(ctx context.Context, p auth.Principal, workspace domain.WorkspaceID, job domain.JobID, id domain.OperationID) (Receipt, error) {
	fresh, err := s.access.Revalidate(ctx, p)
	if err != nil {
		return Receipt{}, err
	}
	if err = auth.Require(fresh, workspace, auth.Execute); err != nil {
		return Receipt{}, err
	}
	if !job.Valid() || !id.Valid() {
		return Receipt{}, ErrNotFound
	}
	return s.repo.ReadWorkloadSecrets(ctx, workspace, fresh.TokenID(), job, id)
}
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.pool.Close()
			return ctx.Err()
		case <-ticker.C:
			now := s.clock.Now().UTC()
			s.pool.Sweep(now, func(key Key, id domain.OperationID) bool {
				bounded, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				usable, err := s.repo.WorkloadSecretsUsable(bounded, key, id)
				return err == nil && usable
			})
		}
	}
}
