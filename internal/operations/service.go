package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

// RetryInputs is a read/verify/compare snapshot. The SQL commit must independently
// recheck authority, active attempt, journal safety and every frozen object. Large
// local reads happen before that transaction; no URL or provider is consulted.
type RetryInputs struct {
	Attempt domain.Attempt
	Objects []admission.FrozenObject
}

type Command struct {
	WorkspaceID domain.WorkspaceID
	TokenID     string
	JobID       domain.JobID
	Kind        domain.OperationKind
	Request     Request
	KeyDigest   string
	RequestHash string
	Inputs      *RetryInputs
	Limits      admission.Limits
	Now         time.Time
}

func (c Command) Validate() error {
	digest, err := c.Request.Digest(c.JobID, c.Kind)
	if err != nil || !c.WorkspaceID.Valid() || c.TokenID == "" || !domain.SHA256Digest(c.KeyDigest).Valid() || digest != c.RequestHash || !c.Limits.Valid() || c.Now.IsZero() {
		return ErrRequest
	}
	if (c.Kind == domain.OperationRetryCompute) != (c.Inputs != nil) {
		return ErrRequest
	}
	return nil
}

type Repository interface {
	ReplayOperation(context.Context, domain.WorkspaceID, string, domain.OperationKind, string, string) (*Record, error)
	RetryInputs(context.Context, domain.WorkspaceID, string, domain.JobID, domain.AttemptID) (RetryInputs, error)
	AdmitOperation(context.Context, Command) (Record, error)
	ReadOperation(context.Context, domain.WorkspaceID, string, domain.OperationID) (Record, error)
}

type BlobReader interface {
	Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error)
}

type Service struct {
	access *auth.Service
	repo   Repository
	blobs  BlobReader
	clock  ports.Clock
	limits admission.Limits
}

// A nil blob reader disables compute retry, not cancellation or observation. There
// is deliberately no fallback to current URL contents or an alternate blob source.
func New(access *auth.Service, repo Repository, blobs BlobReader, clock ports.Clock, limits admission.Limits) (*Service, error) {
	if access == nil || repo == nil || !limits.Valid() {
		return nil, ErrRequest
	}
	if clock == nil {
		clock = ports.SystemClock{}
	}
	return &Service{access: access, repo: repo, blobs: blobs, clock: clock, limits: limits}, nil
}

func (s *Service) Submit(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, kind domain.OperationKind, key string, raw []byte) (Record, error) {
	p, err := s.authorize(ctx, p, w, auth.Operate)
	if err != nil {
		return Record{}, err
	}
	req, err := Parse(kind, raw)
	if err != nil {
		return Record{}, err
	}
	keyHash, err := admission.KeyDigest(key)
	if err != nil {
		return Record{}, err
	}
	hash, err := req.Digest(job, kind)
	if err != nil {
		return Record{}, err
	}
	// Replays precede active-attempt/input checks. A lost response remains recoverable
	// after a new attempt starts, inputs expire, or the original operation completes.
	prior, err := s.repo.ReplayOperation(ctx, w, p.TokenID(), kind, keyHash, hash)
	if err != nil {
		return Record{}, err
	}
	if prior != nil {
		return *prior, nil
	}
	command := Command{WorkspaceID: w, TokenID: p.TokenID(), JobID: job, Kind: kind, Request: req, KeyDigest: keyHash, RequestHash: hash, Limits: s.limits}
	if kind == domain.OperationRetryCompute {
		if s.blobs == nil {
			return Record{}, ErrInputs
		}
		inputs, err := s.repo.RetryInputs(ctx, w, p.TokenID(), job, req.AttemptID)
		if err != nil {
			return Record{}, err
		}
		if err = s.verify(ctx, w, inputs); err != nil {
			return Record{}, err
		}
		command.Inputs = &inputs
	}
	command.Now = s.clock.Now().UTC()
	return s.repo.AdmitOperation(ctx, command)
}

func (s *Service) Get(ctx context.Context, p auth.Principal, w domain.WorkspaceID, id domain.OperationID) (Record, error) {
	p, err := s.authorize(ctx, p, w, auth.Read)
	if err != nil {
		return Record{}, err
	}
	if !id.Valid() {
		return Record{}, ErrNotFound
	}
	return s.repo.ReadOperation(ctx, w, p.TokenID(), id)
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

func (s *Service) verify(ctx context.Context, w domain.WorkspaceID, inputs RetryInputs) error {
	if inputs.Attempt.Validate() != nil || RetryAllowed(inputs.Attempt.State) != nil || len(inputs.Objects) == 0 || len(inputs.Objects) > 65 {
		return ErrInputs
	}
	var total int64
	seen := map[domain.ObjectID]domain.ObjectMetadata{}
	for _, ref := range inputs.Objects {
		m := ref.Object
		if !m.Valid() || m.WorkspaceID != w || m.Bytes > 2<<30 || m.Bytes > (4<<30)+(100<<20)-total {
			return ErrInputs
		}
		total += m.Bytes
		if prior, ok := seen[m.ID]; ok {
			if prior != m {
				return ErrInputs
			}
			continue
		}
		if err := verifyBlob(ctx, s.blobs, m); err != nil {
			return err
		}
		seen[m.ID] = m
	}
	return nil
}

func verifyBlob(ctx context.Context, blobs BlobReader, m domain.ObjectMetadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := blobs.Open(ctx, m.WorkspaceID, m.ID)
	if err != nil || r == nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrInputs
	}
	h := sha256.New()
	n, readErr := io.Copy(h, io.LimitReader(contextReader{ctx, r}, m.Bytes+1))
	closeErr := r.Close()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if readErr != nil || closeErr != nil || n != m.Bytes || hex.EncodeToString(h.Sum(nil)) != string(m.SHA256) {
		return ErrInputs
	}
	return nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
