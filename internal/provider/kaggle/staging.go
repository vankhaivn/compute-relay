package kaggle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// StagingBlobs is the existing workspace-scoped immutable input store. No host
// path, URL fetch, local extraction or command execution is part of staging.
type StagingBlobs interface {
	Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error)
}

type stagingResponse struct {
	Protocol      int                 `json:"protocol"`
	Status        string              `json:"status"`
	DatasetID     string              `json:"dataset_id"`
	Owner         string              `json:"owner"`
	Slug          string              `json:"slug"`
	Version       int                 `json:"version"`
	MarkerSHA256  domain.SHA256Digest `json:"marker_sha256"`
	Private       bool                `json:"private"`
	VerifiedFiles int                 `json:"verified_files"`
	VerifiedBytes int64               `json:"verified_bytes"`
}

type stagingInvocation func(context.Context, Config, string, []byte, stagingPlan, StagingBlobs) (stagingResponse, error)

// Stager is a preparation-only component, NOT a Provider or a scheduler. Only a
// caller that just committed a NEW M3 BeginPreparation may invoke Prepare with
// creation enabled. Recovery must use ReconcilePreparation. No registration or
// executable mutation command is installed by NewStager.
type Stager struct {
	config      Config
	policy      StagingPolicy
	credentials ports.CredentialResolver
	blobs       StagingBlobs
	allowCreate bool
	slot        chan struct{}
	local       invocation
	run         stagingInvocation
}

var _ provider.PreparationObserver = (*Stager)(nil)

func NewStager(c Config, policy StagingPolicy, resolver ports.CredentialResolver, blobs StagingBlobs, allowCreate bool) (*Stager, error) {
	if c.Validate() != nil || !policy.valid() || resolver == nil || blobs == nil {
		return nil, ErrConfig
	}
	return &Stager{config: c, policy: policy, credentials: resolver, blobs: blobs, allowCreate: allowCreate, slot: make(chan struct{}, 1), local: runPython, run: runStaging}, nil
}

func (s *Stager) Prepare(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.Prepared, error) {
	if s == nil || !s.allowCreate {
		return provider.Prepared{}, ErrConfig
	}
	seen, err := s.check(ctx, "create", plan, operation)
	if err != nil {
		return provider.Prepared{}, err
	}
	if seen.Status != provider.ReconciliationFound || seen.Prepared == nil {
		return provider.Prepared{}, ErrStaging
	}
	return *seen.Prepared, nil
}

// A miss is not permission to create/upload/version anything. This path does not
// read local payload bytes; it verifies remote bytes against the original pins.
func (s *Stager) ReconcilePreparation(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	if s == nil {
		return provider.PreparationObservation{}, ErrConfig
	}
	return s.check(ctx, "observe", plan, operation)
}

func (s *Stager) check(parent context.Context, mode string, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	var none provider.PreparationObservation
	frozen := plan.Clone()
	p, err := buildStagingPlan(s.config, s.policy, frozen, operation)
	if err != nil {
		return none, err
	}
	ctx, cancel := context.WithTimeout(parent, s.policy.Timeout)
	defer cancel()
	select {
	case s.slot <- struct{}{}:
		defer func() { <-s.slot }()
	case <-ctx.Done():
		return none, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return none, err
	}
	local, err := s.local(ctx, s.config, Local, nil)
	if err != nil || !local.valid(Local) || local.Local != "ready" {
		return none, ErrProcess
	}
	if mode == "create" {
		// Reject every missing/corrupt local file BEFORE looking up a credential or
		// creating even a provider upload session. The helper rehashes actual upload.
		for _, object := range p.objects {
			if err := verifyStagingInput(ctx, s.blobs, object); err != nil {
				return none, err
			}
		}
	}
	var result stagingResponse
	called := false
	err = s.credentials.WithCredential(ctx, s.config.CredentialRef, func(secret []byte) error {
		if called {
			return ErrProtocol
		}
		called = true
		if len(secret) == 0 || len(secret) > credentials.MaxBytes {
			return ErrStaging
		}
		for _, b := range secret {
			if b < 33 || b > 126 {
				return ErrStaging
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		result, err = s.run(ctx, s.config, mode, secret, p, s.blobs)
		return err
	})
	if ctx.Err() != nil {
		return none, ctx.Err()
	}
	if err != nil || !called {
		return none, ErrStaging
	} // Do not reflect secret-bearing callback/process errors.
	return result.observation(p, frozen, operation)
}

func verifyStagingInput(ctx context.Context, blobs StagingBlobs, m domain.ObjectMetadata) (result error) {
	r, err := blobs.Open(ctx, m.WorkspaceID, m.ID)
	if err != nil {
		return ErrStagingInput
	}
	defer func() {
		if err := r.Close(); err != nil {
			result = ErrStagingInput
		}
	}()
	h := sha256.New()
	n, err := io.CopyBuffer(h, io.LimitReader(stagingContextReader{ctx, r}, m.Bytes+1), make([]byte, 65536))
	if err != nil || n != m.Bytes || hex.EncodeToString(h.Sum(nil)) != string(m.SHA256) {
		return ErrStagingInput
	}
	return ctx.Err()
}

type stagingContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r stagingContextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(b)
}

func (r stagingResponse) observation(p stagingPlan, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	var none provider.PreparationObservation
	if r.Protocol != 1 {
		return none, ErrProtocol
	}
	if r.Status == "unknown" || r.Status == "not_found" || r.Status == "invalid" {
		if r != (stagingResponse{Protocol: 1, Status: r.Status}) {
			return none, ErrProtocol
		}
		if r.Status == "invalid" {
			return none, ErrStagingIdentity
		}
		status := provider.ReconciliationUnknown
		if r.Status == "not_found" {
			status = provider.ReconciliationNotFound
		}
		return provider.PreparationObservation{Status: status}, nil
	}
	id, err := strconv.ParseInt(r.DatasetID, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != r.DatasetID || r.Owner != p.request.Owner || r.Slug != p.request.Slug || r.Version != 1 || r.MarkerSHA256 != p.request.MarkerSHA256 {
		return none, ErrStagingIdentity
	}
	var size int64
	for _, f := range p.request.Files {
		size += f.Bytes
	}
	if r.Status == "pending" {
		if r.VerifiedFiles != 0 || r.VerifiedBytes != 0 {
			return none, ErrProtocol
		}
	} else if r.Status != "ready" || !r.Private || r.VerifiedFiles != len(p.request.Files) || r.VerifiedBytes != size {
		return none, ErrProtocol
	}
	// The existing M3 journal pins this exact string at the first observation,
	// so deletion/recreation or version replacement cannot retarget recovery.
	ref, err := json.Marshal(struct {
		DatasetID string              `json:"dataset_id"`
		Reference string              `json:"reference"`
		Version   int                 `json:"version"`
		Marker    domain.SHA256Digest `json:"marker_sha256"`
	}{r.DatasetID, r.Owner + "/" + r.Slug, 1, r.MarkerSHA256})
	if err != nil {
		return none, ErrProtocol
	}
	prepared := provider.Prepared{Identity: plan.Job.Identity, PreparationID: operation, PlanSHA256: plan.Digest(), Resource: string(ref), Ready: r.Status == "ready", Private: r.Private}
	seen := provider.PreparationObservation{Status: provider.ReconciliationFound, Prepared: &prepared}
	if seen.Validate(plan, operation) != nil {
		return none, ErrProtocol
	}
	return seen, nil
}
