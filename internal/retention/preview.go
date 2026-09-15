package retention

import (
	"context"
	"encoding/json"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// RemotePlan is INTERNAL operator evidence, not a public HTTP view. It contains
// exact private references and must not be logged or sent to workload clients.
// A plan is not an apply permit; the only operation below is an explicit dry run.
type RemotePlan struct {
	WorkspaceID         domain.WorkspaceID
	JobID               domain.JobID
	AttemptID           domain.AttemptID
	ResourceID          domain.ProviderResourceID
	CreationOperationID domain.OperationID
	Purpose             string
	Binding             provider.BindingSnapshot
	Identity            provider.Identity
	Reference           string
	Remote              *provider.RemoteReference
	AttemptRevision     uint64
	PlanSHA256          domain.SHA256Digest
	PublicationSHA256   domain.SHA256Digest
	Decision            Decision
}

func (RemotePlan) String() string   { return "[private cleanup plan]" }
func (RemotePlan) GoString() string { return "[private cleanup plan]" }
func (p RemotePlan) Digest() domain.SHA256Digest {
	data, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	return provider.Digest(data)
}

type PreviewRepository interface {
	PlanRemoteCleanup(context.Context, domain.WorkspaceID, string, domain.ProviderResourceID, Policy, time.Time) (RemotePlan, error)
	RecordCleanupPreview(context.Context, domain.WorkspaceID, string, RemotePlan, Policy, string, time.Time) error
}

// The consumer-sized port never exposes compute creation or an apply operation.
// BoundPreviewSource supplies the dry-run-only adapter projection.
type PreviewSource interface {
	VerifyBinding(context.Context, provider.BindingSnapshot) error
	Preview(context.Context, provider.CleanupRequest) (provider.CleanupOutcome, error)
}

type PreviewResolver interface {
	ResolvePreview(provider.BindingSnapshot) (PreviewSource, error)
}

type SnapshotResolver struct{ Registry *provider.SnapshotRegistry }

type boundPreviewSource struct {
	verifier provider.BindingVerifier
	cleanup  interface {
		Cleanup(context.Context, provider.CleanupRequest) (provider.CleanupOutcome, error)
	}
}

func (r SnapshotResolver) ResolvePreview(binding provider.BindingSnapshot) (PreviewSource, error) {
	if r.Registry == nil {
		return nil, ErrInvalid
	}
	p, err := r.Registry.Resolve(binding)
	if err != nil {
		return nil, err
	}
	v, ok := p.(provider.BindingVerifier)
	if !ok {
		return nil, ErrInvalid
	}
	return boundPreviewSource{verifier: v, cleanup: p}, nil
}

func (p boundPreviewSource) VerifyBinding(ctx context.Context, b provider.BindingSnapshot) error {
	return p.verifier.VerifyBinding(ctx, b)
}
func (p boundPreviewSource) Preview(ctx context.Context, request provider.CleanupRequest) (provider.CleanupOutcome, error) {
	// Neither callers nor operator configuration can turn this projection into apply.
	request.Mode = provider.CleanupDryRun
	return p.cleanup.Cleanup(ctx, request)
}

type Previewer struct {
	repo     PreviewRepository
	resolver PreviewResolver
	clock    Clock
}

type PreviewResult struct {
	ResourceID domain.ProviderResourceID
	Outcome    string
	Reason     string
}

func NewPreviewer(repo PreviewRepository, resolver PreviewResolver, clock Clock) (*Previewer, error) {
	if nilDependency(repo) || nilDependency(resolver) || nilDependency(clock) {
		return nil, ErrInvalid
	}
	return &Previewer{repo: repo, resolver: resolver, clock: clock}, nil
}

// Preview requires an authenticated internal token identity with operate scope.
// It resolves a fresh exact-ledger plan, performs only the supported dry run and
// rechecks authority and pins before recording its observational outcome.
func (p *Previewer) Preview(ctx context.Context, w domain.WorkspaceID, token string, id domain.ProviderResourceID, policy Policy) (PreviewResult, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	plan, err := p.repo.PlanRemoteCleanup(ctx, w, token, id, policy, p.clock.Now())
	if err != nil {
		return PreviewResult{}, err
	}
	result := PreviewResult{ResourceID: id, Outcome: "retained", Reason: plan.Decision.Reason}
	if !plan.Decision.Eligible {
		return result, nil
	}
	if plan.Purpose != "execution" || plan.Remote == nil || plan.Remote.Validate() != nil || plan.Remote.Identity != plan.Identity || !plan.PublicationSHA256.Valid() || !plan.CreationOperationID.Valid() || !plan.ResourceID.Valid() {
		return PreviewResult{}, ErrInvalid
	}
	source, err := p.resolver.ResolvePreview(plan.Binding)
	if err != nil {
		return PreviewResult{}, err
	}
	if nilDependency(source) {
		return PreviewResult{}, ErrInvalid
	}
	if err := source.VerifyBinding(ctx, plan.Binding); err != nil {
		return PreviewResult{}, err
	}
	o, err := source.Preview(ctx, provider.CleanupRequest{Remote: *plan.Remote, LedgerID: plan.ResourceID, CreationOperationID: plan.CreationOperationID, ResultsCollected: true, Mode: provider.CleanupDryRun})
	if err != nil {
		return PreviewResult{}, err
	}
	if o.Deleted || o.WouldDelete && o.AlreadyAbsent {
		return PreviewResult{}, ErrInvalid
	}
	if o.AlreadyAbsent {
		result.Outcome = "already_absent"
	} else if o.WouldDelete {
		result.Outcome = "would_delete"
	}
	if err := p.repo.RecordCleanupPreview(ctx, w, token, plan, policy, result.Outcome, p.clock.Now()); err != nil {
		return PreviewResult{}, err
	}
	return result, nil
}
