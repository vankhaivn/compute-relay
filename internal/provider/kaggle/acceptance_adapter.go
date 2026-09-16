package kaggle

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// AcceptanceScope confines experimental composition to one already admitted job.
// It is not a production instance registry or authority to create another attempt.
type AcceptanceScope struct {
	Binding             provider.BindingSnapshot
	WorkspaceID         domain.WorkspaceID
	JobID               domain.JobID
	AttemptID           domain.AttemptID
	SpecificationSHA256 domain.SHA256Digest
}

// AcceptanceLoader reads the original M3 journal. It may not synthesize a new
// plan/preparation or follow a mutable profile. Missing evidence must return error.
type AcceptanceLoader func(context.Context) (provider.Plan, provider.Prepared, error)

// AcceptanceAdapter combines the real component ports for the finite operator
// acceptance harness. Capabilities describe the experimental component path, not
// passed-live evidence. M3 alone owns new mutation permits and durable recovery.
type AcceptanceAdapter struct {
	config Config
	scope AcceptanceScope
	policy ExecutionPolicy
	allow bool
	load AcceptanceLoader
	clock ports.Clock
	preflight *Preflight
	stager *Stager
	monitor *Monitor
	mu sync.Mutex
	executor *Executor
	artifacts *ArtifactReader
	makeExecutor func(*Stager, ExecutionPolicy, provider.Plan, provider.Prepared, bool) (*Executor, error)
	makeArtifacts func(*Executor, ArtifactPolicy) (*ArtifactReader, error)
}

var (
	_ provider.Provider = (*AcceptanceAdapter)(nil)
	_ provider.BindingVerifier = (*AcceptanceAdapter)(nil)
	_ provider.PreparationObserver = (*AcceptanceAdapter)(nil)
	_ provider.QuotaReader = (*AcceptanceAdapter)(nil)
	_ provider.LogReader = (*AcceptanceAdapter)(nil)
	_ provider.Canceller = (*AcceptanceAdapter)(nil)
	ErrAcceptanceScope = errors.New("acceptance target differs from the original admitted attempt")
	ErrAcceptanceCleanup = errors.New("acceptance cleanup is unavailable; preserve owned resources and recovery evidence")
)

func NewAcceptanceAdapter(c Config, scope AcceptanceScope, resolver ports.CredentialResolver, blobs StagingBlobs, clock ports.Clock, load AcceptanceLoader, shape string, allowMutations bool) (*AcceptanceAdapter, error) {
	if c.Validate() != nil || !scope.Binding.Valid() || scope.Binding.Binding.ProviderInstanceID != domain.ProviderInstanceID(c.InstanceID) || scope.Binding.Binding.ConfigurationRevision != c.Revision || scope.Binding.AccountScope != c.AccountName || scope.Binding.CredentialRef != string(c.CredentialRef) || !scope.WorkspaceID.Valid() || !scope.JobID.Valid() || !scope.AttemptID.Valid() || !scope.SpecificationSHA256.Valid() || clock == nil || load == nil {
		return nil, ErrAcceptanceScope
	}
	policy := DefaultExecutionPolicy()
	policy.MaxWallSeconds, policy.MachineShape = 120, shape
	if !policy.valid() || shape == "" {
		return nil, ErrConfig
	}
	stagePolicy := DefaultStagingPolicy()
	stage, err := NewStager(c, stagePolicy, resolver, blobs, allowMutations)
	if err != nil {
		return nil, err
	}
	preflight, err := New(c, resolver)
	if err != nil {
		return nil, err
	}
	monitor, err := NewMonitor(c, resolver, clock)
	if err != nil {
		return nil, err
	}
	return &AcceptanceAdapter{config:c, scope:scope, policy:policy, allow:allowMutations, load:load, clock:clock, preflight:preflight, stager:stage, monitor:monitor, makeExecutor:NewExecutor, makeArtifacts:NewArtifactReader}, nil
}

func (p *AcceptanceAdapter) matches(id provider.Identity) bool {
	return p != nil && id.WorkspaceID == p.scope.WorkspaceID && id.JobID == p.scope.JobID && id.AttemptID == p.scope.AttemptID && id.InstanceID == domain.ProviderInstanceID(p.config.InstanceID)
}

func (p *AcceptanceAdapter) Describe() provider.Descriptor {
	d, _ := OperationalDescriptor(p.config)
	d.Type = "kaggle-acceptance"
	for i := range d.Capabilities {
		c := &d.Capabilities[i]
		switch c.Name {
		case domain.CapabilityBatchExecution, domain.CapabilityPython, domain.CapabilityPrivateInputStaging, domain.CapabilityStrongIdentity, domain.CapabilityExecutionTimeout, domain.CapabilityRemoteNetworkControl:
			c.Support = domain.CapabilitySupportSupported
			c.Reason = "explicit single-job acceptance path; implementation evidence only, not release qualification"
			c.Conditions = []string{"operator opt-in and original scoped attempt required; provider enforcement, immutable mounts and same-version session identity remain unverified"}
		case domain.CapabilityShell:
			c.Support = domain.CapabilitySupportUnsupported
			c.Reason = "the acceptance workload is fixed Python, not arbitrary shell"
		}
	}
	return d
}

func (p *AcceptanceAdapter) VerifyBinding(ctx context.Context, expected provider.BindingSnapshot) error {
	if p == nil || expected != p.scope.Binding {
		return ErrAcceptanceScope
	}
	r, err := p.preflight.Check(ctx, ReadOnly)
	if err != nil || r.Local != "ready" || r.Authentication != "verified" || r.AccountBinding != "matched" {
		return ErrProcess
	}
	return nil
}
func (p *AcceptanceAdapter) Check(ctx context.Context) (provider.DiagnosticReport, error) {
	if err := p.VerifyBinding(ctx, p.scope.Binding); err != nil {
		return provider.DiagnosticReport{}, err
	}
	// Account verification is not GPU/result/restart qualification.
	return provider.DiagnosticReport{Evidence:domain.EvidenceImplementedOffline, CheckedAt:p.clock.Now().UTC(), Ready:false, Problems:[]domain.Problem{provider.Problem(domain.CodeUnsupportedCapability, domain.FailureStageValidation, "account checked; complete GPU and restart acceptance is still required")}}, nil
}
func (p *AcceptanceAdapter) Validate(ctx context.Context, job provider.ResolvedJob) (provider.Plan, error) {
	if err := ctx.Err(); err != nil {
		return provider.Plan{}, err
	}
	if !p.matches(job.Identity) || job.Validate() != nil || job.Binding != p.scope.Binding.Binding || job.SpecificationSHA256 != p.scope.SpecificationSHA256 || job.Inputs == nil || job.WallSeconds != 120 {
		return provider.Plan{}, ErrAcceptanceScope
	}
	gpu := false
	for _, required := range job.Required {
		if required == domain.CapabilityGPU {
			gpu = true
		} else if p.Describe().Support(required) != domain.CapabilitySupportSupported {
			return provider.Plan{}, ErrAcceptanceScope
		}
	}
	if !gpu {
		return provider.Plan{}, ErrAcceptanceScope
	}
	return provider.Plan{Job:job.Clone(), VerifyAfterStart:[]domain.CapabilityName{domain.CapabilityGPU}}, nil
}
func (p *AcceptanceAdapter) validPlan(ctx context.Context, plan provider.Plan) error {
	expected, err := p.Validate(ctx, plan.Job)
	if err != nil || expected.Digest() != plan.Digest() {
		return ErrAcceptanceScope
	}
	return nil
}
func (p *AcceptanceAdapter) Prepare(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.Prepared, error) {
	if err := p.validPlan(ctx, plan); err != nil || !p.allow {
		return provider.Prepared{}, ErrAcceptanceScope
	}
	return p.stager.Prepare(ctx, plan, operation)
}
func (p *AcceptanceAdapter) ReconcilePreparation(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	if err := p.validPlan(ctx, plan); err != nil {
		return provider.PreparationObservation{}, err
	}
	return p.stager.ReconcilePreparation(ctx, plan, operation)
}

func (p *AcceptanceAdapter) components(ctx context.Context) (*Executor, *ArtifactReader, error) {
	if p == nil {
		return nil, nil, ErrAcceptanceScope
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	plan, prepared, err := p.load(ctx)
	if err != nil || p.validPlan(ctx, plan) != nil {
		return nil, nil, ErrAcceptanceScope
	}
	if p.executor != nil {
		if p.executor.plan.Digest() != plan.Digest() || p.executor.prepared != prepared {
			return nil, nil, ErrAcceptanceScope
		}
		return p.executor, p.artifacts, nil
	}
	e, err := p.makeExecutor(p.stager, p.policy, plan, prepared, p.allow)
	if err != nil {
		return nil, nil, err
	}
	e.now = p.clock.Now
	policy := DefaultArtifactPolicy()
	policy.MaxBytes = 64 << 20
	policy.MaxFiles = 6
	a, err := p.makeArtifacts(e, policy)
	if err != nil {
		return nil, nil, err
	}
	p.executor, p.artifacts = e, a
	return e, a, nil
}
func (p *AcceptanceAdapter) Submit(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
	if !p.matches(prepared.Identity) || !p.allow {
		return unknownExecution() // Missing permission cannot prove no previous execution.
	}
	e, _, err := p.components(ctx)
	if err != nil {
		return unknownExecution()
	}
	return e.Submit(ctx, prepared)
}
func (p *AcceptanceAdapter) ReconcileSubmission(ctx context.Context, id provider.Identity) (provider.Reconciliation, error) {
	if !p.matches(id) {
		return provider.Reconciliation{}, ErrAcceptanceScope
	}
	e, _, err := p.components(ctx)
	if err != nil {
		return provider.Reconciliation{}, err
	}
	return e.ReconcileSubmission(ctx, id)
}
func (p *AcceptanceAdapter) Observe(ctx context.Context, ref provider.RemoteReference) (provider.Observation, error) {
	if !p.matches(ref.Identity) {
		return provider.Observation{}, ErrAcceptanceScope
	}
	e, _, err := p.components(ctx)
	if err != nil {
		return provider.Observation{}, err
	}
	return e.Observe(ctx, ref)
}
func (p *AcceptanceAdapter) ListArtifacts(ctx context.Context, ref provider.RemoteReference, page provider.PageRequest) (provider.ArtifactPage, error) {
	if !p.matches(ref.Identity) {
		return provider.ArtifactPage{}, ErrAcceptanceScope
	}
	_, a, err := p.components(ctx)
	if err != nil {
		return provider.ArtifactPage{}, err
	}
	return a.ListArtifacts(ctx, ref, page)
}
func (p *AcceptanceAdapter) FetchArtifact(ctx context.Context, ref provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64) (provider.TransferResult, error) {
	if !p.matches(ref.Identity) {
		return provider.TransferResult{}, ErrAcceptanceScope
	}
	_, a, err := p.components(ctx)
	if err != nil {
		return provider.TransferResult{}, err
	}
	return a.FetchArtifact(ctx, ref, file, dst, limit)
}
func (p *AcceptanceAdapter) ReadQuota(ctx context.Context) (provider.QuotaObservation, error) {
	return p.monitor.ReadQuota(ctx)
}
func (p *AcceptanceAdapter) ReadLogs(ctx context.Context, ref provider.RemoteReference, page provider.PageRequest) (provider.LogPage, error) {
	if !p.matches(ref.Identity) {
		return provider.LogPage{}, ErrAcceptanceScope
	}
	e, _, err := p.components(ctx)
	if err != nil {
		return provider.LogPage{}, err
	}
	logs, err := NewLogReader(e, p.monitor)
	if err != nil {
		return provider.LogPage{}, err
	}
	return logs.ReadLogs(ctx, ref, page)
}
func (p *AcceptanceAdapter) Cancel(ctx context.Context, ref provider.RemoteReference, operation domain.OperationID) (provider.CancellationOutcome, error) {
	if !p.matches(ref.Identity) {
		return provider.CancellationOutcome{}, ErrAcceptanceScope
	}
	e, _, err := p.components(ctx)
	if err != nil {
		return provider.CancellationOutcome{}, err
	}
	return e.Cancel(ctx, ref, operation)
}
func (p *AcceptanceAdapter) Cleanup(context.Context, provider.CleanupRequest) (provider.CleanupOutcome, error) {
	return provider.CleanupOutcome{}, ErrAcceptanceCleanup
}

// Keep the same cooperative control-call limit as the component path. The harness
// applies its own finite lifetime without pretending local exit cancels remote work.
var _ = time.Minute
