package kaggle

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// RuntimeLoader reloads the original durable plan/preparation for one attempt.
// It is read-only and must never synthesize a new mutation permit.
type RuntimeLoader func(context.Context, provider.Identity) (provider.Plan, provider.Prepared, error)

// RuntimeAdapter composes the reviewed Kaggle components for normal durable
// orchestration. Mutation authorization is finite and process-local; M3 remains
// the durable authority for whether Prepare or Submit may be called at all.
type RuntimeAdapter struct {
	config        Config
	binding       provider.BindingSnapshot
	policy        ExecutionPolicy
	allow         bool
	maxAttempts   int
	load          RuntimeLoader
	clock         ports.Clock
	preflight     *Preflight
	stager        *Stager
	monitor       *Monitor
	mu            sync.Mutex
	authorized    map[provider.Identity]bool
	components    map[provider.Identity]*runtimeComponents
	makeExecutor  func(*Stager, ExecutionPolicy, provider.Plan, provider.Prepared, bool) (*Executor, error)
	makeArtifacts func(*Executor, ArtifactPolicy) (*ArtifactReader, error)
}

type runtimeComponents struct {
	plan      provider.Plan
	prepared  provider.Prepared
	executor  *Executor
	artifacts *ArtifactReader
}

var (
	_ provider.Provider            = (*RuntimeAdapter)(nil)
	_ provider.BindingVerifier     = (*RuntimeAdapter)(nil)
	_ provider.PreparationObserver = (*RuntimeAdapter)(nil)
	_ provider.QuotaReader         = (*RuntimeAdapter)(nil)
	_ provider.LogReader           = (*RuntimeAdapter)(nil)
	_ provider.Canceller           = (*RuntimeAdapter)(nil)

	ErrRuntimeBinding = errors.New("runtime provider binding does not match the configured immutable profile")
	ErrRuntimeBudget  = errors.New("runtime provider mutation budget exhausted; restart only with explicit authorization after resolving uncertain attempts")
	ErrRuntimeCleanup = errors.New("runtime remote cleanup apply is unavailable; preserve owned resources and recovery evidence")
)

// NewRuntimeAdapter creates one immutable Kaggle provider revision. maxAttempts
// bounds how many distinct attempt identities this process may newly mutate.
// Read-only recovery does not consume this budget.
func NewRuntimeAdapter(
	c Config,
	binding provider.BindingSnapshot,
	resolver ports.CredentialResolver,
	blobs StagingBlobs,
	clock ports.Clock,
	load RuntimeLoader,
	policy ExecutionPolicy,
	maxAttempts int,
	allowMutations bool,
) (*RuntimeAdapter, error) {
	if c.Validate() != nil || !binding.Valid() || resolver == nil || blobs == nil || clock == nil || load == nil || !policy.valid() {
		return nil, ErrConfig
	}
	if binding.Binding.ProviderInstanceID != domain.ProviderInstanceID(c.InstanceID) ||
		binding.Binding.ConfigurationRevision != c.Revision ||
		binding.AccountScope != c.AccountName ||
		binding.CredentialRef != string(c.CredentialRef) {
		return nil, ErrRuntimeBinding
	}
	if allowMutations {
		if maxAttempts < 1 || maxAttempts > 64 {
			return nil, ErrRuntimeBudget
		}
	} else if maxAttempts != 0 {
		return nil, ErrRuntimeBudget
	}
	stager, err := NewStager(c, DefaultStagingPolicy(), resolver, blobs, allowMutations)
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
	return &RuntimeAdapter{
		config: c, binding: binding, policy: policy, allow: allowMutations,
		maxAttempts: maxAttempts, load: load, clock: clock, preflight: preflight,
		stager: stager, monitor: monitor, authorized: make(map[provider.Identity]bool),
		components: make(map[provider.Identity]*runtimeComponents),
		makeExecutor: NewExecutor, makeArtifacts: NewArtifactReader,
	}, nil
}

func (p *RuntimeAdapter) Describe() provider.Descriptor {
	d, err := OperationalDescriptor(p.config)
	if err != nil {
		return provider.Descriptor{}
	}
	for i := range d.Capabilities {
		c := &d.Capabilities[i]
		switch c.Name {
		case domain.CapabilityBatchExecution, domain.CapabilityPython, domain.CapabilityShell,
			domain.CapabilityPrivateInputStaging, domain.CapabilityExecutionTimeout,
			domain.CapabilityRemoteNetworkControl, domain.CapabilityStrongIdentity:
			c.Support = domain.CapabilitySupportSupported
			c.Evidence = domain.EvidenceImplementedOffline
			c.Reason = "normal runtime composition uses the reviewed finite-batch Kaggle components"
			if c.Name == domain.CapabilityExecutionTimeout {
				c.Conditions = []string{"runner and provider-request budgets are enforced locally; exact provider timeout enforcement remains unverified"}
			}
			if c.Name == domain.CapabilityRemoteNetworkControl {
				c.Conditions = []string{"maps the frozen job requirement to the provider setting; the runner is not a firewall"}
			}
		case domain.CapabilityGPU:
			if p.policy.MachineShape == "" {
				c.Support = domain.CapabilitySupportUnsupported
				c.Evidence = domain.EvidenceImplementedOffline
				c.Reason = "this provider instance is configured without a GPU machine shape"
			} else {
				c.Support = domain.CapabilitySupportUnknown
				c.Evidence = domain.EvidenceImplementedOffline
				c.Reason = "the requested GPU is verified after execution starts; allocation is not guaranteed"
				c.Conditions = []string{"configured machine shape: " + p.policy.MachineShape}
			}
		}
	}
	return d
}

func (p *RuntimeAdapter) VerifyBinding(ctx context.Context, expected provider.BindingSnapshot) error {
	if p == nil || expected != p.binding {
		return ErrRuntimeBinding
	}
	r, err := p.preflight.Check(ctx, ReadOnly)
	if err != nil || r.Local != "ready" || r.Authentication != "verified" || r.AccountBinding != "matched" {
		return ErrProcess
	}
	return nil
}

func (p *RuntimeAdapter) Check(ctx context.Context) (provider.DiagnosticReport, error) {
	if err := p.VerifyBinding(ctx, p.binding); err != nil {
		return provider.DiagnosticReport{}, err
	}
	return provider.DiagnosticReport{
		Evidence: domain.EvidenceImplementedOffline, CheckedAt: p.clock.Now().UTC(), Ready: true,
	}, nil
}

func (p *RuntimeAdapter) Validate(ctx context.Context, job provider.ResolvedJob) (provider.Plan, error) {
	if err := ctx.Err(); err != nil {
		return provider.Plan{}, err
	}
	if p == nil || job.Validate() != nil || job.Inputs == nil ||
		job.Identity.InstanceID != domain.ProviderInstanceID(p.config.InstanceID) ||
		job.Binding != p.binding.Binding || job.WallSeconds > p.policy.MaxWallSeconds {
		return provider.Plan{}, ErrRuntimeBinding
	}
	req, err := admission.Parse(job.Specification)
	if err != nil {
		return provider.Plan{}, ErrConfig
	}
	spec := req.Spec()
	gpu := spec.Resources.Accelerator == "gpu"
	if gpu != (p.policy.MachineShape != "") || spec.Network.RemoteInternet == "required" && !p.policy.AllowInternet {
		return provider.Plan{}, provider.Problem(domain.CodeResourceRequirementUnsatisfied, domain.FailureStageValidation, "job requirements exceed the configured Kaggle provider policy")
	}
	plan := provider.Plan{Job: job.Clone()}
	for _, name := range job.Required {
		support := p.Describe().Support(name)
		if support == domain.CapabilitySupportUnsupported {
			return provider.Plan{}, provider.Problem(domain.CodeUnsupportedCapability, domain.FailureStageValidation, "required capability is unsupported by this Kaggle provider instance")
		}
		if support != domain.CapabilitySupportSupported {
			if name != domain.CapabilityGPU || !gpu {
				return provider.Plan{}, provider.Problem(domain.CodeUnsupportedCapability, domain.FailureStageValidation, "required capability cannot be verified before dispatch")
			}
			plan.VerifyAfterStart = append(plan.VerifyAfterStart, domain.CapabilityGPU)
		}
	}
	if gpu {
		q, err := provider.ReadAvailableQuota(ctx, p)
		if err != nil || q.Status != provider.QuotaKnown || q.Remaining == nil || *q.Remaining < float64(job.WallSeconds) {
			return provider.Plan{}, provider.Problem(domain.CodeResourceRequirementUnsatisfied, domain.FailureStageValidation, "known free GPU quota is required before dispatch")
		}
	}
	return plan, nil
}

func (p *RuntimeAdapter) authorize(id provider.Identity) error {
	if p == nil || !p.allow || id.Validate() != nil {
		return ErrRuntimeBudget
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.authorized[id] {
		return nil
	}
	if len(p.authorized) >= p.maxAttempts {
		return ErrRuntimeBudget
	}
	p.authorized[id] = true
	return nil
}

func (p *RuntimeAdapter) validPlan(ctx context.Context, plan provider.Plan) error {
	expected, err := p.Validate(ctx, plan.Job)
	if err != nil || expected.Digest() != plan.Digest() {
		if err != nil {
			return err
		}
		return ErrRuntimeBinding
	}
	return nil
}

func (p *RuntimeAdapter) Prepare(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.Prepared, error) {
	if err := p.validPlan(ctx, plan); err != nil {
		return provider.Prepared{}, err
	}
	if err := p.authorize(plan.Job.Identity); err != nil {
		return provider.Prepared{}, err
	}
	return p.stager.Prepare(ctx, plan, operation)
}

func (p *RuntimeAdapter) ReconcilePreparation(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	if p == nil || plan.Job.Validate() != nil || plan.Job.Binding != p.binding.Binding {
		return provider.PreparationObservation{}, ErrRuntimeBinding
	}
	return p.stager.ReconcilePreparation(ctx, plan, operation)
}

func (p *RuntimeAdapter) attempt(ctx context.Context, id provider.Identity) (*runtimeComponents, error) {
	if p == nil || id.Validate() != nil || id.InstanceID != domain.ProviderInstanceID(p.config.InstanceID) {
		return nil, ErrRuntimeBinding
	}
	plan, prepared, err := p.load(ctx, id)
	if err != nil || plan.Job.Identity != id || plan.Job.Binding != p.binding.Binding || prepared.Validate(plan, prepared.PreparationID) != nil {
		return nil, ErrRuntimeBinding
	}
	digest := plan.Digest()

	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.components[id]; existing != nil {
		if existing.plan.Digest() != digest || existing.prepared != prepared {
			return nil, ErrRuntimeBinding
		}
		return existing, nil
	}
	executor, err := p.makeExecutor(p.stager, p.policy, plan, prepared, p.allow)
	if err != nil {
		return nil, err
	}
	executor.now = p.clock.Now
	artifacts, err := p.makeArtifacts(executor, DefaultArtifactPolicy())
	if err != nil {
		return nil, err
	}
	c := &runtimeComponents{plan: plan.Clone(), prepared: prepared, executor: executor, artifacts: artifacts}
	p.components[id] = c
	return c, nil
}

func (p *RuntimeAdapter) Submit(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
	if p == nil || prepared.Identity.Validate() != nil {
		return rejectedExecution()
	}
	if err := p.authorize(prepared.Identity); err != nil {
		problem := provider.Problem(domain.CodeResourceRequirementUnsatisfied, domain.FailureStageSubmission, "provider mutation budget is exhausted before submission")
		return provider.SubmissionOutcome{Status: provider.SubmissionRejected, Problem: &problem}
	}
	c, err := p.attempt(ctx, prepared.Identity)
	if err != nil || c.prepared != prepared {
		return unknownExecution()
	}
	return c.executor.Submit(ctx, prepared)
}

func (p *RuntimeAdapter) ReconcileSubmission(ctx context.Context, id provider.Identity) (provider.Reconciliation, error) {
	c, err := p.attempt(ctx, id)
	if err != nil {
		return provider.Reconciliation{}, err
	}
	return c.executor.ReconcileSubmission(ctx, id)
}

func (p *RuntimeAdapter) Observe(ctx context.Context, ref provider.RemoteReference) (provider.Observation, error) {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		return provider.Observation{}, err
	}
	return c.executor.Observe(ctx, ref)
}

func (p *RuntimeAdapter) ListArtifacts(ctx context.Context, ref provider.RemoteReference, page provider.PageRequest) (provider.ArtifactPage, error) {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		return provider.ArtifactPage{}, err
	}
	return c.artifacts.ListArtifacts(ctx, ref, page)
}

func (p *RuntimeAdapter) FetchArtifact(ctx context.Context, ref provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64) (provider.TransferResult, error) {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		return provider.TransferResult{}, err
	}
	return c.artifacts.FetchArtifact(ctx, ref, file, dst, limit)
}

func (p *RuntimeAdapter) ReadQuota(ctx context.Context) (provider.QuotaObservation, error) {
	return p.monitor.ReadQuota(ctx)
}

func (p *RuntimeAdapter) ReadLogs(ctx context.Context, ref provider.RemoteReference, page provider.PageRequest) (provider.LogPage, error) {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		return provider.LogPage{}, err
	}
	logs, err := NewLogReader(c.executor, p.monitor)
	if err != nil {
		return provider.LogPage{}, err
	}
	return logs.ReadLogs(ctx, ref, page)
}

func (p *RuntimeAdapter) Cancel(ctx context.Context, ref provider.RemoteReference, operation domain.OperationID) (provider.CancellationOutcome, error) {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		return provider.CancellationOutcome{}, err
	}
	return c.executor.Cancel(ctx, ref, operation)
}

func (p *RuntimeAdapter) Cleanup(context.Context, provider.CleanupRequest) (provider.CleanupOutcome, error) {
	return provider.CleanupOutcome{}, ErrRuntimeCleanup
}
