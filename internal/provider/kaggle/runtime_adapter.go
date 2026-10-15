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
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

// RuntimeLoader reloads the original durable plan/preparation for one attempt.
// It is read-only and must never synthesize a new mutation permit.
type RuntimeLoader func(context.Context, provider.Identity) (provider.Plan, provider.Prepared, error)

// RuntimeAdapter composes the reviewed Kaggle components for normal durable
// orchestration. The repository owns durable authorization and one-shot intents
// for every preparation and submission; this adapter keeps no process budget.
type RuntimeAdapter struct {
	config        Config
	binding       provider.BindingSnapshot
	policy        ExecutionPolicy
	allow         bool
	load          RuntimeLoader
	clock         ports.Clock
	preflight     *Preflight
	stager        *Stager
	monitor       *Monitor
	mu            sync.Mutex
	components    map[provider.Identity]*runtimeComponents
	makeExecutor  func(*Stager, ExecutionPolicy, provider.Plan, provider.Prepared, bool) (*Executor, error)
	makeArtifacts func(*Executor, ArtifactPolicy) (*ArtifactReader, error)
	runCleanup    func(context.Context, Config, string, []byte, cleanupRequest) (cleanupResponse, error)
}

type runtimeComponents struct {
	plan      provider.Plan
	prepared  provider.Prepared
	executor  *Executor
	artifacts *ArtifactReader
}

var (
	_ provider.Provider = (*RuntimeAdapter)(nil)

	ErrRuntimeBinding = errors.New("runtime provider binding does not match the configured immutable profile")
)

// NewRuntimeAdapter creates one immutable Kaggle provider revision.
// Read-only recovery never creates a mutation permit.
func NewRuntimeAdapter(
	c Config,
	binding provider.BindingSnapshot,
	resolver ports.CredentialResolver,
	blobs StagingBlobs,
	clock ports.Clock,
	load RuntimeLoader,
	policy ExecutionPolicy,
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
		load: load, clock: clock, preflight: preflight,
		stager: stager, monitor: monitor,
		components:   make(map[provider.Identity]*runtimeComponents),
		makeExecutor: NewExecutor, makeArtifacts: NewArtifactReader,
		runCleanup: runCleanup,
	}, nil
}

func (p *RuntimeAdapter) Describe() provider.Descriptor {
	return provider.Descriptor{Type: "kaggle", InstanceID: domain.ProviderInstanceID(p.config.InstanceID), Version: "native-api/v1",
		Capabilities: RuntimeCapabilities(p.policy.MachineShape)}
}

// RuntimeCapabilities is shared by the runtime port and managed connection
// descriptor so policy cannot drift between their declarations.
func RuntimeCapabilities(shape string) provider.Capabilities {
	c := provider.Capabilities{BatchExecution: true, Python: true, Shell: true,
		GPU: domain.CapabilitySupportUnsupported, PrivateInputStaging: true,
		RemoteCleanup: true, LogsWhileRunning: domain.CapabilitySupportUnknown,
		LogsAfterCompletion: true, QuotaReporting: domain.CapabilitySupportSupported,
		ExecutionTimeout: true, RemoteNetworkControl: true, StrongIdentity: true,
		Accelerators: []string{"cpu"}}
	if shape != "cpu" && shape != "" {
		c.GPU = domain.CapabilitySupportUnknown
		c.GPUShape = shape
		c.Accelerators = []string{"gpu"}
	}
	return c
}

func (p *RuntimeAdapter) Verify(ctx context.Context, expected provider.BindingSnapshot) error {
	if p == nil || expected != p.binding {
		return ErrRuntimeBinding
	}
	r, err := p.preflight.Check(ctx, ReadOnly)
	if provider.IsLocalStateError(err) {
		return err
	}
	if err != nil || r.Local != "ready" || r.Authentication != "verified" || r.AccountBinding != "matched" {
		return ErrUnavailable
	}
	return nil
}

func (p *RuntimeAdapter) Validate(ctx context.Context, job provider.ResolvedJob) (provider.Plan, error) {
	plan, err := p.localPlan(ctx, job)
	if err != nil {
		return provider.Plan{}, err
	}
	if p.policy.MachineShape != "" {
		q, err := p.monitor.ReadQuota(ctx)
		if provider.IsLocalStateError(err) {
			return provider.Plan{}, err
		}
		if err != nil || q.Status != provider.QuotaKnown || q.Remaining == nil || *q.Remaining < float64(job.WallSeconds) {
			return provider.Plan{}, provider.Problem(domain.CodeResourceRequirementUnsatisfied, domain.FailureStageValidation, "known free GPU quota is required before dispatch")
		}
	}
	return plan, nil
}

func (p *RuntimeAdapter) localPlan(ctx context.Context, job provider.ResolvedJob) (provider.Plan, error) {
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
	if spec.ApplicationImage != nil || job.Inputs.ApplicationImage != nil {
		return provider.Plan{}, provider.Problem(domain.CodeUnsupportedCapability, domain.FailureStageValidation,
			"application image archives are unsupported by this Kaggle provider")
	}
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

	return plan, nil
}

func (p *RuntimeAdapter) validPlan(ctx context.Context, plan provider.Plan) error {
	expected, err := p.localPlan(ctx, plan.Job)
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
	return p.stager.Prepare(ctx, plan, operation)
}

func (p *RuntimeAdapter) ReconcilePrepare(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	if p == nil || plan.Job.Validate() != nil || plan.Job.Binding != p.binding.Binding {
		return provider.PreparationObservation{}, ErrRuntimeBinding
	}
	return p.stager.ReconcilePrepare(ctx, plan, operation)
}

func (p *RuntimeAdapter) attempt(ctx context.Context, id provider.Identity) (*runtimeComponents, error) {
	if p == nil || id.Validate() != nil || id.InstanceID != domain.ProviderInstanceID(p.config.InstanceID) {
		return nil, ErrRuntimeBinding
	}
	plan, prepared, err := p.load(ctx, id)
	if err != nil {
		return nil, provider.LocalStateError{Err: err}
	}
	if plan.Job.Identity != id || plan.Job.Binding != p.binding.Binding || prepared.Validate(plan, prepared.PreparationID) != nil {
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

func (p *RuntimeAdapter) Start(ctx context.Context, prepared provider.Prepared, authorization *provider.StartAuthorization, material *workloadsecret.Material) provider.SubmissionOutcome {
	if p == nil || authorization != nil || material != nil {
		return rejectedExecution(reasonSubmissionDisabled)
	}
	if prepared.Identity.Validate() != nil {
		return rejectedExecution(reasonPreparationMismatch)
	}
	c, err := p.attempt(ctx, prepared.Identity)
	if err != nil || c.prepared != prepared {
		return unknownExecution()
	}
	return c.executor.Start(ctx, prepared, nil, nil)
}

func (p *RuntimeAdapter) Reconcile(ctx context.Context, id provider.Identity) (provider.Reconciliation, error) {
	c, err := p.attempt(ctx, id)
	if err != nil {
		return provider.Reconciliation{}, err
	}
	return c.executor.Reconcile(ctx, id)
}

func (p *RuntimeAdapter) Observe(ctx context.Context, ref provider.RemoteReference) (provider.Observation, error) {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		return provider.Observation{}, err
	}
	return c.executor.Observe(ctx, ref)
}

func (p *RuntimeAdapter) ListArtifacts(ctx context.Context, ref provider.RemoteReference) ([]provider.Artifact, error) {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		return nil, err
	}
	return c.artifacts.ListArtifacts(ctx, ref)
}
func (p *RuntimeAdapter) Fetch(ctx context.Context, ref provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		return provider.TransferResult{}, err
	}
	return c.artifacts.Fetch(ctx, ref, file, dst, limit, progress)
}
func (p *RuntimeAdapter) Cancel(ctx context.Context, ref provider.RemoteReference, operation domain.OperationID) (provider.CancellationOutcome, error) {
	if err := ctx.Err(); err != nil {
		return provider.CancellationOutcome{}, err
	}
	if p == nil || ref.Validate() != nil || !operation.Valid() || ref.Identity.InstanceID != domain.ProviderInstanceID(p.config.InstanceID) {
		return provider.CancellationOutcome{}, ErrRuntimeBinding
	}
	return provider.CancellationOutcome{Status: domain.CancellationManual}, nil
}
func (p *RuntimeAdapter) Logs(ctx context.Context, ref provider.RemoteReference, page provider.PageRequest) (provider.LogPage, error) {
	c, err := p.attempt(ctx, ref.Identity)
	if err != nil {
		return provider.LogPage{}, err
	}
	logs, err := NewLogReader(c.executor, p.monitor)
	if err != nil {
		return provider.LogPage{}, err
	}
	return logs.Logs(ctx, ref, page)
}
