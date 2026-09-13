// Package fake simulates provider behavior with explicit fixtures and manual advancement.
// It performs no network, process, filesystem, credential, or GPU operations. It is NOT a
// local execution provider and is not wired into the production CLI as a fallback.
package fake

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type SubmitMode string

const (
	Accept             SubmitMode = "accept"
	Reject             SubmitMode = "reject"
	AcceptLoseResponse SubmitMode = "accept_lose_response"
	Unresolved         SubmitMode = "unresolved"
)

type File struct {
	Path    string
	Content []byte
}
type Scenario struct {
	Mode             SubmitMode
	States           []domain.ExecutionState
	Files            []File
	PreparationReady bool
	GPU              domain.CapabilitySupport
}

func DefaultScenario() Scenario {
	return Scenario{
		Mode:             Accept,
		States:           []domain.ExecutionState{domain.ExecutionQueued, domain.ExecutionRunning, domain.ExecutionSucceeded},
		Files:            []File{{Path: "result.json", Content: []byte("{\"fixture\":true}\n")}},
		PreparationReady: true,
		GPU:              domain.CapabilitySupportUnsupported,
	}
}

// Backend is simulated REMOTE state. Reattaching an adapter to it tests observer recovery,
// not SQLite durability or survival of an actual operating-system process crash.
type Backend struct {
	mu       sync.Mutex
	scenario Scenario
	records  map[string]*record
	stats    Stats
}
type Stats struct {
	PrepareCalls int
	SubmitCalls  int
	Executions   int
	FetchCalls   int
	CleanupCalls int
}
type record struct {
	plan            provider.Plan
	prepared        provider.Prepared
	remote          provider.RemoteReference
	files           []File
	attempted       bool
	submitted       bool
	deleted         bool
	step            int
	cancelRequested bool
	cancelled       bool
}

func NewBackend(s Scenario) (*Backend, error) {
	switch s.Mode {
	case Accept, Reject, AcceptLoseResponse, Unresolved:
	default:
		return nil, errors.New("invalid fixture submit mode")
	}
	if !s.GPU.Valid() || len(s.States) == 0 || len(s.States) > 100 || len(s.Files) > 99 {
		return nil, errors.New("invalid fixture bounds")
	}
	for _, state := range s.States {
		if !state.Valid() || state == domain.ExecutionNotSubmitted {
			return nil, errors.New("invalid fixture observation")
		}
	}
	s.States = append([]domain.ExecutionState(nil), s.States...)
	files := make([]File, len(s.Files))
	seen := make(map[string]bool)
	for i, f := range s.Files {
		if !provider.SafeArtifactPath(f.Path) || f.Path == "execution-result.json" || seen[f.Path] || len(f.Content) > 1<<20 {
			return nil, errors.New("invalid fixture file")
		}
		seen[f.Path] = true
		files[i] = File{Path: f.Path, Content: append([]byte(nil), f.Content...)}
	}
	s.Files = files
	return &Backend{scenario: s, records: make(map[string]*record)}, nil
}
func (b *Backend) Stats() Stats { b.mu.Lock(); defer b.mu.Unlock(); return b.stats }

// Basic intentionally does NOT implement Canceller, LogReader or QuotaReader.
type Basic struct {
	backend  *Backend
	clock    ports.Clock
	instance domain.ProviderInstanceID
	complete bool
}

func New(b *Backend, clock ports.Clock, instance domain.ProviderInstanceID) (*Basic, error) {
	if b == nil || clock == nil || !instance.Valid() {
		return nil, errors.New("backend, clock and instance are required")
	}
	return &Basic{backend: b, clock: clock, instance: instance}, nil
}

var _ provider.Provider = (*Basic)(nil)

func (p *Basic) Describe() provider.Descriptor {
	d := provider.Descriptor{Type: "fake", InstanceID: p.instance, Version: "1"}
	for _, name := range []domain.CapabilityName{domain.CapabilityBatchExecution, domain.CapabilityPython, domain.CapabilityShell,
		domain.CapabilityPrivateInputStaging, domain.CapabilityExecutionTimeout, domain.CapabilityRemoteNetworkControl,
		domain.CapabilityStrongIdentity, domain.CapabilityGPU, domain.CapabilityRemoteCancellation,
		domain.CapabilityLogsWhileRunning, domain.CapabilityLogsAfterCompletion, domain.CapabilityQuotaReporting, domain.CapabilityCustomContainer, domain.CapabilityRetainedSessions} {
		support := domain.CapabilitySupportSupported
		switch name {
		case domain.CapabilityGPU:
			support = p.backend.scenario.GPU
		case domain.CapabilityRemoteCancellation, domain.CapabilityLogsWhileRunning, domain.CapabilityLogsAfterCompletion, domain.CapabilityCustomContainer, domain.CapabilityRetainedSessions:
			support = domain.CapabilitySupportUnsupported
		case domain.CapabilityQuotaReporting:
			support = domain.CapabilitySupportUnknown
		}
		if p.complete {
			switch name {
			case domain.CapabilityRemoteCancellation, domain.CapabilityLogsWhileRunning, domain.CapabilityLogsAfterCompletion, domain.CapabilityQuotaReporting:
				support = domain.CapabilitySupportSupported
			}
		}
		d.Capabilities = append(d.Capabilities, domain.CapabilityStatus{Name: name, Support: support, Evidence: domain.EvidenceImplementedOffline,
			Reason: "deterministic fixture only; no real execution", ClientVersion: "fake/1"})
	}
	return d
}
func (p *Basic) Check(ctx context.Context) (provider.DiagnosticReport, error) {
	if err := ctx.Err(); err != nil {
		return provider.DiagnosticReport{}, err
	}
	return provider.DiagnosticReport{Evidence: domain.EvidenceImplementedOffline, CheckedAt: p.clock.Now(), Ready: true}, nil
}
func (p *Basic) Validate(ctx context.Context, job provider.ResolvedJob) (provider.Plan, error) {
	if err := ctx.Err(); err != nil {
		return provider.Plan{}, err
	}
	if err := job.Validate(); err != nil {
		return provider.Plan{}, err
	}
	if job.Identity.InstanceID != p.instance {
		return provider.Plan{}, identityError()
	}
	plan := provider.Plan{Job: job.Clone()}
	for _, name := range job.Required {
		switch p.Describe().Support(name) {
		case domain.CapabilitySupportUnsupported:
			return provider.Plan{}, provider.Problem(domain.CodeUnsupportedCapability, domain.FailureStageValidation, "required capability is unsupported")
		case domain.CapabilitySupportUnknown:
			if name != domain.CapabilityGPU {
				return provider.Plan{}, provider.Problem(domain.CodeUnsupportedCapability, domain.FailureStageValidation, "required capability cannot be verified by this adapter")
			}
			plan.VerifyAfterStart = append(plan.VerifyAfterStart, name)
		}
	}
	return plan, nil
}
func (p *Basic) Prepare(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.Prepared, error) {
	if err := ctx.Err(); err != nil {
		return provider.Prepared{}, err
	}
	validated, err := p.Validate(ctx, plan.Job)
	if err != nil {
		return provider.Prepared{}, err
	}
	if validated.Digest() != plan.Digest() || !operation.Valid() {
		return provider.Prepared{}, errors.New("invalid preparation plan or operation")
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	p.backend.stats.PrepareCalls++
	key := attemptKey(plan.Job.Identity)
	if r, ok := p.backend.records[key]; ok {
		if r.plan.Digest() != plan.Digest() || r.prepared.PreparationID != operation || r.deleted {
			return provider.Prepared{}, identityError()
		}
		return r.prepared, nil
	}
	prepared := provider.Prepared{Identity: plan.Job.Identity, PreparationID: operation, PlanSHA256: plan.Digest(), Resource: plan.Job.Identity.ResourceKey, Ready: p.backend.scenario.PreparationReady}
	remote := provider.RemoteReference{Identity: prepared.Identity, Resource: prepared.Resource, Version: "fixture-1"}
	r := &record{plan: plan.Clone(), prepared: prepared, remote: remote}
	p.backend.records[key] = r
	return prepared, nil
}
func (p *Basic) Submit(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
	if err := ctx.Err(); err != nil {
		return rejection("submission cancelled before any simulated side effect")
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	p.backend.stats.SubmitCalls++
	r, ok := p.backend.records[attemptKey(prepared.Identity)]
	if !ok || r.prepared != prepared || prepared.Identity.InstanceID != p.instance || r.deleted {
		return rejection("prepared resource identity mismatch")
	}
	if r.attempted {
		return unknown("submission already attempted; reconcile instead of submitting again")
	}
	if !prepared.Ready {
		return rejection("staging is not ready")
	}
	r.attempted = true
	switch p.backend.scenario.Mode {
	case Reject:
		return rejection("fixture proves provider non-acceptance")
	case Unresolved:
		return unknown("fixture cannot establish whether submission was accepted")
	}
	r.submitted = true
	p.backend.stats.Executions++
	if p.backend.scenario.Mode == AcceptLoseResponse {
		return unknown("fixture accepted execution but lost the response")
	}
	remote := r.remote
	return provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: &remote}
}
func (p *Basic) ReconcileSubmission(ctx context.Context, id provider.Identity) (provider.Reconciliation, error) {
	if err := ctx.Err(); err != nil {
		return provider.Reconciliation{}, err
	}
	if err := id.Validate(); err != nil {
		return provider.Reconciliation{}, err
	}
	if id.InstanceID != p.instance {
		return provider.Reconciliation{}, identityError()
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, ok := p.backend.records[attemptKey(id)]
	if !ok {
		return provider.Reconciliation{Status: provider.ReconciliationNotFound}, nil
	}
	if r.prepared.Identity != id {
		return provider.Reconciliation{}, identityError()
	}
	if r.submitted && !r.deleted {
		remote := r.remote
		return provider.Reconciliation{Status: provider.ReconciliationFound, Remote: &remote}, nil
	}
	if r.attempted && p.backend.scenario.Mode == Unresolved {
		return provider.Reconciliation{Status: provider.ReconciliationUnknown}, nil
	}
	return provider.Reconciliation{Status: provider.ReconciliationNotFound}, nil
}
func (p *Basic) Observe(ctx context.Context, remote provider.RemoteReference) (provider.Observation, error) {
	if err := ctx.Err(); err != nil {
		return provider.Observation{}, err
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, err := p.lookup(remote)
	if err != nil {
		return provider.Observation{}, err
	}
	state := p.state(r)
	activity := domain.RemoteActivityPossible
	switch {
	case state.Terminal():
		activity = domain.RemoteActivityInactive
	case state == domain.ExecutionRunning:
		activity = domain.RemoteActivityActive
	case state == domain.ExecutionUnknown:
		activity = domain.RemoteActivityUnknown
	}
	obs := provider.Observation{Remote: remote, Execution: state, RawState: "fixture:" + string(state), RemoteActivity: activity,
		ReleaseEvidence: domain.ReleaseEvidenceNotObservable, ObservedAt: p.clock.Now()}
	return obs, obs.Validate(remote)
}

// Advance is a test-control operation, never part of the Provider port. Ordinary reads
// cannot advance state, create executions or make an unsupported cancellation succeed.
func (p *Basic) Advance(remote provider.RemoteReference) error {
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, err := p.lookup(remote)
	if err != nil {
		return err
	}
	if p.state(r).Terminal() {
		return nil
	}
	if r.cancelRequested {
		r.cancelled = true
		return nil
	}
	if r.step+1 < len(p.backend.scenario.States) {
		r.step++
	}
	return nil
}
func (p *Basic) MarkReady(id provider.Identity) error {
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, ok := p.backend.records[attemptKey(id)]
	if !ok || r.prepared.Identity != id || id.InstanceID != p.instance {
		return identityError()
	}
	r.prepared.Ready = true
	return nil
}
func (p *Basic) state(r *record) domain.ExecutionState {
	if r.cancelled {
		return domain.ExecutionCancelled
	}
	state := p.backend.scenario.States[r.step]
	if state == domain.ExecutionSucceeded && requiresGPU(r.plan.Job) && p.backend.scenario.GPU != domain.CapabilitySupportSupported {
		return domain.ExecutionFailed
	}
	return state
}

// lookup must be called under the backend lock.
func (p *Basic) lookup(remote provider.RemoteReference) (*record, error) {
	if err := remote.Validate(); err != nil {
		return nil, err
	}
	r, ok := p.backend.records[attemptKey(remote.Identity)]
	if !ok || r.remote != remote || remote.Identity.InstanceID != p.instance {
		return nil, identityError()
	}
	if !r.submitted || r.deleted {
		return nil, provider.Problem(domain.CodeProviderStateUnknown, domain.FailureStageObservation, "execution is not observable")
	}
	return r, nil
}
func attemptKey(id provider.Identity) string {
	// Intent/nonce/digests are deliberately excluded from the lookup key, then compared
	// against the complete saved identity. Changing them cannot create a second attempt.
	data, _ := json.Marshal([]string{string(id.InstallationID), string(id.WorkspaceID), string(id.JobID), string(id.AttemptID), string(id.InstanceID)})
	return string(provider.Digest(data))
}
func identityError() domain.Problem {
	return provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "reference does not match recorded ownership")
}
func rejection(message string) provider.SubmissionOutcome {
	problem := provider.Problem(domain.CodeProviderRejected, domain.FailureStageSubmission, message)
	return provider.SubmissionOutcome{Status: provider.SubmissionRejected, Problem: &problem}
}
func unknown(message string) provider.SubmissionOutcome {
	problem := provider.Problem(domain.CodeProviderSubmissionUnknown, domain.FailureStageSubmission, message).
		WithRetrySemantics(false, true).WithRecommendedAction(domain.RecommendedActionReconcile)
	return provider.SubmissionOutcome{Status: provider.SubmissionUnknown, Problem: &problem}
}

func requiresGPU(job provider.ResolvedJob) bool {
	for _, name := range job.Required {
		if name == domain.CapabilityGPU {
			return true
		}
	}
	return false
}
