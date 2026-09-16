package kaggle

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type executionResponse struct {
	Protocol     int                 `json:"protocol"`
	Status       string              `json:"status"`
	KernelID     string              `json:"kernel_id"`
	Reference    string              `json:"reference"`
	Version      int                 `json:"version"`
	SourceSHA256 domain.SHA256Digest `json:"source_sha256"`
	RawState     string              `json:"raw_state"`
}

type kernelReference struct {
	KernelID     string              `json:"kernel_id"`
	Reference    string              `json:"reference"`
	SourceSHA256 domain.SHA256Digest `json:"source_sha256"`
}

type executionInvocation func(context.Context, Config, string, []byte, executionRequest) (executionResponse, error)

// Executor is bound to ONE immutable attempt. Reconstruct it on recovery from the
// original M3 plan/preparation and frozen configuration, never a current alias.
// It is not a full Provider, queue or durable mutation authority. Only a caller
// receiving a successful NEW BeginSubmission commit may invoke Submit. All other
// paths use ReconcileSubmission/Observe; even a genuine miss cannot rearm Submit.
type Executor struct {
	stager      *Stager
	plan        provider.Plan
	prepared    provider.Prepared
	policy      ExecutionPolicy
	request     executionRequest
	allowSubmit bool
	used        atomic.Bool // in-process defense only; M3 supplies durable authority
	slot        chan struct{}
	run         executionInvocation
	now         func() time.Time
}

func NewExecutor(stager *Stager, policy ExecutionPolicy, plan provider.Plan, prepared provider.Prepared, allowSubmit bool) (*Executor, error) {
	if stager == nil {
		return nil, ErrConfig
	}
	frozen := plan.Clone()
	r, err := buildExecutionRequest(stager.config, stager.policy, policy, frozen, prepared)
	if err != nil {
		return nil, err
	}
	return &Executor{stager: stager, plan: frozen, prepared: prepared, policy: policy, request: r, allowSubmit: allowSubmit, slot: make(chan struct{}, 1), run: runExecution, now: time.Now}, nil
}

func rejectedExecution() provider.SubmissionOutcome {
	p := provider.Problem(domain.CodeProviderRejected, domain.FailureStageSubmission, "kernel request stopped before submission; inspect configuration and staging")
	return provider.SubmissionOutcome{Status: provider.SubmissionRejected, Problem: &p}
}
func unknownExecution() provider.SubmissionOutcome {
	p := provider.Problem(domain.CodeProviderSubmissionUnknown, domain.FailureStageSubmission, "kernel acceptance unresolved; reconcile the original identity without resubmitting")
	p.ComputeMayHaveStarted = true
	p.SafeOperationRetry = false
	p.RecommendedAction = domain.RecommendedActionReconcile
	return provider.SubmissionOutcome{Status: provider.SubmissionUnknown, Problem: &p}
}

func (e *Executor) Submit(parent context.Context, prepared provider.Prepared) (out provider.SubmissionOutcome) {
	out = unknownExecution()
	defer func() {
		if recover() != nil {
			out = unknownExecution()
		}
	}()
	if e == nil || !e.allowSubmit || prepared != e.prepared {
		return rejectedExecution()
	}
	if e.used.Swap(true) {
		return unknownExecution()
	}
	ctx, cancel := context.WithTimeout(parent, e.policy.Timeout)
	defer cancel()
	// Recheck complete remote staging without creating anything. Equality includes
	// the first-pinned numeric dataset ID, version, marker, privacy and readiness.
	stage, err := e.stager.ReconcilePreparation(ctx, e.plan, e.prepared.PreparationID)
	if err != nil || stage.Status != provider.ReconciliationFound || stage.Prepared == nil || *stage.Prepared != e.prepared {
		return rejectedExecution()
	}
	r, invoked, err := e.call(ctx, "submit", "")
	if err != nil {
		if !invoked {
			return rejectedExecution()
		}
		return unknownExecution()
	}
	if r.Status == "rejected" {
		return rejectedExecution()
	}
	if r.Status != "found" {
		return unknownExecution()
	}
	remote, err := e.remote(r)
	if err != nil {
		return unknownExecution()
	}
	return provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: &remote}
}

func (e *Executor) ReconcileSubmission(ctx context.Context, identity provider.Identity) (provider.Reconciliation, error) {
	if e == nil || identity != e.plan.Job.Identity {
		return provider.Reconciliation{}, ErrExecutionIdentity
	}
	r, _, err := e.call(ctx, "reconcile", "")
	if err != nil {
		return provider.Reconciliation{}, err
	}
	switch r.Status {
	case "not_found":
		return provider.Reconciliation{Status: provider.ReconciliationNotFound}, nil
	case "unknown":
		return provider.Reconciliation{Status: provider.ReconciliationUnknown}, nil
	case "found":
		remote, err := e.remote(r)
		if err != nil {
			return provider.Reconciliation{}, err
		}
		return provider.Reconciliation{Status: provider.ReconciliationFound, Remote: &remote}, nil
	default:
		return provider.Reconciliation{}, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "kernel no longer matches the original attempt")
	}
}

func (e *Executor) Observe(ctx context.Context, expected provider.RemoteReference) (provider.Observation, error) {
	var none provider.Observation
	if e == nil || expected.Validate() != nil || expected.Identity != e.plan.Job.Identity || expected.Version != "1" {
		return none, ErrExecutionIdentity
	}
	var ref kernelReference
	if closedObject([]byte(expected.Resource), &ref) != nil || !positiveDecimal(ref.KernelID) || ref.Reference != e.request.Owner+"/"+e.request.Slug || ref.SourceSHA256 != e.request.SourceSHA256 {
		return none, ErrExecutionIdentity
	}
	r, _, err := e.call(ctx, "observe", ref.KernelID)
	if err != nil {
		return none, err
	}
	if r.Status == "invalid" {
		return none, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "kernel identity or source was replaced; retain the original reference")
	}
	if r.Status != "found" {
		return none, ErrProcess
	}
	actual, err := e.remote(r)
	if err != nil || actual != expected {
		return none, ErrExecutionIdentity
	}
	execution, activity := executionState(r.RawState)
	result := provider.Observation{Remote: expected, Execution: execution, RawState: r.RawState, RemoteActivity: activity, ReleaseEvidence: domain.ReleaseEvidenceNotObservable, ObservedAt: e.now().UTC()}
	if err := result.Validate(expected); err != nil {
		return none, ErrProtocol
	}
	return result, nil
}

func executionState(raw string) (domain.ExecutionState, domain.RemoteActivityState) {
	switch raw {
	case "QUEUED":
		return domain.ExecutionQueued, domain.RemoteActivityPossible
	case "RUNNING":
		return domain.ExecutionRunning, domain.RemoteActivityActive
	case "COMPLETE":
		return domain.ExecutionSucceeded, domain.RemoteActivityInactive
	case "ERROR":
		return domain.ExecutionFailed, domain.RemoteActivityInactive
	default:
		// Cancellation acknowledgements and NEW_SCRIPT have no established terminal
		// semantics here. Never fabricate cancelled, timeout or capacity release.
		return domain.ExecutionUnknown, domain.RemoteActivityPossible
	}
}

func (e *Executor) remote(r executionResponse) (provider.RemoteReference, error) {
	var none provider.RemoteReference
	if r.Protocol != 1 || r.Status != "found" || !positiveDecimal(r.KernelID) || r.Reference != e.request.Owner+"/"+e.request.Slug || r.Version != 1 || r.SourceSHA256 != e.request.SourceSHA256 {
		return none, ErrExecutionIdentity
	}
	raw, err := json.Marshal(kernelReference{r.KernelID, r.Reference, r.SourceSHA256})
	if err != nil {
		return none, ErrProtocol
	}
	return provider.RemoteReference{Identity: e.plan.Job.Identity, Resource: string(raw), Version: "1"}, nil
}

func (r executionResponse) valid() bool {
	if r.Protocol != 1 {
		return false
	}
	switch r.Status {
	case "unknown", "not_found", "invalid", "rejected":
		return r == (executionResponse{Protocol: 1, Status: r.Status})
	case "found":
		if !positiveDecimal(r.KernelID) || r.Version != 1 || !r.SourceSHA256.Valid() {
			return false
		}
		switch r.RawState {
		case "QUEUED", "RUNNING", "COMPLETE", "ERROR", "CANCEL_REQUESTED", "CANCEL_ACKNOWLEDGED", "NEW_SCRIPT", "UNKNOWN":
			return true
		}
	}
	return false
}

func (e *Executor) call(parent context.Context, mode, kernelID string) (executionResponse, bool, error) {
	var result executionResponse
	ctx, cancel := context.WithTimeout(parent, e.policy.Timeout)
	defer cancel()
	select {
	case e.slot <- struct{}{}:
		defer func() { <-e.slot }()
	case <-ctx.Done():
		return result, false, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return result, false, err
	}
	local, err := e.stager.local(ctx, e.stager.config, Local, nil)
	if err != nil || !local.valid(Local) || local.Local != "ready" {
		return result, false, ErrProcess
	}
	request := e.request
	request.KernelID = kernelID
	if mode != "submit" {
		request.Source = "" // observations need its digest, not another copy of private code
	}
	called, invoked := false, false
	err = e.stager.credentials.WithCredential(ctx, e.stager.config.CredentialRef, func(secret []byte) error {
		if called {
			return ErrProtocol
		}
		called = true
		if len(secret) == 0 || len(secret) > credentials.MaxBytes {
			return ErrConfig
		}
		for _, b := range secret {
			if b < 33 || b > 126 {
				return ErrConfig
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		invoked = true
		var err error
		result, err = e.run(ctx, e.stager.config, mode, secret, request)
		return err
	})
	if ctx.Err() != nil {
		return executionResponse{}, invoked, ctx.Err()
	}
	if err != nil || !called || !invoked {
		return executionResponse{}, invoked, ErrProcess
	}
	if !result.valid() || (mode != "submit" && result.Status == "rejected") {
		return executionResponse{}, invoked, ErrProtocol
	}
	return result, invoked, nil
}
