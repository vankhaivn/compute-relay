package kaggle

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

// Closed reasons for a stop before SaveKernel is sent. They name where the attempt
// stopped and never carry provider, URL or OS text.
const (
	reasonSubmissionDisabled  = "submission_disabled"
	reasonPreparationMismatch = "preparation_mismatch"
	reasonStagingRecheck      = "staging_recheck_failed"
	reasonStagingUnconfirmed  = "staging_not_confirmed"
	reasonCallTimeout         = "call_timeout"
	reasonRuntimeStopped      = "runtime_stopped"
	reasonCredential          = "credential_unavailable"
)

// submissionRefusals are closed refusals before the save call.
var submissionRefusals = map[string]bool{
	"token_inactive": true, "account_mismatch": true, "gpu_quota_unverified": true,
	"auth_read_failed": true, "kernel_read_failed": true, "quota_read_failed": true, "helper_failed": true,
}

// Closed lookup outcomes describe why the original attempt cannot yet be resolved.
var lookupReasons = map[string]bool{
	"lookup_authentication_failed": true, "lookup_access_denied": true,
	"kernel_not_visible": true, "lookup_rate_limited": true, "lookup_timeout": true,
	"lookup_interrupted": true, "lookup_response_invalid": true,
}

type executionResponse struct {
	Protocol         int                 `json:"protocol"`
	Status           string              `json:"status"`
	Reason           string              `json:"reason"`
	KernelID         string              `json:"kernel_id"`
	Reference        string              `json:"reference"`
	Version          int                 `json:"version"`
	SourceSHA256     domain.SHA256Digest `json:"source_sha256"`
	RawState         string              `json:"raw_state"`
	HTTPStatus       int                 `json:"http_status,omitempty"`
	ErrorType        string              `json:"error_type,omitempty"`
	FailureOperation string              `json:"failure_operation,omitempty"`
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
// receiving a successful NEW BeginSubmission commit may invoke Start. All other
// paths use Reconcile/Observe; even a genuine miss cannot rearm Start.
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

func rejectedExecution(reason string) provider.SubmissionOutcome {
	p := provider.Problem(domain.CodeProviderRejected, domain.FailureStageSubmission, "kernel request stopped before submission; inspect configuration and staging").
		WithDetail(domain.ReasonDetail, reason)
	return provider.SubmissionOutcome{Status: provider.SubmissionRejected, Problem: &p}
}

func rejectedStaging(reason, cause string) provider.SubmissionOutcome {
	out := rejectedExecution(reason)
	if cause != "" {
		p := out.Problem.WithDetail(domain.CauseDetail, cause)
		out.Problem = &p
	}
	return out
}

// Kaggle can leave a ready dataset unanswered for minutes (rate limits, outages) or
// briefly answer a missing name. The pre-submit recheck reads again with backoff;
// tests shorten the window.
var (
	stagingRecheckGrace = 10 * time.Minute
	stagingRecheckPoll  = 15 * time.Second
)

const stagingRecheckMaxPoll = 2 * time.Minute

// recheck repeats only the metadata read while staging stays unresolved, until the
// window or the caller's budget ends. Identity, protocol and local faults are final.
func (e *Executor) recheck(ctx context.Context) (provider.PreparationObservation, error) {
	deadline := e.now().Add(stagingRecheckGrace)
	delay := stagingRecheckPoll
	for {
		stage, err := e.stager.RecheckPrepare(ctx, e.plan, e.prepared.PreparationID)
		if !unresolvedRecheck(stage, err) || ctx.Err() != nil || !e.now().Add(delay).Before(deadline) {
			return stage, err
		}
		wait := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			wait.Stop()
			return stage, err
		case <-wait.C:
		}
		delay = min(2*delay, stagingRecheckMaxPoll)
	}
}

func unresolvedRecheck(stage provider.PreparationObservation, err error) bool {
	if err != nil {
		return !provider.IsLocalStateError(err) && (errors.Is(err, ErrStaging) || errors.Is(err, context.DeadlineExceeded))
	}
	return stage.Status == provider.ReconciliationUnknown || stage.Status == provider.ReconciliationNotFound
}

func stagingErrorCause(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "staging_read_timeout"
	}
	return ""
}
func unknownExecution() provider.SubmissionOutcome {
	p := provider.Problem(domain.CodeProviderSubmissionUnknown, domain.FailureStageSubmission, "kernel acceptance unresolved; reconcile the original identity without resubmitting")
	p.ComputeMayHaveStarted = true
	p.SafeOperationRetry = false
	p.RecommendedAction = domain.RecommendedActionReconcile
	return provider.SubmissionOutcome{Status: provider.SubmissionUnknown, Problem: &p}
}

func (e *Executor) Start(parent context.Context, prepared provider.Prepared, authorization *provider.StartAuthorization, material *workloadsecret.Material) (out provider.SubmissionOutcome) {
	if authorization != nil || material != nil {
		return rejectedExecution(reasonSubmissionDisabled)
	}
	out = unknownExecution()
	defer func() {
		if recover() != nil {
			out = unknownExecution()
		}
		if e != nil && out.Problem != nil {
			p := out.Problem.WithDetail("resource_name", e.request.Owner+"/"+e.request.Slug)
			out.Problem = &p
		}
	}()
	if e == nil || !e.allowSubmit {
		return rejectedExecution(reasonSubmissionDisabled)
	}
	if prepared != e.prepared {
		return rejectedExecution(reasonPreparationMismatch)
	}
	if e.used.Swap(true) {
		return unknownExecution()
	}
	// Readiness checked the attempt's staging by metadata, and the remote runner
	// rehashes every input before use. Recheck it here by metadata only, under
	// the preparation budget: equality includes the first-pinned numeric dataset ID,
	// version, marker, privacy, readiness and the exact file list.
	stage, err := e.recheck(parent)
	if err != nil {
		if provider.IsLocalStateError(err) {
			return unknownExecution()
		}
		return rejectedStaging(reasonStagingRecheck, stagingErrorCause(err))
	}
	if stage.Status != provider.ReconciliationFound || stage.Prepared == nil || *stage.Prepared != e.prepared {
		return rejectedStaging(reasonStagingUnconfirmed, stage.Cause)
	}
	r, stopped, err := e.call(parent, "submit", "")
	if stopped != "" {
		return rejectedExecution(stopped)
	}
	if err != nil {
		return unknownExecutionFailure(submissionFailure(err))
	}
	if r.Status == "rejected" {
		return rejectedExecution(r.Reason)
	}
	if r.Status != "found" {
		return unknownExecutionFailure(r)
	}
	remote, err := e.remote(r)
	if err != nil {
		return unknownExecution()
	}
	return provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: &remote}
}

func (e *Executor) Reconcile(ctx context.Context, identity provider.Identity) (provider.Reconciliation, error) {
	if e == nil || identity != e.plan.Job.Identity {
		return provider.Reconciliation{}, ErrExecutionIdentity
	}
	r, stopped, err := e.call(ctx, "reconcile", "")
	if provider.IsLocalStateError(err) {
		return provider.Reconciliation{}, err
	}
	if err != nil || r.Reason != "" {
		reason := r.Reason
		if err != nil {
			reason = stopped
			if reason == "" {
				reason = executionReadReason(err, "lookup_unavailable", "reconcile")
			}
		}
		p := *unknownExecution().Problem
		return provider.Reconciliation{}, p.WithDetail(domain.ReasonDetail, reason).
			WithDetail("resource_name", e.request.Owner+"/"+e.request.Slug)
	}
	switch r.Status {
	case "absent":
		return provider.Reconciliation{Status: provider.ReconciliationAbsent,
			ResourceName: e.request.Owner + "/" + e.request.Slug}, nil
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
	if r.Status == "not_found" {
		return none, provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation,
			"The recorded kernel is not visible; execution and hardware release remain unconfirmed").WithDetail(domain.ReasonDetail, "remote_not_found")
	}
	if r.Status != "found" {
		return none, ErrUnavailable
	}
	actual, err := e.remote(r)
	if err != nil || actual != expected {
		return none, provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation,
			"Kernel identity does not match the recorded attempt").WithCause(ErrExecutionIdentity)
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
	if r.HTTPStatus != 0 || r.ErrorType != "" || r.FailureOperation != "" {
		if r.Status != "unknown" || r.HTTPStatus != 0 && (r.HTTPStatus < 100 || r.HTTPStatus > 599) ||
			!validSubmissionErrorType(r.ErrorType) || r.FailureOperation != "save_kernel" && r.FailureOperation != "verify_kernel" && r.FailureOperation != "unknown" {
			return false
		}
		r.HTTPStatus, r.ErrorType, r.FailureOperation = 0, "", ""
	}
	switch r.Status {
	case "rejected":
		return r == (executionResponse{Protocol: 1, Status: r.Status, Reason: r.Reason}) && submissionRefusals[r.Reason]
	case "unknown", "not_found":
		return r == (executionResponse{Protocol: 1, Status: r.Status, Reason: r.Reason}) &&
			(r.Reason == "" || lookupReasons[r.Reason] || submissionRefusals[r.Reason])
	case "invalid", "absent":
		return r == (executionResponse{Protocol: 1, Status: r.Status})
	case "found":
		if r.Reason != "" || !positiveDecimal(r.KernelID) || r.Version != 1 || !r.SourceSHA256.Valid() {
			return false
		}
		switch r.RawState {
		case "QUEUED", "RUNNING", "COMPLETE", "ERROR", "CANCEL_REQUESTED", "CANCEL_ACKNOWLEDGED", "NEW_SCRIPT", "UNKNOWN":
			return true
		}
	}
	return false
}

func unknownExecutionFailure(r executionResponse) provider.SubmissionOutcome {
	out := unknownExecution()
	if r.HTTPStatus != 0 {
		p := out.Problem.WithDetail("submission_http_status", strconv.Itoa(r.HTTPStatus))
		out.Problem = &p
	}
	if r.ErrorType != "" {
		p := out.Problem.WithDetail("submission_error_type", r.ErrorType).
			WithDetail("submission_operation", r.FailureOperation)
		out.Problem = &p
	}
	return out
}

// call returns a closed stop reason when the native client was never invoked; after
// invocation any failure is ambiguous and the reason is empty.
func (e *Executor) call(parent context.Context, mode, kernelID string) (executionResponse, string, error) {
	var result executionResponse
	ctx, cancel := context.WithTimeout(parent, e.policy.Timeout)
	defer cancel()
	select {
	case e.slot <- struct{}{}:
		defer func() { <-e.slot }()
	case <-ctx.Done():
		return result, stopReason(ctx, ""), ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return result, stopReason(ctx, ""), err
	}
	request := e.request
	request.KernelID = kernelID
	if mode != "submit" {
		request.Source = "" // observations need its digest, not another copy of private code
	}
	called, invoked := false, false
	violated := false
	err := e.stager.credentials.WithCredential(ctx, e.stager.config.CredentialRef, func(secret []byte) error {
		if called {
			violated = true
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
	if provider.IsLocalStateError(err) {
		return executionResponse{}, "", err
	}
	if !invoked {
		if ctx.Err() != nil {
			return executionResponse{}, stopReason(ctx, ""), ctx.Err()
		}
		return executionResponse{}, reasonCredential, ErrUnavailable
	}
	if ctx.Err() != nil {
		return executionResponse{}, "", ctx.Err()
	}
	if err != nil || violated {
		return executionResponse{}, "", ErrUnavailable
	}
	if !result.valid() || (mode != "submit" && result.Status == "rejected") {
		return executionResponse{}, "", ErrProtocol
	}
	return result, "", nil
}

// stopReason prefers an ended context, which explains any failure that follows it.
func stopReason(ctx context.Context, reason string) string {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return reasonCallTimeout
	case ctx.Err() != nil:
		return reasonRuntimeStopped
	}
	return reason
}
