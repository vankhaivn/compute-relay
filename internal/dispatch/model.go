// Package dispatch coordinates finite preparation, one-shot submission and read-only
// recovery. It never executes admitted workload commands on the runtime host.
package dispatch

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

var (
	ErrInvalid          = errors.New("invalid orchestration record")
	ErrConflict         = errors.New("orchestration step changed or is not permitted")
	ErrStaleObservation = errors.New("stale or regressive observation ignored")
	ErrPolicy           = errors.New("new provider mutation blocked by current policy")
)

const MaxJournalBytes = 2 << 20
const MaxFailures = 5

type Phase string

const (
	Local       Phase = "local"
	Staging     Phase = "staging"
	Ready       Phase = "ready"
	Submitting  Phase = "submitting"
	Submitted   Phase = "submitted"
	Rejected    Phase = "rejected"
	Collectible Phase = "collectible"
	Attention   Phase = "attention"
	Failed      Phase = "failed"
	Prevented   Phase = "prevented"
)

// Journal is INTERNAL recovery evidence, not a public status response. Request URLs
// are deliberately absent. Plans and remote references must not be logged wholesale.
type Journal struct {
	Version       int64                     `json:"version"`
	Phase         Phase                     `json:"phase"`
	Plan          *provider.Plan            `json:"plan,omitempty"`
	PreparationID domain.OperationID        `json:"preparation_id,omitempty"`
	Prepared      *provider.Prepared        `json:"prepared,omitempty"`
	SubmitStarted bool                      `json:"submit_started"`
	Remote        *provider.RemoteReference `json:"remote,omitempty"`
	Observation   *provider.Observation     `json:"observation,omitempty"`
	Problem       *domain.Problem           `json:"problem,omitempty"`
	Failures      int                       `json:"failures"`
}

func (j Journal) Recoverable() bool {
	return j.Phase == Staging || j.Phase == Ready || j.Phase == Submitting || j.Phase == Submitted
}
func (j Journal) Valid() bool {
	if j.Version < 0 || j.Failures < 0 || j.Failures > MaxFailures {
		return false
	}
	switch j.Phase {
	case Local, Staging, Ready, Submitting, Submitted, Rejected, Collectible, Attention, Failed, Prevented:
	default:
		return false
	}
	if j.Phase == Prevented && j.SubmitStarted {
		return false
	}
	if j.Plan == nil {
		return (j.Phase == Local || j.Phase == Failed || j.Phase == Prevented) && j.PreparationID == "" && !j.SubmitStarted && j.Prepared == nil && j.Remote == nil && j.Observation == nil && (j.Problem == nil || j.Problem.Validate() == nil)
	}
	if j.Plan.Job.Validate() != nil || j.Plan.Job.Inputs == nil || j.Plan.Job.Inputs.Validate(j.Plan.Job.Identity.WorkspaceID) != nil || !j.PreparationID.Valid() {
		return false
	}
	if j.Prepared != nil && j.Prepared.Validate(*j.Plan, j.PreparationID) != nil {
		return false
	}
	if (j.Phase == Ready || j.SubmitStarted) && (j.Prepared == nil || !j.Prepared.Ready || !j.Prepared.Private) {
		return false
	}
	if j.Phase == Local || j.Phase == Failed || j.Version == 0 {
		return false
	}
	if (j.Phase == Staging || j.Phase == Ready) && j.SubmitStarted {
		return false
	}
	if (j.Phase == Submitting || j.Phase == Rejected) && (j.Remote != nil || j.Observation != nil) {
		return false
	}
	if j.Prepared != nil && !j.Prepared.Private && j.Phase != Attention && j.Phase != Prevented {
		return false
	}
	if j.Phase == Collectible && (j.Observation == nil || !j.Observation.Execution.Terminal()) {
		return false
	}
	if j.Phase == Submitted && j.Observation != nil && j.Observation.Execution.Terminal() {
		return false
	}
	if (j.Phase == Submitting || j.Phase == Submitted || j.Phase == Rejected || j.Phase == Collectible) && !j.SubmitStarted {
		return false
	}
	if j.Remote != nil && (!j.SubmitStarted || j.Remote.Validate() != nil || j.Remote.Identity != j.Plan.Job.Identity) {
		return false
	}
	if (j.Phase == Submitted || j.Phase == Collectible) && j.Remote == nil {
		return false
	}
	if j.Observation != nil && (j.Remote == nil || j.Observation.Validate(*j.Remote) != nil) {
		return false
	}
	return j.Problem == nil || j.Problem.Validate() == nil
}

type Handle struct {
	Claim   scheduler.Claim
	Version int64
}
type Work struct {
	InstallationID domain.RuntimeInstallationID
	Handle         Handle
	Job            admission.Record
	Journal        Journal
	Cancellation   *CancellationControl
}

// Repository commits journal, resource/submission intent, state and events together.
// ClaimRecovery never returns a license to repeat an already journaled mutation.
type Repository interface {
	ClaimNext(context.Context, string, time.Time) (scheduler.Result, error)
	ClaimRecovery(context.Context, string, time.Time) (*scheduler.Claim, error)
	LoadDispatch(context.Context, scheduler.Claim, time.Time) (Work, error)
	FreezeInput(context.Context, Handle, int, domain.ObjectMetadata, time.Time) error
	CommitDispatch(context.Context, Handle, Action, time.Time) (Work, error)
	RenewDispatch(context.Context, Handle, time.Time) (Handle, error)
	YieldDispatch(context.Context, Handle, time.Time, time.Duration) error
}
type Resolver interface {
	Resolve(provider.BindingSnapshot) (provider.Provider, error)
}
type Clock interface{ Now() time.Time }
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type Kind string

const (
	BeginPreparation      Kind = "begin_preparation"
	PreparationSeen       Kind = "preparation_seen"
	BeginSubmission       Kind = "begin_submission"
	SubmissionSeen        Kind = "submission_seen"
	ObservationSeen       Kind = "observation_seen"
	Fault                 Kind = "fault"
	PreventDispatch       Kind = "prevent_dispatch"
	RequestReconciliation Kind = "request_reconciliation"
)

type Action struct {
	Kind          Kind
	Plan          *provider.Plan
	PreparationID domain.OperationID
	Prepared      *provider.Prepared
	Submission    *provider.SubmissionOutcome
	Observation   *provider.Observation
	Code          domain.ErrorCode
}
