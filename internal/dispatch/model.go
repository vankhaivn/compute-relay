// Package dispatch coordinates finite preparation, one-shot submission and read-only
// recovery. It never executes admitted workload commands on the runtime host.
package dispatch

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
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
	Version                       int64                        `json:"version"`
	Phase                         Phase                        `json:"phase"`
	Plan                          *provider.Plan               `json:"plan,omitempty"`
	PreparationID                 domain.OperationID           `json:"preparation_id,omitempty"`
	PreparationObservationStarted *time.Time                   `json:"preparation_observation_started,omitempty"`
	Prepared                      *provider.Prepared           `json:"prepared,omitempty"`
	SubmitStarted                 bool                         `json:"submit_started"`
	SubmissionProblem             *domain.Problem              `json:"submission_problem,omitempty"`
	SubmissionAbsenceObservedAt   *time.Time                   `json:"submission_absence_observed_at,omitempty"`
	SubmissionResourceName        string                       `json:"submission_resource_name,omitempty"`
	OwnerSubmissionPresent        bool                         `json:"owner_submission_present,omitempty"`
	StartAuthorization            *provider.StartAuthorization `json:"start_authorization,omitempty"`
	Remote                        *provider.RemoteReference    `json:"remote,omitempty"`
	Observation                   *provider.Observation        `json:"observation,omitempty"`
	Problem                       *domain.Problem              `json:"problem,omitempty"`
	Failures                      int                          `json:"failures"`
}

func (j Journal) Recoverable() bool {
	return j.Phase == Staging || j.Phase == Ready || j.Phase == Submitting || j.Phase == Submitted
}
func (j Journal) Valid() bool {
	if j.SubmissionProblem != nil && (!j.SubmitStarted || j.SubmissionProblem.Validate() != nil) {
		return false
	}
	if j.SubmissionResourceName != "" && (!j.SubmitStarted || !domain.ValidResourceName(j.SubmissionResourceName)) {
		return false
	}
	if j.SubmissionAbsenceObservedAt != nil && (!j.SubmitStarted || !scheduler.ValidTime(*j.SubmissionAbsenceObservedAt) || j.SubmissionResourceName == "") {
		return false
	}
	if raw := journalDetail(j, localFailureDetail); raw != "" {
		if _, ok := journalTime(raw); !ok {
			return false
		}
	}
	if raw := journalDetail(j, remoteAbsenceDetail); raw != "" {
		if j.RemoteAbsence() == nil {
			return false
		}
	}
	if j.PreparationObservationStarted != nil && (!scheduler.ValidTime(*j.PreparationObservationStarted) || j.Plan == nil) {
		return false
	}
	if j.StartAuthorization != nil && (!j.SubmitStarted || j.Prepared == nil || j.StartAuthorization.Validate(*j.Prepared) != nil) {
		return false
	}
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

const localFailureDetail = "local_failure_started"
const remoteAbsenceDetail = "remote_absence_observed_at"

// Internal timestamps use the existing private problem metadata; public projection
// exposes only the closed reason and older journal decoders retain their shape.
func journalDetail(j Journal, key string) string {
	if j.Problem == nil {
		return ""
	}
	return j.Problem.Details[key]
}
func journalTime(raw string) (time.Time, bool) {
	at, err := time.Parse(time.RFC3339Nano, raw)
	return at, err == nil && scheduler.ValidTime(at)
}

// RemoteAbsence binds its timestamp to the journal's immutable accepted reference.
func (j Journal) RemoteAbsence() *provider.RemoteAbsence {
	if j.Phase != Attention || j.Remote == nil || j.Problem == nil || j.Problem.Code != domain.CodeRemoteExecutionUnresolved ||
		j.Problem.PublicDetails()[domain.ReasonDetail] != "remote_not_found" {
		return nil
	}
	at, ok := journalTime(journalDetail(j, remoteAbsenceDetail))
	if !ok {
		return nil
	}
	return &provider.RemoteAbsence{Remote: *j.Remote, ObservedAt: at}
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
	Gate           *executionauth.Gate
}

// Repository commits journal, resource/submission intent, state and events together.
// ClaimRecovery never returns a license to repeat an already journaled mutation.
type Repository interface {
	ClaimNext(context.Context, string) (scheduler.Result, error)
	ClaimRecovery(context.Context, string) (*scheduler.Claim, error)
	LoadDispatch(context.Context, scheduler.Claim) (Work, error)
	ConsumeAuthorization(context.Context, Handle) (Work, error)
	CommitDispatch(context.Context, Handle, Action) (Work, error)
	RenewDispatch(context.Context, Handle) (Handle, error)
	YieldDispatch(context.Context, Handle, time.Duration) error
	// RecordPreparationProgress stores an observational staging sample for the lease owner.
	RecordPreparationProgress(context.Context, Handle, domain.PreparationProgress) error
}
type Resolver interface {
	Resolve(provider.BindingSnapshot) (provider.Provider, error)
}

// NewWorkGuard runs only before a new preparation or start intent. Recovery,
// observation, cancellation and collection never require new health authority.
type NewWorkGuard interface {
	BeforeNewWork(context.Context, admission.Record) (context.Context, error)
}
type Clock interface{ Now() time.Time }
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type Kind string

const (
	BeginPreparation          Kind = "begin_preparation"
	PreparationSeen           Kind = "preparation_seen"
	EndUnconfirmedPreparation Kind = "end_unconfirmed_preparation"
	BeginSubmission           Kind = "begin_submission"
	SubmissionSeen            Kind = "submission_seen"
	ObservationSeen           Kind = "observation_seen"
	RemoteAbsenceSeen         Kind = "remote_absence_seen"
	Fault                     Kind = "fault"
	PreventDispatch           Kind = "prevent_dispatch"
	ExpireDispatch            Kind = "expire_dispatch"
	RefuseNewWork             Kind = "refuse_new_work"
	RequestReconciliation     Kind = "request_reconciliation"
	SubmissionAbsenceSeen     Kind = "submission_absence_seen"
	OwnerSubmissionAbsent     Kind = "owner_submission_absent"
)

type Action struct {
	RefusalReason      string
	SecretsAvailable   bool
	Kind               Kind
	Plan               *provider.Plan
	PreparationID      domain.OperationID
	Prepared           *provider.Prepared
	Submission         *provider.SubmissionOutcome
	Absence            *provider.RemoteAbsence
	Observation        *provider.Observation
	Code               domain.ErrorCode
	Reason             string // closed adapter reason for a Fault, when known
	Details            map[string]string
	ResourceName       string
	OwnerPresent       bool
	StartAuthorization *provider.StartAuthorization
	PreparationExpired bool
}
