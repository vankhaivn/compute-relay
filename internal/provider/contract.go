// Package provider defines internal, provider-neutral execution ports. Adapters return
// observations; only application/orchestration services own durable state transitions.
package provider

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// Provider is the required finite-batch surface. Implementations must honor context,
// perform no hidden compute retries/fallback, and never execute workload commands locally.
// Instances are already configured; job inputs never carry account credentials.
type Provider interface {
	Describe() Descriptor
	Check(context.Context) (DiagnosticReport, error)
	Validate(context.Context, ResolvedJob) (Plan, error)
	Prepare(context.Context, Plan, domain.OperationID) (Prepared, error)
	// Submit has no bare error return: even a transport failure must say whether
	// acceptance is rejected or unknown. A zero outcome is invalid, never success.
	Submit(context.Context, Prepared) SubmissionOutcome
	Observe(context.Context, RemoteReference) (Observation, error)
	ReconcileSubmission(context.Context, Identity) (Reconciliation, error)
	ListArtifacts(context.Context, RemoteReference, PageRequest) (ArtifactPage, error)
	// Destination must be temporary/unpublished. On any error discard its bytes.
	FetchArtifact(context.Context, RemoteReference, Artifact, io.Writer, int64) (TransferResult, error)
	Cleanup(context.Context, CleanupRequest) (CleanupOutcome, error)
}

// Optional behavior is segregated: absence must not masquerade as successful work.
type Canceller interface {
	Cancel(context.Context, RemoteReference, domain.OperationID) (CancellationOutcome, error)
}
type LogReader interface {
	ReadLogs(context.Context, RemoteReference, PageRequest) (LogPage, error)
}
type QuotaReader interface {
	ReadQuota(context.Context) (QuotaObservation, error)
}

// Identity is written BEFORE provider mutations. The resource key and nonce are opaque,
// non-secret values selected by orchestration, not human workload names.
type Identity struct {
	InstallationID      domain.RuntimeInstallationID
	WorkspaceID         domain.WorkspaceID
	JobID               domain.JobID
	AttemptID           domain.AttemptID
	InstanceID          domain.ProviderInstanceID
	IntentID            domain.SubmissionIntentID
	ResourceKey         string
	Nonce               string
	BundleSHA256        domain.SHA256Digest
	InputManifestSHA256 domain.SHA256Digest
}

// ResolvedJob is a post-admission snapshot, not a second public JobSpec format. The caller
// validates the M2-03 schema, freezes objects, derives Required/WallSeconds under policy,
// and persists the snapshot. Providers must not resolve mutable URLs or read host paths.
type ResolvedJob struct {
	Identity            Identity
	Binding             domain.ProviderBinding
	Specification       json.RawMessage
	SpecificationSHA256 domain.SHA256Digest
	Required            []domain.CapabilityName
	WallSeconds         int64
}
type Plan struct {
	Job              ResolvedJob
	VerifyAfterStart []domain.CapabilityName
}
type Prepared struct {
	Identity      Identity
	PreparationID domain.OperationID
	PlanSHA256    domain.SHA256Digest
	Resource      string
	Ready         bool
}
type Descriptor struct {
	Type         string
	InstanceID   domain.ProviderInstanceID
	Version      string
	Capabilities []domain.CapabilityStatus
}
type DiagnosticReport struct {
	Evidence  domain.EvidenceLevel
	CheckedAt time.Time
	Ready     bool
	Problems  []domain.Problem
}

type RemoteReference struct {
	Identity Identity
	Resource string
	Version  string
}
type SubmissionStatus string

const (
	SubmissionAccepted SubmissionStatus = "accepted"
	SubmissionRejected SubmissionStatus = "rejected"
	SubmissionUnknown  SubmissionStatus = "unknown"
)

type SubmissionOutcome struct {
	Status  SubmissionStatus
	Remote  *RemoteReference
	Problem *domain.Problem
}
type Observation struct {
	Remote          RemoteReference
	Execution       domain.ExecutionState
	RawState        string // bounded, sanitized adapter evidence; never an instruction
	RemoteActivity  domain.RemoteActivityState
	ReleaseEvidence domain.ReleaseEvidence
	ObservedAt      time.Time
}
type ReconciliationStatus string

const (
	ReconciliationFound    ReconciliationStatus = "found"
	ReconciliationNotFound ReconciliationStatus = "not_found"
	ReconciliationUnknown  ReconciliationStatus = "unknown"
)

// NotFound is an observation, NOT proof that a previous submission was rejected. Neither
// NotFound nor Unknown authorizes another Submit; M3 owns explicit resolution policy.
type Reconciliation struct {
	Status ReconciliationStatus
	Remote *RemoteReference
}

type PageRequest struct {
	Cursor string
	Limit  int
}
type Artifact struct {
	Remote RemoteReference
	Path   string
	Bytes  int64
	SHA256 domain.SHA256Digest
}
type ArtifactPage struct {
	Artifacts  []Artifact
	NextCursor string
}
type TransferResult struct {
	Bytes  int64
	SHA256 domain.SHA256Digest
}

// Cleanup requires an exact persisted ownership ledger entry and an explicit mode.
// ResultsCollected is a caller attestation from local durable state, not proof furnished
// by the provider. The adapter must also verify remote identity and terminal state.
type CleanupMode string

const (
	CleanupDryRun CleanupMode = "dry_run"
	CleanupApply  CleanupMode = "apply"
)

type CleanupRequest struct {
	Remote              RemoteReference
	LedgerID            domain.ProviderResourceID
	CreationOperationID domain.OperationID
	ResultsCollected    bool
	Mode                CleanupMode
}
type CleanupOutcome struct {
	WouldDelete   bool
	Deleted       bool
	AlreadyAbsent bool
}
type CancellationOutcome struct {
	Status               domain.CancellationState
	TerminationConfirmed bool
}
type LogPage struct {
	Source       string
	Availability string
	Lines        []string
	NextCursor   string
	Truncated    bool
}
type QuotaStatus string

const (
	QuotaKnown       QuotaStatus = "known"
	QuotaUnknown     QuotaStatus = "unknown"
	QuotaStale       QuotaStatus = "stale"
	QuotaUnavailable QuotaStatus = "unavailable"
)

// Nil values are unknown, not zero or unlimited. ResetAt is populated only from evidence.
type QuotaObservation struct {
	Status     QuotaStatus
	Resource   string
	Unit       string
	Limit      *float64
	Used       *float64
	Remaining  *float64
	ResetAt    *time.Time
	ObservedAt time.Time
	Source     string
	Precision  string
}
