// Package provider defines internal, provider-neutral execution ports. Adapters return
// observations; only application/orchestration services own durable state transitions.
package provider

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

// Provider is the required finite-batch surface. Implementations must honor context,
// perform no hidden compute retries/fallback, and never execute workload commands locally.
// Instances are already configured; job inputs never carry account credentials.
type Provider interface {
	Describe() Descriptor
	// Validate checks a new immutable job before any preparation intent is committed.
	Validate(context.Context, ResolvedJob) (Plan, error)
	// Verify proves the exact frozen configuration/account without granting mutation authority.
	Verify(context.Context, BindingSnapshot) error
	Prepare(context.Context, Plan, domain.OperationID) (Prepared, error)
	ReconcilePrepare(context.Context, Plan, domain.OperationID) (PreparationObservation, error)
	// Start has no bare error: transport uncertainty must remain unknown, never a retry.
	// Authorization and material are nil for ordinary starts; material requires exact authorization.
	Start(context.Context, Prepared, *StartAuthorization, *workloadsecret.Material) SubmissionOutcome
	Reconcile(context.Context, Identity) (Reconciliation, error)
	Observe(context.Context, RemoteReference) (Observation, error)
	Cancel(context.Context, RemoteReference, domain.OperationID) (CancellationOutcome, error)
	Logs(context.Context, RemoteReference, PageRequest) (LogPage, error)
	// ListArtifacts returns the complete catalog or an error; never a truncated page.
	ListArtifacts(context.Context, RemoteReference) ([]Artifact, error)
	// Destination must be temporary/unpublished. On any error discard its bytes.
	// Progress reports absolute target bytes after destination writes; errors stop transfer.
	Fetch(context.Context, RemoteReference, Artifact, io.Writer, int64, func(int64) error) (TransferResult, error)
	DescribeCleanup(CleanupResourceRequest) (CleanupDescription, error)
	CleanupResource(context.Context, CleanupResourceRequest) (CleanupOutcome, error)
}

const MaxArtifactFiles = 10004
const MaxArtifactBytes int64 = 4 << 30

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
	// Inputs is mandatory for durable orchestration, optional only for older isolated
	// provider contract fixtures. The adapter's scoped BlobStore supplies these bytes.
	Inputs *InputSnapshot `json:",omitempty"`
}
type Plan struct {
	Job              ResolvedJob
	VerifyAfterStart []domain.CapabilityName
}
type Prepared struct {
	// Verification hashes the observed runtime, image, policy and staged bytes.
	// Legacy combined preparation leaves both fields empty.
	VerificationSHA256 domain.SHA256Digest `json:",omitempty"`
	ManifestSHA256     domain.SHA256Digest `json:",omitempty"`
	Identity           Identity
	PreparationID      domain.OperationID
	PlanSHA256         domain.SHA256Digest
	Resource           string
	Ready              bool
	// Private is an adapter assertion supported by its verified private-staging path.
	// Unknown/false cannot authorize compute dispatch. Fake true is offline evidence only.
	Private bool
}
type Descriptor struct {
	Type         string
	InstanceID   domain.ProviderInstanceID
	Version      string
	Capabilities Capabilities
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
// NotFound nor Unknown authorizes another Start; M3 owns explicit resolution policy.
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
