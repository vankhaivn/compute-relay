package cleanup

import (
	"context"
	"encoding/json"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// Resource is private durable ownership evidence. Only its typed provider description
// or fingerprint may appear in an operator preview; never serialize Request to HTTP.
type Resource struct {
	Request provider.CleanupResourceRequest
	Reason  string
}

func (r Resource) Fingerprint() domain.SHA256Digest {
	raw, _ := json.Marshal(r.Request)
	return provider.Digest(raw)
}

type ResourceWork struct {
	Resource Resource
	Result   operations.ResourceResult
}

// ResourceRepository commits each intent before its call and fences every outcome.
// Begin rechecks current eligibility and the immutable target; a preview is irrelevant.
type ResourceRepository interface {
	BeginCleanupResource(context.Context, Work, ResourceWork) error
	CompleteCleanupResource(context.Context, Work, operations.ResourceResult) error
}

type PreviewPlan struct {
	WorkspaceID     domain.WorkspaceID
	JobID           domain.JobID
	AttemptID       domain.AttemptID
	Revision        uint64
	Binding         provider.BindingSnapshot
	Resources       []Resource
	Eligible        bool
	Reason          string
	UnconfirmedPlan *provider.Plan
	PreparationID   domain.OperationID
}

func (p PreviewPlan) Fingerprint() domain.SHA256Digest {
	raw, _ := json.Marshal(p)
	return provider.Digest(raw)
}

type PreviewRepository interface {
	PlanCleanup(context.Context, domain.WorkspaceID, string, domain.JobID, domain.AttemptID, bool) (PreviewPlan, error)
}

type ResourcePreview struct {
	ResourceID  domain.ProviderResourceID    `json:"resource_id"`
	Purpose     string                       `json:"purpose"`
	Fingerprint domain.SHA256Digest          `json:"target_sha256"`
	Target      *provider.CleanupDescription `json:"target"`
	Eligible    bool                         `json:"eligible"`
	Observation string                       `json:"observation"`
	Reason      string                       `json:"reason"`
}

type Preview struct {
	WorkspaceID domain.WorkspaceID `json:"workspace_id"`
	JobID       domain.JobID       `json:"job_id"`
	AttemptID   domain.AttemptID   `json:"attempt_id"`
	ObservedAt  time.Time          `json:"observed_at"`
	Eligible    bool               `json:"eligible"`
	Reason      string             `json:"reason"`
	Resources   []ResourcePreview  `json:"resources"`
}

// PreparationRepository records late identity evidence without reopening an attempt.
type PreparationRepository interface {
	RecordCleanupPreparation(context.Context, domain.WorkspaceID, string, domain.JobID, domain.AttemptID, PreviewPlan, provider.Prepared) error
}

type StagingObserver interface {
	ObserveStagingCleanup(context.Context, provider.Plan, domain.OperationID) (provider.PreparationObservation, error)
}
