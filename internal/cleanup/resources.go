package cleanup

import (
	"context"
	"encoding/json"
	"regexp"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// The ledger currently permits one staging and one execution row per attempt.
// Keep the bound explicit at every read and wire boundary.
const MaxResources = 2

var resourceReason = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)

type ResourceState string

const (
	ResourcePending  ResourceState = "pending"
	ResourceRunning  ResourceState = "running"
	ResourceDeleted  ResourceState = "deleted"
	ResourceAbsent   ResourceState = "absent"
	ResourceRetained ResourceState = "retained"
	ResourceFailed   ResourceState = "failed"
	ResourceUnknown  ResourceState = "unknown"
)

func (s ResourceState) Valid() bool {
	switch s {
	case ResourcePending, ResourceRunning, ResourceDeleted, ResourceAbsent, ResourceRetained, ResourceFailed, ResourceUnknown:
		return true
	}
	return false
}

func (s ResourceState) Confirmed() bool { return s == ResourceDeleted || s == ResourceAbsent }

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

type ResourceResult struct {
	ResourceID  domain.ProviderResourceID `json:"resource_id"`
	Purpose     string                    `json:"purpose"`
	Fingerprint domain.SHA256Digest       `json:"target_sha256"`
	State       ResourceState             `json:"state"`
	Reason      string                    `json:"reason"`
}

func (r ResourceResult) Valid() bool {
	return r.ResourceID.Valid() && (r.Purpose == "staging" || r.Purpose == "execution") && r.Fingerprint.Valid() &&
		r.State.Valid() && resourceReason.MatchString(r.Reason)
}

type ResourceWork struct {
	Resource Resource
	Result   ResourceResult
}

// ResourceRepository commits each intent before its call and fences every outcome.
// Begin rechecks current eligibility and the immutable target; a preview is irrelevant.
type ResourceRepository interface {
	BeginCleanupResource(context.Context, Work, ResourceWork) error
	CompleteCleanupResource(context.Context, Work, ResourceResult) error
}

type PreviewPlan struct {
	WorkspaceID domain.WorkspaceID
	JobID       domain.JobID
	AttemptID   domain.AttemptID
	Revision    uint64
	Binding     provider.BindingSnapshot
	Resources   []Resource
	Eligible    bool
	Reason      string
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
