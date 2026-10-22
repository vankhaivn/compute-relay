package hoststorage

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

type ClassTotals struct {
	Count int64  `json:"count"`
	Bytes *int64 `json:"bytes"`
}

type MachineTotals struct {
	ChargedBytes               int64 `json:"charged_bytes"`
	CacheBytes                 int64 `json:"cache_bytes"`
	ImageSizeUpperBound        int64 `json:"image_size_upper_bound"`
	AttemptWritableBytes       int64 `json:"attempt_writable_bytes"`
	OrphanBytes                int64 `json:"orphan_bytes"`
	UnobservedReservationDelta int64 `json:"unobserved_reservation_delta_bytes"`
}

type WorkspaceReference struct {
	WorkspaceID          domain.WorkspaceID `json:"workspace_id"`
	ReferencedBytes      int64              `json:"referenced_bytes"`
	AttemptWritableBytes int64              `json:"attempt_writable_bytes"`
}

type CacheTotals struct {
	GenerationCount int64 `json:"generation_count"`
	ChargedBytes    int64 `json:"charged_bytes"`
	LivePinCount    int64 `json:"live_pin_count"`
}

type RetentionItem struct {
	JobID       domain.JobID     `json:"job_id"`
	AttemptID   domain.AttemptID `json:"attempt_id"`
	DueAt       time.Time        `json:"due_at"`
	Status      string           `json:"status"`
	OperationID string           `json:"operation_id,omitempty"`
	Pending     bool             `json:"pending"`
}

type Orphan struct {
	ID           string `json:"orphan_id"`
	Class        Kind   `json:"class"`
	ChargedBytes int64  `json:"charged_bytes"`
	Eligible     bool   `json:"eligible_for_preview"`
}

type BudgetView struct {
	Configured    Budget `json:"configured"`
	Effective     Budget `json:"effective"`
	ReservedBytes int64  `json:"reserved_bytes"`
	ChargedBytes  int64  `json:"charged_bytes"`
	State         string `json:"state"`
}

// View is a bounded, non-secret projection. It never contains remote object
// IDs, names, labels, paths, credentials or another workspace's job records.
type View struct {
	SchemaVersion int `json:"schema_version"`
	Scope         struct {
		ResourceID         string `json:"resource_id"`
		ConnectionRevision int64  `json:"connection_revision"`
	} `json:"scope"`
	ObservedAt                     *time.Time           `json:"observed_at"`
	Reason                         string               `json:"reason,omitempty"`
	ScanState                      string               `json:"scan_state"`
	Precision                      string               `json:"precision"`
	Machine                        MachineTotals        `json:"machine"`
	Workspaces                     []WorkspaceReference `json:"workspaces"`
	OtherWorkspacesReferencedBytes int64                `json:"other_workspaces_referenced_bytes"`
	Cache                          CacheTotals          `json:"cache"`
	Retention                      []RetentionItem      `json:"retention"`
	Orphans                        []Orphan             `json:"orphans"`
	Foreign                        Foreign              `json:"foreign"`
	Budget                         BudgetView           `json:"budget"`
	Truncated                      bool                 `json:"truncated"`
}

type ReadRepository interface {
	ConnectionStorage(context.Context, domain.WorkspaceID, string, string) (View, error)
}
