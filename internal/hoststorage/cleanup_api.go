package hoststorage

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type Candidate struct {
	WorkspaceID  domain.WorkspaceID
	ConnectionID string
	OrphanID     string
	ScanSeq      int64
	Target       Target
	Binding      provider.BindingSnapshot
}

type Preview struct {
	OrphanID     string    `json:"orphan_id"`
	Class        Kind      `json:"class"`
	ChargedBytes int64     `json:"charged_bytes"`
	Eligible     bool      `json:"eligible"`
	Reason       string    `json:"reason"`
	Fingerprint  string    `json:"fingerprint"`
	ExpiresAt    time.Time `json:"expires_at"`
}

type Operation struct {
	OperationID domain.OperationID `json:"operation_id"`
	Status      string             `json:"status"`
	Result      string             `json:"result"`
	Reason      string             `json:"reason"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

type CleanupRepository interface {
	FindStorageOrphan(context.Context, domain.WorkspaceID, string, string, string) (Candidate, error)
	RecordStoragePreview(context.Context, domain.WorkspaceID, string, Candidate, TargetObservation) (Preview, error)
	AdmitStorageCleanup(context.Context, domain.WorkspaceID, string, string, string, string, string) (Operation, error)
	ReadStorageCleanup(context.Context, domain.WorkspaceID, string, domain.OperationID) (Operation, error)
}

type Controller interface {
	Preview(context.Context, domain.WorkspaceID, string, string, string) (Preview, error)
	Apply(context.Context, domain.WorkspaceID, string, string, string, string, string) (Operation, error)
	Operation(context.Context, domain.WorkspaceID, string, domain.OperationID) (Operation, error)
}
