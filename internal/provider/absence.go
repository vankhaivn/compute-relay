package provider

import (
	"github.com/vankhaivn/compute-relay/internal/domain"
	"time"
)

// RemoteAbsence proves the exact accepted resource is absent in its pinned execution
// scope. A name lookup, permission failure or changed engine cannot supply this proof.
// It says nothing about the final execution outcome or retained result bytes.
type RemoteAbsence struct {
	Remote     RemoteReference `json:"remote"`
	ObservedAt time.Time       `json:"observed_at"`
}

func (a RemoteAbsence) Error() string {
	return "recorded remote resource is absent; final execution outcome is unknown"
}
func (a RemoteAbsence) Unwrap() error {
	return Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation, a.Error()).WithDetail(domain.ReasonDetail, "remote_not_found")
}
