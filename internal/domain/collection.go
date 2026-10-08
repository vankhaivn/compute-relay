package domain

import "time"

// CollectionProgress describes selected output work in one fenced invocation.
// Completed includes rehashed cache and the current unverified provider stream;
// Received counts only new provider stream bytes. Neither proves publication.
type CollectionProgress struct {
	Scope          string    `json:"scope"`
	Generation     int64     `json:"generation"`
	BytesCompleted int64     `json:"bytes_completed"`
	BytesReceived  *int64    `json:"bytes_received"`
	BytesTotal     *int64    `json:"bytes_total"`
	ObservedAt     time.Time `json:"observed_at"`
}

func (p CollectionProgress) Valid() bool {
	if p.Scope != "selected_output_bytes" || p.Generation < 1 || p.ObservedAt.IsZero() || p.BytesCompleted < 0 || p.BytesCompleted > 4<<30 {
		return false
	}
	if p.BytesReceived != nil && (*p.BytesReceived < 0 || *p.BytesReceived > p.BytesCompleted) {
		return false
	}
	if p.BytesTotal == nil {
		return p.BytesCompleted == 0
	}
	return *p.BytesTotal >= 0 && *p.BytesTotal <= 4<<30 && p.BytesCompleted <= *p.BytesTotal
}

// CollectionStatus is an additive projection of durable collection evidence.
// Its stage is separate from attempt orchestration and remote execution.
type CollectionStatus struct {
	Mode        string              `json:"mode"`
	State       string              `json:"state"`
	AttemptID   AttemptID           `json:"attempt_id"`
	OperationID *OperationID        `json:"operation_id"`
	Progress    *CollectionProgress `json:"progress"`
}
