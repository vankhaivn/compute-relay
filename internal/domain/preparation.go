package domain

import "time"

// PreparationProgress describes frozen input bytes an adapter has handed to its provider
// upload in one fenced dispatch lease. It does not prove provider receipt or readiness.
type PreparationProgress struct {
	Scope          string    `json:"scope"`
	Generation     int64     `json:"generation"`
	BytesCompleted int64     `json:"bytes_completed"`
	BytesTotal     int64     `json:"bytes_total"`
	ObservedAt     time.Time `json:"observed_at"`
}

func (p PreparationProgress) Valid() bool {
	return p.Scope == "staged_input_bytes" && p.Generation > 0 && !p.ObservedAt.IsZero() &&
		p.BytesCompleted >= 0 && p.BytesCompleted <= p.BytesTotal && p.BytesTotal <= 5<<30
}

// PreparationStatus is an additive projection while the active attempt is preparing.
type PreparationStatus struct {
	Progress *PreparationProgress `json:"progress"`
}
