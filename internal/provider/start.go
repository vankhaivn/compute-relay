package provider

import (
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

// SecretAuthorization is non-secret immutable delivery policy, not material.
type SecretAuthorization struct {
	AuthorizationID domain.OperationID    `json:"authorization_id"`
	ExpiresAt       string                `json:"expires_at"`
	Slots           []workloadsecret.Slot `json:"slots"`
}

// StartAuthorization tightens an immutable prepared job without rewriting it.
// It contains only non-secret authority and is frozen before any remote write.
type StartAuthorization struct {
	Secrets              *SecretAuthorization `json:"secrets,omitempty"`
	AuthorizationID      domain.OperationID   `json:"authorization_id"`
	ReadinessRevision    domain.SHA256Digest  `json:"readiness_revision"`
	MaxRemoteWallSeconds int64                `json:"max_remote_wall_seconds"`
	ExecutionDeadline    string               `json:"execution_deadline"`
	StartBefore          string               `json:"start_before"`
}

func (s StartAuthorization) Clone() StartAuthorization {
	if s.Secrets != nil {
		copy := *s.Secrets
		copy.Slots = slices.Clone(copy.Slots)
		s.Secrets = &copy
	}
	return s
}
func (s StartAuthorization) Equal(other StartAuthorization) bool {
	a, b := s.Secrets, other.Secrets
	s.Secrets, other.Secrets = nil, nil
	if s != other {
		return false
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.AuthorizationID == b.AuthorizationID && a.ExpiresAt == b.ExpiresAt && slices.Equal(a.Slots, b.Slots)
}

func (s StartAuthorization) Validate(p Prepared) error {
	deadline, err := time.Parse(time.RFC3339, s.ExecutionDeadline)
	before, beforeErr := time.Parse(time.RFC3339, s.StartBefore)
	if !s.AuthorizationID.Valid() || !s.ReadinessRevision.Valid() || s.ReadinessRevision != p.ReadinessRevision() || s.MaxRemoteWallSeconds < 1 || s.MaxRemoteWallSeconds > 86400 || err != nil || beforeErr != nil || deadline.Year() < 2 || before.Year() < 2 || deadline.UTC().Format(time.RFC3339) != s.ExecutionDeadline || before.UTC().Format(time.RFC3339) != s.StartBefore || before.After(deadline) {
		return errors.New("invalid explicit start authorization")
	}
	if s.Secrets != nil {
		expires, ok := workloadsecret.CanonicalTime(s.Secrets.ExpiresAt)
		if !s.Secrets.AuthorizationID.Valid() || !ok || expires.Before(deadline) || len(s.Secrets.Slots) == 0 || !workloadsecret.ValidSlots(s.Secrets.Slots) {
			return errors.New("invalid workload secret authorization")
		}
	}
	return nil
}

func (s StartAuthorization) ValidateMaterial(p Prepared, material workloadsecret.Material) error {
	if s.Validate(p) != nil || s.Secrets == nil || material.AuthorizationID != s.Secrets.AuthorizationID || material.Key.WorkspaceID != p.Identity.WorkspaceID || material.Key.JobID != p.Identity.JobID || material.Key.AttemptID != p.Identity.AttemptID || material.ExpiresAt != s.Secrets.ExpiresAt || len(material.Values) != len(s.Secrets.Slots) {
		return errors.New("invalid workload secret material binding")
	}
	request := workloadsecret.Supply{AttemptID: material.Key.AttemptID, ReadinessRevision: s.ReadinessRevision, ExpiresAt: material.ExpiresAt, Values: material.Values}
	if request.Validate() != nil {
		return errors.New("invalid workload secret material")
	}
	for _, slot := range s.Secrets.Slots {
		if len(material.Values[slot.Name]) == 0 {
			return errors.New("required workload secret material missing")
		}
	}
	return nil
}

func (p Prepared) ReadinessRevision() domain.SHA256Digest {
	if !p.Ready || !p.Private || !p.VerificationSHA256.Valid() || !p.ManifestSHA256.Valid() {
		return ""
	}
	raw, _ := json.Marshal(struct {
		Version  string
		Prepared Prepared
	}{"readiness/v1", p})
	return Digest(raw)
}
