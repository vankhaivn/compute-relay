package provider

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// ControlledStarter is optional. A preparation permit never authorizes this call;
// orchestration must first durably consume the separate exact-attempt start permit.
type ControlledStarter interface {
	Start(context.Context, Prepared, StartAuthorization) SubmissionOutcome
}

// StartAuthorization tightens an immutable prepared job without rewriting it.
// It contains only non-secret authority and is frozen before any remote write.
type StartAuthorization struct {
	AuthorizationID      domain.OperationID  `json:"authorization_id"`
	ReadinessRevision    domain.SHA256Digest `json:"readiness_revision"`
	MaxRemoteWallSeconds int64               `json:"max_remote_wall_seconds"`
	ExecutionDeadline    string              `json:"execution_deadline"`
	StartBefore          string              `json:"start_before"`
}

func (s StartAuthorization) Validate(p Prepared) error {
	deadline, err := time.Parse(time.RFC3339, s.ExecutionDeadline)
	before, beforeErr := time.Parse(time.RFC3339, s.StartBefore)
	if !s.AuthorizationID.Valid() || !s.ReadinessRevision.Valid() || s.ReadinessRevision != p.ReadinessRevision() || s.MaxRemoteWallSeconds < 1 || s.MaxRemoteWallSeconds > 86400 || err != nil || beforeErr != nil || deadline.Year() < 2 || before.Year() < 2 || deadline.UTC().Format(time.RFC3339) != s.ExecutionDeadline || before.UTC().Format(time.RFC3339) != s.StartBefore || before.After(deadline) {
		return errors.New("invalid explicit start authorization")
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
