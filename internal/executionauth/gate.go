package executionauth

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

const (
	MaxPreparationSeconds int64 = 600
	MaxReadyHold                = time.Hour
	MaxStagedBytes        int64 = (4 << 30) + (100 << 20)
	MaxHeldPerAccount           = 4
	MaxHeldGlobal               = 16
)

type PrepareRequest struct {
	AttemptID               domain.AttemptID `json:"attempt_id"`
	MaxPreparationSeconds   int64            `json:"max_preparation_seconds"`
	ReadyUntil              string           `json:"ready_until"`
	MaxStagedBytes          int64            `json:"max_staged_bytes"`
	AuthorizePrivateStaging bool             `json:"authorize_private_staging"`
}

func canonicalTime(raw string) (time.Time, bool) {
	at, err := time.Parse(time.RFC3339, raw)
	return at, err == nil && at.Year() >= 2 && at.Location() == time.UTC && at.Format(time.RFC3339) == raw
}
func (r PrepareRequest) Validate() error {
	_, ok := canonicalTime(r.ReadyUntil)
	if !r.AttemptID.Valid() || r.MaxPreparationSeconds < 1 || r.MaxPreparationSeconds > MaxPreparationSeconds || !ok || r.MaxStagedBytes < 1 || r.MaxStagedBytes > MaxStagedBytes || !r.AuthorizePrivateStaging {
		return ErrRequest
	}
	return nil
}

type StartRequest struct {
	SecretAuthorizationID domain.OperationID  `json:"secret_authorization_id,omitempty"`
	AttemptID             domain.AttemptID    `json:"attempt_id"`
	ReadinessRevision     domain.SHA256Digest `json:"readiness_revision"`
	MaxRemoteWallSeconds  int64               `json:"max_remote_wall_seconds"`
	ExecutionDeadline     string              `json:"execution_deadline"`
	AuthorizeCompute      bool                `json:"authorize_compute"`
}

func (r StartRequest) Validate() error {
	_, ok := canonicalTime(r.ExecutionDeadline)
	if r.SecretAuthorizationID != "" && !r.SecretAuthorizationID.Valid() || !r.AttemptID.Valid() || !r.ReadinessRevision.Valid() || r.MaxRemoteWallSeconds < 1 || r.MaxRemoteWallSeconds > 86400 || !ok || !r.AuthorizeCompute {
		return ErrRequest
	}
	return nil
}

// gateParse rejects duplicate, unknown, missing and null fields before decoding.
func gateParse(raw []byte, fields []string, out any) error {
	if len(raw) == 0 || len(raw) > MaxRequestBytes || !utf8.Valid(raw) {
		return ErrRequest
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return ErrRequest
	}
	remaining := map[string]bool{}
	for _, field := range fields {
		remaining[field] = true
	}
	for d.More() {
		t, err = d.Token()
		key, ok := t.(string)
		if err != nil || !ok || !remaining[key] {
			return ErrRequest
		}
		delete(remaining, key)
		var value json.RawMessage
		if d.Decode(&value) != nil || bytes.Equal(value, []byte("null")) {
			return ErrRequest
		}
	}
	if t, err = d.Token(); err != nil || t != json.Delim('}') || len(remaining) != 0 || d.Decode(new(any)) != io.EOF || json.Unmarshal(raw, out) != nil {
		return ErrRequest
	}
	return nil
}
func ParsePrepare(raw []byte) (PrepareRequest, error) {
	var r PrepareRequest
	if gateParse(raw, []string{"attempt_id", "max_preparation_seconds", "ready_until", "max_staged_bytes", "authorize_private_staging"}, &r) != nil || r.Validate() != nil {
		return PrepareRequest{}, ErrRequest
	}
	return r, nil
}
func ParseStart(raw []byte) (StartRequest, error) {
	var r StartRequest
	if workloadsecret.StrictDecode(raw, MaxRequestBytes, &r) != nil || r.Validate() != nil {
		return StartRequest{}, ErrRequest
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return StartRequest{}, ErrRequest
	}
	if _, present := fields["secret_authorization_id"]; present && !r.SecretAuthorizationID.Valid() {
		return StartRequest{}, ErrRequest
	}
	return r, nil
}
func GateDigest(kind string, job domain.JobID, request any) domain.SHA256Digest {
	raw, _ := json.Marshal(struct {
		Version string
		Kind    string
		Job     domain.JobID
		Request any
	}{"execution-gate/v1", kind, job, request})
	return provider.Digest(raw)
}

type GateReceipt struct {
	AuthorizationID domain.OperationID `json:"authorization_id"`
	WorkspaceID     domain.WorkspaceID `json:"workspace_id"`
	JobID           domain.JobID       `json:"job_id"`
	AttemptID       domain.AttemptID   `json:"attempt_id"`
	Kind            string             `json:"kind"`
	Status          Status             `json:"status"`
	CreatedAt       time.Time          `json:"created_at"`
	ConsumedAt      *time.Time         `json:"consumed_at"`
	Replay          bool               `json:"replay"`
}

func (r GateReceipt) Validate() error {
	if !r.AuthorizationID.Valid() || !r.WorkspaceID.Valid() || !r.JobID.Valid() || !r.AttemptID.Valid() || (r.Kind != "prepare" && r.Kind != "start") || (r.Status != Granted && r.Status != Consumed) || r.CreatedAt.IsZero() || (r.Status == Consumed) != (r.ConsumedAt != nil) || r.ConsumedAt != nil && r.ConsumedAt.Before(r.CreatedAt) {
		return ErrRequest
	}
	return nil
}

// Gate is internal durable authority, separate from the observed dispatch journal.
type Gate struct {
	Preparation   GateReceipt
	Request       PrepareRequest
	StagedBytes   int64
	ReadyRevision domain.SHA256Digest
	ReadyAt       *time.Time
	Start         *provider.StartAuthorization
	StartReceipt  *GateReceipt
}

func (g Gate) Expired(now time.Time) bool {
	at, _ := canonicalTime(g.Request.ReadyUntil)
	if !now.Before(at) {
		return true
	}
	if g.Start != nil {
		deadline, _ := canonicalTime(g.Start.ExecutionDeadline)
		return !now.Before(deadline)
	}
	return false
}

// Readiness is a durable observation; reading it never performs provider calls.
type Readiness struct {
	Secrets         *workloadsecret.Readiness `json:"secrets,omitempty"`
	AttemptID       domain.AttemptID          `json:"attempt_id"`
	State           string                    `json:"state"`
	Revision        domain.SHA256Digest       `json:"revision,omitempty"`
	ObservedAt      *time.Time                `json:"observed_at"`
	ReadyUntil      string                    `json:"ready_until,omitempty"`
	StagedBytes     int64                     `json:"staged_bytes"`
	RequiredActions []string                  `json:"required_actions"`
}
