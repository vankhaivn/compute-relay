// Package executionauth grants one durable finite execution permit to an exact
// managed attempt. Admission, retries and process startup never create a permit.
package executionauth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

const MaxRequestBytes = 4096
const CanonicalVersion = "execution-authorization/v1"

var (
	ErrRequest  = errors.New("invalid execution authorization request")
	ErrNotFound = errors.New("execution authorization or attempt not found")
	ErrConflict = errors.New("execution authorization already exists or request changed")
	ErrState    = errors.New("attempt is not eligible for execution authorization")
	ErrInputs   = errors.New("execution authorization requires frozen immutable inputs")
	ErrQuota    = errors.New("execution authorization requires sufficient fresh quota")
)

type Request struct {
	AttemptID               domain.AttemptID `json:"attempt_id"`
	MaxRemoteWallSeconds    int64            `json:"max_remote_wall_seconds"`
	AuthorizePrivateStaging bool             `json:"authorize_private_staging"`
	AuthorizeCompute        bool             `json:"authorize_compute"`
}

func (r Request) Validate() error {
	if !r.AttemptID.Valid() || r.MaxRemoteWallSeconds < 1 || r.MaxRemoteWallSeconds > domain.MaxRemoteWallSeconds || !r.AuthorizePrivateStaging || !r.AuthorizeCompute {
		return ErrRequest
	}
	return nil
}
func Parse(raw []byte) (Request, error) {
	var r Request
	fields, err := jsonwire.Object(raw, MaxRequestBytes)
	if err != nil || jsonwire.Fields(fields, []string{"attempt_id", "max_remote_wall_seconds", "authorize_private_staging", "authorize_compute"}, nil) != nil || json.Unmarshal(raw, &r) != nil || r.Validate() != nil {
		return Request{}, ErrRequest
	}
	return r, nil
}
func (r Request) Digest(job domain.JobID) (string, error) {
	if !job.Valid() || r.Validate() != nil {
		return "", ErrRequest
	}
	raw, _ := json.Marshal(struct {
		Version string
		Job     domain.JobID
		Request Request
	}{CanonicalVersion, job, r})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

type Status string

const (
	Granted  Status = "granted"
	Consumed Status = "consumed"
	Revoked  Status = "revoked"
)

// Receipt contains only public identities. POST replay returns the original grant;
// GET reads the current status and consumption time.
type Receipt struct {
	AuthorizationID      domain.OperationID `json:"authorization_id"`
	WorkspaceID          domain.WorkspaceID `json:"workspace_id"`
	JobID                domain.JobID       `json:"job_id"`
	AttemptID            domain.AttemptID   `json:"attempt_id"`
	Status               Status             `json:"status"`
	MaxRemoteWallSeconds int64              `json:"max_remote_wall_seconds"`
	CreatedAt            time.Time          `json:"created_at"`
	ConsumedAt           *time.Time         `json:"consumed_at"`
	Replay               bool               `json:"replay"`
}

func (r Receipt) Validate() error {
	if !r.AuthorizationID.Valid() || !r.WorkspaceID.Valid() || !r.JobID.Valid() || !r.AttemptID.Valid() || r.MaxRemoteWallSeconds < 1 || r.MaxRemoteWallSeconds > domain.MaxRemoteWallSeconds || r.CreatedAt.IsZero() {
		return ErrRequest
	}
	if r.Status != Granted && r.Status != Consumed && r.Status != Revoked {
		return ErrRequest
	}
	if (r.Status == Consumed) != (r.ConsumedAt != nil) || r.ConsumedAt != nil && r.ConsumedAt.Before(r.CreatedAt) {
		return ErrRequest
	}
	return nil
}
