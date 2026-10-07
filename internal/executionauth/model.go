// Package executionauth grants one durable finite execution permit to an exact
// managed attempt. Admission, retries and process startup never create a permit.
package executionauth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
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
	if !r.AttemptID.Valid() || r.MaxRemoteWallSeconds < 1 || r.MaxRemoteWallSeconds > 86400 || !r.AuthorizePrivateStaging || !r.AuthorizeCompute {
		return ErrRequest
	}
	return nil
}
func Parse(raw []byte) (Request, error) {
	var r Request
	if len(raw) == 0 || len(raw) > MaxRequestBytes || !utf8.Valid(raw) {
		return r, ErrRequest
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return r, ErrRequest
	}
	seen := map[string]bool{}
	for d.More() {
		token, err = d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return Request{}, ErrRequest
		}
		seen[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil || bytes.Equal(value, []byte("null")) {
			return Request{}, ErrRequest
		}
		switch key {
		case "attempt_id":
			err = json.Unmarshal(value, &r.AttemptID)
		case "max_remote_wall_seconds":
			err = json.Unmarshal(value, &r.MaxRemoteWallSeconds)
		case "authorize_private_staging":
			err = json.Unmarshal(value, &r.AuthorizePrivateStaging)
		case "authorize_compute":
			err = json.Unmarshal(value, &r.AuthorizeCompute)
		default:
			return Request{}, ErrRequest
		}
		if err != nil {
			return Request{}, ErrRequest
		}
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') || d.Decode(new(any)) != io.EOF || len(seen) != 4 || r.Validate() != nil {
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

// Managed requires an already validated immutable profile; malformed vault refs
// must never fall through to the standalone process-budget compatibility mode.
func Managed(p admission.Profile) bool { return strings.HasPrefix(p.CredentialRef, "vault:") }

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
	if !r.AuthorizationID.Valid() || !r.WorkspaceID.Valid() || !r.JobID.Valid() || !r.AttemptID.Valid() || r.MaxRemoteWallSeconds < 1 || r.MaxRemoteWallSeconds > 86400 || r.CreatedAt.IsZero() {
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
