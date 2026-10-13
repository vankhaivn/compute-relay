// Package cleanup admits and applies explicit, attempt-scoped deletion of the remote data a
// provider adapter created for one attempt. HTTP admission only commits durable intent; a
// separately composed engine records fenced per-resource outcomes against the attempt's
// frozen binding. Nothing is ever deleted automatically.
package cleanup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"unicode"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

const MaxRequestBytes = operations.MaxRequestBytes
const CanonicalVersion = "cleanup/v1"

var (
	// ErrUncollected refuses deletion of results that were never verified locally
	// unless the request explicitly confirms discard_uncollected.
	ErrUncollected = errors.New("results were not collected; confirm discard_uncollected")
	ErrInvalid     = errors.New("invalid cleanup record")
	ErrLeaseLost   = errors.New("cleanup ownership expired or changed")
)

// Request always names the intended attempt. DiscardUncollected is an explicit owner
// confirmation; omitted and false are the same canonical request. Reason is non-secret.
type Request struct {
	AttemptID             domain.AttemptID `json:"attempt_id"`
	DiscardUncollected    bool             `json:"discard_uncollected"`
	AcceptNameBasedDelete bool             `json:"accept_name_based_delete,omitempty"`
	Reason                string           `json:"reason,omitempty"`
}

func (r Request) Validate() error {
	if !r.AttemptID.Valid() || !utf8.ValidString(r.Reason) || len(r.Reason) > 512 {
		return operations.ErrRequest
	}
	for _, c := range r.Reason {
		if unicode.IsControl(c) {
			return operations.ErrRequest
		}
	}
	return nil
}

// Parse rejects duplicate keys, null values, trailing JSON, unknown fields and invalid
// UTF-8, matching the other control requests. A missing attempt is never implicit.
func Parse(raw []byte) (Request, error) {
	var r Request
	if len(raw) == 0 || len(raw) > MaxRequestBytes || !utf8.Valid(raw) {
		return r, operations.ErrRequest
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return r, operations.ErrRequest
	}
	seen := map[string]bool{}
	for d.More() {
		token, err = d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return Request{}, operations.ErrRequest
		}
		seen[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil || len(value) == 0 {
			return Request{}, operations.ErrRequest
		}
		switch key {
		case "attempt_id", "reason":
			var text string
			if value[0] != '"' || json.Unmarshal(value, &text) != nil {
				return Request{}, operations.ErrRequest
			}
			if key == "attempt_id" {
				r.AttemptID = domain.AttemptID(text)
			} else {
				r.Reason = text
			}
		case "discard_uncollected", "accept_name_based_delete":
			switch string(value) {
			case "true":
				if key == "discard_uncollected" {
					r.DiscardUncollected = true
				} else {
					r.AcceptNameBasedDelete = true
				}
			case "false":
			default:
				return Request{}, operations.ErrRequest
			}
		default:
			return Request{}, operations.ErrRequest
		}
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') || d.Decode(new(any)) != io.EOF || !seen["attempt_id"] || r.Validate() != nil {
		return Request{}, operations.ErrRequest
	}
	return r, nil
}

// Digest binds the job, so one key cannot be reused against a different job route.
func (r Request) Digest(job domain.JobID) (string, error) {
	if !job.Valid() || r.Validate() != nil {
		return "", operations.ErrRequest
	}
	raw, err := json.Marshal(struct {
		Version string               `json:"version"`
		Job     domain.JobID         `json:"job_id"`
		Kind    domain.OperationKind `json:"kind"`
		Request Request              `json:"request"`
	}{CanonicalVersion, job, domain.OperationCleanup, r})
	if err != nil {
		return "", operations.ErrRequest
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

type Effect string

const (
	Pending       Effect = "pending"
	RemoteDeleted Effect = "remote_deleted"
	AlreadyAbsent Effect = "already_absent"
	Failed        Effect = "failed"
)

func (e Effect) Valid() bool {
	return e == Pending || e == RemoteDeleted || e == AlreadyAbsent || e == Failed
}

// Record is the internal durable view. The HTTP adapter emits only explicit safe fields;
// the request reason, ledger identity and provider reference are never serialized.
type Record struct {
	Operation domain.Operation
	Effect    Effect
	Replay    bool
	Resources []ResourceResult `json:"-"`
}

func (r Record) Validate() error {
	op := r.Operation
	if len(r.Resources) > MaxResources {
		return ErrInvalid
	}
	seen := map[domain.ProviderResourceID]bool{}
	for _, resource := range r.Resources {
		if !resource.Valid() || seen[resource.ResourceID] {
			return ErrInvalid
		}
		seen[resource.ResourceID] = true
	}
	if op.Validate() != nil || op.Kind != domain.OperationCleanup || !op.JobID.Valid() || !op.AttemptID.Valid() || !r.Effect.Valid() {
		return ErrInvalid
	}
	switch op.Status {
	case domain.OperationAccepted, domain.OperationRunning:
		if r.Effect != Pending {
			return ErrInvalid
		}
	case domain.OperationSucceeded:
		if r.Effect != RemoteDeleted && r.Effect != AlreadyAbsent {
			return ErrInvalid
		}
	case domain.OperationFailed:
		if r.Effect != Failed {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
