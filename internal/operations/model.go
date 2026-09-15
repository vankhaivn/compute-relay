// Package operations admits explicit, attempt-scoped controls. It never selects a
// replacement provider, re-fetches a frozen URL, or executes workload code locally.
package operations

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

const MaxRequestBytes = 4096
const CanonicalVersion = "control/v1"

var (
	ErrRequest    = errors.New("invalid control request")
	ErrNotFound   = errors.New("operation or attempt not found")
	ErrConflict   = errors.New("control idempotency conflict")
	ErrChanged    = errors.New("the target attempt changed")
	ErrUnresolved = errors.New("remote execution is unresolved")
	ErrCollect    = errors.New("recover results without rerunning successful compute")
	ErrInputs     = errors.New("original immutable inputs are unavailable")
	ErrState      = errors.New("control is not appropriate for the current state")
	ErrBusy       = errors.New("a control of this kind is already pending")
)

// Request always names the intended attempt. A delayed request must not silently
// target an attempt created by a concurrent retry. Reason is non-secret metadata.
type Request struct {
	AttemptID domain.AttemptID `json:"attempt_id"`
	Reason    string           `json:"reason,omitempty"`
}

func Supported(kind domain.OperationKind) bool {
	return kind == domain.OperationCancel || kind == domain.OperationRetryCompute ||
		kind == domain.OperationReconcile || kind == domain.OperationCollect
}

func (r Request) Validate(kind domain.OperationKind) error {
	if !Supported(kind) || !r.AttemptID.Valid() || !utf8.ValidString(r.Reason) || len(r.Reason) > 512 {
		return ErrRequest
	}
	for _, c := range r.Reason {
		if unicode.IsControl(c) {
			return ErrRequest
		}
	}
	if kind == domain.OperationRetryCompute && strings.TrimSpace(r.Reason) == "" {
		return ErrRequest
	}
	return nil
}

// Parse rejects duplicate keys, null strings, trailing JSON and unknown fields,
// as well as invalid UTF-8. A null/empty body is not an implicit active attempt.
func Parse(kind domain.OperationKind, raw []byte) (Request, error) {
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
		if err != nil || !ok || seen[key] || key != "attempt_id" && key != "reason" {
			return Request{}, ErrRequest
		}
		seen[key] = true
		var value json.RawMessage
		var text string
		if d.Decode(&value) != nil || len(value) == 0 || value[0] != '"' || json.Unmarshal(value, &text) != nil {
			return Request{}, ErrRequest
		}
		if key == "attempt_id" {
			r.AttemptID = domain.AttemptID(text)
		} else {
			r.Reason = text
		}
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') || d.Decode(new(any)) != io.EOF || !seen["attempt_id"] || r.Validate(kind) != nil {
		return Request{}, ErrRequest
	}
	return r, nil
}

// Digest includes the job and kind, so a key scoped to workspace+kind cannot be
// reused against a different route. Optional omitted and empty reasons canonicalize.
func (r Request) Digest(job domain.JobID, kind domain.OperationKind) (string, error) {
	if !job.Valid() || r.Validate(kind) != nil {
		return "", ErrRequest
	}
	raw, err := json.Marshal(struct {
		Version string               `json:"version"`
		Job     domain.JobID         `json:"job_id"`
		Kind    domain.OperationKind `json:"kind"`
		Request Request              `json:"request"`
	}{CanonicalVersion, job, kind, r})
	if err != nil {
		return "", ErrRequest
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

type Effect string

const (
	Pending               Effect = "pending"
	DispatchPrevented     Effect = "dispatch_prevented"
	CancellationRequested Effect = "cancellation_requested"
	CancellationConfirmed Effect = "cancellation_confirmed"
	TooLate               Effect = "too_late"
	ObservationRefreshed  Effect = "observation_refreshed"
	CollectionRequested   Effect = "collection_requested"
	ResultsAvailable      Effect = "results_available"
	NewAttemptCreated     Effect = "new_attempt_created"
	ManualRequired        Effect = "manual_required"
	Failed                Effect = "failed"
)

func (e Effect) Valid() bool {
	switch e {
	case Pending, DispatchPrevented, CancellationRequested, CancellationConfirmed, TooLate,
		ObservationRefreshed, CollectionRequested, ResultsAvailable, NewAttemptCreated, ManualRequired, Failed:
		return true
	default:
		return false
	}
}

// Record is an internal durable view. The HTTP adapter emits only explicit safe
// fields; it never serializes the request reason, token identity or provider journal.
type Record struct {
	Operation            domain.Operation
	Effect               Effect
	NewAttemptID         domain.AttemptID
	TerminationConfirmed bool
	Replay               bool
}

func (r Record) Validate() error {
	if r.Operation.Validate() != nil || !Supported(r.Operation.Kind) || !r.Effect.Valid() {
		return ErrRequest
	}
	if r.NewAttemptID != "" && (!r.NewAttemptID.Valid() || r.Operation.Kind != domain.OperationRetryCompute || r.NewAttemptID == r.Operation.AttemptID || r.Effect != NewAttemptCreated) {
		return ErrRequest
	}
	if r.TerminationConfirmed != (r.Effect == CancellationConfirmed) {
		return ErrRequest
	}
	switch r.Operation.Status {
	case domain.OperationAccepted:
		if r.Effect != Pending && !(r.Operation.Kind == domain.OperationCollect && r.Effect == CollectionRequested) {
			return ErrRequest
		}
	case domain.OperationRunning:
		if r.Operation.Kind != domain.OperationCancel || r.Effect != CancellationRequested {
			return ErrRequest
		}
	case domain.OperationFailed:
		if r.Effect != Failed {
			return ErrRequest
		}
	case domain.OperationManualRequired:
		if r.Effect != ManualRequired {
			return ErrRequest
		}
	case domain.OperationSucceeded:
		switch r.Operation.Kind {
		case domain.OperationCancel:
			if r.Effect != DispatchPrevented && r.Effect != CancellationConfirmed && r.Effect != TooLate {
				return ErrRequest
			}
		case domain.OperationRetryCompute:
			if r.Effect != NewAttemptCreated || r.NewAttemptID == "" {
				return ErrRequest
			}
		case domain.OperationReconcile:
			if r.Effect != ObservationRefreshed {
				return ErrRequest
			}
		case domain.OperationCollect:
			if r.Effect != ResultsAvailable {
				return ErrRequest
			}
		}
	}
	return nil
}

// RetryAllowed is conservative: finish the previous attempt's outcome assessment
// first. Successful execution, even with invalid/missing artifacts, is never recovery
// by re-execution. The store additionally checks the submission journal and bytes.
func RetryAllowed(s domain.AttemptState) error {
	if s.Validate() != nil {
		return ErrState
	}
	if s.RemoteActivity != domain.RemoteActivityNotStarted && s.RemoteActivity != domain.RemoteActivityInactive {
		return ErrUnresolved
	}
	if s.Execution == domain.ExecutionSucceeded {
		return ErrCollect
	}
	if !s.Orchestration.Terminal() {
		return ErrUnresolved
	}
	if s.Execution != domain.ExecutionNotSubmitted && !s.Execution.Terminal() {
		return ErrUnresolved
	}
	return nil
}
