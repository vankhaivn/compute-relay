// Package operations admits explicit, attempt-scoped controls. It never selects a
// replacement provider, re-fetches a frozen URL, or executes workload code locally.
package operations

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

const MaxRequestBytes = 4096

// CanonicalVersion preserves the original request format for each operation family.
func CanonicalVersion(kind domain.OperationKind) string {
	if kind == domain.OperationCleanup {
		return "cleanup/v1"
	}
	return "control/v1"
}

var (
	ErrRequest     = errors.New("invalid control request")
	ErrNotFound    = errors.New("operation or attempt not found")
	ErrConflict    = errors.New("control idempotency conflict")
	ErrChanged     = errors.New("the target attempt changed")
	ErrUnresolved  = errors.New("remote execution is unresolved")
	ErrCollect     = errors.New("recover results without rerunning successful compute")
	ErrInputs      = errors.New("original immutable inputs are unavailable")
	ErrState       = errors.New("control is not appropriate for the current state")
	ErrBusy        = errors.New("a control of this kind is already pending")
	ErrUncollected = errors.New("results were not collected; confirm discard_uncollected")
)

// Request always names the intended attempt. A delayed request must not silently
// target an attempt created by a concurrent retry. Reason is non-secret metadata.
type Request struct {
	AttemptID          domain.AttemptID `json:"attempt_id"`
	Reason             string           `json:"reason,omitempty"`
	DiscardUncollected bool             `json:"discard_uncollected,omitempty"`
}

func Supported(kind domain.OperationKind) bool {
	return kind == domain.OperationCancel || kind == domain.OperationRetryCompute ||
		kind == domain.OperationReconcile || kind == domain.OperationCollect || kind == domain.OperationCleanup
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
	if kind != domain.OperationCleanup && r.DiscardUncollected {
		return ErrRequest
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
	fields, err := jsonwire.Object(raw, MaxRequestBytes)
	optional := []string{"reason"}
	if kind == domain.OperationCleanup {
		optional = append(optional, "discard_uncollected")
	}
	if err != nil || jsonwire.Fields(fields, []string{"attempt_id"}, optional) != nil || json.Unmarshal(raw, &r) != nil || r.Validate(kind) != nil {
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
	request, err := r.CanonicalJSON(kind)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(struct {
		Version string               `json:"version"`
		Job     domain.JobID         `json:"job_id"`
		Kind    domain.OperationKind `json:"kind"`
		Request json.RawMessage      `json:"request"`
	}{CanonicalVersion(kind), job, kind, request})
	if err != nil {
		return "", ErrRequest
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// CanonicalJSON keeps cleanup/v1's explicit false field and historical field order.
func (r Request) CanonicalJSON(kind domain.OperationKind) ([]byte, error) {
	if r.Validate(kind) != nil {
		return nil, ErrRequest
	}
	if kind == domain.OperationCleanup {
		return json.Marshal(struct {
			AttemptID          domain.AttemptID `json:"attempt_id"`
			DiscardUncollected bool             `json:"discard_uncollected"`
			Reason             string           `json:"reason,omitempty"`
		}{r.AttemptID, r.DiscardUncollected, r.Reason})
	}
	return json.Marshal(r)
}

type Effect string

const (
	Pending               Effect = "pending"
	RemoteDeleted         Effect = "remote_deleted"
	AlreadyAbsent         Effect = "already_absent"
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
	case RemoteDeleted, AlreadyAbsent, Pending, DispatchPrevented, CancellationRequested, CancellationConfirmed, TooLate,
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
	Resources            []ResourceResult `json:"-"`
}

func (r Record) Validate() error {
	if r.Operation.Validate() != nil || !Supported(r.Operation.Kind) || !r.Effect.Valid() || !r.Operation.JobID.Valid() || !r.Operation.AttemptID.Valid() || len(r.Resources) > MaxCleanupResources || r.Operation.Kind != domain.OperationCleanup && len(r.Resources) != 0 {
		return ErrRequest
	}
	seen := map[domain.ProviderResourceID]bool{}
	for _, resource := range r.Resources {
		if !resource.Valid() || seen[resource.ResourceID] {
			return ErrRequest
		}
		seen[resource.ResourceID] = true
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
		if !(r.Operation.Kind == domain.OperationCancel && r.Effect == CancellationRequested || r.Operation.Kind == domain.OperationCleanup && r.Effect == Pending) {
			return ErrRequest
		}
	case domain.OperationFailed:
		if r.Effect != Failed {
			return ErrRequest
		}
	case domain.OperationManualRequired:
		if r.Operation.Kind == domain.OperationCleanup || r.Effect != ManualRequired {
			return ErrRequest
		}
	case domain.OperationSucceeded:
		switch r.Operation.Kind {
		case domain.OperationCleanup:
			if r.Effect != RemoteDeleted && r.Effect != AlreadyAbsent {
				return ErrRequest
			}
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
