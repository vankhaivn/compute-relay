// Package workloadsecret owns write-only, attempt-scoped workload material.
// Provider login credentials are outside this package and cannot be referenced.
package workloadsecret

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

const (
	MaxSlots        = 8
	MaxSlotBytes    = 8 << 10
	MaxTotalBytes   = 32 << 10
	MaxRequestBytes = 48 << 10
	MaxHeld         = 16
	MaxLifetime     = 24 * time.Hour
)

var (
	ErrRequest     = errors.New("invalid workload secret request")
	ErrNotFound    = errors.New("workload secret authorization not found")
	ErrConflict    = errors.New("workload secret authorization already exists or request changed")
	ErrState       = errors.New("attempt is not eligible for workload secret supply")
	ErrUnavailable = errors.New("required workload secret material is unavailable")
	ErrLimit       = errors.New("ephemeral workload secret capacity reached")
	namePattern    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
)

func ValidName(name string) bool { return namePattern.MatchString(name) }

type Slot struct {
	Name     string `json:"name"`
	Exposure string `json:"exposure"`
}

func ValidSlots(slots []Slot) bool {
	if len(slots) > MaxSlots {
		return false
	}
	seen := map[string]bool{}
	for _, slot := range slots {
		if !ValidName(slot.Name) || seen[slot.Name] || slot.Exposure != "file" && slot.Exposure != "file_and_local_process_arguments" {
			return false
		}
		seen[slot.Name] = true
	}
	return true
}

type Readiness struct {
	RequiredSlots   []Slot             `json:"required_slots"`
	AuthorizationID domain.OperationID `json:"authorization_id,omitempty"`
	Status          string             `json:"status"`
	ExpiresAt       string             `json:"expires_at,omitempty"`
}

type Key struct {
	WorkspaceID domain.WorkspaceID
	JobID       domain.JobID
	AttemptID   domain.AttemptID
}

func (k Key) Valid() bool { return k.WorkspaceID.Valid() && k.JobID.Valid() && k.AttemptID.Valid() }

// Supply contains temporary byte slices only; no value-bearing representation
// may be included in a receipt, error, persistent request or dispatch journal.
type Supply struct {
	AttemptID         domain.AttemptID    `json:"attempt_id"`
	ReadinessRevision domain.SHA256Digest `json:"readiness_revision"`
	ExpiresAt         string              `json:"expires_at"`
	Values            map[string][]byte   `json:"values"`
}

func (Supply) String() string   { return "[workload secret request redacted]" }
func (Supply) GoString() string { return "[workload secret request redacted]" }
func (r *Supply) Clear() {
	for name, value := range r.Values {
		clear(value)
		delete(r.Values, name)
	}
}
func CanonicalTime(raw string) (time.Time, bool) {
	at, err := time.Parse(time.RFC3339, raw)
	return at, err == nil && at.Year() >= 2 && at.UTC().Format(time.RFC3339) == raw
}
func (r Supply) Validate() error {
	_, ok := CanonicalTime(r.ExpiresAt)
	if !r.AttemptID.Valid() || !r.ReadinessRevision.Valid() || !ok || len(r.Values) < 1 || len(r.Values) > MaxSlots {
		return ErrRequest
	}
	total := 0
	for name, value := range r.Values {
		if !ValidName(name) || len(value) < 1 || len(value) > MaxSlotBytes {
			return ErrRequest
		}
		total += len(value)
	}
	if total > MaxTotalBytes {
		return ErrRequest
	}
	return nil
}
func (r Supply) Names() []string {
	names := make([]string, 0, len(r.Values))
	for name := range r.Values {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func ParseSupply(raw []byte) (Supply, error) {
	var r Supply
	fields, err := jsonwire.Object(raw, MaxRequestBytes)
	if err != nil || jsonwire.Fields(fields, []string{"attempt_id", "readiness_revision", "expires_at", "values"}, nil) != nil || json.Unmarshal(raw, &r) != nil || r.Validate() != nil {
		r.Clear()
		return Supply{}, ErrRequest
	}
	// encoding/json's []byte decoder tolerates base64 line breaks. The wire
	// contract uses canonical standard base64, also enforced by the receiver.
	var wire struct {
		Values map[string]string `json:"values"`
	}
	if json.Unmarshal(raw, &wire) != nil {
		r.Clear()
		return Supply{}, ErrRequest
	}
	for name, encoded := range wire.Values {
		if base64.StdEncoding.EncodeToString(r.Values[name]) != encoded {
			r.Clear()
			return Supply{}, ErrRequest
		}
	}
	return r, nil
}

type Metadata struct {
	AttemptID         domain.AttemptID    `json:"attempt_id"`
	ReadinessRevision domain.SHA256Digest `json:"readiness_revision"`
	ExpiresAt         string              `json:"expires_at"`
	Names             []string            `json:"names"`
}

func (m Metadata) Validate() error {
	_, ok := CanonicalTime(m.ExpiresAt)
	if !m.AttemptID.Valid() || !m.ReadinessRevision.Valid() || !ok || len(m.Names) < 1 || len(m.Names) > MaxSlots {
		return ErrRequest
	}
	for i, name := range m.Names {
		if !ValidName(name) || i > 0 && m.Names[i-1] >= name {
			return ErrRequest
		}
	}
	return nil
}

type Receipt struct {
	AuthorizationID domain.OperationID `json:"authorization_id"`
	WorkspaceID     domain.WorkspaceID `json:"workspace_id"`
	JobID           domain.JobID       `json:"job_id"`
	Metadata
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	ConsumedAt *time.Time `json:"consumed_at"`
	RevokedAt  *time.Time `json:"revoked_at"`
	Replay     bool       `json:"replay"`
}

func (r Receipt) Validate() error {
	if !r.AuthorizationID.Valid() || !r.WorkspaceID.Valid() || !r.JobID.Valid() || r.Metadata.Validate() != nil || r.CreatedAt.IsZero() {
		return ErrRequest
	}
	if r.Status != "granted" && r.Status != "consumed" && r.Status != "revoked" && r.Status != "expired" {
		return ErrRequest
	}
	if (r.Status == "consumed") != (r.ConsumedAt != nil) || (r.Status == "revoked") != (r.RevokedAt != nil) || r.ConsumedAt != nil && r.ConsumedAt.Before(r.CreatedAt) || r.RevokedAt != nil && r.RevokedAt.Before(r.CreatedAt) {
		return ErrRequest
	}
	return nil
}
func (r Receipt) Key() Key { return Key{r.WorkspaceID, r.JobID, r.AttemptID} }

type Material struct {
	AuthorizationID domain.OperationID
	Key             Key
	ExpiresAt       string
	Values          map[string][]byte
}

func (Material) String() string   { return "[workload secret material redacted]" }
func (Material) GoString() string { return "[workload secret material redacted]" }
func (m *Material) Clear() {
	for name, value := range m.Values {
		clear(value)
		delete(m.Values, name)
	}
}
