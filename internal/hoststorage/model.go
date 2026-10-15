// Package hoststorage defines the bounded machine-scoped accounting and exact
// storage targets used by SSH Docker hosts. Provider observation stays in its
// adapter and authorization stays in the store and HTTP service.
package hoststorage

import (
	"context"
	"errors"
	"math"
	"regexp"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

const (
	DefaultSoftBytes      int64 = 64 << 30
	DefaultHardBytes      int64 = 80 << 30
	MaximumHardBytes      int64 = 1 << 50
	AttemptAllowanceBytes int64 = 4 << 30
	ScanMaxObjects              = 1024
	ScanMaxBytes                = 4 << 20
	ScanTimeout                 = 30 * time.Second
	ScanFreshness               = 2 * time.Minute
)

var (
	ErrInvalid    = errors.New("invalid host storage identity or policy")
	ErrUnknown    = errors.New("host storage observation is unknown")
	ErrHardLimit  = errors.New("host storage hard limit reached")
	ErrUnverified = errors.New("host storage target is unverified")
	ErrIneligible = errors.New("host storage target remains referenced")
)

// Budget is non-secret and frozen with a managed connection revision.
type Budget struct {
	SoftBytes int64 `json:"soft_bytes"`
	HardBytes int64 `json:"hard_bytes"`
}

func DefaultBudget() Budget { return Budget{DefaultSoftBytes, DefaultHardBytes} }

func (b Budget) Valid() bool {
	return b.SoftBytes > 0 && b.HardBytes > b.SoftBytes && b.HardBytes <= MaximumHardBytes
}

func MinBudget(a, b Budget) Budget {
	return Budget{SoftBytes: min(a.SoftBytes, b.SoftBytes), HardBytes: min(a.HardBytes, b.HardBytes)}
}

type Scope struct {
	InstallationID domain.RuntimeInstallationID
	MachineID      domain.SHA256Digest
	EngineID       domain.SHA256Digest
}

func (s Scope) Valid() bool {
	return s.InstallationID.Valid() && s.MachineID.Valid() && s.EngineID.Valid()
}

type Kind string

const (
	Container Kind = "container"
	Volume    Kind = "volume"
	Image     Kind = "image"
)

func (k Kind) Valid() bool { return k == Container || k == Volume || k == Image }

// Object is private provider evidence. ID and Labels must never be serialized
// into a public storage projection, including for foreign installations.
type Object struct {
	Kind       Kind
	ID         string
	SizeBytes  int64
	SizeKnown  bool
	Labels     map[string]string
	Running    bool
	References int64
}

var fullContainerID = regexp.MustCompile(`^[0-9a-f]{64}$`)
var fullImageID = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var volumeID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}$`)

func (o Object) Valid() bool {
	if !o.Kind.Valid() || o.SizeBytes < 0 || o.References < 0 || len(o.Labels) > 32 {
		return false
	}
	switch o.Kind {
	case Container:
		return fullContainerID.MatchString(o.ID)
	case Image:
		return fullImageID.MatchString(o.ID)
	case Volume:
		return volumeID.MatchString(o.ID)
	}
	return false
}

type Foreign struct {
	Containers int64 `json:"containers"`
	Volumes    int64 `json:"volumes"`
	Images     int64 `json:"images"`
}

func (f Foreign) Valid() bool { return f.Containers >= 0 && f.Volumes >= 0 && f.Images >= 0 }

// Scan is a complete, bounded cohort observation. Failure or truncation is an
// error, never a partial successful scan from which absence may be inferred.
type Scan struct {
	Scope      Scope
	ObservedAt time.Time
	Objects    []Object
	Foreign    Foreign
}

func (s Scan) Valid() bool {
	if !s.Scope.Valid() || s.ObservedAt.IsZero() || !s.Foreign.Valid() || len(s.Objects) > ScanMaxObjects*3 {
		return false
	}
	seen := map[string]bool{}
	for _, object := range s.Objects {
		if !object.Valid() || object.Labels["io.compute-relay.installation"] != string(s.Scope.InstallationID) {
			return false
		}
		key := string(object.Kind) + "\x00" + object.ID
		if seen[key] {
			return false
		}
		seen[key] = true
	}
	return true
}

type Observer interface {
	Scan(context.Context, Scope) (Scan, error)
}

// Charge adds one conservative object charge per physical identity. A
// reservation and its observation are alternatives, not additive duplicates.
func Charge(observed, reserved int64) (int64, error) {
	if observed < 0 || reserved < 0 {
		return 0, ErrInvalid
	}
	return max(observed, reserved), nil
}

func AddChecked(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, ErrInvalid
	}
	return a + b, nil
}
