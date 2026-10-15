// Package hostcache defines immutable SSH host content identities and the
// durable boundary around cache population. Remote observations come from a
// trusted adapter; a ledger row alone never proves that host bytes are sound.
package hostcache

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

var (
	ErrInvalid  = errors.New("invalid host cache request")
	ErrConflict = errors.New("host cache identity changed")
	ErrUnknown  = errors.New("host cache remote outcome is unknown")
)

type Key struct {
	InstallationID domain.RuntimeInstallationID
	MachineID      domain.SHA256Digest
	EngineID       domain.SHA256Digest
	SHA256         domain.SHA256Digest
	Bytes          int64
}

func (k Key) Valid() bool {
	return k.InstallationID.Valid() && k.MachineID.Valid() && k.EngineID.Valid() && k.SHA256.Valid() && k.Bytes >= 0
}

type Attempt struct {
	WorkspaceID domain.WorkspaceID
	JobID       domain.JobID
	AttemptID   domain.AttemptID
}

func (a Attempt) Valid() bool {
	return a.WorkspaceID.Valid() && a.JobID.Valid() && a.AttemptID.Valid()
}

type State string

const (
	Claimed  State = "claimed"
	Verified State = "verified"
	Retired  State = "retired"
)

type WriterPhase string

const (
	WriterIntent   WriterPhase = "intent"
	WriterBegun    WriterPhase = "begun"
	WriterFinished WriterPhase = "finished"
)

type Generation struct {
	Key         Key
	ID          string
	VolumeName  string
	OperationID domain.OperationID
	Owner       Attempt
	State       State
	WriterPhase WriterPhase
	CreatedAt   time.Time
	VerifiedAt  time.Time
	LastUsedAt  time.Time
	// Pins count all unreleased attempts for the key, including other
	// generations. Mounts count exact observed container references here.
	Pins   int64
	Mounts int64
}

func (g Generation) Valid() bool {
	return g.Key.Valid() && validGenerationID(g.ID) && validVolumeName(g.VolumeName) && g.OperationID.Valid() && g.Owner.Valid() &&
		(g.State == Claimed || g.State == Verified || g.State == Retired) &&
		(g.WriterPhase == WriterIntent || g.WriterPhase == WriterBegun || g.WriterPhase == WriterFinished) &&
		(g.State == Claimed || g.WriterPhase == WriterFinished) && !g.CreatedAt.IsZero() &&
		(g.State != Verified || !g.VerifiedAt.IsZero())
}

func validGenerationID(id string) bool {
	if len(id) != len("hcgen_")+32 || id[:len("hcgen_")] != "hcgen_" {
		return false
	}
	return lowerHex(id[len("hcgen_"):])
}

func validVolumeName(name string) bool {
	if len(name) != len("relay-cache-")+32 || name[:len("relay-cache-")] != "relay-cache-" {
		return false
	}
	return lowerHex(name[len("relay-cache-"):])
}

func lowerHex(value string) bool {
	for i := range value {
		c := value[i]
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

const LabelPrefix = "io.compute-relay."

// Labels bind a Docker volume to one generation and one one-shot writer intent.
func (g Generation) Labels() map[string]string {
	return map[string]string{
		LabelPrefix + "installation": string(g.Key.InstallationID),
		LabelPrefix + "machine":      string(g.Key.MachineID),
		LabelPrefix + "engine":       string(g.Key.EngineID),
		LabelPrefix + "kind":         "cache",
		LabelPrefix + "sha256":       string(g.Key.SHA256),
		LabelPrefix + "bytes":        strconv.FormatInt(g.Key.Bytes, 10),
		LabelPrefix + "generation":   g.ID,
		LabelPrefix + "operation":    string(g.OperationID),
	}
}

type ObservationKind string

const (
	ObservedComplete ObservationKind = "complete"
	ObservedMissing  ObservationKind = "missing"
	ObservedCorrupt  ObservationKind = "corrupt"
)

// Observation is produced after inspecting the exact named volume and the
// original writer helper. Unknown is intentionally not representable as a
// terminal observation: it cannot authorize a replacement generation.
type Observation struct {
	GenerationID   string
	VolumeName     string
	OperationID    domain.OperationID
	Key            Key
	Kind           ObservationKind
	Labels         map[string]string
	ContentSHA256  domain.SHA256Digest
	ContentBytes   int64
	WriterInactive bool
}

func (o Observation) Matches(g Generation) bool {
	if o.GenerationID != g.ID || o.VolumeName != g.VolumeName || o.OperationID != g.OperationID || o.Key != g.Key || !o.WriterInactive {
		return false
	}
	if o.Kind == ObservedMissing {
		return true
	}
	if o.Kind == ObservedComplete {
		for k, v := range g.Labels() {
			if o.Labels[k] != v {
				return false
			}
		}
		return o.ContentSHA256 == g.Key.SHA256 && o.ContentBytes == g.Key.Bytes
	}
	return o.Kind == ObservedCorrupt
}

// Repository leaves every remote mutation to the adapter. Claim commits a
// fresh, immutable intent only when no active generation exists. A false
// created result must never be interpreted as permission to run its writer.
// The claimant must call BeginWrite immediately before the one-shot host call;
// only its successful transition grants that call. FinishWrite is permitted
// only after that call is known to have terminated with no future send. An
// ambiguous SSH outcome leaves the phase begun and blocks replacement forever
// until explicit external reconciliation proves the writer cannot resume.
type Repository interface {
	Pins(context.Context, Attempt) ([]Key, error)
	Active(context.Context, Key) (*Generation, error)
	Inventory(context.Context, domain.RuntimeInstallationID, domain.SHA256Digest, domain.SHA256Digest) ([]Generation, error)
	Claim(context.Context, Attempt, Key) (Generation, bool, error)
	BeginWrite(context.Context, Attempt, string) error
	FinishWrite(context.Context, Attempt, string) error
	Verify(context.Context, Observation) error
	Retire(context.Context, Observation) error
	RecordMount(context.Context, MountReceipt) error
}

// MountReceipt is accepted only after an adapter has inspected the exact
// container mount as read-only and checked the volume's generation labels.
type MountReceipt struct {
	Attempt     Attempt
	Generation  Generation
	ContainerID string
	ReadOnly    bool
	Labels      map[string]string
}

func (m MountReceipt) Valid() bool {
	if !m.Attempt.Valid() || !m.Generation.Valid() || !m.ReadOnly || len(m.ContainerID) != 64 || !lowerHex(m.ContainerID) {
		return false
	}
	for k, v := range m.Generation.Labels() {
		if m.Labels[k] != v {
			return false
		}
	}
	return true
}
