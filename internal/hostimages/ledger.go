package hostimages

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

var ErrImageState = errors.New("application image load state is not authorizable")

type Key struct {
	InstallationID domain.RuntimeInstallationID
	MachineID      domain.SHA256Digest
	EngineID       domain.SHA256Digest
	ArchiveSHA256  domain.SHA256Digest
	ArchiveBytes   int64
}

func (k Key) Valid() bool {
	return k.InstallationID.Valid() && k.MachineID.Valid() && k.EngineID.Valid() &&
		k.ArchiveSHA256.Valid() && k.ArchiveBytes > 0 && k.ArchiveBytes <= DefaultLimits().MaxInputBytes
}

type Owner struct {
	WorkspaceID domain.WorkspaceID
	JobID       domain.JobID
	AttemptID   domain.AttemptID
	ObjectID    domain.ObjectID
}

// Pin is the frozen archive identity for one admitted attempt. It stays
// readable after release so a late observation cannot select another image.
type Pin struct {
	Key      Key
	ObjectID domain.ObjectID
	Released bool
}

func (p Pin) Valid() bool { return p.Key.Valid() && p.ObjectID.Valid() }

func (o Owner) Valid() bool {
	return o.WorkspaceID.Valid() && o.JobID.Valid() && o.AttemptID.Valid() && o.ObjectID.Valid()
}

type State string

const (
	StatePreparing State = "preparing"
	StateLoading   State = "loading"
	StateReady     State = "ready"
	StateUnknown   State = "unknown"
	StateRetired   State = "retired"
)

type Generation struct {
	ID              string
	Key             Key
	Owner           Owner
	OperationID     domain.OperationID
	ExpectedImageID string
	RootFSDiffIDs   []string
	State           State
	CreatedAt       time.Time
	LeaseUntil      time.Time
	LoadingAt       time.Time
	LastUsedAt      time.Time
}

func (g Generation) Valid() bool {
	if g.ID == "" || len(g.ID) > 128 || !g.Key.Valid() || !g.Owner.Valid() || !g.OperationID.Valid() ||
		g.CreatedAt.IsZero() || g.LeaseUntil.IsZero() {
		return false
	}
	switch g.State {
	case StatePreparing:
		return g.ExpectedImageID == "" && len(g.RootFSDiffIDs) == 0
	case StateRetired:
		return g.ExpectedImageID == "" && len(g.RootFSDiffIDs) == 0 || validImageIdentity(g.ExpectedImageID, g.RootFSDiffIDs)
	case StateLoading, StateReady, StateUnknown:
		return validImageIdentity(g.ExpectedImageID, g.RootFSDiffIDs)
	default:
		return false
	}
}

func validImageIdentity(id string, diffs []string) bool {
	if !strings.HasPrefix(id, "sha256:") || !domain.SHA256Digest(strings.TrimPrefix(id, "sha256:")).Valid() || len(diffs) < 1 || len(diffs) > 128 {
		return false
	}
	for _, diff := range diffs {
		if !strings.HasPrefix(diff, "sha256:") || !domain.SHA256Digest(strings.TrimPrefix(diff, "sha256:")).Valid() {
			return false
		}
	}
	return true
}

// Repository elects one remote writer and records its one-shot load permit.
// Observations may resolve a lost load response, but never grant a second send.
type Repository interface {
	PinnedImage(context.Context, domain.WorkspaceID, domain.JobID, domain.AttemptID) (*Pin, error)
	ClaimImage(context.Context, Owner, Key) (Generation, bool, error)
	ActiveImage(context.Context, Key) (*Generation, error)
	BeginImageLoad(context.Context, Generation, string, []string) (bool, error)
	RetirePreloadImage(context.Context, Generation) error
	MarkImageReady(context.Context, Generation) error
	MarkImageUnknown(context.Context, Generation) error
	RetireMissingImage(context.Context, Generation) error
	TouchImage(context.Context, Generation) error
}
