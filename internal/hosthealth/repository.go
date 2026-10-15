package hosthealth

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

var (
	ErrUnavailable = errors.New("host health unavailable")
	ErrUnverified  = errors.New("host health requires a verified machine")
	ErrUnsupported = errors.New("host health is unsupported for this provider")
)

// Target is private verified connection evidence, never an HTTP projection.
// RuntimeConfig may be absent when the execution image has not been installed.
type Target struct {
	Workspace        domain.WorkspaceID  `json:"-"`
	ConnectionID     string              `json:"-"`
	ProviderType     string              `json:"-"`
	CanonicalAccount string              `json:"-"`
	CredentialRef    ports.CredentialRef `json:"-"`
	ResourceID       string              `json:"-"`
	RuntimeConfig    []byte              `json:"-"`
}

func (Target) String() string   { return "[host health target redacted]" }
func (Target) GoString() string { return "[host health target redacted]" }

func (t Target) Valid() bool {
	return t.Workspace.Valid() && domain.ObjectID(t.ConnectionID).Valid() &&
		domain.ObjectID(t.ProviderType).Valid() && domain.ObjectID(t.CanonicalAccount).Valid() &&
		t.CredentialRef == ports.CredentialRef("vault:"+t.ConnectionID) && resourcePattern.MatchString(t.ResourceID) && len(t.RuntimeConfig) <= 8192
}

// Repository performs only local state operations. Resolving a target checks the
// current actor and workspace before any credentials or remote host are touched.
type Repository interface {
	ResolveHostHealthTarget(context.Context, domain.WorkspaceID, string, string) (Target, error)
	ActiveHostHealthTargets(context.Context) ([]Target, error)
	ReadHostHealth(context.Context, string) ([]Snapshot, error)
	RecordHostHealth(context.Context, Snapshot) error
}

type Sampler interface {
	Sample(context.Context, Target) (Snapshot, error)
}

type SamplerFunc func(context.Context, Target) (Snapshot, error)

func (f SamplerFunc) Sample(ctx context.Context, target Target) (Snapshot, error) {
	return f(ctx, target)
}
