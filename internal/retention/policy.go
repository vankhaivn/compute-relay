// Package retention owns conservative local byte expiry and remote cleanup planning.
// A decision is not a deletion permit: durable stores must recheck it in the transaction
// that records an irreversible tombstone. No provider side effect is performed here.
package retention

import (
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

var (
	ErrInvalid = errors.New("invalid retention request")
	ErrPinned  = errors.New("retention target is pinned")
	ErrExpired = errors.New("bytes expired by retention policy")
	ErrChanged = errors.New("retention target changed")
)

const MetadataMinimum = 30 * 24 * time.Hour
const MaxReferences = 100000

type Kind string

const (
	Input  Kind = "input"
	Result Kind = "result"
)

func (k Kind) Valid() bool { return k == Input || k == Result }

type Policy struct {
	UnreferencedFor time.Duration
	CompletedFor    time.Duration
}

func DefaultPolicy() Policy {
	return Policy{UnreferencedFor: 24 * time.Hour, CompletedFor: 7 * 24 * time.Hour}
}

func (p Policy) Valid() bool {
	return p.UnreferencedFor >= time.Hour && p.UnreferencedFor <= 365*24*time.Hour &&
		p.CompletedFor >= time.Hour && p.CompletedFor <= 365*24*time.Hour
}

// Reference is trusted, transaction-local evidence, never an application attestation.
// All references must be supplied; an oversized/truncated scan fails closed.
type Reference struct {
	State            domain.AttemptState
	UpdatedAt        time.Time
	Busy             bool
	Held             bool
	RecoveryRequired bool
}

type Decision struct {
	Eligible  bool
	Reason    string
	NotBefore time.Time
}

func (p Policy) Assess(now, recordedAt time.Time, refs []Reference, held bool) (Decision, error) {
	if !p.Valid() || now.IsZero() || recordedAt.IsZero() || recordedAt.After(now) || len(refs) > MaxReferences {
		return Decision{}, ErrInvalid
	}
	if held {
		return Decision{Reason: "manual_hold"}, nil
	}
	due := recordedAt.Add(p.UnreferencedFor)
	if len(refs) > 0 {
		due = recordedAt.Add(p.CompletedFor)
	}
	for _, r := range refs {
		if r.State.Validate() != nil || r.UpdatedAt.IsZero() || r.UpdatedAt.After(now) {
			return Decision{}, ErrInvalid
		}
		if r.Held {
			return Decision{Reason: "manual_hold"}, nil
		}
		if r.Busy {
			return Decision{Reason: "local_work"}, nil
		}
		if !r.State.Orchestration.Terminal() || (r.State.RemoteActivity != domain.RemoteActivityInactive && r.State.RemoteActivity != domain.RemoteActivityNotStarted) {
			return Decision{Reason: "active_or_unresolved"}, nil
		}
		if r.RecoveryRequired || (r.State.Execution.Terminal() && r.State.Result != domain.ResultAvailable && r.State.Result != domain.ResultExpired) {
			return Decision{Reason: "recovery_required"}, nil
		}
		if t := r.UpdatedAt.Add(p.CompletedFor); t.After(due) {
			due = t
		}
	}
	if now.Before(due) {
		return Decision{Reason: "retention_window", NotBefore: due}, nil
	}
	return Decision{Eligible: true, Reason: "retention_elapsed", NotBefore: due}, nil
}

// Deletion is a committed local tombstone returned by a repository, not a host path.
// Input and result stores are distinct, explicitly supplied composition dependencies.
type Deletion struct {
	Kind   Kind
	Object domain.ObjectMetadata
}

func (d Deletion) Valid() bool { return d.Kind.Valid() && d.Object.Valid() }

type SweepReport struct {
	Examined      int
	Expired       int
	Deleted       int
	AlreadyAbsent int
	Pending       int
}
