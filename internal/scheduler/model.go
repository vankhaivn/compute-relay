// Package scheduler allocates durable LOCAL preparation ownership. A claim is not
// permission to call Provider.Submit: input readiness and write-ahead remote intent
// belong to orchestration. This package performs no network or workload execution.
package scheduler

import (
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

var (
	ErrInvalid   = errors.New("invalid scheduler record or policy")
	ErrLeaseLost = errors.New("scheduler claim is expired or no longer owned")
	ErrPhaseGate = errors.New("remote side effects require durable orchestration")
	ErrScanLimit = errors.New("scheduler snapshot exceeds safe scan bound")
)

// ScanLimit is a fail-closed ceiling, not a truncated scheduling window. Ignoring
// older possibly-active attempts would undercount account capacity.
const ScanLimit = 100000

type Settings struct {
	Paused              bool          `json:"paused"`
	MaxWorkers          int           `json:"max_workers"`
	MaxActivePerAccount int           `json:"max_active_per_account"`
	LeaseDuration       time.Duration `json:"lease_duration_ns"`
	QuotaFreshFor       time.Duration `json:"quota_fresh_for_ns"`
	BlockedRecheck      time.Duration `json:"blocked_recheck_ns"`
}

func DefaultSettings() Settings {
	return Settings{MaxWorkers: 4, MaxActivePerAccount: 1, LeaseDuration: 30 * time.Second, QuotaFreshFor: 5 * time.Minute, BlockedRecheck: 5 * time.Minute}
}
func (s Settings) Valid() bool {
	return s.MaxWorkers > 0 && s.MaxWorkers <= 64 && s.MaxActivePerAccount > 0 && s.MaxActivePerAccount <= 64 &&
		s.LeaseDuration >= time.Second && s.LeaseDuration <= 5*time.Minute && s.LeaseDuration%time.Millisecond == 0 &&
		s.QuotaFreshFor >= time.Second && s.QuotaFreshFor <= time.Hour && s.QuotaFreshFor%time.Millisecond == 0 &&
		s.BlockedRecheck >= time.Second && s.BlockedRecheck <= time.Hour && s.BlockedRecheck%time.Millisecond == 0
}

// AccountScope is the frozen operator-configured account key, not a claim of
// verified upstream identity or coordination across runtime installations.
type AccountPolicy struct {
	Disabled    bool `json:"disabled"`
	MaxActive   int  `json:"max_active"`
	StrictQuota bool `json:"strict_quota"`
}

func (p AccountPolicy) Valid() bool { return p.MaxActive > 0 && p.MaxActive <= 64 }

type Identity struct {
	WorkspaceID domain.WorkspaceID
	JobID       domain.JobID
	AttemptID   domain.AttemptID
}

func (i Identity) Valid() bool {
	return i.WorkspaceID.Valid() && i.JobID.Valid() && i.AttemptID.Valid()
}

// Fence is an unguessable ownership identity, not an API bearer token. Generation
// never decreases. Workers must present the whole identity on every mutation.
type Claim struct {
	Identity
	Sequence           int64
	Owner              string
	Generation         int64
	Fence              string
	ExpiresAt          time.Time
	AttemptRevision    uint64
	AccountScope       string
	ProviderInstanceID domain.ProviderInstanceID
	QuotaWarning       Reason
}

func (c Claim) Valid() bool {
	return c.Identity.Valid() && c.Sequence > 0 && domain.ObjectID(c.Owner).Valid() && c.Generation > 0 &&
		domain.SHA256Digest(c.Fence).Valid() && ValidTime(c.ExpiresAt) && c.AttemptRevision > 0 &&
		domain.ObjectID(c.AccountScope).Valid() && c.ProviderInstanceID.Valid()
}

// ValidTime bounds arithmetic and persisted Unix milliseconds. Lease validity is
// decided inside the store transaction, never from an HTTP timestamp.
func ValidTime(t time.Time) bool { return !t.IsZero() && t.Year() >= 1970 && t.Year() < 9999 }

type Reason string

const (
	Eligible              Reason = "eligible"
	AuthorizationRequired Reason = "execution_authorization_required"
	Paused                Reason = "scheduler_paused"
	WorkerLimit           Reason = "worker_limit"
	AccountLimit          Reason = "account_capacity"
	ResourceLimit         Reason = "resource_capacity"
	ResourceUnverified    Reason = "resource_identity_unverified"
	ResourceUnresolved    Reason = "resource_reservation_unresolved"
	AccountDisabled       Reason = "account_disabled"
	WorkspaceDisabled     Reason = "workspace_disabled"
	Deferred              Reason = "local_preparation_deferred"
	QuotaUnknown          Reason = "quota_unknown"
	QuotaStale            Reason = "quota_stale"
	QuotaUnavailable      Reason = "quota_unavailable"
	QuotaExhausted        Reason = "quota_exhausted"
	QuotaInsufficient     Reason = "quota_insufficient"
	QuotaUncertain        Reason = "quota_precision_or_units_unusable"
)

type QueueStatus struct {
	Identity
	Sequence      int64
	AccountScope  string
	ResourceScope string
	Reason        Reason
	Warning       Reason
	NotBefore     time.Time
}
type View struct {
	MaxWorkers           int
	LiveWorkers          int
	AccountReservations  map[string]int
	ResourceReservations map[string]int
	ActiveAccountLimits  map[string]int
	ActiveResourceLimits map[string]int
	UnresolvedResources  map[string]bool
	Heads                []QueueStatus
}
type Result struct {
	Claim *Claim
	View  View
}
