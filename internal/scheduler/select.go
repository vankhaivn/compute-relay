package scheduler

import (
	"sort"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// Candidate is an internal, transaction-consistent snapshot. Current profile
// remapping is intentionally absent: the account/instance came from the job's
// immutable profile revision. Lease expiry never rewrites remote observations.
type Candidate struct {
	Identity
	Sequence           int64
	State              domain.AttemptState
	ActiveAttempt      bool
	WorkspaceEnabled   bool
	AccountScope       string
	ProviderInstanceID domain.ProviderInstanceID
	Policy             AccountPolicy
	Resource           string
	WallSeconds        int64
	DispatchBarrier    bool
	LeaseUntil         time.Time
	NotBefore          time.Time
	Quota              Quota
}

// LocalOnly is deliberately narrower than "not terminal". In particular an
// unknown, cancelling, reconciling or dispatching attempt is not safe to reclaim
// for preparation, even when its worker disappeared.
func LocalOnly(s domain.AttemptState) bool {
	if s.Validate() != nil || s.DeadlineExceeded || s.Cancellation != domain.CancellationNotRequested ||
		s.Execution != domain.ExecutionNotSubmitted || s.RemoteActivity != domain.RemoteActivityNotStarted || s.Result != domain.ResultNotAvailable {
		return false
	}
	switch s.Orchestration {
	case domain.OrchestrationQueued, domain.OrchestrationPreparing, domain.OrchestrationBlocked:
		return true
	default:
		return false
	}
}

// HoldsAccount is independent from worker leases. Terminal execution evidence may
// free capacity before artifact collection finishes; local deadlines and expired
// leases never free a potentially active remote execution.
func HoldsAccount(s domain.AttemptState, barrier bool) bool {
	if s.Validate() != nil {
		return true
	}
	if s.RemoteActivity == domain.RemoteActivityInactive ||
		(s.Orchestration.Terminal() && s.Execution == domain.ExecutionNotSubmitted && s.RemoteActivity == domain.RemoteActivityNotStarted) {
		return false
	}
	if barrier {
		return true
	}
	if s.RemoteActivity != domain.RemoteActivityNotStarted {
		return true
	}
	switch s.Orchestration {
	case domain.OrchestrationDispatching, domain.OrchestrationSubmitted, domain.OrchestrationRunning,
		domain.OrchestrationCancelling, domain.OrchestrationReconciling, domain.OrchestrationNeedsAttention:
		return true
	}
	return s.Execution != domain.ExecutionNotSubmitted
}

// Select returns one index, or -1 when no head is eligible. FIFO is start order:
// already leased attempts are in progress and do not block another start. A waiting
// head (quota, account, local backoff) cannot be overtaken within its workspace.
// The caller persists the winning workspace cursor ONLY with a successful claim.
func Select(rows []Candidate, last domain.WorkspaceID, settings Settings, now time.Time) (int, View, error) {
	view := View{MaxWorkers: settings.MaxWorkers, AccountReservations: map[string]int{}, Heads: []QueueStatus{}}
	if !settings.Valid() || !ValidTime(now) || (last != "" && !last.Valid()) {
		return -1, View{}, ErrInvalid
	}
	if len(rows) > ScanLimit {
		return -1, View{}, ErrScanLimit
	}
	heads := map[domain.WorkspaceID]int{}
	seen := map[int64]bool{}
	for i, r := range rows {
		if !r.Identity.Valid() || r.Sequence <= 0 || seen[r.Sequence] || r.State.Validate() != nil || !domain.ObjectID(r.AccountScope).Valid() ||
			!r.ProviderInstanceID.Valid() || !r.Policy.Valid() || (r.Resource != "cpu" && r.Resource != "gpu") || r.WallSeconds < 1 || r.WallSeconds > 86400 ||
			(!r.LeaseUntil.IsZero() && !ValidTime(r.LeaseUntil)) || (!r.NotBefore.IsZero() && !ValidTime(r.NotBefore)) {
			return -1, View{}, ErrInvalid
		}
		seen[r.Sequence] = true
		live := r.LeaseUntil.After(now)
		if live {
			view.LiveWorkers++
		}
		if (live && LocalOnly(r.State)) || HoldsAccount(r.State, r.DispatchBarrier) {
			view.AccountReservations[r.AccountScope]++
		}
		if !r.ActiveAttempt || live || r.DispatchBarrier || !LocalOnly(r.State) {
			continue
		}
		if prior, ok := heads[r.WorkspaceID]; !ok || r.Sequence < rows[prior].Sequence {
			heads[r.WorkspaceID] = i
		}
	}
	spaces := make([]domain.WorkspaceID, 0, len(heads))
	for w := range heads {
		spaces = append(spaces, w)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i] < spaces[j] })
	eligible := map[domain.WorkspaceID]bool{}
	for _, w := range spaces {
		r := rows[heads[w]]
		status := QueueStatus{Identity: r.Identity, Sequence: r.Sequence, AccountScope: r.AccountScope, Reason: Eligible, NotBefore: r.NotBefore}
		switch {
		case settings.Paused:
			status.Reason = Paused
		case !r.WorkspaceEnabled:
			status.Reason = WorkspaceDisabled
		case r.Policy.Disabled:
			status.Reason = AccountDisabled
		case r.NotBefore.After(now):
			status.Reason = Deferred
		case view.LiveWorkers >= settings.MaxWorkers:
			status.Reason = WorkerLimit
		case view.AccountReservations[r.AccountScope] >= min(settings.MaxActivePerAccount, r.Policy.MaxActive):
			status.Reason = AccountLimit
		default:
			status.Reason, status.Warning = r.Quota.Decide(r.Resource, r.WallSeconds, r.Policy.StrictQuota, settings.QuotaFreshFor, now)
		}
		view.Heads = append(view.Heads, status)
		eligible[w] = status.Reason == Eligible
	}
	for _, w := range spaces {
		if w > last && eligible[w] {
			return heads[w], view, nil
		}
	}
	for _, w := range spaces {
		if eligible[w] {
			return heads[w], view, nil
		}
	}
	return -1, view, nil
}
