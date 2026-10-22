package hosthealth

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

const (
	SampleCadence    = 30 * time.Second
	DemandCache      = 15 * time.Second
	StaleAfter       = 90 * time.Second
	HistoryMaxAge    = 24 * time.Hour
	HistoryMaxRows   = 120
	MaxSnapshotBytes = 16 << 10
	RefreshTimeout   = 35 * time.Second
)

// View separates durable history from observations confirmed by this process.
// The guard evaluator must treat !Fresh as unknown, including after a restart.
type View struct {
	ResourceID      string     `json:"resource_id"`
	Latest          *Snapshot  `json:"latest"`
	History         []Snapshot `json:"history"`
	Fresh           bool       `json:"-"`
	UnknownReason   string     `json:"-"`
	ReobservedSince time.Time  `json:"-"`
	Policy          *Policy    `json:"-"`
}

type machineSample struct {
	inflight  chan struct{}
	attempted time.Time
	confirmed time.Time
	since     time.Time
	failed    bool
	digest    string
}

type Service struct {
	access   *auth.Service
	repo     Repository
	sampler  Sampler
	clock    ports.Clock
	mu       sync.Mutex
	machines map[string]*machineSample
}

func NewService(access *auth.Service, repo Repository, sampler Sampler, clock ports.Clock) (*Service, error) {
	if access == nil || repo == nil || sampler == nil || clock == nil {
		return nil, ErrUnavailable
	}
	return &Service{access: access, repo: repo, sampler: sampler, clock: clock, machines: map[string]*machineSample{}}, nil
}

// Get authorizes before resolving a private target or initiating an idle probe.
func (s *Service) Get(ctx context.Context, p auth.Principal, w domain.WorkspaceID, id string) (View, error) {
	p, err := s.access.Revalidate(ctx, p)
	if err != nil {
		return View{}, err
	}
	if err = auth.Require(p, w, auth.Read); err != nil {
		return View{}, err
	}
	target, err := s.repo.ResolveHostHealthTarget(ctx, w, p.TokenID(), id)
	if err != nil {
		return View{}, err
	}
	view, err := s.Refresh(ctx, target)
	if err != nil {
		return View{}, err
	}
	p, err = s.access.Revalidate(ctx, p)
	if err != nil {
		return View{}, err
	}
	if err := auth.Require(p, w, auth.Read); err != nil {
		return View{}, err
	}
	return view, nil
}

// Refresh is for trusted, locally resolved targets. Admission can call it after
// authorizing and checking receipt replay, before entering its SQLite transaction.
// Aliases share both the cache and the retry limit, including failed probes.
func (s *Service) Refresh(ctx context.Context, target Target) (View, error) {
	if !target.Valid() {
		return View{}, ErrUnverified
	}
	for {
		if err := ctx.Err(); err != nil {
			return View{}, err
		}
		now := s.clock.Now().UTC()
		s.mu.Lock()
		m := s.machines[target.ResourceID]
		if m == nil {
			m = &machineSample{}
			s.machines[target.ResourceID] = m
		}
		if m.inflight != nil {
			done := m.inflight
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return View{}, ctx.Err()
			case <-done:
				continue
			}
		}
		// A wall-clock rollback must not turn an earlier failed attempt into an
		// unlimited retry loop. Its cached view remains unknown until time catches up.
		if !m.attempted.IsZero() && now.Sub(m.attempted) < DemandCache {
			s.mu.Unlock()
			view, err := s.Cached(ctx, target.ResourceID)
			return withTargetPolicy(view, target), err
		}
		m.attempted = now
		m.inflight = make(chan struct{})
		for id, old := range s.machines {
			if id != target.ResourceID && old.inflight == nil && now.Sub(old.attempted) > HistoryMaxAge {
				delete(s.machines, id)
			}
		}
		s.mu.Unlock()

		probeCtx, cancel := context.WithTimeout(ctx, RefreshTimeout)
		snapshot, sampleErr := sampleHealth(probeCtx, s.sampler, target)
		probeErr := probeCtx.Err()
		cancel()
		finished := s.clock.Now().UTC()
		failed := sampleErr != nil || probeErr != nil || snapshot.Validate() != nil || snapshot.ResourceID != target.ResourceID ||
			snapshot.ObservedAt.Before(now) || snapshot.ObservedAt.After(finished)
		if failed {
			cause := "health_snapshot_invalid"
			switch {
			case errors.Is(sampleErr, context.DeadlineExceeded) || errors.Is(probeErr, context.DeadlineExceeded):
				cause = "health_collection_timeout"
			case sampleErr != nil:
				cause = provider.Reason(sampleErr, "health_collection_failed")
			case probeErr != nil:
				cause = "health_collection_timeout"
			case snapshot.Validate() != nil:
				cause = "health_snapshot_invalid"
			case snapshot.ResourceID != target.ResourceID:
				cause = "health_binding_mismatch"
			}
			snapshot = Parse(target.ResourceID, finished, nil)
			snapshot.Cause = cause
		}
		err := s.repo.RecordHostHealth(ctx, snapshot)
		s.mu.Lock()
		m.attempted = finished
		m.failed = failed || err != nil || snapshot.Platform.Status != Available || snapshot.Platform.Value == nil
		if m.failed {
			m.confirmed, m.since = time.Time{}, time.Time{}
			m.digest = ""
		} else {
			if m.since.IsZero() || snapshot.ObservedAt.Before(m.confirmed) || snapshot.ObservedAt.Sub(m.confirmed) > StaleAfter {
				m.since = snapshot.ObservedAt
			}
			m.confirmed = snapshot.ObservedAt
			m.digest = snapshotDigest(snapshot)
		}
		close(m.inflight)
		m.inflight = nil
		s.mu.Unlock()
		if err != nil {
			return View{}, err
		}
		view, err := s.Cached(ctx, target.ResourceID)
		return withTargetPolicy(view, target), err
	}
}

func sampleHealth(ctx context.Context, sampler Sampler, target Target) (snapshot Snapshot, err error) {
	defer func() {
		if recover() != nil {
			snapshot = Snapshot{}
			err = provider.WithReason(ErrUnavailable, "health_collector_panic")
		}
	}()
	return sampler.Sample(ctx, target)
}

func withTargetPolicy(view View, target Target) View {
	if target.Policy != nil {
		policy := *target.Policy
		view.Policy = &policy
	}
	return view
}

// Cached never probes. Persisted snapshots are not current evidence until this
// service has reobserved the same machine in its current process lifetime.
func (s *Service) Cached(ctx context.Context, resourceID string) (View, error) {
	if !resourcePattern.MatchString(resourceID) {
		return View{}, ErrUnverified
	}
	history, err := s.repo.ReadHostHealth(ctx, resourceID)
	if err != nil {
		return View{}, err
	}
	view := View{ResourceID: resourceID, History: history, UnknownReason: "health_not_observed"}
	if len(history) == 0 {
		return view, nil
	}
	latest := history[0]
	view.Latest = &latest
	now := s.clock.Now().UTC()
	s.mu.Lock()
	m := s.machines[resourceID]
	var confirmed time.Time
	var since time.Time
	var digest string
	failed := false
	if m != nil {
		confirmed, since, failed, digest = m.confirmed, m.since, m.failed, m.digest
		if now.Before(latest.ObservedAt) {
			m.confirmed, m.since = time.Time{}, time.Time{}
			m.digest = ""
		}
	}
	s.mu.Unlock()
	switch {
	case failed:
		view.UnknownReason = "health_observation_failed"
	case confirmed.IsZero():
		view.UnknownReason = "health_not_reobserved"
	case !latest.ObservedAt.Equal(confirmed) || snapshotDigest(latest) != digest:
		view.UnknownReason = "health_not_reobserved"
	case now.Before(latest.ObservedAt):
		view.UnknownReason = "health_clock_changed"
	case now.Sub(latest.ObservedAt) > StaleAfter:
		view.UnknownReason = "health_stale"
	case latest.Platform.Status != Available || latest.Platform.Value == nil:
		view.UnknownReason = "health_observation_failed"
	default:
		view.Fresh, view.UnknownReason = true, ""
		view.ReobservedSince = since
	}
	return view, nil
}

// RunOnce samples only verified machines with queued, preparing or running work.
// A small pool keeps one slow host from delaying every other active machine.
func (s *Service) RunOnce(ctx context.Context) error {
	targets, err := s.repo.ActiveHostHealthTargets(ctx)
	if err != nil {
		return err
	}
	work := make(chan Target)
	results := make(chan error, len(targets))
	var workers sync.WaitGroup
	for range min(4, len(targets)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for target := range work {
				_, err := s.Refresh(ctx, target)
				results <- err
			}
		}()
	}
	for _, target := range targets {
		work <- target
	}
	close(work)
	workers.Wait()
	close(results)
	var failures error
	for err := range results {
		failures = errors.Join(failures, err)
	}
	return failures
}

func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(SampleCadence)
	defer ticker.Stop()
	for {
		if err := s.RunOnce(ctx); err != nil && worker.Classify(err) == worker.Stop {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
