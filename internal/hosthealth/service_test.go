package hosthealth_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type serviceClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *serviceClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *serviceClock) advance(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

type serviceRepository struct {
	mu         sync.Mutex
	targets    map[string]hosthealth.Target
	active     []hosthealth.Target
	history    map[string][]hosthealth.Snapshot
	resolves   int
	writes     int
	fail       error
	activeErr  error
	activeRead chan struct{}
}

func (r *serviceRepository) ResolveHostHealthTarget(_ context.Context, w domain.WorkspaceID, _ string, id string) (hosthealth.Target, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolves++
	target, ok := r.targets[id]
	if !ok || target.Workspace != w {
		return hosthealth.Target{}, hosthealth.ErrUnverified
	}
	return target, nil
}
func (r *serviceRepository) ActiveHostHealthTargets(context.Context) ([]hosthealth.Target, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeRead != nil {
		select {
		case r.activeRead <- struct{}{}:
		default:
		}
	}
	return append([]hosthealth.Target{}, r.active...), r.activeErr
}
func (r *serviceRepository) ReadHostHealth(_ context.Context, id string) ([]hosthealth.Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw, _ := json.Marshal(r.history[id])
	out := []hosthealth.Snapshot{}
	_ = json.Unmarshal(raw, &out)
	return out, nil
}
func (r *serviceRepository) RecordHostHealth(_ context.Context, snapshot hosthealth.Snapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return r.fail
	}
	r.writes++
	r.history[snapshot.ResourceID] = append([]hosthealth.Snapshot{snapshot}, r.history[snapshot.ResourceID]...)
	return nil
}

type serviceFixture struct {
	service  *hosthealth.Service
	repo     *serviceRepository
	clock    *serviceClock
	access   *auth.Service
	actor    auth.Principal
	tokenID  string
	mu       sync.Mutex
	calls    int
	sample   func(context.Context, hosthealth.Target) (hosthealth.Snapshot, error)
	lan, vpn hosthealth.Target
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	ctx := context.Background()
	f := &serviceFixture{clock: &serviceClock{now: time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)}}
	s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "state"), sqlite.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err = s.PutWorkspace(ctx, auth.Workspace{ID: "a", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	f.access, err = auth.New(s, s)
	if err != nil {
		t.Fatal(err)
	}
	secret, token, err := f.access.Issue(ctx, "a", []auth.Scope{auth.Read}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	f.tokenID = token.ID
	f.actor, err = f.access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	f.lan = hosthealth.Target{Workspace: "a", ConnectionID: "con_lan", ProviderType: "ssh", CanonicalAccount: "private_account_lan", CredentialRef: "vault:con_lan", ResourceID: "resource_" + strings.Repeat("a", 64)}
	f.vpn = f.lan
	f.vpn.ConnectionID, f.vpn.CanonicalAccount, f.vpn.CredentialRef = "con_vpn", "private_account_vpn", "vault:con_vpn"
	f.repo = &serviceRepository{targets: map[string]hosthealth.Target{f.lan.ConnectionID: f.lan, f.vpn.ConnectionID: f.vpn}, history: map[string][]hosthealth.Snapshot{}}
	f.sample = func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		return healthSample(target.ResourceID, f.clock.Now(), "Darwin x86_64"), nil
	}
	f.service = f.newService(t)
	return f
}

func healthSample(id string, now time.Time, platform string) hosthealth.Snapshot {
	return hosthealth.Parse(id, now, map[hosthealth.Source]hosthealth.Sample{
		hosthealth.Platform:           {Status: hosthealth.Available, Data: []byte(platform)},
		hosthealth.NativeArchitecture: {Status: hosthealth.Available, Data: []byte("7\n")},
		hosthealth.Sysctl:             {Status: hosthealth.Available, Data: []byte("hw.ncpu: 16\nhw.memsize: 68719476736\nvm.loadavg: { 1.0 1.0 1.0 }\nmachdep.xcpm.cpu_thermal_level: 0\n")},
	})
}

func (f *serviceFixture) newService(t *testing.T) *hosthealth.Service {
	t.Helper()
	sampler := hosthealth.SamplerFunc(func(ctx context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		f.mu.Lock()
		f.calls++
		sample := f.sample
		f.mu.Unlock()
		return sample(ctx, target)
	})
	service, err := hosthealth.NewService(f.access, f.repo, sampler, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestHealthDemandSharesMachineCacheAndRefreshesAfterTTL(t *testing.T) {
	f := newServiceFixture(t)
	first, err := f.service.Get(context.Background(), f.actor, "a", f.lan.ConnectionID)
	if err != nil || !first.Fresh || first.Latest == nil || len(first.History) != 1 {
		t.Fatal(first, err)
	}
	f.clock.advance(hosthealth.DemandCache - time.Second)
	second, err := f.service.Get(context.Background(), f.actor, "a", f.vpn.ConnectionID)
	if err != nil || !second.Fresh || f.calls != 1 || len(second.History) != 1 {
		t.Fatal(second, err, f.calls)
	}
	// Returned snapshot pointers cannot change the persisted history or cache.
	*second.Latest.Platform.Value = "unsupported"
	f.clock.advance(time.Second)
	third, err := f.service.Refresh(context.Background(), f.vpn)
	if err != nil || !third.Fresh || f.calls != 2 || len(third.History) != 2 || *third.History[1].Platform.Value != "macos_intel" {
		t.Fatal(third, err, f.calls)
	}
}

func TestHealthConcurrentAliasesShareOneInFlightProbe(t *testing.T) {
	f := newServiceFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	f.sample = func(ctx context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return hosthealth.Snapshot{}, ctx.Err()
		case <-release:
		}
		return healthSample(target.ResourceID, f.clock.Now(), "Darwin x86_64"), nil
	}
	done := make(chan error, 2)
	go func() { _, err := f.service.Refresh(context.Background(), f.lan); done <- err }()
	<-entered
	go func() { _, err := f.service.Refresh(context.Background(), f.vpn); done <- err }()
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if f.calls != 1 || f.repo.writes != 1 {
		t.Fatal("aliases duplicated remote observation", f.calls, f.repo.writes)
	}
}

func TestHealthFailedProbeIsUnknownAndRateLimitedAcrossAliases(t *testing.T) {
	f := newServiceFixture(t)
	f.sample = func(context.Context, hosthealth.Target) (hosthealth.Snapshot, error) {
		return hosthealth.Snapshot{}, errors.New("private remote text")
	}
	for _, target := range []hosthealth.Target{f.lan, f.vpn, f.lan} {
		view, err := f.service.Refresh(context.Background(), target)
		if err != nil || view.Fresh || view.UnknownReason != "health_observation_failed" || view.Latest == nil || view.Latest.Platform.Value != nil {
			t.Fatal(view, err)
		}
	}
	if f.calls != 1 {
		t.Fatal("failed probes bypassed the rate limit", f.calls)
	}
	f.clock.advance(hosthealth.DemandCache)
	f.sample = func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		return healthSample(target.ResourceID, f.clock.Now(), "Darwin x86_64"), nil
	}
	view, err := f.service.Refresh(context.Background(), f.vpn)
	if err != nil || !view.Fresh || f.calls != 2 || len(view.History) != 2 {
		t.Fatal(view, err, f.calls)
	}
}

func TestHealthRestartReobservesAndStaleOrFailedUnsupportedStaysUnknown(t *testing.T) {
	f := newServiceFixture(t)
	f.sample = func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		return healthSample(target.ResourceID, f.clock.Now(), "Linux x86_64"), nil
	}
	view, err := f.service.Refresh(context.Background(), f.lan)
	if err != nil || !view.Fresh || *view.Latest.Platform.Value != "unsupported" {
		t.Fatal(view, err)
	}
	restarted := f.newService(t)
	view, err = restarted.Cached(context.Background(), f.lan.ResourceID)
	if err != nil || view.Fresh || view.UnknownReason != "health_not_reobserved" || view.Latest == nil {
		t.Fatal(view, err)
	}
	view, err = restarted.Refresh(context.Background(), f.lan)
	if err != nil || !view.Fresh || f.calls != 2 {
		t.Fatal(view, err, f.calls)
	}
	f.clock.advance(hosthealth.StaleAfter + time.Second)
	view, err = restarted.Cached(context.Background(), f.lan.ResourceID)
	if err != nil || view.Fresh || view.UnknownReason != "health_stale" {
		t.Fatal(view, err)
	}
	f.sample = func(context.Context, hosthealth.Target) (hosthealth.Snapshot, error) {
		return hosthealth.Snapshot{}, errors.New("SSH unavailable")
	}
	view, err = restarted.Refresh(context.Background(), f.vpn)
	if err != nil || view.Fresh || view.Latest.Platform.Value != nil || view.UnknownReason != "health_observation_failed" {
		t.Fatal(view, err)
	}
}

func TestHealthWrongMachineFutureOrMissingPlatformNeverBecomeFresh(t *testing.T) {
	for _, mode := range []string{"wrong-machine", "future", "old", "missing-platform"} {
		t.Run(mode, func(t *testing.T) {
			f := newServiceFixture(t)
			f.sample = func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
				snapshot := healthSample(target.ResourceID, f.clock.Now(), "Darwin x86_64")
				switch mode {
				case "wrong-machine":
					snapshot.ResourceID = "resource_" + strings.Repeat("b", 64)
				case "future":
					snapshot.ObservedAt = snapshot.ObservedAt.Add(time.Second)
				case "old":
					snapshot.ObservedAt = snapshot.ObservedAt.Add(-time.Second)
				case "missing-platform":
					snapshot = hosthealth.Parse(target.ResourceID, f.clock.Now(), nil)
				}
				return snapshot, nil
			}
			view, err := f.service.Refresh(context.Background(), f.lan)
			if err != nil || view.Fresh || view.UnknownReason != "health_observation_failed" {
				t.Fatal(view, err)
			}
		})
	}
}

func TestHealthAuthorizationPrecedesTargetLookupAndProbe(t *testing.T) {
	f := newServiceFixture(t)
	if _, err := f.service.Get(context.Background(), f.actor, "b", f.lan.ConnectionID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal(err)
	}
	if err := f.access.Revoke(context.Background(), f.tokenID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Get(context.Background(), f.actor, "a", f.lan.ConnectionID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal(err)
	}
	if f.repo.resolves != 0 || f.calls != 0 {
		t.Fatal("unauthorized read touched private target or host")
	}
}

func TestHealthPeriodicSamplingHasNoIdleProbesAndUsesDemandCache(t *testing.T) {
	f := newServiceFixture(t)
	if err := f.service.RunOnce(context.Background()); err != nil || f.calls != 0 {
		t.Fatal(err, f.calls)
	}
	f.repo.active = []hosthealth.Target{f.lan, f.vpn}
	if err := f.service.RunOnce(context.Background()); err != nil || f.calls != 1 {
		t.Fatal(err, f.calls)
	}
	f.clock.advance(hosthealth.SampleCadence)
	if err := f.service.RunOnce(context.Background()); err != nil || f.calls != 2 {
		t.Fatal(err, f.calls)
	}
	f.repo.active = nil
	f.clock.advance(hosthealth.SampleCadence)
	if err := f.service.RunOnce(context.Background()); err != nil || f.calls != 2 {
		t.Fatal(err, f.calls)
	}
}

func TestHealthSlowProbeKeepsItsCacheForTheFullTTLAfterCompletion(t *testing.T) {
	f := newServiceFixture(t)
	f.sample = func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		f.clock.advance(20 * time.Second)
		return healthSample(target.ResourceID, f.clock.Now(), "Darwin x86_64"), nil
	}
	if view, err := f.service.Refresh(context.Background(), f.lan); err != nil || !view.Fresh {
		t.Fatal(view, err)
	}
	f.clock.advance(hosthealth.DemandCache - time.Second)
	if view, err := f.service.Refresh(context.Background(), f.vpn); err != nil || !view.Fresh || f.calls != 1 {
		t.Fatal(view, err, f.calls)
	}
}

func TestHealthContinuousObservationWindowRestartsAfterFailureGapRestartOrClockRollback(t *testing.T) {
	f := newServiceFixture(t)
	start := f.clock.Now()
	view, err := f.service.Refresh(context.Background(), f.lan)
	if err != nil || !view.ReobservedSince.Equal(start) {
		t.Fatal(view, err)
	}
	f.clock.advance(hosthealth.SampleCadence)
	view, err = f.service.Refresh(context.Background(), f.vpn)
	if err != nil || !view.ReobservedSince.Equal(start) {
		t.Fatal("healthy cadence lost process window", view, err)
	}
	f.clock.advance(hosthealth.SampleCadence)
	f.sample = func(context.Context, hosthealth.Target) (hosthealth.Snapshot, error) {
		return hosthealth.Snapshot{}, errors.New("SSH unavailable")
	}
	view, err = f.service.Refresh(context.Background(), f.lan)
	if err != nil || !view.ReobservedSince.IsZero() {
		t.Fatal("failed observation retained a sustained window", view, err)
	}
	f.clock.advance(hosthealth.DemandCache)
	f.sample = func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		return healthSample(target.ResourceID, f.clock.Now(), "Darwin x86_64"), nil
	}
	view, err = f.service.Refresh(context.Background(), f.lan)
	if err != nil || !view.ReobservedSince.Equal(f.clock.Now()) {
		t.Fatal(view, err)
	}
	f.clock.advance(hosthealth.StaleAfter + time.Second)
	view, err = f.service.Refresh(context.Background(), f.lan)
	if err != nil || !view.ReobservedSince.Equal(f.clock.Now()) {
		t.Fatal("stale gap counted prior observations", view, err)
	}
	restarted := f.newService(t)
	view, err = restarted.Refresh(context.Background(), f.vpn)
	if err != nil || !view.ReobservedSince.Equal(f.clock.Now()) {
		t.Fatal("restart counted historical observations", view, err)
	}
	f.clock.advance(-time.Second)
	view, err = restarted.Cached(context.Background(), f.lan.ResourceID)
	if err != nil || view.Fresh || !view.ReobservedSince.IsZero() {
		t.Fatal("clock rollback retained a sustained window", view, err)
	}
	f.clock.advance(time.Second)
	view, err = restarted.Cached(context.Background(), f.lan.ResourceID)
	if err != nil || view.Fresh {
		t.Fatal("clock recovery inferred a current observation", view, err)
	}
}

func TestHealthGetRechecksRevocationAfterRemoteObservation(t *testing.T) {
	f := newServiceFixture(t)
	f.sample = func(ctx context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		if err := f.access.Revoke(ctx, f.tokenID); err != nil {
			t.Fatal(err)
		}
		return healthSample(target.ResourceID, f.clock.Now(), "Darwin x86_64"), nil
	}
	if _, err := f.service.Get(context.Background(), f.actor, "a", f.lan.ConnectionID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revoked authority received the new snapshot", err)
	}
}

func TestHealthWorkerRetainsTransientStateFailuresAndStopsOnCorruption(t *testing.T) {
	for _, failure := range []error{sqlite.ErrBusy, context.DeadlineExceeded, sqlite.ErrCorrupt} {
		t.Run(failure.Error(), func(t *testing.T) {
			f := newServiceFixture(t)
			f.repo.activeErr, f.repo.activeRead = failure, make(chan struct{}, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- f.service.Run(ctx) }()
			<-f.repo.activeRead
			if errors.Is(failure, sqlite.ErrCorrupt) {
				select {
				case err := <-done:
					if !errors.Is(err, failure) {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("corrupt state did not stop health worker")
				}
				return
			}
			select {
			case err := <-done:
				t.Fatal("transient state failure stopped health worker", err)
			case <-time.After(10 * time.Millisecond):
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("health worker did not join cancellation")
			}
		})
	}
}

var _ ports.Clock = (*serviceClock)(nil)
