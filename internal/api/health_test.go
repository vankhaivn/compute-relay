package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type healthHTTPClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *healthHTTPClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *healthHTTPClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type healthHTTPRepository struct {
	mu      sync.Mutex
	targets map[string]hosthealth.Target
	history map[string][]hosthealth.Snapshot
	readErr error
}

func (r *healthHTTPRepository) ResolveHostHealthTarget(_ context.Context, w domain.WorkspaceID, _ string, id string) (hosthealth.Target, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == "con_kaggle" && w == "a" {
		return hosthealth.Target{}, hosthealth.ErrUnsupported
	}
	if id == "con_pending" && w == "a" {
		return hosthealth.Target{}, hosthealth.ErrUnverified
	}
	t, ok := r.targets[string(w)+"/"+id]
	if !ok {
		return hosthealth.Target{}, connections.ErrNotFound
	}
	return t, nil
}
func (r *healthHTTPRepository) ActiveHostHealthTargets(context.Context) ([]hosthealth.Target, error) {
	return nil, nil
}
func (r *healthHTTPRepository) RecordHostHealth(_ context.Context, s hosthealth.Snapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.history[s.ResourceID] = append([]hosthealth.Snapshot{s}, r.history[s.ResourceID]...)
	return nil
}
func (r *healthHTTPRepository) ReadHostHealth(_ context.Context, id string) ([]hosthealth.Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readErr != nil {
		return nil, r.readErr
	}
	raw, _ := json.Marshal(r.history[id])
	var copy []hosthealth.Snapshot
	if err := json.Unmarshal(raw, &copy); err != nil {
		return nil, err
	}
	return copy, nil
}

type healthHTTP struct {
	*jobHTTP
	clock   *healthHTTPClock
	repo    *healthHTTPRepository
	service *hosthealth.Service
	access  *auth.Service
	calls   atomic.Int32
	mu      sync.Mutex
	sample  func(context.Context, hosthealth.Target) (hosthealth.Snapshot, error)
}

func healthHTTPSample(target hosthealth.Target, at time.Time, platform string) hosthealth.Snapshot {
	return hosthealth.Parse(target.ResourceID, at, map[hosthealth.Source]hosthealth.Sample{
		hosthealth.Platform: {Status: hosthealth.Available, Data: []byte(platform)}, hosthealth.NativeArchitecture: {Status: hosthealth.Available, Data: []byte("7\n")},
		hosthealth.ThermalState: {Status: hosthealth.Available, Data: []byte("0\n")},
		hosthealth.Sysctl:       {Status: hosthealth.Available, Data: []byte("hw.ncpu: 16\nhw.memsize: 68719476736\nvm.loadavg: { 1.0 1.0 1.0 }\n")},
		hosthealth.Thermal:      {Status: hosthealth.Available, Data: []byte("CPU_Speed_Limit = 100\n")}, hosthealth.Power: {Status: hosthealth.Available, Data: []byte("Now drawing from 'AC Power'\n")},
	})
}
func setupHealthHTTP(t *testing.T) *healthHTTP {
	t.Helper()
	f := &healthHTTP{clock: &healthHTTPClock{now: time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)}}
	p := hosthealth.DefaultPolicy()
	vpn := p
	vpn.LowBatteryPercent = 20
	id := "resource_" + strings.Repeat("a", 64)
	target := func(connection string, policy *hosthealth.Policy) hosthealth.Target {
		return hosthealth.Target{Workspace: "a", ConnectionID: connection, ProviderType: "ssh", CanonicalAccount: "private-account-canary", CredentialRef: ports.CredentialRef("vault:" + connection), ResourceID: id, Policy: policy, RuntimeConfig: []byte(`{"private_runtime_canary":"never_publish"}`)}
	}
	f.repo = &healthHTTPRepository{targets: map[string]hosthealth.Target{"a/con_lan": target("con_lan", &p), "a/con_vpn": target("con_vpn", &vpn)}, history: map[string][]hosthealth.Snapshot{}}
	f.sample = func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		return healthHTTPSample(target, f.clock.Now(), "Darwin x86_64"), nil
	}
	f.jobHTTP = setupJobHTTPWith(t, jobHTTPOptions{extraScopes: map[string][]auth.Scope{"manage": {auth.Manage}}, configure: func(cfg *api.Config, access *auth.Service, _ *sqlite.Store) {
		f.access = access
		var err error
		f.service, err = hosthealth.NewService(access, f.repo, hosthealth.SamplerFunc(func(ctx context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
			f.calls.Add(1)
			f.mu.Lock()
			sample := f.sample
			f.mu.Unlock()
			return sample(ctx, target)
		}), f.clock)
		requireNoError(t, err)
		cfg.Health = f.service
		cfg.Now = f.clock.Now
	}})
	return f
}
func (f *healthHTTP) setSample(sample func(context.Context, hosthealth.Target) (hosthealth.Snapshot, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sample = sample
}
func healthResponse(t *testing.T, raw []byte) api.ConnectionHealth {
	t.Helper()
	wireSchema(t, "connection-health", raw)
	managedSafeResponse(t, raw)
	for _, private := range []string{"private_runtime_canary", "never_publish", "runtime_config", "canonical_account", "credential_ref", "ReobservedSince", "reobserved_since", "unknown_reason", "Fresh", "fresh"} {
		if strings.Contains(string(raw), private) {
			t.Fatal("private health state leaked", private)
		}
	}
	return decodeHTTPJSON[api.ConnectionHealth](t, raw)
}

const healthPath = "/v1/workspaces/a/connections/con_lan/health"

func TestConnectionHealthHTTPReadAuthorityAndErrors(t *testing.T) {
	f := setupHealthHTTP(t)
	for _, tt := range []struct {
		method, path, who string
		status            int
	}{{"GET", healthPath, "", 401}, {"GET", healthPath, "b", 403}, {"GET", healthPath, "manage", 403}, {"GET", "/v1/workspaces/b/connections/con_lan/health", "b", 404}, {"GET", "/v1/workspaces/a/connections/con_missing/health", "read", 404}, {"GET", "/v1/workspaces/a/connections/con_pending/health", "read", 409}, {"GET", "/v1/workspaces/a/connections/con_kaggle/health", "read", 400}, {"GET", "/v1/workspaces/a/connections/!bad/health", "read", 400}, {"POST", healthPath, "read", 405}, {"GET", healthPath + "?refresh=true", "read", 400}} {
		t.Run(tt.method+tt.path+tt.who, func(t *testing.T) {
			_, raw := f.call(t, tt.method, tt.path, tt.who, "", "", tt.status)
			managedSafeResponse(t, raw)
			if strings.Contains(tt.path, "con_kaggle") && !strings.Contains(string(raw), "UNSUPPORTED_CAPABILITY") {
				t.Fatal("wrong unsupported code", string(raw))
			}
		})
	}
	r := httptest.NewRequest("HEAD", f.url+healthPath, nil)
	r.Header.Set("Authorization", "Bearer "+f.tokens["read"])
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, r)
	if recorder.Code != 405 || recorder.Header().Get("Allow") != "GET" {
		t.Fatal("HEAD invoked health reader")
	}
	if f.calls.Load() != 0 {
		t.Fatal("rejected read probed host")
	}
	_, raw := f.call(t, "GET", healthPath, "read", "", "", 200)
	response := healthResponse(t, raw)
	if response.Guard.Level != hosthealth.LevelOK || response.Guard.RefuseNew || response.Guard.Stop || len(response.History) != 1 || response.Latest == nil {
		t.Fatal(response)
	}
}
func TestConnectionHealthHTTPAliasesShareSnapshotsAndKeepEffectivePolicy(t *testing.T) {
	f := setupHealthHTTP(t)
	f.setSample(func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		s := healthHTTPSample(target, f.clock.Now(), "Darwin x86_64")
		source, charge := "battery", 15.0
		s.PowerSource = hosthealth.Metric[string]{Status: hosthealth.Available, Value: &source}
		s.BatteryChargePercent = hosthealth.Metric[float64]{Status: hosthealth.Available, Value: &charge}
		return s, nil
	})
	_, raw := f.call(t, "GET", healthPath, "read", "", "", 200)
	first := healthResponse(t, raw)
	_, raw = f.call(t, "GET", "/v1/workspaces/a/connections/con_vpn/health", "read", "", "", 200)
	second := healthResponse(t, raw)
	if f.calls.Load() != 1 || first.ResourceID != second.ResourceID || len(first.History) != 1 || len(second.History) != 1 || first.Policy.LowBatteryPercent != 10 || second.Policy.LowBatteryPercent != 20 || first.Guard.Level != hosthealth.LevelOK || second.Guard.Level != hosthealth.LevelCritical || !second.Guard.Stop {
		t.Fatal("alias metric/policy state mixed")
	}
}
func TestConnectionHealthHTTPUnsupportedSSHAndUnknownObservations(t *testing.T) {
	for _, mode := range []string{"linux", "arm", "failed", "stale"} {
		t.Run(mode, func(t *testing.T) {
			f := setupHealthHTTP(t)
			f.setSample(func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
				switch mode {
				case "failed":
					return hosthealth.Snapshot{}, errors.New("never_publish")
				case "linux":
					return healthHTTPSample(target, f.clock.Now(), "Linux x86_64"), nil
				case "arm":
					return healthHTTPSample(target, f.clock.Now(), "Darwin arm64"), nil
				}
				s := healthHTTPSample(target, f.clock.Now(), "Darwin x86_64")
				f.clock.advance(hosthealth.StaleAfter + time.Second)
				return s, nil
			})
			_, raw := f.call(t, "GET", healthPath, "read", "", "", 200)
			response := healthResponse(t, raw)
			if mode == "linux" || mode == "arm" {
				if response.Guard.Level != hosthealth.LevelUnsupported || response.Guard.RefuseNew || response.Latest.ThermalLevel.Status != hosthealth.Unsupported {
					t.Fatal(response.Guard)
				}
			} else if response.Guard.Level != hosthealth.LevelUnknown || !response.Guard.RefuseNew || response.Guard.Stop {
				t.Fatal(response.Guard)
			}
		})
	}
}
func TestConnectionHealthHTTPReportsSustainedCriticalWithoutControlAuthority(t *testing.T) {
	f := setupHealthHTTP(t)
	f.repo.mu.Lock()
	target := f.repo.targets["a/con_lan"]
	policy := *target.Policy
	policy.CriticalDurationSeconds = 60
	target.Policy = &policy
	f.repo.targets["a/con_lan"] = target
	f.repo.mu.Unlock()
	f.setSample(func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
		s := healthHTTPSample(target, f.clock.Now(), "Darwin x86_64")
		level := hosthealth.DefaultPolicy().CriticalThermalLevel
		s.ThermalLevel = hosthealth.Metric[int]{Status: hosthealth.Available, Value: &level}
		return s, nil
	})
	for i := 0; i < 3; i++ {
		_, raw := f.call(t, "GET", healthPath, "read", "", "", 200)
		response := healthResponse(t, raw)
		if response.Guard.Level != hosthealth.LevelCritical || response.Guard.Stop != (i == 2) {
			t.Fatal(response.Guard)
		}
		if response.Policy.CriticalDurationSeconds != 60 || len(response.History) != i+1 || f.calls.Load() != int32(i+1) {
			t.Fatal("critical interval did not use the configured policy and distinct samples")
		}
		f.clock.advance(30 * time.Second)
	}
}
func TestConnectionHealthHTTPRevocationAndRepositoryFailureStayPrivate(t *testing.T) {
	t.Run("revoked_during_probe", func(t *testing.T) {
		f := setupHealthHTTP(t)
		f.setSample(func(_ context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
			requireNoError(t, f.access.Revoke(context.Background(), f.tokenIDs["read"]))
			return healthHTTPSample(target, f.clock.Now(), "Darwin x86_64"), nil
		})
		f.call(t, "GET", healthPath, "read", "", "", 401)
	})
	t.Run("repository_error", func(t *testing.T) {
		f := setupHealthHTTP(t)
		f.repo.mu.Lock()
		f.repo.readErr = errors.New("private-account-canary never_publish")
		f.repo.mu.Unlock()
		_, raw := f.call(t, "GET", healthPath, "read", "", "", 503)
		managedSafeResponse(t, raw)
		if strings.Contains(string(raw), "never_publish") {
			t.Fatal("repository error leaked")
		}
	})
	t.Run("unconfigured", func(t *testing.T) { f := setupJobHTTP(t); f.call(t, "GET", healthPath, "a", "", "", 404) })
}

func TestConnectionHealthHTTPUsesBoundedProbeDeadlineAndRejectsCorruptHistory(t *testing.T) {
	t.Run("probe_deadline", func(t *testing.T) {
		f := setupHealthHTTP(t)
		f.setSample(func(ctx context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
			deadline, ok := ctx.Deadline()
			remaining := time.Until(deadline)
			if !ok || remaining < 30*time.Second || remaining > hosthealth.RefreshTimeout {
				t.Fatalf("health probe deadline=%v remaining=%v", ok, remaining)
			}
			return healthHTTPSample(target, f.clock.Now(), "Darwin x86_64"), nil
		})
		_, raw := f.call(t, "GET", healthPath, "read", "", "", 200)
		healthResponse(t, raw)
	})
	t.Run("corrupt_history", func(t *testing.T) {
		f := setupHealthHTTP(t)
		target := f.repo.targets["a/con_lan"]
		bad := healthHTTPSample(target, f.clock.Now().Add(-time.Minute), "Darwin x86_64")
		private := "never_publish"
		bad.BatteryCondition = hosthealth.Metric[string]{Status: hosthealth.Available, Value: &private}
		f.repo.mu.Lock()
		f.repo.history[target.ResourceID] = []hosthealth.Snapshot{bad}
		f.repo.mu.Unlock()
		_, raw := f.call(t, "GET", healthPath, "read", "", "", 503)
		if strings.Contains(string(raw), private) {
			t.Fatal("corrupt historical source escaped")
		}
	})
}
