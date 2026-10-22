package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
)

func TestManagedHealthCompositionDefersCredentialReadsUntilAuthorizedDemand(t *testing.T) {
	f := newManagedTestFixture(t, true, &managedSSHAdapter{verify: sshFixtureVerify, config: ManagedServeConfig{MaxRemoteWallSeconds: 600}})
	if f.services.health == nil {
		t.Fatal("managed host health service is missing")
	}
	_, connection := f.sshConnection(t, "health-demand-ssh", "SYNTHETIC_SSH")
	before := f.vault.calls
	if err := f.services.health.RunOnce(context.Background()); err != nil || f.vault.calls != before {
		t.Fatal("idle saved connection read credentials", err)
	}
	calls := 0
	sampler := managedHealthSampler{credentials: f.services.connections, collect: func(ctx context.Context, raw []byte, account, resource, engine string) (hosthealth.Snapshot, error) {
		calls++
		endpoint, err := sshhost.ParseEndpoint(raw)
		if err != nil || string(endpoint.Credential.Password) != "SYNTHETIC_SSH" || engine != strings.Repeat("3", 64) || resource != "resource_"+strings.Repeat("2", 64) {
			t.Fatal("health sampler changed verified target")
		}
		if _, err := endpoint.Pin(account); err != nil {
			t.Fatal("health account was not canonical")
		}
		// The remote callback can perform a fresh local read: no SQLite transaction
		// spans credential access or sampling.
		if _, err := f.host.store.Info(ctx); err != nil {
			t.Fatal("sampling retained a SQLite transaction", err)
		}
		return hosthealth.Parse(resource, time.Now().UTC(), map[hosthealth.Source]hosthealth.Sample{hosthealth.Platform: {Status: hosthealth.Available, Data: []byte("Linux x86_64")}}), nil
	}}
	service, err := hosthealth.NewService(f.host.access, f.host.store, sampler, ports.SystemClock{})
	requireNoError(t, err)
	if _, err := service.Get(context.Background(), f.actor, "other", connection.ID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("foreign workspace accessed health", err)
	}
	if calls != 0 || f.vault.calls != before {
		t.Fatal("authorization followed credential access")
	}
	view, err := service.Get(context.Background(), f.actor, "app", connection.ID)
	if err != nil || !view.Fresh || calls != 1 || len(view.History) != 1 {
		t.Fatal("authorized demand did not sample", err, calls)
	}
	raw, err := json.Marshal(view)
	if err != nil || strings.Contains(string(raw), "SYNTHETIC_SSH") || strings.Contains(string(raw), "ssh-") || strings.Contains(string(raw), "engine_id") || strings.Contains(string(raw), "100.64.") {
		t.Fatal("private target escaped health view")
	}
	restarted, err := hosthealth.NewService(f.host.access, f.host.store, sampler, ports.SystemClock{})
	requireNoError(t, err)
	view, err = restarted.Cached(context.Background(), view.ResourceID)
	if err != nil || view.Fresh || view.UnknownReason != "health_not_reobserved" || calls != 1 {
		t.Fatal("new service inferred freshness from SQLite", err)
	}
}

type healthCredentialFixture struct{ calls int }

func (r *healthCredentialFixture) WithCredential(ctx context.Context, ref ports.CredentialRef, use func([]byte) error) error {
	r.calls++
	raw := []byte("synthetic vault value")
	defer clear(raw)
	return use(raw)
}

func TestManagedHealthSamplerAllowsMissingImageAndRejectsChangedMachineBeforeCredentials(t *testing.T) {
	resolver := &healthCredentialFixture{}
	target := hosthealth.Target{Workspace: "app", ConnectionID: "con_health", ProviderType: "ssh", CanonicalAccount: "private_canonical_account", CredentialRef: "vault:con_health", ResourceID: "resource_" + strings.Repeat("2", 64)}
	sampler := managedHealthSampler{credentials: resolver, collect: func(_ context.Context, _ []byte, _ string, resource, engine string) (hosthealth.Snapshot, error) {
		if engine != "" {
			t.Fatal("missing image acquired an inferred engine pin")
		}
		return hosthealth.Parse(resource, time.Now().UTC(), map[hosthealth.Source]hosthealth.Sample{hosthealth.Platform: {Status: hosthealth.Available, Data: []byte("Darwin x86_64")}, hosthealth.NativeArchitecture: {Status: hosthealth.Available, Data: []byte("7\n")}}), nil
	}}
	if snapshot, err := sampler.Sample(context.Background(), target); err != nil || snapshot.Platform.Value == nil || resolver.calls != 1 {
		t.Fatal("missing image blocked machine health", err)
	}
	raw, err := json.Marshal(managedSSHRuntimeConfig{Version: 3, Image: "sha256:" + strings.Repeat("1", 64), Architecture: "amd64", RecipeSHA256: sshhost.ImageRecipeDigest(), MachineID: strings.Repeat("9", 64), EngineID: strings.Repeat("3", 64)})
	requireNoError(t, err)
	target.RuntimeConfig = raw
	if _, err = sampler.Sample(context.Background(), target); !errors.Is(err, hosthealth.ErrUnverified) || resolver.calls != 1 {
		t.Fatal("changed frozen machine accessed credentials", err)
	}
}

func TestManagedHealthRetainsSampleStepThroughGuard(t *testing.T) {
	target := hosthealth.Target{Workspace: "app", ConnectionID: "con_health", ProviderType: "ssh", CanonicalAccount: "fixture_account", CredentialRef: "vault:con_health", ResourceID: "resource_" + strings.Repeat("2", 64)}
	sampler := managedHealthSampler{credentials: &healthCredentialFixture{}, collect: func(context.Context, []byte, string, string, string) (hosthealth.Snapshot, error) {
		return hosthealth.Snapshot{}, provider.WithReason(errors.New("private-provider-canary"), "health_ssh_command_failed")
	}}
	_, err := sampler.Sample(t.Context(), target)
	if !errors.Is(err, hosthealth.ErrUnavailable) || provider.Reason(err, "") != "health_ssh_command_failed" || strings.Contains(err.Error(), "private") {
		t.Fatal("lost health step", err)
	}
	snapshot := hosthealth.Parse(target.ResourceID, time.Now().UTC(), nil)
	snapshot.Cause = provider.Reason(err, "")
	refusal := healthRefusal(hosthealth.View{Latest: &snapshot}, hosthealth.Policy{}, time.Now().UTC())
	if !errors.Is(refusal, admission.ErrHostUnhealthy) || provider.Reason(refusal, "") != snapshot.Cause {
		t.Fatal("guard discarded cause", refusal)
	}
}
