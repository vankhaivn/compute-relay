package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

func runtimeHealthPolicy(t *testing.T, f *managedStateFixture, c connections.Connection) ([]byte, hosthealth.Policy) {
	t.Helper()
	var raw []byte
	requireNoError(t, f.s.db.QueryRow(`SELECT config FROM managed_profile_runtime WHERE profile=? AND revision=?`, c.Selection.Profile, c.Selection.ConfigurationRevision).Scan(&raw))
	p, err := hosthealth.PolicyFromRuntime(raw)
	requireNoError(t, err)
	return raw, p
}
func configureHealthPolicy(t *testing.T, f *managedStateFixture, c connections.Connection, key, patch string) connections.Connection {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":1800,"host_health":%s}}`, c.Revision, patch))
	op, err := f.service.Submit(context.Background(), f.principals[c.Workspace], c.Workspace, c.ID, key, body)
	if err != nil || op.Status != "succeeded" {
		t.Fatal(op, err)
	}
	return f.get(t, c.Workspace, c.ID)
}
func TestManagedHostHealthPolicyFreezesAndPreservesOmittedThresholds(t *testing.T) {
	f, _ := healthStateFixture(t)
	ctx := context.Background()
	c := createHealthConnection(t, f, "a", "health-policy-create", "SYNTHETIC_LAN")
	old := c
	original, p := runtimeHealthPolicy(t, f, c)
	want := hosthealth.Policy{WarningThermalLevel: 1, CriticalThermalLevel: 2,
		WarningCPUSpeedPercent: 90, CriticalCPUSpeedPercent: 50,
		WarningDieTemperatureCelsius: 90, CriticalDieTemperatureCelsius: 95,
		LowBatteryPercent: 10, CriticalDurationSeconds: 30}
	if p != want || !bytes.Contains(original, []byte(`"host_health"`)) {
		t.Fatal("new profile did not materialize defaults")
	}
	c = configureHealthPolicy(t, f, c, "health-policy-change", `{"warning_thermal_level":2,"critical_thermal_level":3,"critical_duration_seconds":120}`)
	_, changed := runtimeHealthPolicy(t, f, c)
	if changed.WarningThermalLevel != 2 || changed.CriticalThermalLevel != 3 || changed.CriticalDurationSeconds != 120 || changed.LowBatteryPercent != p.LowBatteryPercent {
		t.Fatal(changed)
	}
	c = configureHealthPolicy(t, f, c, "health-policy-partial", `{"low_battery_percent":0}`)
	_, partial := runtimeHealthPolicy(t, f, c)
	if partial.LowBatteryPercent != 0 || partial.CriticalThermalLevel != 3 || partial.CriticalDurationSeconds != 120 {
		t.Fatal("patch lost saved thresholds", partial)
	}
	configureConnection(t, f, c, "health-policy-omitted", 2400)
	c = f.get(t, "a", c.ID)
	_, omitted := runtimeHealthPolicy(t, f, c)
	if omitted != partial {
		t.Fatal("wall-only configure changed health policy")
	}
	preserved, oldPolicy := runtimeHealthPolicy(t, f, old)
	if !bytes.Equal(original, preserved) || oldPolicy != p {
		t.Fatal("configure rewrote old job policy")
	}
	beforeCalls := f.discovery.count()
	target, err := f.s.ResolveHostHealthTarget(ctx, "a", f.principals["a"].TokenID(), c.ID)
	if err != nil || target.Policy == nil || *target.Policy != partial || f.discovery.count() != beforeCalls {
		t.Fatal("target lost effective policy", err)
	}
	op := f.action(t, "a", c, "health-policy-recheck", "check", "")
	f.run(t)
	c = f.get(t, "a", op.ConnectionID)
	_, rechecked := runtimeHealthPolicy(t, f, c)
	if rechecked != partial {
		t.Fatal("recheck reset saved thresholds")
	}
	c = configureHealthPolicy(t, f, c, "health-policy-nominal", `{"low_battery_percent":10}`)
	var raw []byte
	requireNoError(t, f.s.db.QueryRow(`SELECT host_health FROM managed_connections WHERE connection_id=?`, c.ID).Scan(&raw))
	var saved hosthealth.Policy
	requireNoError(t, json.Unmarshal(raw, &saved))
	if saved.LowBatteryPercent != 10 {
		t.Fatal("effective policy not persisted")
	}
}

func TestManagedHostHealthLegacyPolicyAndAttemptRemainFrozen(t *testing.T) {
	legacy := hosthealth.Policy{WarningThermalLevel: 1, CriticalThermalLevel: 2,
		WarningCPUSpeedPercent: 90, CriticalCPUSpeedPercent: 50,
		WarningDieTemperatureCelsius: 90, CriticalDieTemperatureCelsius: 100,
		LowBatteryPercent: 10, CriticalDurationSeconds: 60}
	fallback, err := managedHostHealthPolicy("ssh", nil)
	if err != nil || fallback == nil || *fallback != legacy {
		t.Fatal("historical connection adopted future defaults", err)
	}
	f, _ := healthStateFixture(t)
	c := createHealthConnection(t, f, "a", "policy-legacy-create", "SYNTHETIC_LAN")
	raw, err := json.Marshal(legacy)
	requireNoError(t, err)
	c = configureHealthPolicy(t, f, c, "policy-legacy-frozen", string(raw))
	old := c
	original, policy := runtimeHealthPolicy(t, f, old)
	if policy != legacy {
		t.Fatal("legacy runtime was not frozen", policy)
	}
	_, attempt := f.admit(t, "a", c, "policy-legacy-attempt")
	frozen, err := f.s.HostHealthTargetForAttempt(t.Context(), attempt)
	if err != nil || frozen == nil || frozen.Policy == nil || *frozen.Policy != legacy || !bytes.Equal(frozen.RuntimeConfig, original) {
		t.Fatal("attempt did not bind original policy", err)
	}
	c = configureHealthPolicy(t, f, c, "policy-final-future-revision", `{"critical_die_temperature_celsius":95,"critical_duration_seconds":30}`)
	want := legacy
	want.CriticalDieTemperatureCelsius = 95
	want.CriticalDurationSeconds = 30
	_, current := runtimeHealthPolicy(t, f, c)
	if current != want {
		t.Fatal("future revision did not receive requested final policy", current)
	}
	f.restartManaged(t)
	preserved, prior := runtimeHealthPolicy(t, f, old)
	stillFrozen, err := f.s.HostHealthTargetForAttempt(t.Context(), attempt)
	if err != nil || stillFrozen == nil || stillFrozen.Policy == nil || *stillFrozen.Policy != legacy || prior != legacy || !bytes.Equal(original, preserved) || !bytes.Equal(original, stillFrozen.RuntimeConfig) {
		t.Fatal("configuration or restart rewrote historical policy", err)
	}
	target, err := f.s.ResolveHostHealthTarget(t.Context(), "a", f.principals["a"].TokenID(), c.ID)
	if err != nil || target.Policy == nil || *target.Policy != want {
		t.Fatal("current connection lost its explicit future policy", err)
	}
}

func TestManagedHostHealthPolicyRejectsUnsupportedProviderAndOrdering(t *testing.T) {
	f, _ := healthStateFixture(t)
	c := createHealthConnection(t, f, "a", "health-invalid-create", "SYNTHETIC_LAN")
	before, _ := runtimeHealthPolicy(t, f, c)
	for i, patch := range []string{`{"critical_duration_seconds":901}`, `{"low_battery_percent":101}`, `{"warning_thermal_level":5,"critical_thermal_level":4}`, `{"critical_cpu_speed_percent":95}`, `{"critical_die_temperature_celsius":80}`, `{"critical_duration_seconds":null}`, `{"anything":1}`} {
		body := []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":1800,"host_health":%s}}`, c.Revision, patch))
		_, err := f.service.Submit(context.Background(), f.principals["a"], "a", c.ID, fmt.Sprintf("bad-policy-%d", i), body)
		if !errors.Is(err, connections.ErrRequest) {
			t.Fatal("invalid policy accepted", patch, err)
		}
	}
	after, _ := runtimeHealthPolicy(t, f, c)
	if !bytes.Equal(before, after) || f.get(t, "a", c.ID).Revision != c.Revision {
		t.Fatal("failed configuration changed policy")
	}
	other := newManagedStateFixture(t)
	fixture := other.create(t, "a", "other-provider", "SYNTHETIC_ACCOUNT_TOKEN_ALPHA")
	body := []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":1800,"host_health":{"low_battery_percent":10}}}`, fixture.Revision))
	_, err := other.service.Submit(context.Background(), other.principals["a"], "a", fixture.ID, "other-health", body)
	if !errors.Is(err, connections.ErrRequest) {
		t.Fatal("non-SSH health policy accepted", err)
	}
}
func TestManagedHostHealthPolicyExistsBeforeImageAndAliasesKeepPolicies(t *testing.T) {
	f, a := healthStateFixture(t)
	a.noImage = true
	lan := createHealthConnection(t, f, "a", "policy-no-image", "SYNTHETIC_LAN")
	lan = configureHealthPolicy(t, f, lan, "policy-no-image-change", `{"low_battery_percent":20}`)
	vpn := createHealthConnection(t, f, "b", "policy-vpn", "SYNTHETIC_VPN")
	ctx := context.Background()
	first, err := f.s.ResolveHostHealthTarget(ctx, "a", f.principals["a"].TokenID(), lan.ID)
	requireNoError(t, err)
	second, err := f.s.ResolveHostHealthTarget(ctx, "b", f.principals["b"].TokenID(), vpn.ID)
	requireNoError(t, err)
	if first.ResourceID != second.ResourceID || first.Policy == nil || second.Policy == nil || first.Policy.LowBatteryPercent != 20 || second.Policy.LowBatteryPercent != 10 || len(first.RuntimeConfig) != 0 {
		t.Fatal("alias policy merged or requires installed image")
	}
}
