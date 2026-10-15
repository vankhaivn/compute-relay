package hosthealth

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func guardSnapshot(t *testing.T, at time.Time) Snapshot {
	t.Helper()
	s := Parse(testResource, at, fixtures(t))
	s.CPUDieTemperatureCelsius = missing[float64]("sudo_grant_absent")
	s.GPUDieTemperatureCelsius = missing[float64]("sudo_grant_absent")
	return s
}
func guardView(t *testing.T, at time.Time) View {
	s := guardSnapshot(t, at)
	return View{ResourceID: testResource, Latest: &s, History: []Snapshot{s}, Fresh: true, ReobservedSince: at}
}
func TestGuardLevelsAndInclusiveThresholds(t *testing.T) {
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	p := DefaultPolicy()
	for _, tt := range []struct {
		name   string
		mutate func(*View)
		level  Level
		reason Reason
		stop   bool
	}{
		{"healthy", func(*View) {}, LevelOK, "", false},
		{"thermal_warning", func(v *View) { v.Latest.ThermalLevel = value(p.WarningThermalLevel) }, LevelWarning, ReasonThermalWarning, false},
		{"thermal_critical", func(v *View) { v.Latest.ThermalLevel = value(p.CriticalThermalLevel) }, LevelCritical, ReasonThermalCritical, false},
		{"speed_warning", func(v *View) { v.Latest.CPUSpeedLimitPercent = value(p.WarningCPUSpeedPercent) }, LevelWarning, ReasonSpeedWarning, false},
		{"speed_critical", func(v *View) { v.Latest.CPUSpeedLimitPercent = value(p.CriticalCPUSpeedPercent) }, LevelCritical, ReasonSpeedCritical, false},
		{"cpu_warning", func(v *View) { v.Latest.CPUDieTemperatureCelsius = value(p.WarningDieTemperatureCelsius) }, LevelWarning, ReasonCPUWarning, false},
		{"cpu_critical", func(v *View) { v.Latest.CPUDieTemperatureCelsius = value(p.CriticalDieTemperatureCelsius) }, LevelCritical, ReasonCPUCritical, false},
		{"gpu_warning", func(v *View) { v.Latest.GPUDieTemperatureCelsius = value(p.WarningDieTemperatureCelsius) }, LevelWarning, ReasonGPUWarning, false},
		{"gpu_critical", func(v *View) { v.Latest.GPUDieTemperatureCelsius = value(p.CriticalDieTemperatureCelsius) }, LevelCritical, ReasonGPUCritical, false},
		{"low_battery", func(v *View) {
			v.Latest.PowerSource = value("battery")
			v.Latest.BatteryChargePercent = value(p.LowBatteryPercent)
		}, LevelCritical, ReasonBatteryLow, true},
		{"battery_above_limit", func(v *View) {
			v.Latest.PowerSource = value("battery")
			v.Latest.BatteryChargePercent = value(p.LowBatteryPercent + 1)
		}, LevelOK, "", false},
		{"low_battery_independent", func(v *View) {
			v.Latest.PowerSource = value("battery")
			v.Latest.BatteryChargePercent = value(p.LowBatteryPercent)
			v.Latest.ThermalLevel = missing[int]("source_unavailable")
		}, LevelCritical, ReasonBatteryLow, true},
		{"ac_low_charge", func(v *View) { v.Latest.BatteryChargePercent = value(0.0) }, LevelOK, "", false},
		{"ac_desktop", func(v *View) { v.Latest.BatteryChargePercent = missing[float64]("source_not_exposed") }, LevelOK, "", false},
		{"missing_thermal", func(v *View) { v.Latest.ThermalLevel = missing[int]("source_unavailable") }, LevelUnknown, ReasonCoreUnavailable, false},
		{"missing_speed", func(v *View) { v.Latest.CPUSpeedLimitPercent = missing[float64]("source_unavailable") }, LevelUnknown, ReasonCoreUnavailable, false},
		{"missing_power", func(v *View) { v.Latest.PowerSource = missing[string]("source_unavailable") }, LevelUnknown, ReasonCoreUnavailable, false},
		{"battery_unknown_charge", func(v *View) {
			v.Latest.PowerSource = value("battery")
			v.Latest.BatteryChargePercent = missing[float64]("source_unavailable")
		}, LevelUnknown, ReasonCoreUnavailable, false},
		{"unsupported", func(v *View) {
			f := fixtures(t)
			f[Platform] = Sample{Status: Available, Data: []byte("Linux x86_64")}
			s := Parse(testResource, at, f)
			v.Latest = &s
			v.History = []Snapshot{s}
		}, LevelUnsupported, ReasonUnsupported, false},
		{"stale", func(v *View) { v.Fresh = false; v.UnknownReason = "health_stale" }, LevelUnknown, ReasonStale, false},
		{"restart", func(v *View) { v.Fresh = false; v.UnknownReason = "health_not_reobserved" }, LevelUnknown, ReasonNotReobserved, false},
		{"unknown_reason_safe", func(v *View) { v.Fresh = false; v.UnknownReason = "NEVER_PERSIST" }, LevelUnknown, ReasonObservationFailed, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v := guardView(t, at)
			tt.mutate(&v)
			d := Evaluate(v, p, at)
			if d.Level != tt.level || d.Stop != tt.stop || d.RefuseNew != (tt.level == LevelWarning || tt.level == LevelCritical || tt.level == LevelUnknown) {
				t.Fatalf("decision=%+v", d)
			}
			if tt.reason != "" {
				found := false
				for _, r := range d.Reasons {
					found = found || r == tt.reason
				}
				if !found {
					t.Fatalf("missing reason %s: %+v", tt.reason, d)
				}
			}
		})
	}
}
func TestGuardDiagnosticsDoNotRefuseNew(t *testing.T) {
	at := time.Now()
	v := guardView(t, at)
	v.Latest.SleepEnabled = value(true)
	v.Latest.SudoGrant = value("broader")
	d := Evaluate(v, DefaultPolicy(), at)
	if d.Level != LevelOK || d.RefuseNew || d.Stop || len(d.Diagnostics) != 2 {
		t.Fatal(d)
	}
}
func TestGuardSustainedCriticalRequiresConsecutiveObservations(t *testing.T) {
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	p := DefaultPolicy()
	critical := func(delta time.Duration) Snapshot {
		s := guardSnapshot(t, at.Add(delta))
		s.ThermalLevel = value(p.CriticalThermalLevel)
		return s
	}
	base := func() View {
		history := []Snapshot{critical(0), critical(-30 * time.Second), critical(-60 * time.Second)}
		latest := history[0]
		return View{ResourceID: testResource, Latest: &latest, History: history, Fresh: true, ReobservedSince: at.Add(-time.Hour)}
	}
	for _, tt := range []struct {
		name   string
		mutate func(*View)
		now    time.Time
		stop   bool
	}{
		{"sustained", func(*View) {}, at, true},
		{"single_observation", func(v *View) { v.History = v.History[:1] }, at.Add(60 * time.Second), false},
		{"not_yet_sustained", func(v *View) { v.History = v.History[:2] }, at, false},
		{"healthy_resets", func(v *View) { v.History[1].ThermalLevel = value(0) }, at, false},
		{"unknown_resets", func(v *View) { v.History[1].PowerSource = missing[string]("source_unavailable") }, at, false},
		{"gap_resets", func(v *View) { v.History[1] = critical(-91 * time.Second); v.History[2] = critical(-120 * time.Second) }, at, false},
		{"restart_bound", func(v *View) { v.ReobservedSince = at.Add(-30 * time.Second) }, at, false},
		{"rollback_history", func(v *View) { v.History[1] = critical(1 * time.Second) }, at, false},
		{"duplicate_time", func(v *View) { v.History[1] = critical(0) }, at, false},
		{"no_history", func(v *View) { v.History = nil }, at, false},
		{"failed_reobservation", func(v *View) { v.Fresh = false }, at, false},
		{"stale_current", func(*View) {}, at.Add(StaleAfter + time.Second), false},
		{"clock_backwards", func(*View) {}, at.Add(-time.Second), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v := base()
			tt.mutate(&v)
			d := Evaluate(v, p, tt.now)
			if d.Stop != tt.stop {
				t.Fatal(d)
			}
		})
	}
}
func TestGuardMaximumDurationFitsBoundedHistory(t *testing.T) {
	at := time.Now()
	p := DefaultPolicy()
	p.CriticalDurationSeconds = MaxCriticalDurationSeconds
	v := guardView(t, at)
	v.ReobservedSince = at.Add(-time.Hour)
	v.History = nil
	for i := 0; i < HistoryMaxRows; i++ {
		s := guardSnapshot(t, at.Add(-time.Duration(i)*DemandCache))
		s.ThermalLevel = value(p.CriticalThermalLevel)
		v.History = append(v.History, s)
	}
	s := v.History[0]
	v.Latest = &s
	if d := Evaluate(v, p, at); !d.Stop {
		t.Fatal(d)
	}
}
func TestPolicyValidationPatchAndImmutableRuntime(t *testing.T) {
	p := DefaultPolicy()
	if p.Validate() != nil {
		t.Fatal("bad defaults")
	}
	for _, mutate := range []func(*Policy){func(p *Policy) { p.WarningThermalLevel = -1 }, func(p *Policy) { p.CriticalThermalLevel = 256 }, func(p *Policy) { p.WarningThermalLevel = p.CriticalThermalLevel + 1 }, func(p *Policy) { p.CriticalCPUSpeedPercent = p.WarningCPUSpeedPercent + 1 }, func(p *Policy) { p.LowBatteryPercent = 101 }, func(p *Policy) { p.WarningDieTemperatureCelsius = math.NaN() }, func(p *Policy) { p.CriticalDieTemperatureCelsius = math.Inf(1) }, func(p *Policy) { p.CriticalDurationSeconds = 0 }, func(p *Policy) { p.CriticalDurationSeconds = MaxCriticalDurationSeconds + 1 }} {
		bad := p
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatal("invalid policy accepted")
		}
	}
	zero := 0
	duration := 30
	changed, err := p.Apply(&PolicyPatch{WarningThermalLevel: &zero, CriticalDurationSeconds: &duration})
	if err != nil || changed.WarningThermalLevel != 0 || changed.CriticalDurationSeconds != 30 || changed.CriticalThermalLevel != p.CriticalThermalLevel {
		t.Fatal(changed, err)
	}
	original := []byte(`{"schema_version":3,"image":"sha256:synthetic"}`)
	frozen, err := FreezeRuntimePolicy(original, changed)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(original), "host_health") {
		t.Fatal("mutated old runtime")
	}
	read, err := PolicyFromRuntime(frozen)
	if err != nil || read != changed {
		t.Fatal(read, err)
	}
	legacy, err := PolicyFromRuntime(original)
	if err != nil || legacy != LegacyPolicyV1() {
		t.Fatal(legacy, err)
	}
	for _, raw := range []string{`{"host_health":null}`, `{"host_health":{}}`, `{"host_health":{"critical_duration_seconds":60}}`} {
		if _, err := PolicyFromRuntime([]byte(raw)); err == nil {
			t.Fatal("partial runtime policy accepted")
		}
	}
	encoded, _ := json.Marshal(changed)
	if _, err := PolicyFromRuntime([]byte(`{"host_health":` + string(encoded) + `}`)); err != nil {
		t.Fatal(err)
	}
}
