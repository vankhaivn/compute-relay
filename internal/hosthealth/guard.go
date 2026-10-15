package hosthealth

import "time"

type Level string

const (
	LevelOK          Level = "ok"
	LevelWarning     Level = "warning"
	LevelCritical    Level = "critical"
	LevelUnknown     Level = "unknown"
	LevelUnsupported Level = "unsupported"
)

type Reason string

const (
	ReasonPolicyInvalid     Reason = "health_policy_invalid"
	ReasonSnapshotInvalid   Reason = "health_snapshot_invalid"
	ReasonCoreUnavailable   Reason = "health_core_metrics_unavailable"
	ReasonUnsupported       Reason = "host_platform_unsupported"
	ReasonThermalWarning    Reason = "thermal_level_warning"
	ReasonThermalCritical   Reason = "thermal_level_critical"
	ReasonSpeedWarning      Reason = "cpu_speed_warning"
	ReasonSpeedCritical     Reason = "cpu_speed_critical"
	ReasonCPUWarning        Reason = "cpu_temperature_warning"
	ReasonCPUCritical       Reason = "cpu_temperature_critical"
	ReasonGPUWarning        Reason = "gpu_temperature_warning"
	ReasonGPUCritical       Reason = "gpu_temperature_critical"
	ReasonBatteryLow        Reason = "battery_low"
	ReasonSustained         Reason = "critical_sustained"
	ReasonNotObserved       Reason = "health_not_observed"
	ReasonNotReobserved     Reason = "health_not_reobserved"
	ReasonObservationFailed Reason = "health_observation_failed"
	ReasonClockChanged      Reason = "health_clock_changed"
	ReasonStale             Reason = "health_stale"
	ReasonSudoBroader       Reason = "sudo_grant_broader"
	ReasonSleepEnabled      Reason = "system_sleep_enabled"
)

type Decision struct {
	Level         Level      `json:"level"`
	Reasons       []Reason   `json:"reasons"`
	Diagnostics   []Reason   `json:"diagnostics,omitempty"`
	RefuseNew     bool       `json:"refuse_new"`
	Stop          bool       `json:"stop"`
	CriticalSince *time.Time `json:"critical_since,omitempty"`
}

func unknown(reason Reason) Decision {
	return Decision{Level: LevelUnknown, Reasons: []Reason{reason}, RefuseNew: true}
}
func unknownReason(reason string) Reason {
	switch Reason(reason) {
	case ReasonNotObserved, ReasonNotReobserved, ReasonObservationFailed, ReasonClockChanged, ReasonStale:
		return Reason(reason)
	}
	return ReasonObservationFailed
}
func available[T any](m Metric[T]) bool { return m.Status == Available && m.Value != nil }

// Evaluate is pure. Freshness must be confirmed in this process, and sustained
// danger must be measured at consecutive observation endpoints, never inferred
// from elapsed time since one critical sample or from pre-restart history.
// History is evaluated under the supplied policy; callers must use the exact
// immutable policy of a running attempt when making a stop decision.
func Evaluate(view View, p Policy, now time.Time) Decision {
	if !view.Fresh {
		return unknown(unknownReason(view.UnknownReason))
	}
	if p.Validate() != nil {
		return unknown(ReasonPolicyInvalid)
	}
	if view.Latest == nil {
		return unknown(ReasonNotObserved)
	}
	latest := *view.Latest
	if latest.Validate() != nil || latest.ResourceID != view.ResourceID {
		return unknown(ReasonSnapshotInvalid)
	}
	if now.Before(latest.ObservedAt) {
		return unknown(ReasonClockChanged)
	}
	if now.Sub(latest.ObservedAt) > StaleAfter {
		return unknown(ReasonStale)
	}
	if view.ReobservedSince.IsZero() {
		return unknown(ReasonNotReobserved)
	}
	if latest.ObservedAt.Before(view.ReobservedSince) {
		return unknown(ReasonClockChanged)
	}
	d := evaluateSnapshot(latest, p)
	if d.Level != LevelCritical || d.Stop {
		return d
	}
	start := latest.ObservedAt
	if len(view.History) == 0 || len(view.History) > HistoryMaxRows || !view.History[0].ObservedAt.Equal(latest.ObservedAt) {
		return d
	}
	previous := latest.ObservedAt
	for i, s := range view.History {
		if s.ResourceID != view.ResourceID || s.Validate() != nil || s.ObservedAt.Before(view.ReobservedSince) || now.Sub(s.ObservedAt) > HistoryMaxAge {
			break
		}
		if i > 0 && (!s.ObservedAt.Before(previous) || previous.Sub(s.ObservedAt) > StaleAfter) {
			break
		}
		if evaluateSnapshot(s, p).Level != LevelCritical {
			break
		}
		start = s.ObservedAt
		previous = s.ObservedAt
	}
	d.CriticalSince = &start
	if latest.ObservedAt.Sub(start) >= time.Duration(p.CriticalDurationSeconds)*time.Second {
		d.Stop = true
		d.Reasons = append(d.Reasons, ReasonSustained)
	}
	return d
}
func evaluateSnapshot(s Snapshot, p Policy) Decision {
	if !available(s.Platform) {
		return unknown(ReasonCoreUnavailable)
	}
	if *s.Platform.Value == "unsupported" {
		return Decision{Level: LevelUnsupported, Reasons: []Reason{ReasonUnsupported}}
	}
	if *s.Platform.Value != "macos_intel" {
		return unknown(ReasonCoreUnavailable)
	}
	d := Decision{Level: LevelOK, Reasons: []Reason{}}
	if available(s.SudoGrant) && *s.SudoGrant.Value == "broader" {
		d.Diagnostics = append(d.Diagnostics, ReasonSudoBroader)
	}
	if available(s.SleepEnabled) && *s.SleepEnabled.Value {
		d.Diagnostics = append(d.Diagnostics, ReasonSleepEnabled)
	}
	// Confirmed low battery is independently sufficient, even if thermal readings fail.
	if available(s.PowerSource) && *s.PowerSource.Value == "battery" && available(s.BatteryChargePercent) && *s.BatteryChargePercent.Value <= p.LowBatteryPercent {
		d.Level = LevelCritical
		d.Reasons = []Reason{ReasonBatteryLow}
		d.RefuseNew = true
		d.Stop = true
		return d
	}
	if !available(s.ThermalLevel) || !available(s.CPUSpeedLimitPercent) || !available(s.PowerSource) || *s.PowerSource.Value == "battery" && !available(s.BatteryChargePercent) {
		u := unknown(ReasonCoreUnavailable)
		u.Diagnostics = d.Diagnostics
		return u
	}
	add := func(warning, critical bool, wr, cr Reason) {
		if critical {
			d.Level = LevelCritical
			d.Reasons = append(d.Reasons, cr)
		} else if warning {
			if d.Level != LevelCritical {
				d.Level = LevelWarning
			}
			d.Reasons = append(d.Reasons, wr)
		}
	}
	add(*s.ThermalLevel.Value >= p.WarningThermalLevel, *s.ThermalLevel.Value >= p.CriticalThermalLevel, ReasonThermalWarning, ReasonThermalCritical)
	add(*s.CPUSpeedLimitPercent.Value <= p.WarningCPUSpeedPercent, *s.CPUSpeedLimitPercent.Value <= p.CriticalCPUSpeedPercent, ReasonSpeedWarning, ReasonSpeedCritical)
	for _, temp := range []struct {
		m                 Metric[float64]
		warning, critical Reason
	}{{s.CPUDieTemperatureCelsius, ReasonCPUWarning, ReasonCPUCritical}, {s.GPUDieTemperatureCelsius, ReasonGPUWarning, ReasonGPUCritical}} {
		if available(temp.m) {
			add(*temp.m.Value >= p.WarningDieTemperatureCelsius, *temp.m.Value >= p.CriticalDieTemperatureCelsius, temp.warning, temp.critical)
		}
	}
	d.RefuseNew = d.Level == LevelWarning || d.Level == LevelCritical
	return d
}
