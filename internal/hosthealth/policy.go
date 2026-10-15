package hosthealth

import (
	"encoding/json"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

var ErrPolicy = errors.New("invalid host health policy")

// MaxCriticalDurationSeconds stays below the history window even when on-demand
// observations arrive every DemandCache (120 rows cover at least 1785 seconds).
const MaxCriticalDurationSeconds = 900

// Policy thresholds are inclusive. Thermal levels are raw
// machdep.xcpm.cpu_thermal_level units, not Apple's separate pressure enum.
type Policy struct {
	WarningThermalLevel           int     `json:"warning_thermal_level"`
	CriticalThermalLevel          int     `json:"critical_thermal_level"`
	WarningCPUSpeedPercent        float64 `json:"warning_cpu_speed_percent"`
	CriticalCPUSpeedPercent       float64 `json:"critical_cpu_speed_percent"`
	WarningDieTemperatureCelsius  float64 `json:"warning_die_temperature_celsius"`
	CriticalDieTemperatureCelsius float64 `json:"critical_die_temperature_celsius"`
	LowBatteryPercent             float64 `json:"low_battery_percent"`
	CriticalDurationSeconds       int     `json:"critical_duration_seconds"`
}

// DefaultPolicy is provisional pending sustained-load qualification. Discovery
// measured raw thermal 0 and CPU speed 100%; no die-temperature baseline exists yet.
func DefaultPolicy() Policy { return LegacyPolicyV1() }

// LegacyPolicyV1 fixes the semantics of runtime revisions predating host health.
// Changing future defaults must never change these legacy values.
func LegacyPolicyV1() Policy { return Policy{1, 2, 90, 50, 90, 100, 10, 60} }
func (p Policy) Validate() error {
	if p.WarningThermalLevel < 0 || p.CriticalThermalLevel > 255 || p.WarningThermalLevel > p.CriticalThermalLevel ||
		!between(0, 100)(p.WarningCPUSpeedPercent) || !between(0, p.WarningCPUSpeedPercent)(p.CriticalCPUSpeedPercent) ||
		!between(0, 150)(p.WarningDieTemperatureCelsius) || !between(p.WarningDieTemperatureCelsius, 150)(p.CriticalDieTemperatureCelsius) ||
		!between(0, 100)(p.LowBatteryPercent) || p.CriticalDurationSeconds < 1 || p.CriticalDurationSeconds > MaxCriticalDurationSeconds {
		return ErrPolicy
	}
	return nil
}

// PolicyPatch preserves omitted thresholds, including valid zero-valued overrides.
type PolicyPatch struct {
	WarningThermalLevel           *int     `json:"warning_thermal_level,omitempty"`
	CriticalThermalLevel          *int     `json:"critical_thermal_level,omitempty"`
	WarningCPUSpeedPercent        *float64 `json:"warning_cpu_speed_percent,omitempty"`
	CriticalCPUSpeedPercent       *float64 `json:"critical_cpu_speed_percent,omitempty"`
	WarningDieTemperatureCelsius  *float64 `json:"warning_die_temperature_celsius,omitempty"`
	CriticalDieTemperatureCelsius *float64 `json:"critical_die_temperature_celsius,omitempty"`
	LowBatteryPercent             *float64 `json:"low_battery_percent,omitempty"`
	CriticalDurationSeconds       *int     `json:"critical_duration_seconds,omitempty"`
}

func (p Policy) Patch() *PolicyPatch {
	return &PolicyPatch{&p.WarningThermalLevel, &p.CriticalThermalLevel, &p.WarningCPUSpeedPercent, &p.CriticalCPUSpeedPercent, &p.WarningDieTemperatureCelsius, &p.CriticalDieTemperatureCelsius, &p.LowBatteryPercent, &p.CriticalDurationSeconds}
}
func (p Policy) Apply(patch *PolicyPatch) (Policy, error) {
	if patch != nil {
		if patch.WarningThermalLevel != nil {
			p.WarningThermalLevel = *patch.WarningThermalLevel
		}
		if patch.CriticalThermalLevel != nil {
			p.CriticalThermalLevel = *patch.CriticalThermalLevel
		}
		if patch.WarningCPUSpeedPercent != nil {
			p.WarningCPUSpeedPercent = *patch.WarningCPUSpeedPercent
		}
		if patch.CriticalCPUSpeedPercent != nil {
			p.CriticalCPUSpeedPercent = *patch.CriticalCPUSpeedPercent
		}
		if patch.WarningDieTemperatureCelsius != nil {
			p.WarningDieTemperatureCelsius = *patch.WarningDieTemperatureCelsius
		}
		if patch.CriticalDieTemperatureCelsius != nil {
			p.CriticalDieTemperatureCelsius = *patch.CriticalDieTemperatureCelsius
		}
		if patch.LowBatteryPercent != nil {
			p.LowBatteryPercent = *patch.LowBatteryPercent
		}
		if patch.CriticalDurationSeconds != nil {
			p.CriticalDurationSeconds = *patch.CriticalDurationSeconds
		}
	}
	return p, p.Validate()
}

// Validate validates individual overrides; ordering is checked against the saved
// policy when the patch is applied in the configuration transaction.
func (p PolicyPatch) Validate() error {
	if p.WarningThermalLevel != nil && (*p.WarningThermalLevel < 0 || *p.WarningThermalLevel > 255) || p.CriticalThermalLevel != nil && (*p.CriticalThermalLevel < 0 || *p.CriticalThermalLevel > 255) {
		return ErrPolicy
	}
	for _, v := range []*float64{p.WarningCPUSpeedPercent, p.CriticalCPUSpeedPercent, p.LowBatteryPercent} {
		if v != nil && !between(0, 100)(*v) {
			return ErrPolicy
		}
	}
	for _, v := range []*float64{p.WarningDieTemperatureCelsius, p.CriticalDieTemperatureCelsius} {
		if v != nil && !between(0, 150)(*v) {
			return ErrPolicy
		}
	}
	if p.CriticalDurationSeconds != nil && (*p.CriticalDurationSeconds < 1 || *p.CriticalDurationSeconds > MaxCriticalDurationSeconds) {
		return ErrPolicy
	}
	return nil
}

// FreezeRuntimePolicy adds the effective non-secret policy to a new immutable
// runtime revision. Historical runtime JSON is never modified in place.
func FreezeRuntimePolicy(raw []byte, p Policy) ([]byte, error) {
	fields, err := jsonwire.Object(raw, 8192)
	if err != nil || p.Validate() != nil {
		return nil, ErrPolicy
	}
	encoded, err := json.Marshal(p)
	if err != nil {
		return nil, ErrPolicy
	}
	fields["host_health"] = encoded
	out, err := json.Marshal(fields)
	if err != nil || len(out) > 8192 {
		return nil, ErrPolicy
	}
	return out, nil
}

// PolicyFromRuntime preserves the explicit v1 legacy policy when old immutable
// runtime JSON lacks host_health. It does not rewrite that historical JSON.
func PolicyFromRuntime(raw []byte) (Policy, error) {
	fields, err := jsonwire.Object(raw, 8192)
	if err != nil {
		return Policy{}, ErrPolicy
	}
	data, ok := fields["host_health"]
	if !ok {
		return LegacyPolicyV1(), nil
	}
	var p Policy
	expected := []string{"warning_thermal_level", "critical_thermal_level", "warning_cpu_speed_percent", "critical_cpu_speed_percent", "warning_die_temperature_celsius", "critical_die_temperature_celsius", "low_battery_percent", "critical_duration_seconds"}
	object, err := jsonwire.Object(data, 2048)
	if err != nil || jsonwire.Fields(object, expected, nil) != nil || json.Unmarshal(data, &p) != nil || p.Validate() != nil {
		return Policy{}, ErrPolicy
	}
	for _, value := range object {
		if string(value) == "null" {
			return Policy{}, ErrPolicy
		}
	}
	return p, nil
}
