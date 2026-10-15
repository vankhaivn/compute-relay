// Package hosthealth collects bounded, read-only macOS Intel host observations.
package hosthealth

import "time"

const SchemaVersion = 1

type Status string

const (
	Available   Status = "available"
	Unavailable Status = "unavailable"
	Unsupported Status = "unsupported"
)

// Metric never contains source text. Reason is a fixed machine-readable code.
type Metric[T any] struct {
	Status Status `json:"status"`
	Value  *T     `json:"value,omitempty"`
	Reason string `json:"reason,omitempty"`
}

func value[T any](v T) Metric[T]             { return Metric[T]{Status: Available, Value: &v} }
func missing[T any](reason string) Metric[T] { return Metric[T]{Status: Unavailable, Reason: reason} }

type Load struct {
	One     float64 `json:"one"`
	Five    float64 `json:"five"`
	Fifteen float64 `json:"fifteen"`
}
type Snapshot struct {
	SchemaVersion        int             `json:"schema_version"`
	ObservedAt           time.Time       `json:"observed_at"`
	ResourceID           string          `json:"resource_id"`
	Platform             Metric[string]  `json:"platform"`
	ThermalLevel         Metric[int]     `json:"thermal_level"` // Foundation NSProcessInfoThermalState (0..3).
	CPUSpeedLimitPercent Metric[float64] `json:"cpu_speed_limit_percent"`
	CPUCount             Metric[int]     `json:"cpu_count"`
	LoadAverage          Metric[Load]    `json:"load_average"`
	CPUUsagePercent      Metric[float64] `json:"cpu_usage_percent"`
	MemoryTotalBytes     Metric[uint64]  `json:"memory_total_bytes"`
	MemoryUsedBytes      Metric[uint64]  `json:"memory_used_bytes"`
	// MemoryFreePercent is the memory_pressure system-wide free percentage, not RAM free pages.
	MemoryFreePercent         Metric[float64] `json:"memory_free_percent"`
	MemoryPressure            Metric[string]  `json:"memory_pressure"`
	GPUUsagePercent           Metric[float64] `json:"gpu_usage_percent"`
	DockerCPUCount            Metric[int]     `json:"docker_cpu_count"`
	DockerMemoryBytes         Metric[uint64]  `json:"docker_memory_bytes"`
	DockerDataFreeBytes       Metric[uint64]  `json:"docker_data_free_bytes"`
	PowerSource               Metric[string]  `json:"power_source"`
	BatteryChargePercent      Metric[float64] `json:"battery_charge_percent"`
	BatteryCharging           Metric[bool]    `json:"battery_charging"`
	BatteryTemperatureCelsius Metric[float64] `json:"battery_temperature_celsius"`
	BatteryCycleCount         Metric[int]     `json:"battery_cycle_count"`
	BatteryHealthPercent      Metric[float64] `json:"battery_health_percent"`
	BatteryCondition          Metric[string]  `json:"battery_condition"`
	SleepEnabled              Metric[bool]    `json:"sleep_enabled"`
	SudoGrant                 Metric[string]  `json:"sudo_grant"`
	CPUDieTemperatureCelsius  Metric[float64] `json:"cpu_die_temperature_celsius"`
	GPUDieTemperatureCelsius  Metric[float64] `json:"gpu_die_temperature_celsius"`
	FanRPM                    Metric[float64] `json:"fan_rpm"`
	Warnings                  []string        `json:"warnings,omitempty"`
}
