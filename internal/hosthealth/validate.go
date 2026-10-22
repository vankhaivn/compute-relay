package hosthealth

import (
	"errors"
	"math"
	"regexp"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

var ErrSnapshot = errors.New("invalid host health snapshot")
var resourcePattern = regexp.MustCompile(`^resource_[0-9a-f]{64}$`)

func safeReason(reason string) bool {
	switch reason {
	case "source_unavailable", "source_missing_or_invalid", "source_not_exposed", "docker_vm_free_space_not_exposed", "platform_unknown", "platform_unsupported", "sudo_grant_absent", "output_limit", "collection_timeout", "command_failed":
		return true
	}
	return false
}
func (m Metric[T]) metricStatus() Status { return m.Status }
func (m Metric[T]) validState() bool {
	switch m.Status {
	case Available:
		return m.Value != nil && m.Reason == ""
	case Unavailable, Unsupported:
		return m.Value == nil && safeReason(m.Reason)
	}
	return false
}
func metricRange[T any](m Metric[T], valid func(T) bool) bool {
	return m.Value == nil || valid(*m.Value)
}
func between(min, max float64) func(float64) bool {
	return func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= min && v <= max }
}
func oneOf(allowed ...string) func(string) bool {
	return func(v string) bool {
		for _, a := range allowed {
			if v == a {
				return true
			}
		}
		return false
	}
}

// Validate protects persistence and API boundaries, including snapshots decoded from JSON.
// Errors contain no source text or caller-supplied identifiers.
func (s Snapshot) Validate() error {
	if s.Cause != "" && !domain.ValidReason(s.Cause) {
		return ErrSnapshot
	}
	if s.Platform.Status == Unsupported || s.SchemaVersion != SchemaVersion || !resourcePattern.MatchString(s.ResourceID) || s.ObservedAt.IsZero() || s.ObservedAt.Year() < 2000 || s.ObservedAt.Year() > 9999 {
		return ErrSnapshot
	}
	metrics := []interface {
		validState() bool
		metricStatus() Status
	}{s.Platform, s.ThermalLevel, s.CPUSpeedLimitPercent, s.CPUCount, s.LoadAverage, s.CPUUsagePercent, s.MemoryTotalBytes, s.MemoryUsedBytes, s.MemoryFreePercent, s.MemoryPressure, s.GPUUsagePercent, s.DockerCPUCount, s.DockerMemoryBytes, s.DockerDataFreeBytes, s.PowerSource, s.BatteryChargePercent, s.BatteryCharging, s.BatteryTemperatureCelsius, s.BatteryCycleCount, s.BatteryHealthPercent, s.BatteryCondition, s.SleepEnabled, s.SudoGrant, s.CPUDieTemperatureCelsius, s.GPUDieTemperatureCelsius, s.FanRPM}
	for _, m := range metrics {
		if !m.validState() {
			return ErrSnapshot
		}
	}
	for _, m := range metrics[1:] {
		if s.Platform.Value == nil && m.metricStatus() != Unavailable {
			return ErrSnapshot
		}
		if s.Platform.Value != nil && *s.Platform.Value == "unsupported" && m.metricStatus() != Unsupported {
			return ErrSnapshot
		}
		if s.Platform.Value != nil && *s.Platform.Value == "macos_intel" && m.metricStatus() == Unsupported {
			return ErrSnapshot
		}
	}

	if !metricRange(s.Platform, oneOf("macos_intel", "unsupported")) || !metricRange(s.PowerSource, oneOf("ac", "battery")) || !metricRange(s.SudoGrant, oneOf("absent", "narrow", "broader")) || !metricRange(s.MemoryPressure, oneOf("normal", "warning", "critical")) || !metricRange(s.BatteryCondition, oneOf("normal", "service_recommended", "replace_soon", "replace_now", "service_battery")) {
		return ErrSnapshot
	}
	for _, m := range []Metric[float64]{s.CPUSpeedLimitPercent, s.CPUUsagePercent, s.MemoryFreePercent, s.GPUUsagePercent, s.BatteryChargePercent} {
		if !metricRange(m, between(0, 100)) {
			return ErrSnapshot
		}
	}
	if !metricRange(s.BatteryHealthPercent, between(0, 200)) || !metricRange(s.BatteryTemperatureCelsius, between(-50.05, 100)) || !metricRange(s.CPUDieTemperatureCelsius, between(0, 150)) || !metricRange(s.GPUDieTemperatureCelsius, between(0, 150)) || !metricRange(s.FanRPM, between(0, 30000)) {
		return ErrSnapshot
	}
	if !metricRange(s.CPUCount, func(v int) bool { return v > 0 && v <= 65536 }) || !metricRange(s.DockerCPUCount, func(v int) bool { return v > 0 && v <= 65536 }) || !metricRange(s.ThermalLevel, func(v int) bool { return v >= 0 && v <= 3 }) || !metricRange(s.BatteryCycleCount, func(v int) bool { return v >= 0 && v <= 100000 }) {
		return ErrSnapshot
	}
	for _, m := range []Metric[uint64]{s.MemoryTotalBytes, s.MemoryUsedBytes, s.DockerMemoryBytes, s.DockerDataFreeBytes} {
		if !metricRange(m, func(v uint64) bool { return v <= 1<<53 }) {
			return ErrSnapshot
		}
	}
	if !metricRange(s.LoadAverage, func(v Load) bool {
		return between(0, 1e6)(v.One) && between(0, 1e6)(v.Five) && between(0, 1e6)(v.Fifteen)
	}) {
		return ErrSnapshot
	}
	if len(s.Warnings) > 1 || len(s.Warnings) == 1 && s.Warnings[0] != "sudo_grant_broader" {
		return ErrSnapshot
	}
	return nil
}
