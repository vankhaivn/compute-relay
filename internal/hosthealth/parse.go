package hosthealth

import (
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func blank[T any](status Status, reason string) Metric[T] {
	return Metric[T]{Status: status, Reason: reason}
}
func emptySnapshot(resource string, observed time.Time, status Status, reason string) Snapshot {
	return Snapshot{SchemaVersion: SchemaVersion, ObservedAt: observed.UTC(), ResourceID: resource,
		Platform: blank[string](status, reason), ThermalLevel: blank[int](status, reason), CPUSpeedLimitPercent: blank[float64](status, reason), CPUCount: blank[int](status, reason), LoadAverage: blank[Load](status, reason), CPUUsagePercent: blank[float64](status, reason), MemoryTotalBytes: blank[uint64](status, reason), MemoryUsedBytes: blank[uint64](status, reason), MemoryFreePercent: blank[float64](status, reason), MemoryPressure: blank[string](status, reason), GPUUsagePercent: blank[float64](status, reason), DockerCPUCount: blank[int](status, reason), DockerMemoryBytes: blank[uint64](status, reason), DockerDataFreeBytes: blank[uint64](status, reason), PowerSource: blank[string](status, reason), BatteryChargePercent: blank[float64](status, reason), BatteryCharging: blank[bool](status, reason), BatteryTemperatureCelsius: blank[float64](status, reason), BatteryCycleCount: blank[int](status, reason), BatteryHealthPercent: blank[float64](status, reason), BatteryCondition: blank[string](status, reason), SleepEnabled: blank[bool](status, reason), SudoGrant: blank[string](status, reason), CPUDieTemperatureCelsius: blank[float64](status, reason), GPUDieTemperatureCelsius: blank[float64](status, reason), FanRPM: blank[float64](status, reason)}
}
func sourceText(s Sample) string {
	if s.Status != Available || s.Reason != "" || len(s.Data) > MaxSourceBytes || strings.ContainsRune(string(s.Data), '\x00') {
		return ""
	}
	return string(s.Data)
}
func number(text, pattern string, max float64) (float64, bool) {
	matches := regexp.MustCompile(pattern).FindAllStringSubmatch(text, -1)
	if len(matches) == 0 {
		return 0, false
	}
	var n float64
	for i, m := range matches {
		v, e := strconv.ParseFloat(m[1], 64)
		if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > max {
			return 0, false
		}
		if i > 0 && n != v {
			return 0, false
		}
		n = v
	}
	return n, true
}

// Apple top prints integer hundredths without zero padding (globalstats.c).
// For example, its "0.9" represents 0.09%, not 0.9%.
func topPercent(text string) (float64, bool) {
	if !regexp.MustCompile(`^[0-9]{1,3}\.[0-9]{1,2}$`).MatchString(text) {
		return 0, false
	}
	parts := strings.SplitN(text, ".", 2)
	whole, _ := strconv.Atoi(parts[0])
	hundredths, _ := strconv.Atoi(parts[1])
	n := float64(whole) + float64(hundredths)/100
	return n, n <= 100
}
func numeric(s Sample, pattern string, max float64) Metric[float64] {
	if n, ok := number(sourceText(s), pattern, max); ok {
		return value(n)
	}
	return missing[float64](sampleReason(s))
}
func integer(s Sample, pattern string, max float64) Metric[int] {
	m := numeric(s, pattern, max)
	if m.Value != nil && *m.Value == math.Trunc(*m.Value) {
		return value(int(*m.Value))
	}
	return missing[int](sampleReason(s))
}
func sampleReason(s Sample) string {
	switch s.Reason {
	case "output_limit", "collection_timeout", "command_failed":
		return s.Reason
	}
	if s.Status != Available {
		return "source_unavailable"
	}
	return "source_missing_or_invalid"
}

// Parse accepts only bounded, complete successful samples. Every output string is an
// allowlisted constant; arbitrary host properties cannot enter a snapshot.
func Parse(resourceID string, observed time.Time, samples map[Source]Sample) Snapshot {
	if !resourcePattern.MatchString(resourceID) {
		return emptySnapshot("", observed, Unavailable, "platform_unknown")
	}
	platform := samples[Platform]
	if !supported(samples) {
		p := strings.TrimSpace(sourceText(platform))
		if regexp.MustCompile(`^(Linux [a-zA-Z0-9_]+|Darwin arm64)$`).MatchString(p) || intelProcess(platform) && strings.TrimSpace(sourceText(samples[NativeArchitecture])) == "16777228" {
			s := emptySnapshot(resourceID, observed, Unsupported, "platform_unsupported")
			s.Platform = value("unsupported")
			return s
		}
		return emptySnapshot(resourceID, observed, Unavailable, "platform_unknown")
	}
	s := emptySnapshot(resourceID, observed, Unavailable, "source_missing_or_invalid")
	s.Platform = value("macos_intel")
	sys := samples[Sysctl]
	s.CPUCount = integer(sys, `(?m)^hw\.ncpu:\s*([0-9]+)\s*$`, 65536)
	if s.CPUCount.Value != nil && *s.CPUCount.Value == 0 {
		s.CPUCount = missing[int]("source_missing_or_invalid")
	}
	s.ThermalLevel = integer(sys, `(?m)^machdep\.xcpm\.cpu_thermal_level:\s*([0-9]+)\s*$`, 255)
	s.CPUSpeedLimitPercent = numeric(samples[Thermal], `(?m)^\s*CPU_Speed_Limit\s*=\s*([0-9]+)\s*$`, 100)
	if n, ok := number(sourceText(sys), `(?m)^hw\.memsize:\s*([0-9]+)\s*$`, 1<<53); ok && n > 0 {
		s.MemoryTotalBytes = value(uint64(n))
	}
	loads := regexp.MustCompile(`(?m)^vm\.loadavg:\s*\{ ([0-9.]+) ([0-9.]+) ([0-9.]+) \}\s*$`).FindStringSubmatch(sourceText(sys))
	if len(loads) == 4 {
		a, aok := number(loads[1], `^([0-9.]+)$`, 1e6)
		b, bok := number(loads[2], `^([0-9.]+)$`, 1e6)
		c, cok := number(loads[3], `^([0-9.]+)$`, 1e6)
		if aok && bok && cok {
			s.LoadAverage = value(Load{a, b, c})
		}
	}
	top := sourceText(samples[Top])
	cpus := regexp.MustCompile(`(?m)^CPU usage: ([0-9.]+)% user, ([0-9.]+)% sys, ([0-9.]+)% idle\s*$`).FindAllStringSubmatch(top, -1)
	if len(cpus) > 0 {
		last := cpus[len(cpus)-1]
		u, uok := topPercent(last[1])
		sy, sok := topPercent(last[2])
		idle, iok := topPercent(last[3])
		if uok && sok && iok && math.Abs(u+sy+idle-100) < 0.2 {
			s.CPUUsagePercent = value(u + sy)
		}
	}
	mem := regexp.MustCompile(`(?m)^PhysMem: ([0-9.]+)([BKMGTP]) used \(`).FindAllStringSubmatch(top, -1)
	if len(mem) > 0 {
		m := mem[len(mem)-1]
		n, ok := number(m[1], `^([0-9.]+)$`, 1<<53)
		power := map[string]int{"B": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5}[m[2]]
		n *= math.Pow(1024, float64(power))
		if ok && n <= 1<<53 && (s.MemoryTotalBytes.Value == nil || n <= float64(*s.MemoryTotalBytes.Value)) {
			s.MemoryUsedBytes = value(uint64(n))
		}
	}
	s.MemoryFreePercent = numeric(samples[MemoryPressure], `(?m)^System-wide memory free percentage:\s*([0-9.]+)%\s*$`, 100)
	s.MemoryPressure = missing[string]("source_not_exposed")
	s.GPUUsagePercent = numeric(samples[GPU], `(?m)^gpu_utilization_percent = ([0-9.]+)\s*$`, 100)
	s.DockerDataFreeBytes = missing[uint64]("docker_vm_free_space_not_exposed")
	var docker struct {
		NCPU     int
		MemTotal uint64
	}
	d := sourceText(samples[Docker])
	if json.Unmarshal([]byte(d), &docker) == nil && docker.NCPU > 0 && docker.NCPU <= 65536 && docker.MemTotal > 0 && docker.MemTotal <= 1<<53 {
		s.DockerCPUCount = value(docker.NCPU)
		s.DockerMemoryBytes = value(docker.MemTotal)
	}
	power := sourceText(samples[Power])
	if strings.HasPrefix(power, "Now drawing from 'AC Power'\n") {
		s.PowerSource = value("ac")
	}
	if strings.HasPrefix(power, "Now drawing from 'Battery Power'\n") {
		s.PowerSource = value("battery")
	}
	s.BatteryChargePercent = numeric(samples[Power], `(?m)^\s*-InternalBattery-[0-9]+ \(id=[0-9]+\)\s+([0-9.]+)%;`, 100)
	state := regexp.MustCompile(`(?m)^\s*-InternalBattery-[0-9]+ \(id=[0-9]+\)\s+[0-9.]+%; (charging|charged|discharging|not charging|AC attached);`).FindStringSubmatch(power)
	if len(state) == 2 {
		s.BatteryCharging = value(state[1] == "charging")
	}
	battery := samples[Battery]
	s.BatteryCycleCount = integer(battery, `(?m)^\s*CycleCount\s*=\s*([0-9]+)\s*$`, 100000)
	full, ok := number(sourceText(battery), `(?m)^\s*AppleRawMaxCapacity\s*=\s*([0-9]+)\s*$`, 1e7)
	design, dok := number(sourceText(battery), `(?m)^\s*DesignCapacity\s*=\s*([0-9]+)\s*$`, 1e7)
	if ok && dok && design > 0 && full > 0 && full/design <= 2 {
		s.BatteryHealthPercent = value(full / design * 100)
	}
	// AppleSmartBattery.cpp preserves SBS Temperature(0x08) units on Intel OSX:
	// https://github.com/apple-oss-distributions/PowerManagement/blob/main/AppleSmartBatteryManager/AppleSmartBattery.cpp
	// SBS section 5.1.9 specifies tenths kelvin: https://sbs-forum.org/specs/sbdata10.pdf
	rawTemperature, tok := number(sourceText(battery), `(?m)^\s*Temperature\s*=\s*([0-9]+)\s*$`, 3731)
	if tok && rawTemperature >= 2231 {
		s.BatteryTemperatureCelsius = value(rawTemperature/10 - 273.15)
	}
	s.BatteryCondition = missing[string]("source_not_exposed")
	if s.PowerSource.Value != nil {
		heading := "AC Power:"
		if *s.PowerSource.Value == "battery" {
			heading = "Battery Power:"
		}
		text := sourceText(samples[Sleep])
		active := false
		var sleeps []int
		for _, line := range strings.Split(text, "\n") {
			if strings.HasSuffix(strings.TrimSpace(line), "Power:") {
				active = strings.TrimSpace(line) == heading
				continue
			}
			if active {
				fields := strings.Fields(line)
				if len(fields) == 2 && fields[0] == "sleep" {
					n, e := strconv.Atoi(fields[1])
					if e == nil && n >= 0 {
						sleeps = append(sleeps, n)
					}
				}
			}
		}
		if len(sleeps) == 1 {
			s.SleepEnabled = value(sleeps[0] > 0)
		}
	}
	grant := ClassifyGrant(samples[Sudo])
	s.SudoGrant = value(grant)
	if grant == "broader" {
		s.Warnings = append(s.Warnings, "sudo_grant_broader")
	}
	if grant == "absent" {
		s.CPUDieTemperatureCelsius = missing[float64]("sudo_grant_absent")
		s.GPUDieTemperatureCelsius = missing[float64]("sudo_grant_absent")
		s.FanRPM = missing[float64]("sudo_grant_absent")
	} else {
		pm := samples[Powermetrics]
		s.CPUDieTemperatureCelsius = numeric(pm, `(?m)^CPU die temperature:\s*([0-9.]+) C\s*$`, 150)
		s.GPUDieTemperatureCelsius = numeric(pm, `(?m)^GPU die temperature:\s*([0-9.]+) C\s*$`, 150)
		s.FanRPM = numeric(pm, `(?m)^Fan:\s*([0-9.]+) rpm\s*$`, 30000)
	}
	return s
}

// ClassifyGrant accepts narrow only for one unambiguous passwordless root rule.
// Any successful listing with other or ambiguous grants is conservatively broader.
func ClassifyGrant(sample Sample) string {
	text := sourceText(sample)
	if text == "" {
		return "absent"
	}
	var rules []string
	started := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "(") {
			started = true
			rules = append(rules, line)
			continue
		}
		// A wrapped command, PASSWD tag or any unexplained trailing text makes the
		// full command section ambiguous, even if its first line looks narrow.
		if started {
			return "broader"
		}
	}
	if len(rules) == 0 {
		return "broader"
	}
	if len(rules) == 1 {
		r := rules[0]
		for _, prefix := range []string{"(root) NOPASSWD: ", "(ALL) NOPASSWD: ", "(ALL : ALL) NOPASSWD: "} {
			if strings.HasPrefix(r, prefix) {
				cmd := strings.TrimPrefix(r, prefix)
				if cmd == "/usr/bin/powermetrics" || cmd == "/usr/bin/powermetrics --samplers smc -n 1 -i 1000" {
					return "narrow"
				}
			}
		}
	}
	return "broader"
}
