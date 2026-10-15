package hosthealth

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

const testResource = "resource_" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func fixtures(t *testing.T) map[Source]Sample {
	t.Helper()
	out := map[Source]Sample{}
	for _, source := range []Source{Platform, NativeArchitecture, Thermal, Power, Sleep, Battery, Sysctl, Top, MemoryPressure, Docker, Powermetrics} {
		data, err := os.ReadFile("testdata/" + string(source) + ".txt")
		if err != nil {
			t.Fatal(err)
		}
		out[source] = Sample{Status: Available, Data: data}
	}
	out[Sudo] = Sample{Status: Available, Data: []byte("User REDACTED may run the following commands:\n    (root) NOPASSWD: /usr/bin/powermetrics\n")}
	return out
}
func requireValue[T comparable](t *testing.T, m Metric[T], want T) {
	t.Helper()
	if m.Status != Available || m.Value == nil || *m.Value != want {
		t.Fatalf("metric = %+v, want %v", m, want)
	}
}
func TestRecordedSnapshot(t *testing.T) {
	f := fixtures(t)
	now := time.Now()
	s := Parse(testResource, now, f)
	if s.SchemaVersion != 1 || !s.ObservedAt.Equal(now) || s.ResourceID != testResource {
		t.Fatal("snapshot identity")
	}
	requireValue(t, s.Platform, "macos_intel")
	requireValue(t, s.ThermalLevel, 0)
	requireValue(t, s.CPUSpeedLimitPercent, 100.0)
	requireValue(t, s.CPUCount, 16)
	requireValue(t, s.LoadAverage, Load{0.66, 0.94, 1.15})
	requireValue(t, s.CPUUsagePercent, 3.52)
	requireValue(t, s.MemoryTotalBytes, uint64(64<<30))
	requireValue(t, s.MemoryUsedBytes, uint64(28<<30))
	requireValue(t, s.MemoryFreePercent, 93.0)
	requireValue(t, s.DockerCPUCount, 16)
	requireValue(t, s.DockerMemoryBytes, uint64(8320425984))
	requireValue(t, s.PowerSource, "ac")
	requireValue(t, s.BatteryChargePercent, 100.0)
	requireValue(t, s.BatteryCharging, false)
	requireValue(t, s.BatteryCycleCount, 289)
	if s.BatteryHealthPercent.Value == nil || math.Abs(*s.BatteryHealthPercent.Value-7559.0/8790*100) > 1e-9 {
		t.Fatal("battery capacity")
	}
	if s.BatteryTemperatureCelsius.Value == nil || math.Abs(*s.BatteryTemperatureCelsius.Value-28.35) > 1e-9 {
		t.Fatal("Intel temperature must be decikelvin")
	}
	requireValue(t, s.SleepEnabled, false)
	requireValue(t, s.CPUDieTemperatureCelsius, 62.5)
	requireValue(t, s.GPUDieTemperatureCelsius, 55.25)
	requireValue(t, s.FanRPM, 2200.0)
	if s.DockerDataFreeBytes.Status != Unavailable || s.GPUUsagePercent.Status != Unavailable || s.BatteryCondition.Status != Unavailable || s.MemoryPressure.Status != Unavailable {
		t.Fatal("unexposed metrics must remain unavailable")
	}
}
func TestAbsentGrantDoesNotDegradeOtherMetrics(t *testing.T) {
	f := fixtures(t)
	f[Sudo] = Sample{Status: Unavailable, Reason: "command_failed"}
	s := Parse(testResource, time.Now(), f)
	requireValue(t, s.SudoGrant, "absent")
	requireValue(t, s.CPUCount, 16)
	if s.CPUDieTemperatureCelsius.Status != Unavailable || s.CPUDieTemperatureCelsius.Reason != "sudo_grant_absent" {
		t.Fatal(s.CPUDieTemperatureCelsius)
	}
}
func TestGrantClassifier(t *testing.T) {
	for _, tt := range []struct{ data, want string }{
		{"", "absent"}, {"sudo: a password is required", "broader"},
		{"    (root) NOPASSWD: /usr/bin/powermetrics", "narrow"},
		{"    (ALL : ALL) NOPASSWD: /usr/bin/powermetrics --samplers smc -n 1 -i 1000", "narrow"},
		{"    (root) NOPASSWD: /usr/bin/powermetrics *", "broader"},
		{"    (ALL) NOPASSWD: ALL", "broader"},
		{"    (root) NOPASSWD: /usr/bin/powermetrics, /bin/sh", "broader"},
		{"    (root) NOPASSWD: /usr/bin/powermetrics\n    (ALL) ALL", "broader"},
		{"    (root) /usr/bin/powermetrics", "broader"},
		{"    (root) NOPASSWD: /usr/bin/powermetrics\n        /bin/sh", "broader"},
		{"    (root) NOPASSWD: /usr/bin/powermetrics,\n        /bin/sh", "broader"},
		{"    (root) NOPASSWD: /usr/bin/powermetrics\n        PASSWD: /bin/sh", "broader"},
		{"    (nobody) NOPASSWD: /usr/bin/powermetrics", "broader"},
		{"    (root) NOPASSWD: !/usr/bin/powermetrics", "broader"},
		{"malformed successful listing", "broader"},
	} {
		t.Run(tt.want+tt.data, func(t *testing.T) {
			if got := ClassifyGrant(Sample{Status: Available, Data: []byte(tt.data)}); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}
func TestUnsupportedAndUnknownPlatform(t *testing.T) {
	for _, platform := range []string{"Linux x86_64", "Linux aarch64", "Darwin arm64"} {
		f := fixtures(t)
		f[Platform] = Sample{Status: Available, Data: []byte(platform)}
		s := Parse(testResource, time.Now(), f)
		if s.ThermalLevel.Status != Unsupported || s.BatteryChargePercent.Status != Unsupported {
			t.Fatal("unsupported platform accepted")
		}
	}
	for _, sample := range []Sample{{}, {Status: Unavailable, Reason: "collection_timeout"}, {Status: Available, Data: []byte("Darwin x86_64\nHOSTILE")}} {
		f := fixtures(t)
		f[Platform] = sample
		s := Parse(testResource, time.Now(), f)
		if s.Platform.Status != Unavailable || s.ThermalLevel.Status != Unavailable {
			t.Fatal("unknown platform must fail closed")
		}
	}
}
func TestMissingTruncatedAndHostileSources(t *testing.T) {
	for _, sample := range []Sample{{}, {Status: Available, Data: []byte("hw.ncpu: nan\nhw.memsize: 999999999999999999999999\nmachdep.xcpm.cpu_thermal_level: -1\n")}, {Status: Unavailable, Reason: "output_limit", Data: []byte("hw.ncpu: 16\n")}, {Status: Available, Data: []byte("hw.ncpu: 16\nhw.ncpu: 1\n")}, {Status: Available, Data: []byte("hw.ncpu: 16\x00\n")}, {Status: Available, Data: []byte(strings.Repeat("a", MaxSourceBytes+1))}} {
		f := fixtures(t)
		f[Sysctl] = sample
		s := Parse(testResource, time.Now(), f)
		if s.CPUCount.Status != Unavailable {
			t.Fatal("malformed sample accepted")
		}
	}
	f := fixtures(t)
	f[Battery] = Sample{Status: Available, Data: []byte("Temperature = 999999\nCycleCount = -4\nDesignCapacity = 0\nAppleRawMaxCapacity = 7559\n")}
	s := Parse(testResource, time.Now(), f)
	if s.BatteryTemperatureCelsius.Status != Unavailable || s.BatteryCycleCount.Status != Unavailable || s.BatteryHealthPercent.Status != Unavailable {
		t.Fatal("malformed battery accepted")
	}
	f = fixtures(t)
	for source, sample := range f {
		sample.Data = append(sample.Data, []byte("\nSERIAL_HOSTILE=NEVER_PERSIST\nIOPlatformUUID=NEVER_PERSIST\n")...)
		if source != Platform {
			f[source] = sample
		}
	}
	s = Parse(testResource, time.Now(), f)
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "NEVER_PERSIST") {
		t.Fatal("source identity leaked")
	}
}
func TestBatteryPowerAndSleep(t *testing.T) {
	f := fixtures(t)
	f[Power] = Sample{Status: Available, Data: []byte("Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1)\t17%; discharging; 1:00 remaining present: true\n")}
	s := Parse(testResource, time.Now(), f)
	requireValue(t, s.PowerSource, "battery")
	requireValue(t, s.BatteryChargePercent, 17.0)
	requireValue(t, s.BatteryCharging, false)
	requireValue(t, s.SleepEnabled, true)
}

func TestGPUWhenExposed(t *testing.T) {
	f := fixtures(t)
	f[GPU] = Sample{Status: Available, Data: []byte("gpu_utilization_percent = 42\n")}
	s := Parse(testResource, time.Now(), f)
	requireValue(t, s.GPUUsagePercent, 42.0)
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestSnapshotValidation(t *testing.T) {
	valid := Parse(testResource, time.Now(), fixtures(t))
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.ResourceID = "host-name" }, func(s *Snapshot) { s.SchemaVersion = 2 }, func(s *Snapshot) { s.ObservedAt = time.Time{} },
		func(s *Snapshot) { s.CPUCount.Status = Unavailable }, func(s *Snapshot) { s.CPUCount.Reason = "HOSTILE" },
		func(s *Snapshot) { s.Platform = value("HOSTILE") }, func(s *Snapshot) { s.PowerSource = value("HOSTILE") },
		func(s *Snapshot) { s.CPUUsagePercent = value(math.NaN()) }, func(s *Snapshot) { s.BatteryChargePercent = value(101.0) },
		func(s *Snapshot) { s.Warnings = []string{"HOSTILE"} }, func(s *Snapshot) { s.BatteryCondition = value("arbitrary host string") },
	} {
		s := valid
		mutate(&s)
		if err := s.Validate(); err == nil {
			t.Fatal("malformed snapshot validated")
		}
	}
}

func TestEverySourceFailureRemovesItsValues(t *testing.T) {
	for _, source := range []Source{Thermal, Power, Sleep, Battery, Sysctl, Top, MemoryPressure, Docker, Powermetrics, GPU} {
		t.Run(string(source), func(t *testing.T) {
			f := fixtures(t)
			f[source] = Sample{Status: Unavailable, Reason: "output_limit", Data: f[source].Data}
			s := Parse(testResource, time.Now(), f)
			switch source {
			case Thermal:
				if s.CPUSpeedLimitPercent.Value != nil {
					t.Fatal("partial thermal")
				}
			case Power:
				if s.PowerSource.Value != nil || s.BatteryChargePercent.Value != nil || s.SleepEnabled.Value != nil {
					t.Fatal("partial power")
				}
			case Sleep:
				if s.SleepEnabled.Value != nil {
					t.Fatal("partial sleep")
				}
			case Battery:
				if s.BatteryTemperatureCelsius.Value != nil || s.BatteryHealthPercent.Value != nil || s.BatteryCycleCount.Value != nil {
					t.Fatal("partial battery")
				}
			case Sysctl:
				if s.CPUCount.Value != nil || s.ThermalLevel.Value != nil || s.MemoryTotalBytes.Value != nil || s.LoadAverage.Value != nil {
					t.Fatal("partial sysctl")
				}
			case Top:
				if s.CPUUsagePercent.Value != nil || s.MemoryUsedBytes.Value != nil {
					t.Fatal("partial top")
				}
			case MemoryPressure:
				if s.MemoryFreePercent.Value != nil {
					t.Fatal("partial pressure")
				}
			case Docker:
				if s.DockerCPUCount.Value != nil || s.DockerMemoryBytes.Value != nil {
					t.Fatal("partial docker")
				}
			case Powermetrics:
				if s.CPUDieTemperatureCelsius.Value != nil || s.GPUDieTemperatureCelsius.Value != nil || s.FanRPM.Value != nil {
					t.Fatal("partial powermetrics")
				}
			case GPU:
				if s.GPUUsagePercent.Value != nil {
					t.Fatal("partial gpu")
				}
			}
			if err := s.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestUntrustedIdentityAndReasonsNeverSurvive(t *testing.T) {
	s := Parse("host-NO_PERSIST", time.Now(), fixtures(t))
	if strings.Contains(s.ResourceID, "NO_PERSIST") || s.Platform.Status != Unavailable {
		t.Fatal("untrusted identity leaked")
	}
	f := fixtures(t)
	f[Sysctl] = Sample{Status: Unavailable, Reason: "NO_PERSIST"}
	s = Parse(testResource, time.Now(), f)
	encoded, _ := json.Marshal(s)
	if strings.Contains(string(encoded), "NO_PERSIST") {
		t.Fatal("untrusted source reason leaked")
	}
}

func TestNativeArchitectureQualification(t *testing.T) {
	for _, tt := range []struct {
		name   string
		sample Sample
		want   Status
	}{
		{"native_intel", Sample{Status: Available, Data: []byte("7\n")}, Available},
		{"rosetta_arm", Sample{Status: Available, Data: []byte("16777228\n")}, Unsupported},
		{"missing", Sample{}, Unavailable},
		{"failed", Sample{Status: Unavailable, Reason: "command_failed", Data: []byte("7\n")}, Unavailable},
		{"truncated", Sample{Status: Unavailable, Reason: "output_limit", Data: []byte("7\n")}, Unavailable},
		{"malformed", Sample{Status: Available, Data: []byte("hw.cputype: 7\n")}, Unavailable},
		{"conflicting", Sample{Status: Available, Data: []byte("7\n16777228\n")}, Unavailable},
		{"hostile", Sample{Status: Available, Data: []byte("7\nHOST_IDENTITY_NEVER_PERSIST")}, Unavailable},
		{"negative", Sample{Status: Available, Data: []byte("-1\n")}, Unavailable},
		{"unknown_scalar", Sample{Status: Available, Data: []byte("2\n")}, Unavailable},
		{"zero_is_not_native_intel", Sample{Status: Available, Data: []byte("0\n")}, Unavailable},
		{"nul", Sample{Status: Available, Data: []byte("7\x00")}, Unavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := fixtures(t)
			f[NativeArchitecture] = tt.sample
			s := Parse(testResource, time.Now(), f)
			if s.ThermalLevel.Status != tt.want {
				t.Fatalf("thermal status=%s want %s", s.ThermalLevel.Status, tt.want)
			}
			if tt.want == Unavailable && (s.Platform.Status != Unavailable || s.Platform.Reason != "platform_unknown") {
				t.Fatal("uncertain native probe must fail closed")
			}
			if tt.want == Unsupported {
				requireValue(t, s.Platform, "unsupported")
				if s.SudoGrant.Status != Unsupported {
					t.Fatal("Rosetta accepted")
				}
			}
			if err := s.Validate(); err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(s)
			if strings.Contains(string(encoded), "HOST_IDENTITY_NEVER_PERSIST") {
				t.Fatal("probe identity leaked")
			}
		})
	}
}

func TestTopUnpaddedHundredths(t *testing.T) {
	f := fixtures(t)
	data, err := os.ReadFile("testdata/top-unpadded.txt")
	if err != nil {
		t.Fatal(err)
	}
	f[Top] = Sample{Status: Available, Data: data}
	s := Parse(testResource, time.Now(), f)
	requireValue(t, s.CPUUsagePercent, 1.59)
	for _, tt := range []struct {
		text  string
		want  float64
		valid bool
	}{
		{"0.9", 0.09, true}, {"0.09", 0.09, true}, {"0.0", 0, true},
		{"100.0", 100, true}, {"99.99", 99.99, true},
		{"100.1", 0, false}, {"0.900", 0, false}, {"0", 0, false},
		{".9", 0, false}, {"-1.0", 0, false}, {"1.2.3", 0, false},
	} {
		t.Run(tt.text, func(t *testing.T) {
			got, ok := topPercent(tt.text)
			if ok != tt.valid || ok && got != tt.want {
				t.Fatalf("got %v, %v", got, ok)
			}
		})
	}
	f[Top] = Sample{Status: Available, Data: []byte("CPU usage: 10.0% user, 10.0% sys, 99.0% idle\n")}
	if s := Parse(testResource, time.Now(), f); s.CPUUsagePercent.Status != Unavailable {
		t.Fatal("invalid total must remain unavailable")
	}
}
