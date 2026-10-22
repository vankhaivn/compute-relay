package hosthealth

import (
	"strings"
	"testing"
	"time"
)

func TestHealthParseRetainsCoreCommandStep(t *testing.T) {
	id := "resource_" + strings.Repeat("a", 64)
	for _, tc := range []struct {
		source Source
		want   string
	}{{Platform, "health_platform_command_failed"}, {NativeArchitecture, "health_native_architecture_command_failed"}, {ThermalState, "health_thermal_state_command_failed"}, {Thermal, "health_cpu_speed_command_failed"}, {Power, "health_power_command_failed"}} {
		samples := map[Source]Sample{Platform: {Status: Available, Data: []byte("Darwin x86_64")}, NativeArchitecture: {Status: Available, Data: []byte("7")}}
		samples[tc.source] = Sample{Status: Unavailable, Reason: "command_failed"}
		got := Parse(id, time.Now(), samples)
		if got.Cause != tc.want || got.Validate() != nil {
			t.Fatal("command cause lost", got.Cause, got.Validate())
		}
	}
	got := Parse(id, time.Now(), nil)
	got.Cause = "private/path canary"
	if got.Validate() == nil {
		t.Fatal("unsafe cause accepted")
	}
}
