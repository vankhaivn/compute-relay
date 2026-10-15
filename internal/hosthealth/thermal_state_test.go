package hosthealth

import (
	"fmt"
	"testing"
	"time"
)

func TestFoundationThermalStateAcceptsOnlyOneEnumScalar(t *testing.T) {
	for state := 0; state <= 3; state++ {
		samples := fixtures(t)
		samples[ThermalState] = Sample{Status: Available, Data: []byte(fmt.Sprintf("%d\n", state))}
		snapshot := Parse(testResource, time.Now(), samples)
		requireValue(t, snapshot.ThermalLevel, state)
		if err := snapshot.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, text := range []string{"", "-1\n", "4\n", "60\n", "255\n", "0.0\n", "01\n", "+0\n", "NaN\n", "nominal\n", "0\n1\n", "0\n0\n", "thermalState: 0\n", "0 extra\n", "0\nSERIAL=NEVER_PERSIST\n", "0\x00\n"} {
		samples := fixtures(t)
		samples[ThermalState] = Sample{Status: Available, Data: []byte(text)}
		snapshot := Parse(testResource, time.Now(), samples)
		if snapshot.ThermalLevel.Status != Unavailable || snapshot.ThermalLevel.Value != nil {
			t.Fatal("non-enum thermal state was accepted")
		}
	}
	for _, sample := range []Sample{{}, {Status: Unavailable, Reason: "command_failed", Data: []byte("0\n")}, {Status: Available, Reason: "output_limit", Data: []byte("0\n")}} {
		samples := fixtures(t)
		samples[ThermalState] = sample
		if snapshot := Parse(testResource, time.Now(), samples); snapshot.ThermalLevel.Status != Unavailable || snapshot.ThermalLevel.Value != nil {
			t.Fatal("failed thermal observation was accepted")
		}
	}
}

func TestFoundationThermalStateNeverFallsBackToRawXCPMCounter(t *testing.T) {
	at := time.Now()
	samples := fixtures(t)
	samples[Sysctl] = Sample{Status: Available, Data: append(append([]byte{}, samples[Sysctl].Data...), []byte("machdep.xcpm.cpu_thermal_level: 60\n")...)}
	samples[Thermal] = Sample{Status: Available, Data: []byte("CPU_Speed_Limit = 98\n")}
	samples[Top] = Sample{Status: Available, Data: []byte("CPU usage: 31.81% user, 0.00% sys, 68.19% idle\n")}
	snapshot := Parse(testResource, at, samples)
	requireValue(t, snapshot.ThermalLevel, 0)
	requireValue(t, snapshot.CPUSpeedLimitPercent, float64(98))
	requireValue(t, snapshot.CPUUsagePercent, 31.81)
	view := View{ResourceID: testResource, Latest: &snapshot, History: []Snapshot{snapshot}, Fresh: true, ReobservedSince: at}
	if decision := Evaluate(view, DefaultPolicy(), at); decision.Level != LevelOK || decision.RefuseNew || decision.Stop {
		t.Fatal("raw counter changed the Foundation guard decision", decision)
	}
	delete(samples, ThermalState)
	snapshot = Parse(testResource, at, samples)
	view.Latest, view.History = &snapshot, []Snapshot{snapshot}
	if snapshot.ThermalLevel.Status != Unavailable {
		t.Fatal("missing Foundation state fell back to raw XCPM")
	}
	if decision := Evaluate(view, DefaultPolicy(), at); decision.Level != LevelUnknown || !decision.RefuseNew || decision.Stop {
		t.Fatal("missing Foundation state did not stay unknown", decision)
	}
}

func TestThermalPolicyAndPatchStayWithinFoundationEnum(t *testing.T) {
	base := DefaultPolicy()
	if base.WarningThermalLevel != 1 || base.CriticalThermalLevel != 2 {
		t.Fatal("changing sources raised the conservative default thresholds")
	}
	for warning := -1; warning <= 4; warning++ {
		for critical := -1; critical <= 4; critical++ {
			policy := base
			policy.WarningThermalLevel, policy.CriticalThermalLevel = warning, critical
			ordered := warning >= 0 && critical <= 3 && warning <= critical
			if (policy.Validate() == nil) != ordered {
				t.Fatal("policy accepted a non-Foundation threshold or rejected an enum pair", warning, critical)
			}
			patch := PolicyPatch{WarningThermalLevel: &warning, CriticalThermalLevel: &critical}
			individual := warning >= 0 && warning <= 3 && critical >= 0 && critical <= 3
			if (patch.Validate() == nil) != individual {
				t.Fatal("patch accepted a non-Foundation threshold", warning, critical)
			}
			if _, err := base.Apply(&patch); (err == nil) != ordered {
				t.Fatal("policy application accepted a non-Foundation threshold", warning, critical)
			}
		}
	}
}
