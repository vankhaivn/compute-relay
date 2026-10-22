package contracts

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

func healthContract(t *testing.T) (*jsonschema.Schema, map[string]any) {
	t.Helper()
	root := repositoryRoot(t)
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	schema, err := compiler.Compile(filepath.Join(root, "api/schemas/connection-health.v1alpha1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := readJSONValue(filepath.Join(root, "api/examples/connection-health.ok.valid.json"))
	if err != nil {
		t.Fatal(err)
	}
	return schema, value.(map[string]any)
}
func TestHealthContractRejectsUnknownAndPrivateFields(t *testing.T) {
	schema, example := healthContract(t)
	for _, tt := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"private_target", func(v map[string]any) { v["credential_ref"] = "vault:private" }},
		{"private_view", func(v map[string]any) { v["fresh"] = true }},
		{"unknown_metric", func(v map[string]any) { v["latest"].(map[string]any)["serial_number"] = "private" }},
		{"unknown_reason", func(v map[string]any) { v["guard"].(map[string]any)["reasons"] = []any{"unrecognized_reason"} }},
		{"unknown_diagnostic", func(v map[string]any) { v["guard"].(map[string]any)["diagnostics"] = []any{"private_error"} }},
		{"unknown_level", func(v map[string]any) { v["guard"].(map[string]any)["level"] = "healthy" }},
		{"unknown_metric_reason", func(v map[string]any) {
			v["latest"].(map[string]any)["gpu_usage_percent"] = map[string]any{"status": "unavailable", "reason": "private stderr"}
		}},
		{"value_and_unavailable", func(v map[string]any) {
			v["latest"].(map[string]any)["gpu_usage_percent"] = map[string]any{"status": "unavailable", "reason": "source_not_exposed", "value": 42}
		}},
		{"available_without_value", func(v map[string]any) {
			v["latest"].(map[string]any)["gpu_usage_percent"] = map[string]any{"status": "available"}
		}},
		{"unbounded_history", func(v map[string]any) {
			history := make([]any, hosthealth.HistoryMaxRows+1)
			for i := range history {
				history[i] = v["latest"]
			}
			v["history"] = history
		}},
		{"unbounded_duration", func(v map[string]any) {
			v["policy"].(map[string]any)["critical_duration_seconds"] = hosthealth.MaxCriticalDurationSeconds + 1
		}},
		{"incomplete_policy", func(v map[string]any) { delete(v["policy"].(map[string]any), "warning_thermal_level") }},
		{"unsupported_stop", func(v map[string]any) { g := v["guard"].(map[string]any); g["level"] = "unsupported"; g["stop"] = true }},
		{"unknown_stop", func(v map[string]any) {
			g := v["guard"].(map[string]any)
			g["level"] = "unknown"
			g["refuse_new"] = true
			g["stop"] = true
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(example)
			if err != nil {
				t.Fatal(err)
			}
			var value map[string]any
			if err = json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
			tt.mutate(value)
			if err = schema.Validate(value); err == nil {
				t.Fatal("invalid health response accepted")
			}
		})
	}
}

func TestHealthContractAcceptsOnlyClosedCollectionCauses(t *testing.T) {
	schema, value := healthContract(t)
	for _, reason := range []string{"host_unavailable", "health_thermal_state_command_failed", "private/path", "private error", "", strings.Repeat("a", 65)} {
		value["latest"].(map[string]any)["cause"] = reason
		err := schema.Validate(value)
		wantValid := reason == "host_unavailable" || reason == "health_thermal_state_command_failed"
		if (err == nil) != wantValid {
			t.Fatalf("cause %q validity=%v, want %v", reason, err == nil, wantValid)
		}
	}
}
func TestHealthSchemaGuardEnumsMatchEvaluator(t *testing.T) {
	value, err := readJSONValue(filepath.Join(repositoryRoot(t), "api/schemas/connection-health.v1alpha1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	defs := value.(map[string]any)["$defs"].(map[string]any)
	guard := defs["guard"].(map[string]any)["properties"].(map[string]any)
	extract := func(v any) []string {
		raw := v.(map[string]any)["enum"].([]any)
		out := make([]string, len(raw))
		for i, item := range raw {
			out[i] = item.(string)
		}
		return out
	}
	if !sameUniqueStrings(extract(guard["level"]), stringsOf(hosthealth.LevelOK, hosthealth.LevelWarning, hosthealth.LevelCritical, hosthealth.LevelUnknown, hosthealth.LevelUnsupported)) {
		t.Fatal("health levels drifted")
	}
	expected := stringsOf(hosthealth.ReasonPolicyInvalid, hosthealth.ReasonSnapshotInvalid, hosthealth.ReasonCoreUnavailable, hosthealth.ReasonUnsupported, hosthealth.ReasonThermalWarning, hosthealth.ReasonThermalCritical, hosthealth.ReasonSpeedWarning, hosthealth.ReasonSpeedCritical, hosthealth.ReasonCPUWarning, hosthealth.ReasonCPUCritical, hosthealth.ReasonGPUWarning, hosthealth.ReasonGPUCritical, hosthealth.ReasonBatteryLow, hosthealth.ReasonSustained, hosthealth.ReasonNotObserved, hosthealth.ReasonNotReobserved, hosthealth.ReasonObservationFailed, hosthealth.ReasonClockChanged, hosthealth.ReasonStale)
	if !sameUniqueStrings(extract(guard["reasons"].(map[string]any)["items"]), expected) {
		t.Fatal("health reasons drifted")
	}
	if !sameUniqueStrings(extract(guard["diagnostics"].(map[string]any)["items"]), stringsOf(hosthealth.ReasonSudoBroader, hosthealth.ReasonSleepEnabled)) {
		t.Fatal("health diagnostics drifted")
	}
}
