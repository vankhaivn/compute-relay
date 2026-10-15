package hosthealth

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPowermetricsDieTemperaturesAllowOnlyKnownAnnotations(t *testing.T) {
	for _, unit := range []string{"CPU", "GPU"} {
		for _, test := range []struct {
			suffix string
			valid  bool
		}{
			{"", true}, {" (fan)", true}, {" (power)", true}, {" (fan) \t", true},
			{"(fan)", false}, {" (Fan)", false}, {" (fans)", false}, {" (powers)", false}, {" (fan, power)", false}, {" (fan: 1)", false},
			{" (fan) (other)", false}, {" (fan) extra", false}, {" NEVER_PERSIST", false},
		} {
			t.Run(unit+test.suffix, func(t *testing.T) {
				samples := fixtures(t)
				samples[Powermetrics] = Sample{Status: Available, Data: []byte(fmt.Sprintf("%s die temperature: 44.55 C%s\n", unit, test.suffix))}
				snapshot := Parse(testResource, time.Now(), samples)
				metric := snapshot.CPUDieTemperatureCelsius
				if unit == "GPU" {
					metric = snapshot.GPUDieTemperatureCelsius
				}
				if test.valid {
					requireValue(t, metric, 44.55)
				} else if metric.Status != Unavailable || metric.Value != nil {
					t.Fatal("unrecognized temperature suffix was accepted")
				}
				encoded, err := json.Marshal(snapshot)
				if err != nil || snapshot.Validate() != nil || strings.Contains(string(encoded), "fan)") || strings.Contains(string(encoded), "NEVER_PERSIST") {
					t.Fatal("temperature annotation text escaped the typed snapshot", err)
				}
			})
		}
	}
}

func TestPowermetricsAnnotatedTemperatureBoundsAndContradictions(t *testing.T) {
	for _, unit := range []string{"CPU", "GPU"} {
		for _, text := range []string{
			"151.00 C (fan)\n", "-1.00 C (fan)\n", "NaN C (fan)\n",
			"44.55 C (fan)\n" + unit + " die temperature: 99.00 C\n",
		} {
			samples := fixtures(t)
			samples[Powermetrics] = Sample{Status: Available, Data: []byte(unit + " die temperature: " + text)}
			snapshot := Parse(testResource, time.Now(), samples)
			metric := snapshot.CPUDieTemperatureCelsius
			if unit == "GPU" {
				metric = snapshot.GPUDieTemperatureCelsius
			}
			if metric.Status != Unavailable || metric.Value != nil {
				t.Fatal("invalid annotated temperature or contradictory samples were accepted")
			}
		}
	}
}

func TestAnnotatedDieTemperatureStillDrivesCriticalGuard(t *testing.T) {
	at := time.Now()
	samples := fixtures(t)
	samples[Powermetrics] = Sample{Status: Available, Data: []byte("CPU die temperature: 100.00 C (fan)\nGPU die temperature: 35.00 C (fan)\nFan: 1800.00 rpm\n")}
	snapshot := Parse(testResource, at, samples)
	view := View{ResourceID: testResource, Latest: &snapshot, History: []Snapshot{snapshot}, Fresh: true, ReobservedSince: at}
	decision := Evaluate(view, DefaultPolicy(), at)
	if decision.Level != LevelCritical || !decision.RefuseNew || decision.Stop || len(decision.Reasons) != 1 || decision.Reasons[0] != ReasonCPUCritical {
		t.Fatal("recognized annotation withheld critical temperature evidence", decision)
	}
}

func TestRecordedPowermetricsPowerAnnotation(t *testing.T) {
	raw, err := os.ReadFile("testdata/powermetrics-annotated.txt")
	if err != nil {
		t.Fatal(err)
	}
	samples := fixtures(t)
	samples[Powermetrics] = Sample{Status: Available, Data: raw}
	snapshot := Parse(testResource, time.Now(), samples)
	requireValue(t, snapshot.CPUDieTemperatureCelsius, 100.29)
	requireValue(t, snapshot.GPUDieTemperatureCelsius, 35.0)
	requireValue(t, snapshot.FanRPM, 3241.52)
	if snapshot.Validate() != nil {
		t.Fatal("recorded annotated sample is invalid")
	}
}
