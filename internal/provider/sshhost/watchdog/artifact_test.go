package watchdog

import (
	"bytes"
	"encoding/xml"
	"math"
	"os/exec"
	"path"
	"reflect"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

func testConfig() Config {
	return Config{
		Home: "/Users/relay", InstallationID: "runtime_fixture", DockerPath: "/usr/local/bin/docker",
		DockerHost: "unix:///Users/relay/.docker/run/docker.sock", EngineID: "ENGINE:fixture:123456789",
		CriticalThermalLevel: 2, MinimumCPUSpeedPercent: 20, CriticalDieTemperatureCelsius: 100,
		CriticalDurationSeconds: 2, LowBatteryPercent: 10, SampleIntervalSeconds: 1,
	}
}

func TestRenderFrozenArtifact(t *testing.T) {
	c := testConfig()
	c.Home = "/Users/relay's & <home>"
	a, err := Render(c)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Render(c)
	if err != nil || !reflect.DeepEqual(a, again) {
		t.Fatal("render is not deterministic")
	}
	if a.Version != Version || path.Dir(a.Directory) != c.Home || len(a.Files) != 3 || a.SourceDigest != digest([]byte(source+"\x00"+hosthealth.ThermalStateScript)) {
		t.Fatalf("unexpected artifact metadata: %+v", a)
	}
	for _, f := range a.Files {
		if f.SHA256 != digest(f.Content) || path.Base(f.Name) != f.Name || f.Mode&0077 != 0 {
			t.Fatalf("unsafe file: %s", f.Name)
		}
		switch f.Name {
		case ScriptName:
			cmd := exec.Command("/bin/sh", "-n")
			cmd.Stdin = bytes.NewReader(f.Content)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("shell syntax: %v: %s", err, out)
			}
			text := string(f.Content)
			for _, fixed := range []string{"sysctl='/usr/sbin/sysctl'", "osascript='/usr/bin/osascript'", "thermal_state_script=" + quote(hosthealth.ThermalStateScript), "pmset='/usr/bin/pmset'", "sudo='/usr/bin/sudo'", "flush='/usr/bin/perl'", "/usr/bin/powermetrics --samplers smc -n 1 -i 1000", `"$sysctl" -n hw.cputype`, `"$osascript" -l JavaScript -e "$thermal_state_script"`} {
				if !strings.Contains(text, fixed) {
					t.Errorf("missing fixed command %q", fixed)
				}
			}
			if strings.Contains(text, "@@") || strings.Contains(text, "eval ") || strings.Contains(text, "kill -TERM -") || strings.Contains(text, "machdep.xcpm.cpu_thermal_level") {
				t.Fatal("unresolved or unsafe script")
			}
		case ManifestName:
			if f.SHA256 != a.ManifestDigest || !bytes.Contains(f.Content, []byte(a.SourceDigest)) {
				t.Fatal("manifest lacks source binding")
			}
		case PlistName:
			d := xml.NewDecoder(bytes.NewReader(f.Content))
			for {
				if _, err := d.Token(); err != nil {
					if err.Error() != "EOF" {
						t.Fatal(err)
					}
					break
				}
			}
			if !bytes.Contains(f.Content, []byte(a.ManifestDigest)) || !bytes.Contains(f.Content, []byte("&amp;")) || bytes.Contains(f.Content, []byte("Library/LaunchAgents")) {
				t.Fatal("plist did not pin manifest within owned directory")
			}
		}
	}
}

func TestConfigRejectsHostileOrUnboundedMaterial(t *testing.T) {
	for name, change := range map[string]func(*Config){
		"remote docker":        func(c *Config) { c.DockerHost = "ssh://foreign" },
		"unclean socket":       func(c *Config) { c.DockerHost = "unix:///tmp/../docker.sock" },
		"relative docker":      func(c *Config) { c.DockerPath = "docker" },
		"root home":            func(c *Config) { c.Home = "/" },
		"home newline":         func(c *Config) { c.Home += "\n" },
		"home backslash":       func(c *Config) { c.Home += "\\path" },
		"thermal excess":       func(c *Config) { c.CriticalThermalLevel = 4 },
		"installation command": func(c *Config) { c.InstallationID = "runtime; touch pwned" },
		"engine command":       func(c *Config) { c.EngineID = "$(touch pwned)" },
		"thermal negative":     func(c *Config) { c.CriticalThermalLevel = -1 },
		"speed excess":         func(c *Config) { c.MinimumCPUSpeedPercent = 101 },
		"temperature nan":      func(c *Config) { c.CriticalDieTemperatureCelsius = math.NaN() },
		"temperature infinite": func(c *Config) { c.CriticalDieTemperatureCelsius = math.Inf(1) },
		"temperature negative": func(c *Config) { c.CriticalDieTemperatureCelsius = -1 },
		"duration unbounded":   func(c *Config) { c.CriticalDurationSeconds = 901 },
		"battery excess":       func(c *Config) { c.LowBatteryPercent = 101 },
		"interval zero":        func(c *Config) { c.SampleIntervalSeconds = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			c := testConfig()
			change(&c)
			if _, err := Render(c); err != ErrConfig {
				t.Fatalf("got %v, want ErrConfig", err)
			}
		})
	}
}

func TestStatusIsBoundedAndContainsOnlyPublicFacts(t *testing.T) {
	raw := []byte(`{"version":1,"last_run":"2026-10-15T00:00:00Z","last_action":"stop_unknown"}`)
	if _, err := ParseStatus(raw); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Repeat(" ", MaxStatus+1),
		`{"version":2,"last_run":"2026-10-15T00:00:00Z","last_action":"idle"}`,
		`{"version":1,"last_run":"invalid","last_action":"idle"}`,
		`{"version":1,"last_run":"2026-10-15T00:00:00Z","last_action":"/Users/private"}`,
		`{"version":1,"last_run":"2026-10-15T00:00:00Z","last_action":"idle","home":"private"}`,
		string(raw) + `{}`,
	} {
		if _, err := ParseStatus([]byte(bad)); err != ErrStatus {
			t.Fatalf("unexpected status acceptance: %q (%v)", bad, err)
		}
	}
}

func TestConfigPreservesFractionalGuardThresholds(t *testing.T) {
	c := testConfig()
	c.MinimumCPUSpeedPercent = 20.5
	c.LowBatteryPercent = 10.5
	c.CriticalDieTemperatureCelsius = 0
	a, err := Render(c)
	if err != nil {
		t.Fatal(err)
	}
	script := string(a.Files[0].Content)
	for _, want := range []string{"speed_threshold=20.5", "low_battery=10.5", "temperature_threshold=0"} {
		if !strings.Contains(script, want) {
			t.Fatal("threshold not preserved", want)
		}
	}
}
