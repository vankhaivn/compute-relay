package watchdog

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

const ownedID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const foreignID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const unlabelledID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

type sample struct {
	level, therm, batt, temperatures string
	epoch, elapsed                   int64
	hang, fail                       string
	ids                              []string
}

func healthy() sample {
	return sample{level: "0\n", therm: "Note: No thermal warning level has been recorded\n CPU_Speed_Limit = 100\n", batt: "Now drawing from 'AC Power'\n -InternalBattery-0 (id=123)\t100%; charged; 0:00 remaining present: true\n"}
}

func hot() sample { s := healthy(); s.level = "3\n"; return s }
func lowBattery() sample {
	s := healthy()
	s.batt = "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=123)\t10%; discharging; 0:10 remaining present: true\n"
	return s
}

type fixture struct {
	t        *testing.T
	home     string
	control  string
	bin      string
	config   Config
	artifact Artifact
	samples  []sample
}

func newFixture(t *testing.T, samples ...sample) *fixture {
	t.Helper()
	f := &fixture{t: t, home: t.TempDir(), control: t.TempDir(), bin: t.TempDir(), samples: samples}
	f.config = testConfig()
	f.config.Home, f.config.DockerPath = f.home, filepath.Join(f.bin, "docker")
	f.config.DockerHost = "unix:///tmp/relay-fixture.sock"
	for i, s := range samples {
		dir := filepath.Join(f.control, "samples", strconv.Itoa(i))
		mustMkdir(t, dir)
		if s.epoch == 0 {
			s.epoch = int64(1000 + i)
		}
		for name, value := range map[string]string{"osascript": s.level, "therm": s.therm, "batt": s.batt, "sudo": s.temperatures,
			"epoch": strconv.FormatInt(s.epoch, 10), "elapsed": strconv.FormatInt(s.elapsed, 10)} {
			mustWrite(t, filepath.Join(dir, name), value, 0600)
		}
		if s.hang != "" {
			mustWrite(t, filepath.Join(dir, s.hang+".hang"), "", 0600)
		}
		if s.fail != "" {
			mustWrite(t, filepath.Join(dir, s.fail+".fail"), "", 0600)
		}
		ids := s.ids
		if ids == nil {
			ids = []string{ownedID}
		}
		mustWrite(t, filepath.Join(dir, "ids"), strings.Join(ids, "\n"), 0600)
	}
	for _, name := range []string{"sysctl", "osascript", "pmset", "sudo", "uname", "date", "stat", "id", "sample-sleep", "docker", "sync"} {
		script := "#!/bin/sh\ncontrol=" + quote(f.control) + "\ncount=" + strconv.Itoa(len(samples)) + "\nthermal_payload=" + quote(hosthealth.ThermalStateScript) + "\n" + fakeHost
		mustWrite(t, filepath.Join(f.bin, name), script, 0700)
	}
	f.render()
	f.container(ownedID, "runtime_fixture", "instance", "attempt", "preparation", "workspace", "job", strings.Repeat("n", 32), strings.Repeat("d", 64), f.config.EngineID)
	f.container(foreignID, "foreign_installation", "instance", "attempt", "preparation", "workspace", "job", strings.Repeat("n", 32), strings.Repeat("d", 64), f.config.EngineID)
	f.container(unlabelledID, "", "", "", "", "", "", "", "", "")
	// Finish launching the generated fixture executable before measuring the
	// watchdog's command deadlines. The watchdog itself retains every bound.
	mustWrite(t, filepath.Join(f.control, "index"), "0", 0600)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	started := time.Now()
	out, err := exec.CommandContext(ctx, filepath.Join(f.bin, "id"), "-u").CombinedOutput()
	if err != nil || string(out) != "501\n" {
		t.Fatalf("fixture command setup failed: %v: %s", err, out)
	}
	t.Logf("fixture command setup: %s", time.Since(started))
	return f
}

func (f *fixture) render() {
	f.t.Helper()
	a, err := render(f.config, commands{sysctl: "sysctl", osascript: "osascript", pmset: "pmset", sudo: "sudo", uname: "uname", date: "date", stat: "stat", id: "id", sleep: "sample-sleep", sync: "sync", path: f.bin + ":/usr/bin:/bin:/usr/sbin:/sbin"})
	if err != nil {
		f.t.Fatal(err)
	}
	f.artifact = a
	mustMkdir(f.t, a.Directory)
	for _, file := range a.Files {
		mustWrite(f.t, filepath.Join(a.Directory, file.Name), string(file.Content), file.Mode)
	}
	mustWrite(f.t, filepath.Join(f.control, "root"), a.Directory, 0600)
	mustWrite(f.t, filepath.Join(f.control, "engine"), f.config.EngineID, 0600)
}

func (f *fixture) container(id string, labels ...string) {
	f.t.Helper()
	mustMkdir(f.t, filepath.Join(f.control, "containers"))
	mustWrite(f.t, filepath.Join(f.control, "containers", id), id+"\n"+strings.Join(labels, "\n")+"\n", 0600)
}

func (f *fixture) run() Status {
	f.t.Helper()
	mustWrite(f.t, filepath.Join(f.control, "index"), "0", 0600)
	mustWrite(f.t, filepath.Join(f.control, "epoch-count"), "0", 0600)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", filepath.Join(f.artifact.Directory, ScriptName), f.artifact.ManifestDigest)
	cmd.Env = append(os.Environ(), "DOCKER_CONTEXT=foreign", "DOCKER_HOST=tcp://foreign:2375", "DOCKER_TLS_VERIFY=1", "PERL5OPT=-MRelayHostileFixtureModule")
	out, runErr := cmd.CombinedOutput()
	if runErr != nil {
		if ctx.Err() != nil {
			f.t.Fatalf("watchdog exceeded outer bound: %v: %s", runErr, out)
		}
		// Integrity refusal exits nonzero after recording its status.
		if len(out) != 0 {
			f.t.Logf("watchdog output: %s", out)
		}
	}
	raw, err := os.ReadFile(filepath.Join(f.artifact.Directory, StatusName))
	if err != nil {
		f.t.Fatalf("missing status: %v (run %v, process %v, output %s, commands:\n%s)", err, runErr, cmd.ProcessState, out, f.log())
	}
	s, err := ParseStatus(raw)
	if err != nil {
		f.t.Fatalf("invalid status %q: %v", raw, err)
	}
	if strings.Contains(string(raw), f.home) || strings.Contains(string(raw), f.config.EngineID) {
		f.t.Fatal("private host identity escaped into status")
	}
	return s
}

func (f *fixture) log() string {
	f.t.Helper()
	raw, _ := os.ReadFile(filepath.Join(f.control, "log"))
	return string(raw)
}

func (f *fixture) assertStops(want int) {
	f.t.Helper()
	if got := strings.Count(f.log(), "\tcontainer\tstop\t"); got != want {
		f.t.Fatalf("got %d stops, want %d; log:\n%s", got, want, f.log())
	}
	for _, forbidden := range []string{"intent_missing", "unsafe_endpoint", "ambient_context", "unexpected_sysctl", "machdep.xcpm.cpu_thermal_level", "\tcontainer\trm", "\timage\t", "\tvolume\t", "\tcontainer\tstart", "\tcontainer\trestart", unlabelledID} {
		if strings.Contains(f.log(), forbidden) {
			f.t.Fatalf("unsafe command %q in log:\n%s", forbidden, f.log())
		}
	}
}

func TestCriticalRequiresFreshConsecutiveCoreSamples(t *testing.T) {
	for name, samples := range map[string][]sample{
		"sustained": {hot(), hot(), hot()},
		"cleared":   {hot(), healthy(), hot(), hot()},
		"gap":       {hot(), {level: "3\n", therm: healthy().therm, batt: healthy().batt, epoch: 1004}, {level: "3\n", therm: healthy().therm, batt: healthy().batt, epoch: 1005}},
		"unknown":   {hot(), {level: "unknown\n", therm: healthy().therm, batt: healthy().batt}, hot(), hot()},
		"stale":     {hot(), {level: "3\n", therm: healthy().therm, batt: healthy().batt, elapsed: 3}, hot(), hot()},
		"clock back": {hot(), {level: "3\n", therm: healthy().therm, batt: healthy().batt, epoch: 999},
			{level: "3\n", therm: healthy().therm, batt: healthy().batt, epoch: 1000}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, samples...)
			s := f.run()
			want := 0
			if name == "sustained" {
				want = 1
				if s.LastAction != "confirmed_inactive" {
					t.Fatalf("got %s", s.LastAction)
				}
			}
			f.assertStops(want)
		})
	}
}

func TestCriticalSourcesAndInclusiveThresholds(t *testing.T) {
	for name, s := range map[string]sample{
		"critical thermal state": hot(),
		"serious thermal state":  func() sample { s := healthy(); s.level = "2\n"; return s }(),
		"cpu speed":              func() sample { s := healthy(); s.therm = "CPU_Speed_Limit = 20\n"; return s }(),
		"cpu temperature": func() sample {
			s := healthy()
			s.temperatures = "CPU die temperature: 100.0 C\nGPU die temperature: 55 C\n"
			return s
		}(),
		"gpu temperature": func() sample {
			s := healthy()
			s.temperatures = "CPU die temperature: 60.0 C\nGPU die temperature: 100.0 C\n"
			return s
		}(),
		"cpu temperature power": func() sample {
			s := healthy()
			s.temperatures = "CPU die temperature: 100.00 C (power)\nGPU die temperature: 55.00 C (power)\n"
			return s
		}(),
		"cpu temperature fan": func() sample {
			s := healthy()
			s.temperatures = "CPU die temperature: 100.00 C (fan)\nGPU die temperature: 55.00 C (fan)\n"
			return s
		}(),
		"gpu temperature fan": func() sample {
			s := healthy()
			s.temperatures = "CPU die temperature: 60.00 C (fan)\nGPU die temperature: 100.00 C (fan)\n"
			return s
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, s, s, s)
			f.config.PowermetricsGranted = strings.Contains(name, "temperature")
			f.render()
			if got := f.run().LastAction; got != "confirmed_inactive" {
				t.Fatalf("got %s", got)
			}
			f.assertStops(1)
			if !strings.Contains(f.log(), "osascript\t-l\tJavaScript\t-e\t"+hosthealth.ThermalStateScript) {
				t.Fatal("thermal state invocation differs from the shared Foundation probe")
			}
			if f.config.PowermetricsGranted && !strings.Contains(f.log(), "sudo\t-n\t/usr/bin/powermetrics\t--samplers\tsmc\t-n\t1\t-i\t1000") {
				t.Fatal("powermetrics invocation differs from fixed grant command")
			}
		})
	}
}

func TestFoundationThermalStatesMatchConfiguredThreshold(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    string
		critical int
		stops    int
	}{
		{"nominal", "0\n", 2, 0},
		{"fair", "1\n", 2, 0},
		{"serious", "2\n", 2, 1},
		{"critical", "3\n", 3, 1},
		{"serious below configured critical", "2\n", 3, 0},
		{"lowered nominal threshold", "0\n", 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := healthy()
			s.level = tc.state
			s.therm = "CPU_Speed_Limit = 98\n"
			f := newFixture(t, s, s, s)
			f.config.CriticalThermalLevel = tc.critical
			f.render()
			want := "idle"
			if tc.stops != 0 {
				want = "confirmed_inactive"
			}
			if got := f.run().LastAction; got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
			f.assertStops(tc.stops)
		})
	}
}

func TestMissingOptionalTemperatureDoesNotInvalidateCore(t *testing.T) {
	s := hot()
	s.fail = "sudo"
	f := newFixture(t, s, s, s)
	f.config.PowermetricsGranted = true
	f.render()
	if got := f.run().LastAction; got != "confirmed_inactive" {
		t.Fatalf("got %s", got)
	}
	f.assertStops(1)
}

func TestLowBatteryStopsImmediatelyWithKnownSourceAndCharge(t *testing.T) {
	s := lowBattery()
	s.level = "unknown\n"
	f := newFixture(t, s)
	if got := f.run().LastAction; got != "confirmed_inactive" {
		t.Fatalf("got %s", got)
	}
	f.assertStops(1)
}

func TestUnknownAndHostileSourcesNeverStop(t *testing.T) {
	for name, change := range map[string]func(*sample){
		"unknown thermal": func(s *sample) { s.level = "not available\n" },
		"thermal command": func(s *sample) { s.level = "3; touch pwned\n" },
		"out of enum":     func(s *sample) { s.level = "4\n" },
		"negative enum":   func(s *sample) { s.level = "-1\n" },
		"legacy counter":  func(s *sample) { s.level = "60\n" },
		"fractional enum": func(s *sample) { s.level = "2.0\n" },
		"duplicate enum":  func(s *sample) { s.level = "2\n3\n" },
		"failed probe":    func(s *sample) { s.fail = "osascript" },
		"duplicate speed": func(s *sample) { s.therm += "CPU_Speed_Limit = 1\n" },
		"missing speed":   func(s *sample) { s.therm = "No data\n" },
		"malformed speed": func(s *sample) { s.therm = "CPU_Speed_Limit = 0; touch pwned\n" },
		"unknown source": func(s *sample) {
			s.batt = "Now drawing from 'Unverified Power'\n -InternalBattery-0 1%; discharging; present: true\n"
		},
		"missing charge": func(s *sample) { s.batt = "Now drawing from 'Battery Power'\n" },
		"excess charge": func(s *sample) {
			s.batt = "Now drawing from 'Battery Power'\n -InternalBattery-0 101%; discharging; present: true\n"
		},
		"duplicate battery": func(s *sample) { s.batt += " -InternalBattery-1 1%; discharging; present: true\n" },
		"charge in identifier": func(s *sample) {
			s.batt = "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1%;) 95%; discharging; present: true\n"
		},
		"duplicate presence": func(s *sample) {
			s.batt = "Now drawing from 'Battery Power'\n -InternalBattery-0 1%; discharging; present: false present: true\n"
		},
		"oversize output": func(s *sample) { s.level = strings.Repeat("0", 30000) },
	} {
		t.Run(name, func(t *testing.T) {
			s := hot()
			change(&s)
			f := newFixture(t, s, s, s)
			if got := f.run().LastAction; got != "unknown" {
				t.Fatalf("got %s", got)
			}
			f.assertStops(0)
		})
	}
}

func TestTemperatureParserNeverGuessesUnits(t *testing.T) {
	for _, raw := range []string{"CPU die temperature: 100000\n", "CPU die temperature: 100 K\n", "CPU die temperature: 100 C\nCPU die temperature: 40 C\n", "CPU die temperature: 100; touch pwned C\n", "CPU die temperature: 100 C (fans)\n", "CPU die temperature: 100 C(fan)\n", "CPU die temperature: 100 C (fan) trailing\n"} {
		t.Run(strings.TrimSpace(raw), func(t *testing.T) {
			s := healthy()
			s.temperatures = raw
			f := newFixture(t, s, s, s)
			f.config.PowermetricsGranted = true
			f.render()
			if got := f.run().LastAction; got != "idle" {
				t.Fatalf("got %s", got)
			}
			f.assertStops(0)
		})
	}
}

func TestLabelFilteringAndEveryOwnershipLabel(t *testing.T) {
	f := newFixture(t, lowBattery())
	f.run()
	f.assertStops(1)
	for _, label := range []string{"installation=runtime_fixture", "instance", "attempt", "preparation", "workspace", "job", "nonce", "plan", "engine"} {
		if !strings.Contains(f.log(), "\t--filter\tlabel=io.compute-relay."+label) {
			t.Fatalf("missing label filter %s", label)
		}
	}
	for i, label := range []string{"installation", "instance", "attempt", "preparation", "workspace", "job", "nonce", "plan", "engine"} {
		t.Run(label, func(t *testing.T) {
			f := newFixture(t, lowBattery())
			labels := []string{"runtime_fixture", "instance", "attempt", "preparation", "workspace", "job", strings.Repeat("n", 32), strings.Repeat("d", 64), f.config.EngineID}
			labels[i] = ""
			f.container(ownedID, labels...)
			if got := f.run().LastAction; got != "unknown" {
				t.Fatalf("got %s", got)
			}
			f.assertStops(0)
		})
	}
}

func TestForeignContainerOrMalformedIDIsNeverStopped(t *testing.T) {
	for name, ids := range map[string][]string{
		"foreign": {foreignID}, "short id": {"abcdef"}, "option": {"--all"}, "command": {ownedID + ";touch pwned"}, "duplicate": {ownedID, ownedID},
	} {
		t.Run(name, func(t *testing.T) {
			s := lowBattery()
			s.ids = ids
			f := newFixture(t, s)
			f.run()
			f.assertStops(0)
			if name != "foreign" && strings.Contains(f.log(), "\tcontainer\tinspect\t") {
				t.Fatal("inspected an invalid or ambiguous candidate ID")
			}
		})
	}
}

func TestOwnershipComparisonsUseExactStringsForNumericIDs(t *testing.T) {
	for _, name := range []string{"container", "installation", "engine"} {
		t.Run(name, func(t *testing.T) {
			s := lowBattery()
			candidate := ownedID
			if name == "container" {
				candidate = strings.Repeat("1", 64)
				s.ids = []string{candidate}
			}
			f := newFixture(t, s)
			labels := []string{"runtime_fixture", "instance", "attempt", "preparation", "workspace", "job", strings.Repeat("n", 32), strings.Repeat("d", 64), f.config.EngineID}
			reportedID := candidate
			switch name {
			case "container":
				reportedID = strings.Repeat("1", 63) + "2"
			case "installation":
				f.config.InstallationID = "12345"
				labels[0] = "012345"
			case "engine":
				f.config.EngineID = "1111111111111111"
				labels[8] = "01111111111111111"
			}
			f.render()
			mustWrite(t, filepath.Join(f.control, "containers", candidate), reportedID+"\n"+strings.Join(labels, "\n")+"\n", 0600)
			if got := f.run().LastAction; got != "unknown" {
				t.Fatalf("numeric %s alias accepted as %s", name, got)
			}
			f.assertStops(0)
		})
	}
}

func TestOwnedFlushProgramUsesOnlyExplicitFiles(t *testing.T) {
	a, err := Render(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, after, found := strings.Cut(string(a.Files[0].Content), "-MIO::Handle -e '")
	if !found {
		t.Fatal("missing fixed owned-file flush program")
	}
	program, _, found := strings.Cut(after, "' \"$ledger\" \"$root\"")
	if !found {
		t.Fatal("flush program is not followed by explicit owned paths")
	}
	dir := t.TempDir()
	ledger := filepath.Join(dir, LedgerName)
	mustWrite(t, ledger+".tmp", "durable owned intent\n", 0600)
	if err := os.Rename(ledger+".tmp", ledger); err != nil {
		t.Fatal(err)
	}
	run := func(paths ...string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		args := append([]string{"-MFcntl=O_RDONLY,O_NOFOLLOW", "-MIO::Handle", "-e", program}, paths...)
		return exec.CommandContext(ctx, "/usr/bin/perl", args...).Run()
	}
	if err := run(ledger, dir); err != nil {
		t.Fatalf("stock Perl cannot sync the owned file and rename directory: %v", err)
	}
	if err := run(filepath.Join(dir, "missing"), dir); err == nil {
		t.Fatal("flush accepted a missing intent file")
	}
	link := filepath.Join(dir, "symlink")
	if err := os.Symlink(ledger, link); err != nil {
		t.Fatal(err)
	}
	if err := run(link, dir); err == nil {
		t.Fatal("flush followed a substituted symlink")
	}
}

func TestChangedDockerDaemonNeverListsContainers(t *testing.T) {
	f := newFixture(t, lowBattery())
	mustWrite(t, filepath.Join(f.control, "engine"), "ENGINE:replacement:123456", 0600)
	if got := f.run().LastAction; got != "unknown" {
		t.Fatalf("got %s", got)
	}
	f.assertStops(0)
	if strings.Contains(f.log(), "\tcontainer\t") {
		t.Fatal("touched containers on a changed daemon")
	}
}

func TestStopAmbiguityIsObservedWithoutRetryAcrossRestart(t *testing.T) {
	f := newFixture(t, lowBattery(), lowBattery(), healthy())
	mustWrite(t, filepath.Join(f.control, "stop-behavior"), "failure", 0600)
	if got := f.run().LastAction; got != "stop_unknown" {
		t.Fatalf("got %s", got)
	}
	f.assertStops(1)
	// Re-rendering an identical installation never clears its stop ledger.
	f.render()
	if got := f.run().LastAction; got != "stop_unknown" {
		t.Fatalf("restart got %s", got)
	}
	f.assertStops(1)
}

func TestFilteredAbsenceAfterStopDoesNotConfirmOriginalInactivity(t *testing.T) {
	f := newFixture(t, lowBattery(), healthy())
	if got := f.run().LastAction; got != "stop_unknown" {
		t.Fatalf("filtered running-list absence became %s", got)
	}
	f.assertStops(1)
}

func TestNewBatchDoesNotConfirmAnOlderUnobservedID(t *testing.T) {
	f := newFixture(t, lowBattery())
	mustWrite(t, filepath.Join(f.artifact.Directory, LedgerName), "compute-relay-watchdog-stops-v1 "+StopReason+"\n"+foreignID+"\n", 0600)
	if got := f.run().LastAction; got != "stop_unknown" {
		t.Fatalf("new batch hid an older unknown ID as %s", got)
	}
	f.assertStops(1)
}

func TestOnlyTerminalExactObservationConfirmsInactivity(t *testing.T) {
	for _, state := range []string{"created", "paused", "restarting", "removing"} {
		t.Run(state, func(t *testing.T) {
			f := newFixture(t, lowBattery())
			mustWrite(t, filepath.Join(f.control, "post-stop-status"), state, 0600)
			if got := f.run().LastAction; got != "stop_unknown" {
				t.Fatalf("%s became %s", state, got)
			}
			f.assertStops(1)
		})
	}
}

func TestStaleSampleCannotAuthorizeScanOrMutation(t *testing.T) {
	for _, stage := range []string{"inspect", "sync"} {
		t.Run(stage, func(t *testing.T) {
			f := newFixture(t, lowBattery())
			mustWrite(t, filepath.Join(f.control, "slow-"+stage), "", 0600)
			got := f.run().LastAction
			want := "unknown"
			if stage == "sync" {
				want = "attention_required"
			}
			if got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
			f.assertStops(0)
			_, err := os.Stat(filepath.Join(f.artifact.Directory, LedgerName))
			if stage == "inspect" && !os.IsNotExist(err) {
				t.Fatal("persisted intent from an expired sample")
			}
			if stage == "sync" && err != nil {
				t.Fatal("lost intent after the flush made the sample stale")
			}
		})
	}
}

func TestFailedIntentFlushNeverStopsOrRetries(t *testing.T) {
	s := lowBattery()
	s.fail = "sync"
	f := newFixture(t, s, lowBattery())
	if got := f.run().LastAction; got != "stop_unknown" {
		t.Fatalf("got %s", got)
	}
	f.assertStops(0)
	if raw, err := os.ReadFile(filepath.Join(f.artifact.Directory, LedgerName)); err != nil || !strings.Contains(string(raw), ownedID) {
		t.Fatal("failed flush lost conservative stop intent")
	}
}

func TestNonzeroStopExitCanStillBeObservedInactive(t *testing.T) {
	f := newFixture(t, lowBattery())
	mustWrite(t, filepath.Join(f.control, "stop-behavior"), "nonzero-inactive", 0600)
	if got := f.run().LastAction; got != "confirmed_inactive" {
		t.Fatalf("got %s", got)
	}
	f.assertStops(1)
}

func TestStopLedgerMalformedOrFullRefusesMutation(t *testing.T) {
	for _, name := range []string{"malformed", "full"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, lowBattery())
			ledger := "not a valid ledger\n"
			want := "attention_required"
			if name == "full" {
				ledger = "compute-relay-watchdog-stops-v1 " + StopReason + "\n"
				for i := range MaxStopIDs {
					ledger += fmt.Sprintf("%064x\n", i)
				}
				want = "ledger_full"
			}
			mustWrite(t, filepath.Join(f.artifact.Directory, LedgerName), ledger, 0600)
			if got := f.run().LastAction; got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
			f.assertStops(0)
		})
	}
}

func TestManifestAndScriptTamperingPreventsSampling(t *testing.T) {
	for _, file := range []string{ManifestName, ScriptName} {
		t.Run(file, func(t *testing.T) {
			f := newFixture(t, lowBattery())
			p := filepath.Join(f.artifact.Directory, file)
			handle, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = handle.WriteString("\n# tampered\n")
			_ = handle.Close()
			if got := f.run().LastAction; got != "integrity_failed" {
				t.Fatalf("got %s", got)
			}
			f.assertStops(0)
			if strings.Contains(f.log(), "sysctl") || strings.Contains(f.log(), "osascript") || strings.Contains(f.log(), "pmset") {
				t.Fatal("sampled after integrity failure")
			}
		})
	}
}

func TestIntegrityIsVerifiedAgainBeforeEachCycle(t *testing.T) {
	f := newFixture(t, healthy(), lowBattery())
	mustWrite(t, filepath.Join(f.control, "tamper-after-cycle"), "", 0600)
	if got := f.run().LastAction; got != "integrity_failed" {
		t.Fatalf("got %s", got)
	}
	f.assertStops(0)
	if got := strings.Count(f.log(), "osascript\t-l\tJavaScript\t-e\t"); got != 1 {
		t.Fatalf("sampled %d cycles despite manifest tampering", got)
	}
}

func TestQuotedHostPathsAreLiteralMaterial(t *testing.T) {
	f := newFixture(t, lowBattery())
	f.config.Home = filepath.Join(f.home, "home's $(touch PWNED) & <value>")
	f.render()
	if got := f.run().LastAction; got != "confirmed_inactive" {
		t.Fatalf("got %s", got)
	}
	f.assertStops(1)
	if _, err := os.Stat(filepath.Join(f.artifact.Directory, "PWNED")); !os.IsNotExist(err) {
		t.Fatal("evaluated rendered path as shell code")
	}
}

func TestScratchFilesNeverTruncateHardlinkTargets(t *testing.T) {
	for _, name := range []string{".command-v1.out", ".status-v1.tmp", ".stop-ledger-v1.tmp"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, lowBattery())
			sentinel := filepath.Join(f.control, "outside-sentinel")
			mustWrite(t, sentinel, "preserve unrelated bytes", 0600)
			if err := os.Link(sentinel, filepath.Join(f.artifact.Directory, name)); err != nil {
				t.Fatal(err)
			}
			if got := f.run().LastAction; got != "confirmed_inactive" {
				t.Fatalf("got %s", got)
			}
			f.assertStops(1)
			if raw, err := os.ReadFile(sentinel); err != nil || string(raw) != "preserve unrelated bytes" {
				t.Fatal("scratch write truncated an unrelated hardlink target")
			}
		})
	}
}

func TestUnsupportedHostNeverSamplesOrStops(t *testing.T) {
	for _, platform := range []string{"Linux x86_64", "Darwin arm64"} {
		t.Run(platform, func(t *testing.T) {
			f := newFixture(t, lowBattery())
			mustWrite(t, filepath.Join(f.control, "platform"), platform, 0600)
			if got := f.run().LastAction; got != "unsupported" {
				t.Fatalf("got %s", got)
			}
			f.assertStops(0)
			if strings.Contains(f.log(), "sysctl") || strings.Contains(f.log(), "osascript") || strings.Contains(f.log(), "pmset") {
				t.Fatal("sampled unsupported host")
			}
		})
	}
}

func TestNativeCPUTypeRejectsTranslatedOrUnknownHardware(t *testing.T) {
	for name, value := range map[string]string{
		"translated Apple Silicon": "16777228", "malformed": "7junk", "empty": "", "other hardware": "12", "failed": "failure",
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, lowBattery())
			mustWrite(t, filepath.Join(f.control, "cpu-type"), value, 0600)
			got := f.run().LastAction
			want := "unknown"
			if name == "translated Apple Silicon" {
				want = "unsupported"
			}
			if got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
			f.assertStops(0)
			for _, forbidden := range []string{"osascript\t", "pmset\t", "sudo\t", "docker\t"} {
				if strings.Contains(f.log(), forbidden) {
					t.Fatalf("acted without positive native Intel evidence: %s", forbidden)
				}
			}
		})
	}
}

func TestStuckSourceIsBoundedAndDoesNotSignalUnrelatedProcess(t *testing.T) {
	other := exec.Command("/bin/sleep", "30")
	if err := other.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Process.Kill(); _ = other.Wait() }()
	s := hot()
	s.hang = "osascript"
	f := newFixture(t, s)
	start := time.Now()
	if got := f.run().LastAction; got != "unknown" {
		t.Fatalf("got %s", got)
	}
	if elapsed := time.Since(start); elapsed > 12*time.Second {
		t.Fatalf("source exceeded finite bound: %v", elapsed)
	}
	f.assertStops(0)
	// A liveness probe uses a separate wait command and never signals its group.
	probe := exec.Command("/bin/kill", "-0", strconv.Itoa(other.Process.Pid))
	if err := probe.Run(); err != nil {
		t.Fatal("watchdog signalled an unrelated process")
	}
}

func TestStuckStopIsRecordedAndNeverRepeated(t *testing.T) {
	f := newFixture(t, lowBattery(), lowBattery())
	mustWrite(t, filepath.Join(f.control, "stop-behavior"), "timeout", 0600)
	start := time.Now()
	if got := f.run().LastAction; got != "stop_unknown" {
		t.Fatalf("got %s", got)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("stop exceeded finite bound: %v", elapsed)
	}
	f.assertStops(1)
}

func TestTerminationReapsItsOwnUnresponsiveChild(t *testing.T) {
	s := hot()
	s.hang = "osascript"
	f := newFixture(t, s)
	mustWrite(t, filepath.Join(f.control, "index"), "0", 0600)
	mustWrite(t, filepath.Join(f.control, "epoch-count"), "0", 0600)
	cmd := exec.Command("/bin/sh", filepath.Join(f.artifact.Directory, ScriptName), f.artifact.ManifestDigest)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var childPID string
	defer func() {
		_ = cmd.Process.Kill()
		if childPID != "" {
			_ = exec.Command("/bin/kill", "-KILL", childPID).Run()
		}
	}()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		raw, err := os.ReadFile(filepath.Join(f.control, "hanging-pid"))
		if err == nil {
			childPID = strings.TrimSpace(string(raw))
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if childPID == "" {
		t.Fatal("fake source did not start")
	}
	start := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	ended := make(chan error, 1)
	go func() { ended <- cmd.Wait() }()
	select {
	case err := <-ended:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("watchdog termination exceeded its cleanup bound")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("slow termination: %v", elapsed)
	}
	if err := exec.Command("/bin/kill", "-0", childPID).Run(); err == nil {
		t.Fatal("watchdog left its unresponsive child running")
	}
	childPID = ""
	f.assertStops(0)
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, name, data string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(name, []byte(data), mode); err != nil {
		t.Fatal(err)
	}
}

const fakeHost = `name=${0##*/}
IFS= read -r index <"$control/index"
sample=$control/samples/$index
IFS= read -r root <"$control/root"
{
    printf '%s' "$name"
    for arg in "$@"; do printf '\t%s' "$arg"; done
    printf '\n'
} >>"$control/log"
key=$name
if [ "$name" = pmset ]; then key=$2; fi
if [ "$name" = sysctl ] && [ "$2" = hw.cputype ]; then key=cpu-type; fi
if [ -f "$sample/$key.hang" ]; then trap '' TERM; printf '%s' "$$" >"$control/hanging-pid"; exec /bin/sleep 30; fi
if [ -f "$sample/$key.fail" ]; then exit 1; fi
case "$name" in
    sync) if [ -f "$control/slow-sync" ]; then printf 4 >"$control/advance"; fi; exit 0;;
    id) printf '501\n';;
    stat) printf '501 700\n';;
    uname) if [ -f "$control/platform" ]; then /bin/cat "$control/platform"; else printf 'Darwin x86_64\n'; fi;;
    sysctl)
        if [ "$key" = cpu-type ]; then
            if [ -f "$control/cpu-type" ]; then
                IFS= read -r value <"$control/cpu-type"
                [ "$value" != failure ] || exit 1
                printf '%s\n' "$value"
            else printf '7\n'; fi
        else printf 'unexpected_sysctl\n' >>"$control/log"; exit 1; fi;;
    osascript)
        [ "$#" = 4 ] && [ "$1" = -l ] && [ "$2" = JavaScript ] && [ "$3" = -e ] && [ "$4" = "$thermal_payload" ] || exit 1
        /bin/cat "$sample/$key";;
    sudo) /bin/cat "$sample/$key";;
    pmset) /bin/cat "$sample/$key";;
    date)
        if [ "$2" = '+%s' ]; then
            IFS= read -r at <"$sample/epoch"
            IFS= read -r calls <"$control/epoch-count"
            if [ "$calls" -gt 0 ]; then IFS= read -r elapsed <"$sample/elapsed"; at=$((at + elapsed)); fi
            if [ -f "$control/advance" ]; then IFS= read -r advance <"$control/advance"; at=$((at + advance)); fi
            printf '%s\n' "$at"
            printf '%s' "$((calls + 1))" >"$control/epoch-count"
        else printf '2026-10-15T00:00:00Z\n'; fi;;
    sample-sleep)
        if [ -f "$control/tamper-after-cycle" ]; then printf '\n# tampered\n' >>"$root/manifest-v1.txt"; fi
        index=$((index + 1))
        [ "$index" -lt "$count" ] || exit 1
        printf '%s' "$index" >"$control/index"
        printf 0 >"$control/epoch-count";;
    docker)
        if [ "$1" != --host ] || [ "$2" != unix:///tmp/relay-fixture.sock ]; then printf 'unsafe_endpoint\n' >>"$control/log"; exit 1; fi
        if [ -n "${DOCKER_CONTEXT-}${DOCKER_HOST-}${DOCKER_TLS_VERIFY-}" ]; then printf 'ambient_context\n' >>"$control/log"; exit 1; fi
        shift 2
        case "$1 $2" in
            'info --format') IFS= read -r value <"$control/engine"; printf '%s\n' "$value";;
            'container ls')
                while IFS= read -r candidate || [ -n "$candidate" ]; do
                    [ -f "$control/stopped-$candidate" ] || printf '%s\n' "$candidate"
                done <"$sample/ids";;
            'container inspect')
                for arg in "$@"; do candidate=$arg; done
                [ -f "$control/containers/$candidate" ] || exit 1
                /bin/cat "$control/containers/$candidate"
                if [ -f "$control/stopped-$candidate" ]; then
                    printf 'false\n'
                    if [ -f "$control/post-stop-status" ]; then /bin/cat "$control/post-stop-status"; printf '\n'; else printf 'exited\n'; fi
                else printf 'true\nrunning\n'; fi
                if [ -f "$control/slow-inspect" ]; then printf '4' >"$control/advance"; fi
                printf 'relay-watchdog-v1-end\n';;
            'container stop')
                shift 2
                while [ "$1" != -- ]; do shift; done
                shift
                for candidate in "$@"; do
                    /usr/bin/grep -Fx "$candidate" "$root/stop-ledger-v1.txt" >/dev/null || { printf 'intent_missing\n' >>"$control/log"; exit 1; }
                done
                /usr/bin/grep -F '"last_action":"stop_requested"' "$root/status-v1.json" >/dev/null || { printf 'intent_missing\n' >>"$control/log"; exit 1; }
                behavior=success
                if [ -f "$control/stop-behavior" ]; then IFS= read -r behavior <"$control/stop-behavior"; fi
                case "$behavior" in timeout) trap '' TERM; exec /bin/sleep 30;; failure) exit 1;; esac
                for candidate in "$@"; do : >"$control/stopped-$candidate"; printf '%s\n' "$candidate"; done
                [ "$behavior" != nonzero-inactive ];;
            *) exit 1;;
        esac;;
    *) exit 1;;
esac
`

func TestFractionalSpeedAndBatteryThresholdsMatchGuard(t *testing.T) {
	t.Run("speed", func(t *testing.T) {
		s := healthy()
		s.therm = "CPU_Speed_Limit = 20\n"
		f := newFixture(t, s, s, s)
		f.config.MinimumCPUSpeedPercent = 20.5
		f.render()
		if got := f.run().LastAction; got != "confirmed_inactive" {
			t.Fatal(got)
		}
		f.assertStops(1)
	})
	t.Run("battery", func(t *testing.T) {
		f := newFixture(t, lowBattery())
		f.config.LowBatteryPercent = 10.5
		f.render()
		if got := f.run().LastAction; got != "confirmed_inactive" {
			t.Fatal(got)
		}
		f.assertStops(1)
	})
}
