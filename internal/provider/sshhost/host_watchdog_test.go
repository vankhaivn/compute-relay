package sshhost

import (
	"context"
	"errors"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/hostwatchdog"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost/watchdog"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func watchdogTestPlan(t *testing.T) (hostwatchdog.Plan, watchdog.Artifact) {
	t.Helper()
	p := hostwatchdog.Plan{UID: strconv.Itoa(os.Getuid()), Config: watchdog.Config{Home: t.TempDir(), InstallationID: "synthetic-installation", DockerPath: "/usr/local/bin/docker", DockerHost: "unix:///var/run/docker.sock", EngineID: "synthetic-engine-01", CriticalThermalLevel: 2, MinimumCPUSpeedPercent: 50.5, CriticalDieTemperatureCelsius: 100, CriticalDurationSeconds: 60, LowBatteryPercent: 10.5, SampleIntervalSeconds: 30}}
	a, err := watchdog.Render(p.Config)
	if err != nil {
		t.Fatal(err)
	}
	return p, a
}
func TestWatchdogMutationExactFilesReplayAndPartialRemoval(t *testing.T) {
	p, a := watchdogTestPlan(t)
	fake := filepath.Join(t.TempDir(), "launchctl")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	run := func(action string) error {
		script, err := watchdogMutation(a, p, action, false)
		if err != nil {
			return err
		}
		script = strings.ReplaceAll(script, "/bin/launchctl", watchdogQuote(fake))
		if runtime.GOOS == "linux" {
			script = strings.ReplaceAll(script, "/usr/bin/base64 -D", "/usr/bin/base64 -d")
			script = strings.ReplaceAll(script, "/usr/bin/stat -f '%u:%Lp'", "/usr/bin/stat -c '%u:%a'")
			script = strings.ReplaceAll(script, "/usr/bin/stat -f '%u'", "/usr/bin/stat -c '%u'")
		}
		cmd := exec.Command("/bin/sh", "-s")
		cmd.Stdin = strings.NewReader(script)
		out, err := cmd.CombinedOutput()
		if len(out) > 0 {
			t.Logf("bounded shell output: %.200s", out)
		}
		return err
	}
	if err := run("install_host_watchdog"); err != nil {
		t.Fatal(err)
	}
	ledger := filepath.Join(a.Directory, watchdog.LedgerName)
	if err := os.WriteFile(ledger, []byte("preserve\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run("install_host_watchdog"); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(ledger)
	if string(raw) != "preserve\n" {
		t.Fatal("replay reset stop ledger")
	}
	scriptPath := filepath.Join(a.Directory, watchdog.ScriptName)
	if err := os.WriteFile(scriptPath, []byte("tampered"), 0700); err != nil {
		t.Fatal(err)
	}
	if run("remove_host_watchdog") == nil {
		t.Fatal("digest mismatch removed files")
	}
	if _, err := os.Stat(ledger); err != nil {
		t.Fatal("failed integrity mutated runtime files")
	}
	if err := os.WriteFile(scriptPath, a.Files[0].Content, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(a.Directory, watchdog.PlistName)); err != nil {
		t.Fatal(err)
	}
	if err := run("remove_host_watchdog"); err != nil {
		t.Fatal("partial owned install cannot be removed", err)
	}
	if _, err := os.Stat(a.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("directory retained", err)
	}
	if err := run("remove_host_watchdog"); err != nil {
		t.Fatal("remove not idempotent", err)
	}
}
func TestWatchdogLaunchdAbsenceIsSpecificAndForeignPathRefused(t *testing.T) {
	p, a := watchdogTestPlan(t)
	for _, tc := range []struct {
		name, out, stderr string
		code              int
		loaded, fail      bool
	}{
		{name: "loaded", out: "path = " + a.Directory + "/" + watchdog.PlistName + "\n", loaded: true},
		{name: "absent", stderr: "Could not find service \"" + a.Label + "\" in domain for user gui: 501", code: 113},
		{name: "permission", stderr: "Operation not permitted", code: 113, fail: true},
		{name: "domain missing", stderr: "Could not find domain for", code: 113, fail: true},
		{name: "foreign label", out: "path = /unrelated/agent.plist\n", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := &scriptedExec{answer: func(_ []string, _ io.Reader, w io.Writer) (Result, error) {
				_, err := io.WriteString(w, tc.out)
				return Result{ExitCode: tc.code, Stderr: []byte(tc.stderr)}, err
			}}
			loaded, err := watchdogServiceState(context.Background(), watchdogBoundedExecutor{x}, a, p)
			if loaded != tc.loaded || (err != nil) != tc.fail {
				t.Fatal(loaded, err)
			}
		})
	}
}

type watchdogTestSession struct{ *scriptedExec }

func (*watchdogTestSession) Close() error { return nil }
func TestWatchdogUnsupportedPlatformsRefuseBeforeMutation(t *testing.T) {
	for _, tc := range []struct{ name, system, platform, native string }{{"linux", "Linux", "Linux x86_64", ""}, {"arm", "Darwin", "Darwin arm64", "16777228"}, {"translated", "Darwin", "Darwin x86_64", "16777228"}} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint := Target{Host: "synthetic.invalid", Port: 22, User: "synthetic", HostKey: testFingerprint}
			account, err := Account(endpoint)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := EncodeFields(map[string]string{FieldHost: endpoint.Host, FieldUsername: endpoint.User, FieldPassword: "SYNTHETIC_PASSWORD"})
			if err != nil {
				t.Fatal(err)
			}
			nativeID := healthNativeMachine
			kind := "darwin-platform-uuid"
			if tc.system == "Linux" {
				nativeID = strings.Repeat("a", 32)
				kind = "linux-machine-id"
			}
			target := hostwatchdog.Target{Health: hosthealth.Target{Workspace: "workspace", ConnectionID: "ssh-fixture", CanonicalAccount: account, CredentialRef: "vault:ssh-fixture", ResourceID: "resource_" + resourceDigest(kind, nativeID)}, Installation: "synthetic-installation", Engine: strings.Repeat("e", 64)}
			w := NewWatchdogRemote(&testVault{raw: raw})
			w.dial = func(context.Context, Target, Credential) (session, error) {
				return &watchdogTestSession{&scriptedExec{answer: func(args []string, _ io.Reader, out io.Writer) (Result, error) {
					var value string
					switch strings.Join(args, " ") {
					case "/usr/bin/uname -s":
						value = tc.system
					case "/usr/bin/uname -sm":
						value = tc.platform
					case "/bin/cat /etc/machine-id":
						value = nativeID
					case "/usr/sbin/ioreg -rd1 -c IOPlatformExpertDevice":
						value = `"IOPlatformUUID" = "` + nativeID + `"`
					case "/usr/sbin/sysctl -n hw.cputype":
						value = tc.native
					default:
						t.Fatal("unsupported host reached unexpected command", args)
					}
					_, err := io.WriteString(out, value+"\n")
					return Result{}, err
				}}}, nil
			}
			_, err = w.Plan(context.Background(), target)
			if !errors.Is(err, hostwatchdog.ErrUnsupported) {
				t.Fatal(err)
			}
		})
	}
}

type watchdogPipelineSession struct {
	base             *healthTestExecutor
	artifact         watchdog.Artifact
	loaded           bool
	bootstraps       int
	bootouts         int
	deletions        int
	filesPresent     bool
	bootoutDelay     time.Duration
	bootoutUncertain bool
	unloadingReads   int
	unloadingForeign bool
}

func (s *watchdogPipelineSession) Close() error { return nil }
func (s *watchdogPipelineSession) Run(ctx context.Context, env map[string]string, args []string, in io.Reader, out io.Writer) (Result, error) {
	s.base.t.Helper()
	deadline, ok := ctx.Deadline()
	budget := hosthealth.CommandTimeout
	if len(args) == 3 && args[0] == "/bin/launchctl" && args[1] == "bootout" {
		budget = watchdogBootoutTimeout
		if args[2] != "gui/501/"+s.artifact.Label {
			s.base.t.Fatal("bootout target widened")
		}
	}
	if !ok || time.Until(deadline) > budget+time.Second {
		s.base.t.Fatal("watchdog command lost per-command bound")
	}
	write := func(value string) (Result, error) { _, err := io.WriteString(out, value); return Result{}, err }
	switch {
	case args[0] == "/bin/launchctl" && args[1] == "bootout":
		s.bootouts++
		select {
		case <-ctx.Done():
			if s.bootoutUncertain {
				s.loaded = false
			}
			return Result{}, ctx.Err()
		case <-time.After(s.bootoutDelay):
		}
		if s.unloadingReads == 0 {
			s.loaded = false
		}
		return Result{}, nil
	case args[0] == "/bin/launchctl":
		if s.bootouts > 0 && s.unloadingForeign {
			return write("path = /unrelated/agent.plist\n")
		}
		if s.bootouts > 0 && s.unloadingReads > 0 {
			s.unloadingReads--
			if s.unloadingReads == 0 {
				s.loaded = false
			}
			return write("path = " + s.artifact.Directory + "/" + watchdog.PlistName + "\n")
		}
		if !s.loaded {
			return Result{ExitCode: 113, Stderr: []byte("Could not find service \"" + s.artifact.Label + "\" in domain for user gui: 501")}, nil
		}
		return write("path = " + s.artifact.Directory + "/" + watchdog.PlistName + "\n")
	case strings.Join(args, " ") == "/usr/bin/uname -sm":
		return write("Darwin x86_64\n")
	case strings.Join(args, " ") == "/usr/sbin/sysctl -n hw.cputype":
		return write("7\n")
	case args[len(args)-1] == "{{.ID}}":
		return write(s.base.engine + "\n")
	case len(args) > 2 && args[0] == "/usr/bin/env" && args[1] == "-i":
		script, err := io.ReadAll(in)
		if err != nil {
			return Result{}, err
		}
		if strings.Contains(string(script), "/bin/launchctl bootout") {
			s.base.t.Fatal("bootout was embedded in short shell command")
		}
		if !s.filesPresent && strings.Contains(string(script), "/bin/rm -f") {
			return Result{}, nil
		}
		if !strings.Contains(string(script), "/usr/bin/shasum -a 256") && strings.Contains(string(script), "test ! -e") {
			if s.filesPresent {
				return Result{ExitCode: 35}, nil
			}
			return Result{}, nil
		}
		if !strings.Contains(string(script), "/usr/bin/shasum -a 256") {
			s.base.t.Fatal("mutation skipped exact digest verification")
		}
		if strings.Contains(string(script), "/bin/launchctl bootstrap") {
			s.bootstraps++
			s.loaded = true
			s.filesPresent = true
		}
		if strings.Contains(string(script), "/bin/rm -f") {
			if s.loaded {
				s.base.t.Fatal("deleted files before unload confirmation")
			}
			s.deletions++
			s.filesPresent = false
		}
		return Result{}, nil
	default:
		return s.base.Run(ctx, env, args, in, out)
	}
}
func newWatchdogPipeline(t *testing.T) (*WatchdogRemote, *watchdogPipelineSession, hostwatchdog.Target, hostwatchdog.Plan) {
	t.Helper()
	p, a := watchdogTestPlan(t)
	p.UID = "501"
	endpoint := Target{Host: "synthetic.invalid", Port: 22, User: "synthetic", HostKey: testFingerprint}
	account, err := Account(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeFields(map[string]string{FieldHost: endpoint.Host, FieldUsername: endpoint.User, FieldPassword: "SYNTHETIC_PASSWORD"})
	if err != nil {
		t.Fatal(err)
	}
	base := newHealthTestExecutor(t)
	p.Config.EngineID = base.engine
	p.Config.DockerHost = base.socket
	target := hostwatchdog.Target{Health: hosthealth.Target{Workspace: "workspace", ConnectionID: "ssh-fixture", CanonicalAccount: account, CredentialRef: "vault:ssh-fixture", ResourceID: "resource_" + resourceDigest("darwin-platform-uuid", healthNativeMachine)}, Installation: p.Config.InstallationID, Engine: resourceDigest("docker-engine-id", base.engine)}
	// Render after the final pins, matching production's private frozen plan.
	a, err = watchdog.Render(p.Config)
	if err != nil {
		t.Fatal(err)
	}
	fake := &watchdogPipelineSession{base: base, artifact: a}
	remote := NewWatchdogRemote(&testVault{raw: raw})
	remote.dial = func(context.Context, Target, Credential) (session, error) { return fake, nil }
	return remote, fake, target, p
}
func TestWatchdogApplyBoundedPipelineRecognizesInitialAbsenceAndLoadedReplay(t *testing.T) {
	remote, fake, target, p := newWatchdogPipeline(t)
	if err := remote.Apply(context.Background(), target, p, "install_host_watchdog"); err != nil {
		t.Fatal("bounded wrapper discarded service-absence proof", err)
	}
	if !fake.loaded || fake.bootstraps != 1 {
		t.Fatal("initial install did not bootstrap once")
	}
	if err := remote.Apply(context.Background(), target, p, "install_host_watchdog"); err != nil {
		t.Fatal(err)
	}
	if fake.bootstraps != 1 {
		t.Fatal("installed service bootstrapped again")
	}
}
func TestWatchdogBoundedDiagnosticsArePrivateAndCapped(t *testing.T) {
	for _, args := range [][]string{{"/bin/launchctl", "print", "gui/501/synthetic"}, {"/usr/bin/sudo", "-n", "-l"}} {
		diagnostic := []byte(strings.Repeat("x", maxStderrBytes+16))
		fake := &scriptedExec{answer: func(_ []string, _ io.Reader, _ io.Writer) (Result, error) { return Result{Stderr: diagnostic}, nil }}
		result, err := (watchdogBoundedExecutor{fake}).Run(context.Background(), nil, args, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if args[0] == "/bin/launchctl" {
			if len(result.Stderr) != maxStderrBytes {
				t.Fatal("diagnostic not bounded")
			}
			clear(result.Stderr)
		} else if len(result.Stderr) != 0 {
			t.Fatal("unconsumed private diagnostic retained")
		}
		for _, value := range diagnostic {
			if value != 0 {
				t.Fatal("discarded private diagnostic was not cleared")
			}
		}
	}
}

func TestWatchdogDelayedBootoutUsesDedicatedBudgetAndDeletesOnce(t *testing.T) {
	remote, fake, target, p := newWatchdogPipeline(t)
	fake.loaded = true
	fake.filesPresent = true
	fake.bootoutDelay = hosthealth.CommandTimeout + 200*time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), hostwatchdog.Timeout)
	defer cancel()
	started := time.Now()
	if err := remote.Apply(ctx, target, p, "remove_host_watchdog"); err != nil {
		t.Fatal("bootout exceeding ordinary probe budget failed", err)
	}
	if time.Since(started) <= hosthealth.CommandTimeout || fake.bootouts != 1 || fake.deletions != 1 || fake.loaded || fake.filesPresent {
		t.Fatal("removal did not unload and clean exactly once")
	}
	for range 2 {
		confirmed, err := remote.Observe(context.Background(), target, p, "remove_host_watchdog")
		if err != nil || !confirmed {
			t.Fatal("read-only removal confirmation failed", confirmed, err)
		}
	}
	if fake.bootouts != 1 || fake.deletions != 1 {
		t.Fatal("observation replayed mutation")
	}
}

func TestWatchdogAsynchronousUnloadWaitsWithoutRepeatingMutation(t *testing.T) {
	remote, fake, target, p := newWatchdogPipeline(t)
	fake.loaded, fake.filesPresent = true, true
	fake.unloadingReads = 2
	ctx, cancel := context.WithTimeout(context.Background(), hostwatchdog.Timeout)
	defer cancel()
	if err := remote.Apply(ctx, target, p, "remove_host_watchdog"); err != nil {
		t.Fatal("asynchronous unload failed", err)
	}
	if fake.bootouts != 1 || fake.deletions != 1 || fake.loaded || fake.filesPresent {
		t.Fatal("unload must complete before exactly one cleanup")
	}
}

func TestWatchdogUnloadingUncertaintyRetainsFiles(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(strconv.FormatBool(foreign), func(t *testing.T) {
			remote, fake, target, p := newWatchdogPipeline(t)
			fake.loaded, fake.filesPresent = true, true
			fake.unloadingReads, fake.unloadingForeign = 100, foreign
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if err := remote.Apply(ctx, target, p, "remove_host_watchdog"); err == nil {
				t.Fatal("uncertain unload reported success")
			}
			if fake.bootouts != 1 || fake.deletions != 0 || !fake.filesPresent {
				t.Fatal("uncertain unload repeated mutation or deleted files")
			}
			confirmed, _ := remote.Observe(context.Background(), target, p, "remove_host_watchdog")
			if confirmed || fake.bootouts != 1 || fake.deletions != 0 {
				t.Fatal("observation replayed an uncertain removal")
			}
		})
	}
}
func TestWatchdogCancelledBootoutRetainsFilesAndObservationNeverRetries(t *testing.T) {
	remote, fake, target, p := newWatchdogPipeline(t)
	fake.loaded = true
	fake.filesPresent = true
	fake.bootoutDelay = time.Second
	fake.bootoutUncertain = true
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := remote.Apply(ctx, target, p, "remove_host_watchdog"); err == nil {
		t.Fatal("cancelled bootout reported success")
	}
	if time.Since(started) > time.Second || fake.bootouts != 1 || fake.deletions != 0 || !fake.filesPresent {
		t.Fatal("cancellation did not preserve cleanup ambiguity")
	}
	for range 2 {
		confirmed, _ := remote.Observe(context.Background(), target, p, "remove_host_watchdog")
		if confirmed {
			t.Fatal("files remain but removal confirmed")
		}
	}
	if fake.bootouts != 1 || fake.deletions != 0 {
		t.Fatal("ambiguous bootout or cleanup retried")
	}
}

type blockedWatchdogBootout struct {
	closed     chan struct{}
	started    chan []string
	once       sync.Once
	diagnostic []byte
}

func (b *blockedWatchdogBootout) Close() error { b.once.Do(func() { close(b.closed) }); return nil }
func (b *blockedWatchdogBootout) Run(_ context.Context, _ map[string]string, args []string, _ io.Reader, _ io.Writer) (Result, error) {
	b.started <- append([]string(nil), args...)
	<-b.closed
	return Result{Stderr: b.diagnostic}, ErrUnavailable
}
func TestWatchdogBootoutCancellationClosesBlockedClientAndDiscardsDiagnostic(t *testing.T) {
	p, a := watchdogTestPlan(t)
	p.UID = "501"
	blocked := &blockedWatchdogBootout{closed: make(chan struct{}), started: make(chan []string, 1), diagnostic: []byte("private host diagnostic")}
	defer blocked.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- (watchdogBoundedExecutor{blocked}).bootout(ctx, a, p) }()
	select {
	case args := <-blocked.started:
		if len(args) != 3 || args[0] != "/bin/launchctl" || args[1] != "bootout" || args[2] != "gui/501/"+a.Label {
			t.Fatal("bootout scope widened", args)
		}
	case <-time.After(time.Second):
		t.Fatal("bootout did not start")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked cancelled command succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation failed to close blocked client")
	}
	for _, value := range blocked.diagnostic {
		if value != 0 {
			t.Fatal("private stderr retained")
		}
	}
}
