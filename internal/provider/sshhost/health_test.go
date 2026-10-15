package sshhost

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

const healthNativeMachine = "a1234567-89ab-cdef-0123-456789abcdef"

type healthTestExecutor struct {
	t           *testing.T
	platform    string
	engine      string
	socket      string
	failMachine bool
	failDocker  bool
	oversize    bool
	calls       [][]string
}

func newHealthTestExecutor(t *testing.T) *healthTestExecutor {
	return &healthTestExecutor{t: t, platform: "Darwin x86_64", engine: "synthetic-engine-01", socket: "unix:///var/run/docker.sock"}
}

func (e *healthTestExecutor) Run(ctx context.Context, env map[string]string, argv []string, _ io.Reader, out io.Writer) (Result, error) {
	e.t.Helper()
	e.calls = append(e.calls, append([]string(nil), argv...))
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > hosthealth.CommandTimeout+time.Second {
		e.t.Fatal("health command has no finite per-command bound")
	}
	if out == nil {
		out = io.Discard
	}
	cleared := false
	if argv[0] == "/usr/bin/env" {
		if !slices.Equal(argv[:5], []string{"/usr/bin/env", "-u", "DOCKER_CONTEXT", "-u", "DOCKER_HOST"}) {
			e.t.Fatal("ambient Docker overrides were not cleared")
		}
		argv, cleared = argv[5:], true
	}
	write := func(text string) (Result, error) {
		_, err := io.WriteString(out, text)
		return Result{Stderr: []byte("private host diagnostic")}, err
	}
	switch {
	case slices.Equal(argv, []string{"/usr/bin/uname", "-s"}):
		if e.failMachine {
			return Result{ExitCode: 1}, nil
		}
		return write("Darwin\n")
	case slices.Equal(argv, []string{"/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice"}):
		return write(`"IOPlatformUUID" = "` + healthNativeMachine + `"` + "\n")
	case argv[0] == "test":
		if e.failDocker {
			return Result{ExitCode: 1}, nil
		}
		return Result{}, nil
	case argv[0] == "/bin/sh" && strings.Contains(argv[2], "DOCKER_CONTEXT"):
		return write("desktop-linux\n\n")
	case argv[0] == "/usr/local/bin/docker":
		if argv[1] == "context" {
			return write(strconv.Quote(e.socket))
		}
		if !cleared || len(argv) < 6 || argv[1] != "--host" || argv[2] != e.socket {
			e.t.Fatal("Docker did not use its verified local socket")
		}
		if argv[len(argv)-1] == "{{json .ID}}" {
			return write(strconv.Quote(e.engine))
		}
		if !strings.Contains(argv[len(argv)-1], `"NCPU"`) {
			e.t.Fatal("unexpected Docker probe")
		}
		if env["PATH"] != "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin" {
			e.t.Fatal("Docker helper path lost")
		}
		return write(`{"NCPU":16,"MemTotal":8320749568}`)
	}
	if !cleared {
		e.t.Fatal("health source did not clear ambient Docker overrides")
	}
	name := ""
	switch argv[0] {
	case "/usr/bin/uname":
		if e.oversize {
			return write(strings.Repeat("x", hosthealth.MaxSourceBytes+1))
		}
		return write(e.platform + "\n")
	case "/usr/bin/osascript":
		if !slices.Equal(argv, []string{"/usr/bin/osascript", "-l", "JavaScript", "-e", hosthealth.ThermalStateScript}) {
			e.t.Fatal("unexpected Foundation thermal probe")
		}
		name = "thermal-state.txt"
	case "/usr/sbin/sysctl":
		if slices.Equal(argv, []string{"/usr/sbin/sysctl", "-n", "hw.cputype"}) {
			return write("7\n")
		}
		name = "sysctl-metrics.txt"
	case "/usr/bin/pmset":
		name = map[string]string{"therm": "thermal-pressure.txt", "batt": "power-battery.txt", "custom": "sleep-settings.txt"}[argv[2]]
	case "/usr/bin/top":
		name = "top-sample.txt"
	case "/usr/bin/memory_pressure":
		name = "memory-pressure.txt"
	case "/usr/bin/sudo":
		if !slices.Equal(argv, []string{"/usr/bin/sudo", "-n", "-l"}) {
			e.t.Fatal("unexpected privileged command")
		}
		return Result{ExitCode: 1}, nil
	case "/bin/sh":
		if strings.Contains(argv[2], "AppleSmartBattery") {
			name = "smart-battery-ioreg.txt"
		} else if strings.Contains(argv[2], "IOAccelerator") {
			return Result{}, nil
		}
	}
	if name == "" {
		e.t.Fatal("unexpected health source command", argv)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "hosthealth", "testdata", name))
	if err != nil {
		e.t.Fatal(err)
	}
	return write(string(raw))
}

func healthMachineResource() string {
	return "resource_" + resourceDigest("darwin-platform-uuid", healthNativeMachine)
}

func TestHealthBridgeVerifiesMachineEngineSocketAndUsesBoundedSources(t *testing.T) {
	exec := newHealthTestExecutor(t)
	snapshot, err := collectHealth(context.Background(), healthBoundedExecutor{exec}, Endpoint{DockerPath: "/usr/local/bin/docker"}, healthMachineResource(), resourceDigest("docker-engine-id", exec.engine))
	if err != nil || snapshot.Validate() != nil || snapshot.Platform.Value == nil || *snapshot.Platform.Value != "macos_intel" || snapshot.DockerCPUCount.Value == nil || *snapshot.DockerCPUCount.Value != 16 {
		t.Fatal("bounded host snapshot failed", err)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || strings.Contains(string(raw), healthNativeMachine) || strings.Contains(string(raw), exec.engine) || strings.Contains(string(raw), "private host") || strings.Contains(string(raw), "desktop-linux") {
		t.Fatal("private host identity escaped snapshot")
	}
}

func TestHealthBridgeIdentityFailuresNeverBecomeUnsupported(t *testing.T) {
	for _, mode := range []string{"machine-unavailable", "machine-changed", "engine-changed", "remote-context"} {
		t.Run(mode, func(t *testing.T) {
			exec := newHealthTestExecutor(t)
			resource := healthMachineResource()
			engine := resourceDigest("docker-engine-id", exec.engine)
			switch mode {
			case "machine-unavailable":
				exec.failMachine = true
			case "machine-changed":
				resource = "resource_" + strings.Repeat("b", 64)
			case "engine-changed":
				exec.engine = "synthetic-other-engine"
			case "remote-context":
				exec.socket = "ssh://remote.example"
			}
			snapshot, err := collectHealth(context.Background(), healthBoundedExecutor{exec}, Endpoint{DockerPath: "/usr/local/bin/docker"}, resource, engine)
			if !errors.Is(err, ErrResourceIdentity) || snapshot.Platform.Value != nil {
				t.Fatal("unverified machine acquired a health classification", err)
			}
			for _, args := range exec.calls {
				if slices.Contains(args, "-sm") {
					t.Fatal("identity failure reached host sources")
				}
			}
		})
	}
}

func TestHealthBridgeNoImageCanObserveHostWithoutDockerBudget(t *testing.T) {
	exec := newHealthTestExecutor(t)
	exec.failDocker = true
	snapshot, err := collectHealth(context.Background(), healthBoundedExecutor{exec}, Endpoint{DockerPath: "/usr/local/bin/docker"}, healthMachineResource(), "")
	if err != nil || snapshot.Platform.Value == nil || *snapshot.Platform.Value != "macos_intel" || snapshot.ThermalLevel.Value == nil || snapshot.DockerCPUCount.Status != hosthealth.Unavailable {
		t.Fatal("image or Docker budget failure erased host metrics", err)
	}
}

func TestHealthBridgeOnlyObservedPlatformIsUnsupported(t *testing.T) {
	for _, platform := range []string{"Linux x86_64", "Darwin arm64"} {
		exec := newHealthTestExecutor(t)
		exec.platform = platform
		snapshot, err := collectHealth(context.Background(), healthBoundedExecutor{exec}, Endpoint{DockerPath: "/usr/local/bin/docker"}, healthMachineResource(), "")
		if err != nil || snapshot.Validate() != nil || snapshot.Platform.Value == nil || *snapshot.Platform.Value != "unsupported" {
			t.Fatal("observed unsupported platform lost", platform, err)
		}
	}
	exec := newHealthTestExecutor(t)
	exec.oversize = true
	snapshot, err := collectHealth(context.Background(), healthBoundedExecutor{exec}, Endpoint{DockerPath: "/usr/local/bin/docker"}, healthMachineResource(), "")
	if err != nil || snapshot.Platform.Value != nil || snapshot.Platform.Status != hosthealth.Unavailable {
		t.Fatal("bounded failed platform became supported", err)
	}
}

func TestCollectHealthPinsCanonicalAccountBeforeConnecting(t *testing.T) {
	server := newTestServer(t)
	server.Password = "synthetic-health-password"
	exec := newHealthTestExecutor(t)
	server.Handle = func(command string, stdin io.Reader, stdout, stderr io.Writer) int {
		ctx, cancel := context.WithTimeout(context.Background(), hosthealth.CommandTimeout)
		defer cancel()
		result, err := exec.Run(ctx, map[string]string{"PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"}, commandArgs(t, command), stdin, stdout)
		if err != nil {
			return 255
		}
		_, _ = stderr.Write(result.Stderr)
		return result.ExitCode
	}
	target := server.target()
	account, err := Account(target)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeFields(map[string]string{FieldHost: target.Host, FieldPort: strconv.Itoa(target.Port), FieldUsername: target.User, FieldPassword: server.Password, FieldDockerPath: "/usr/local/bin/docker"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := CollectHealth(context.Background(), raw, account, healthMachineResource(), resourceDigest("docker-engine-id", exec.engine))
	if err != nil || snapshot.Platform.Value == nil || *snapshot.Platform.Value != "macos_intel" {
		t.Fatal("pinned authenticated bridge failed", err)
	}
	before := len(server.Commands())
	other := target
	other.User = "different_user"
	otherAccount, err := Account(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = CollectHealth(context.Background(), raw, otherAccount, healthMachineResource(), ""); !errors.Is(err, ErrHostKeyMismatch) {
		t.Fatal("canonical account changed", err)
	}
	if len(server.Commands()) != before {
		t.Fatal("wrong canonical account reached SSH")
	}
	other = target
	other.HostKey = "SHA256:" + strings.Repeat("A", 43)
	otherAccount, err = Account(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = CollectHealth(context.Background(), raw, otherAccount, healthMachineResource(), ""); !errors.Is(err, ErrHostKeyMismatch) {
		t.Fatal("changed host key was accepted", err)
	}
}

type healthReaderOnly struct{ io.Reader }

func TestSSHStderrLimitCannotBeBypassedByIOCopy(t *testing.T) {
	b := &boundedBuffer{limit: 64}
	if _, err := io.Copy(b, healthReaderOnly{strings.NewReader(strings.Repeat("x", 1024))}); err != nil {
		t.Fatal(err)
	}
	if len(b.Bytes()) != 64 {
		t.Fatal("io.Copy bypassed stderr byte limit", len(b.Bytes()))
	}
}

type blockedHealthSession struct {
	closed chan struct{}
	once   sync.Once
}

func (e *blockedHealthSession) Close() error { e.once.Do(func() { close(e.closed) }); return nil }
func (e *blockedHealthSession) Run(ctx context.Context, _ map[string]string, _ []string, _ io.Reader, _ io.Writer) (Result, error) {
	<-e.closed
	return Result{}, ctx.Err()
}

func TestHealthCommandDeadlineAlsoClosesBlockedSessionCreation(t *testing.T) {
	session := &blockedHealthSession{closed: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (healthBoundedExecutor{session}).Run(ctx, nil, []string{"test", "-x", "/usr/bin/docker"}, nil, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		_ = session.Close()
		t.Fatal("per-command deadline did not close blocked SSH session")
	}
}
