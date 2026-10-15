package hosthealth

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

type executorFunc func(context.Context, []string, io.Writer, io.Writer) error

func (f executorFunc) Run(ctx context.Context, argv []string, out, err io.Writer) error {
	return f(ctx, argv, out, err)
}
func TestCollectorUsesFixedCommandsAndPinnedDocker(t *testing.T) {
	f := fixtures(t)
	seen := map[Source]bool{}
	cfg := Config{DockerPath: "/usr/local/bin/docker", DockerHost: "unix:///Users/REDACTED/.docker/run/docker.sock"}
	fixed := commands(cfg)
	exec := executorFunc(func(ctx context.Context, argv []string, out, stderr io.Writer) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > CommandTimeout {
			t.Fatal("missing timeout")
		}
		source := Powermetrics
		for _, cmd := range fixed {
			if reflect.DeepEqual(argv, cmd.argv) {
				source = cmd.source
			}
		}
		if source == Powermetrics && !reflect.DeepEqual(argv, []string{"/usr/bin/sudo", "-n", "/usr/bin/powermetrics", "--samplers", "smc", "-n", "1", "-i", "1000"}) {
			t.Fatalf("unexpected command %q", argv)
		}
		seen[source] = true
		_, err := out.Write(f[source].Data)
		return err
	})
	s := Collect(context.Background(), exec, testResource, cfg)
	requireValue(t, s.CPUCount, 16)
	requireValue(t, s.SudoGrant, "narrow")
	requireValue(t, s.CPUDieTemperatureCelsius, 44.55)
	if !seen[Docker] || !seen[GPU] || !seen[Powermetrics] {
		t.Fatal("missing fixed sources")
	}
	for _, cfg := range []Config{{DockerPath: "/bin/docker"}, {DockerPath: "/bin/docker", DockerHost: "tcp://remote:2375"}, {DockerPath: "docker", DockerHost: "unix:///docker.sock"}} {
		for _, cmd := range commands(cfg) {
			if cmd.source == Docker {
				t.Fatal("unverified Docker endpoint accepted")
			}
		}
	}
}
func TestCollectorCapsCopyAndStderr(t *testing.T) {
	for _, stderr := range []bool{false, true} {
		exec := executorFunc(func(_ context.Context, _ []string, out, err io.Writer) error {
			if stderr {
				out = err
			}
			_, e := io.Copy(out, strings.NewReader(strings.Repeat("x", MaxSourceBytes+1)))
			return e
		})
		sample := collectSource(context.Background(), exec, []string{"fixed"})
		if sample.Status != Unavailable || sample.Reason != "output_limit" || len(sample.Data) != 0 {
			t.Fatal("output cap bypassed")
		}
	}
	var buffer cappedBuffer
	if _, ok := any(&buffer).(io.ReaderFrom); ok {
		t.Fatal("ReadFrom bypasses Write cap")
	}
	_, _ = io.Copy(&buffer, strings.NewReader(strings.Repeat("x", MaxSourceBytes*2)))
	if buffer.buffer.Len() != MaxSourceBytes {
		t.Fatal("unbounded buffer")
	}
}
func TestCollectorFailuresAndCancellation(t *testing.T) {
	partial := executorFunc(func(_ context.Context, _ []string, out, stderr io.Writer) error {
		_, _ = io.WriteString(out, "hw.ncpu: 16\n")
		return errors.New("NEVER_PERSIST")
	})
	sample := collectSource(context.Background(), partial, []string{"fixed"})
	if sample.Status != Unavailable || sample.Reason != "command_failed" || len(sample.Data) != 0 {
		t.Fatal("partial failed output accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waiting := executorFunc(func(ctx context.Context, _ []string, out, stderr io.Writer) error { <-ctx.Done(); return ctx.Err() })
	sample = collectSource(ctx, waiting, []string{"fixed"})
	if sample.Reason != "collection_timeout" {
		t.Fatal(sample)
	}
	s := Collect(ctx, waiting, testResource, Config{})
	if s.Platform.Status != Unavailable {
		t.Fatal("failed platform became unsupported")
	}
	s = Collect(context.Background(), nil, testResource, Config{})
	if s.Platform.Status != Unavailable {
		t.Fatal("nil executor")
	}
}
func TestUnsupportedCollectorDoesNotRunMacCommands(t *testing.T) {
	calls := 0
	exec := executorFunc(func(_ context.Context, argv []string, out, stderr io.Writer) error {
		calls++
		_, err := io.WriteString(out, "Linux x86_64\n")
		return err
	})
	s := Collect(context.Background(), exec, testResource, Config{})
	if calls != 1 || s.ThermalLevel.Status != Unsupported {
		t.Fatal("unsupported host sampled")
	}
}

func TestCollectionHonorsOverallCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	calls := 0
	exec := executorFunc(func(ctx context.Context, argv []string, out, stderr io.Writer) error {
		calls++
		if calls == 1 {
			_, err := io.WriteString(out, "Darwin x86_64\n")
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
	start := time.Now()
	s := Collect(ctx, exec, testResource, Config{})
	if time.Since(start) > time.Second || s.CPUCount.Status != Unavailable {
		t.Fatal("collection exceeded caller deadline")
	}
}

func TestCollectorQualifiesNativeArchitectureBeforeMetrics(t *testing.T) {
	for _, tt := range []struct {
		name, platform, native string
		failed                 bool
		want                   Status
		calls                  int
	}{
		{"rosetta", "Darwin x86_64\n", "16777228\n", false, Unsupported, 2},
		{"native_arm", "Darwin arm64\n", "", false, Unsupported, 1},
		{"linux", "Linux x86_64\n", "", false, Unsupported, 1},
		{"failed_probe", "Darwin x86_64\n", "7\n", true, Unavailable, 2},
		{"empty_probe", "Darwin x86_64\n", "", false, Unavailable, 2},
		{"hostile_probe", "Darwin x86_64\n", "7\nHOSTILE", false, Unavailable, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			exec := executorFunc(func(_ context.Context, argv []string, out, stderr io.Writer) error {
				calls++
				switch calls {
				case 1:
					if !reflect.DeepEqual(argv, []string{"/usr/bin/uname", "-sm"}) {
						t.Fatal("first command is not uname")
					}
					_, err := io.WriteString(out, tt.platform)
					return err
				case 2:
					if !reflect.DeepEqual(argv, []string{"/usr/sbin/sysctl", "-n", "hw.cputype"}) {
						t.Fatal("native probe missing")
					}
					_, err := io.WriteString(out, tt.native)
					if tt.failed {
						return errors.New("probe failed")
					}
					return err
				default:
					t.Fatalf("unexpected metric or privileged command %q", argv)
					return nil
				}
			})
			s := Collect(context.Background(), exec, testResource, Config{})
			if calls != tt.calls || s.ThermalLevel.Status != tt.want {
				t.Fatalf("calls=%d status=%s", calls, s.ThermalLevel.Status)
			}
			if err := s.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
