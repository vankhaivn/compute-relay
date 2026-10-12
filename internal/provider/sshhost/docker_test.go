package sshhost

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

// scriptedExec answers remote commands from a function and records every call.
type scriptedExec struct {
	calls  [][]string
	envs   []map[string]string
	answer func(argv []string, stdin io.Reader, stdout io.Writer) (Result, error)
}

func (s *scriptedExec) Run(_ context.Context, env map[string]string, argv []string, stdin io.Reader, stdout io.Writer) (Result, error) {
	s.calls = append(s.calls, argv)
	s.envs = append(s.envs, env)
	if stdout == nil {
		stdout = io.Discard
	}
	return s.answer(argv, stdin, stdout)
}

func TestFindDockerUsesConfiguredPathOrFirstExecutableCandidate(t *testing.T) {
	exec := &scriptedExec{answer: func(argv []string, _ io.Reader, _ io.Writer) (Result, error) {
		if argv[2] == "/usr/bin/docker" {
			return Result{}, nil
		}
		return Result{ExitCode: 1}, nil
	}}
	found, err := findDocker(context.Background(), exec, "")
	if err != nil || found != "/usr/bin/docker" {
		t.Fatalf("found %q, %v", found, err)
	}
	if len(exec.calls) != 3 || exec.calls[0][0] != "test" || exec.calls[0][2] != DockerCandidates[0] {
		t.Fatalf("calls = %q", exec.calls)
	}
	if _, err = findDocker(context.Background(), exec, "/opt/docker/bin/docker"); !errors.Is(err, ErrRemoteDocker) {
		t.Fatalf("missing configured path: %v", err)
	}
	for _, bad := range []string{"docker", "/usr/bin/../bin/docker", "/usr/bin/doc ker"} {
		if _, err = findDocker(context.Background(), exec, bad); !errors.Is(err, ErrRequest) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

func TestDockerCommandsRunTheAbsoluteCLIWithItsDirectoryOnPath(t *testing.T) {
	exec := &scriptedExec{answer: func([]string, io.Reader, io.Writer) (Result, error) { return Result{}, nil }}
	d := docker{exec: exec, binary: "/usr/local/bin/docker"}
	if err := d.ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(exec.calls[0], []string{"/usr/local/bin/docker", "version"}) ||
		exec.envs[0]["PATH"] != "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin" {
		t.Fatalf("call %q env %q", exec.calls[0], exec.envs[0])
	}
}

func TestInspectDistinguishesMissingFoundAndMalformedContainers(t *testing.T) {
	reply := func(code int, stdout, stderr string) func([]string, io.Reader, io.Writer) (Result, error) {
		return func(_ []string, _ io.Reader, w io.Writer) (Result, error) {
			_, _ = io.WriteString(w, stdout)
			return Result{ExitCode: code, Stderr: []byte(stderr)}, nil
		}
	}
	ctx := context.Background()
	d := docker{exec: &scriptedExec{answer: reply(1, "[]", "Error: No such container: relay-x")}, binary: "/usr/bin/docker"}
	if _, found, err := d.inspect(ctx, "relay-x"); found || err != nil {
		t.Fatalf("missing: %v %v", found, err)
	}
	d.exec = &scriptedExec{answer: reply(0, `[{"Id":"abc","Name":"/relay-x","Image":"sha256:1","State":{"Status":"exited","ExitCode":3,
		"StartedAt":"2026-10-12T01:02:03.5Z","FinishedAt":"2026-10-12T01:03:03Z"},"Config":{"Labels":{"a":"b"}}}]`, "")}
	info, found, err := d.inspect(ctx, "relay-x")
	if err != nil || !found || !info.Started() || info.State.ExitCode != 3 || info.Config.Labels["a"] != "b" {
		t.Fatalf("found=%v err=%v info=%+v", found, err, info)
	}
	d.exec = &scriptedExec{answer: reply(0, `[{"Id":"abc","Name":"/relay-x","State":{"Status":"created","StartedAt":"0001-01-01T00:00:00Z"}}]`, "")}
	if info, _, _ = d.inspect(ctx, "relay-x"); info.Started() {
		t.Fatal("created container reported as started")
	}
	for _, bad := range []string{`[]`, `[{"Id":"abc","Name":"/other"}]`, `not json`} {
		d.exec = &scriptedExec{answer: reply(0, bad, "")}
		if _, _, err = d.inspect(ctx, "relay-x"); !errors.Is(err, ErrRemoteDocker) {
			t.Errorf("%s: %v", bad, err)
		}
	}
	d.exec = &scriptedExec{answer: reply(1, "", "Cannot connect to the Docker daemon")}
	if _, _, err = d.inspect(ctx, "relay-x"); !errors.Is(err, ErrRemoteDocker) {
		t.Fatalf("daemon down: %v", err)
	}
}

func TestContainerSpecIsHardenedAndDeterministic(t *testing.T) {
	spec := containerSpec{Name: "relay-x", Image: "sha256:abc", Labels: map[string]string{"z": "1", "a": "2"},
		CPUs: "2", MemoryBytes: 1 << 30, PIDs: 512, Command: []string{"python3", "run.py"}}
	got := strings.Join(spec.args(), " ")
	for _, want := range []string{"--user 65534:65534", "--network none", "--cap-drop ALL", "--security-opt no-new-privileges",
		"--pids-limit 512", "--memory 1073741824", "--memory-swap 1073741824", "--cpus 2", "--restart no",
		"--label a=2 --label z=1 sha256:abc python3 run.py"} {
		if !strings.Contains(got, want) {
			t.Errorf("args %q lack %q", got, want)
		}
	}
	spec.Internet, spec.CPUs, spec.MemoryBytes = true, "", 0
	got = strings.Join(spec.args(), " ")
	if !strings.Contains(got, "--network bridge") || strings.Contains(got, "--cpus") || strings.Contains(got, "--memory") {
		t.Fatalf("unlimited spec args %q", got)
	}
}

func TestCreateRequiresAContainerID(t *testing.T) {
	id := strings.Repeat("a", 64)
	for name, tc := range map[string]struct {
		code   int
		stdout string
		ok     bool
	}{"created": {0, id + "\n", true}, "conflict": {1, "", false}, "garbled": {0, "warning\n" + id, false}} {
		d := docker{exec: &scriptedExec{answer: func(_ []string, _ io.Reader, w io.Writer) (Result, error) {
			_, _ = io.WriteString(w, tc.stdout)
			return Result{ExitCode: tc.code}, nil
		}}, binary: "/usr/bin/docker"}
		got, err := d.create(context.Background(), containerSpec{Name: "relay-x", Image: "sha256:abc", PIDs: 1, Command: []string{"true"}})
		if tc.ok != (err == nil) || tc.ok && got != id {
			t.Errorf("%s: %q %v", name, got, err)
		}
	}
}

func TestCopyOutReportsMissingPathsSeparately(t *testing.T) {
	d := docker{exec: &scriptedExec{answer: func([]string, io.Reader, io.Writer) (Result, error) {
		return Result{ExitCode: 1, Stderr: []byte("Error response from daemon: Could not find the file /relay/work/x in container relay-x")}, nil
	}}, binary: "/usr/bin/docker"}
	found, err := d.copyOut(context.Background(), "relay-x", "/relay/work/x", io.Discard)
	if found || err != nil {
		t.Fatalf("missing path: %v %v", found, err)
	}
}

func TestLimitedWriterFailsInsteadOfTruncating(t *testing.T) {
	var out bytes.Buffer
	w := &limitedWriter{w: &out, limit: 4}
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("de")); !errors.Is(err, errResponseLimit) || out.String() != "abc" {
		t.Fatalf("err=%v out=%q", err, out.String())
	}
}
