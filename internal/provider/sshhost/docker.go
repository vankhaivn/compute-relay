package sshhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// StandardImage is the Relay-defined execution image: a multi-platform index pinned by
// digest (Python 3.11.17 on Debian bookworm, recorded 2026-10-12). The runner needs only
// the standard library; jobs that need more install pinned wheels during setup.
const StandardImage = "python:3.11-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89"

const secretTmpfsPath = "/relay/secrets"
const secretTmpfsOptions = "rw,noexec,nosuid,nodev,size=131072,mode=0700,uid=65534,gid=65534"

// DockerCandidates are checked in order when no explicit Docker path is configured.
// Non-interactive SSH sessions on macOS omit /usr/local/bin from PATH.
var DockerCandidates = []string{
	"/usr/local/bin/docker",
	"/opt/homebrew/bin/docker",
	"/usr/bin/docker",
	"/Applications/Docker.app/Contents/Resources/bin/docker",
}

var ErrRemoteDocker = errors.New("remote Docker command failed")

var pinnedImage = regexp.MustCompile(`^[a-z0-9][a-z0-9._/:-]{0,200}@sha256:[0-9a-f]{64}$`)
var engineIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:-]{15,127}$`)

// ValidImage accepts only an image reference pinned by digest.
func ValidImage(ref string) bool { return pinnedImage.MatchString(ref) }

type executor interface {
	Run(context.Context, map[string]string, []string, io.Reader, io.Writer) (Result, error)
}

// docker issues fixed argument arrays to one absolute Docker CLI. PATH includes the CLI
// directory so credential helpers installed beside it resolve without a login shell.
type docker struct {
	exec   executor
	binary string
	host   string
}

func (d docker) env() map[string]string {
	return map[string]string{"PATH": path.Dir(d.binary) + ":/usr/bin:/bin:/usr/sbin:/sbin"}
}

func (d docker) run(ctx context.Context, stdin io.Reader, stdout io.Writer, args ...string) (Result, error) {
	command := []string{d.binary}
	if d.host != "" {
		// Freeze this call's verified local socket instead of allowing an ambient
		// context change to redirect a later command in the same operation.
		command = []string{"/usr/bin/env", "-u", "DOCKER_CONTEXT", "-u", "DOCKER_HOST", d.binary, "--host", d.host}
	}
	return d.exec.Run(ctx, d.env(), append(command, args...), stdin, stdout)
}

// findDocker returns the configured path or the first executable candidate.
func findDocker(ctx context.Context, exec executor, configured string) (string, error) {
	candidates := DockerCandidates
	if configured != "" {
		candidates = []string{configured}
	}
	for _, candidate := range candidates {
		if !path.IsAbs(candidate) || path.Clean(candidate) != candidate || !safeArg.MatchString(candidate) {
			return "", ErrRequest
		}
		result, err := exec.Run(ctx, nil, []string{"test", "-x", candidate}, nil, nil)
		if err != nil {
			return "", err
		}
		if result.ExitCode == 0 {
			return candidate, nil
		}
	}
	return "", ErrRemoteDocker
}

func missing(result Result) bool {
	text := string(result.Stderr)
	return result.ExitCode != 0 && (strings.Contains(text, "No such container") || strings.Contains(text, "No such object") ||
		strings.Contains(text, "No such image"))
}

// ping proves the CLI reaches its daemon.
func (d docker) ping(ctx context.Context) error {
	result, err := d.run(ctx, nil, nil, "version")
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return ErrRemoteDocker
	}
	return nil
}

// platform reports the daemon platform, including Docker Desktop's Linux VM.
func (d docker) platform(ctx context.Context) (string, string, error) {
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 256}, "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}")
	if err != nil {
		return "", "", err
	}
	parts := strings.Split(strings.TrimSpace(out.String()), "/")
	if result.ExitCode != 0 || len(parts) != 2 {
		return "", "", ErrRemoteDocker
	}
	return parts[0], parts[1], nil
}

// systemTime reads the engine clock, including Docker Desktop's Linux VM clock.
// It launches no container or workload code.
func (d docker) systemTime(ctx context.Context) (time.Time, error) {
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 128}, "info", "--format", "{{.SystemTime}}")
	if err != nil || result.ExitCode != 0 {
		return time.Time{}, ErrRemoteDocker
	}
	at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(out.String()))
	if err != nil || at.IsZero() {
		return time.Time{}, ErrRemoteDocker
	}
	return at, nil
}

func (d docker) engineID(ctx context.Context) (string, error) {
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 256}, "info", "--format", "{{.ID}}")
	id := strings.TrimSpace(out.String())
	if err != nil || result.ExitCode != 0 || !engineIdentity.MatchString(id) {
		return "", ErrRemoteDocker
	}
	return id, nil
}

func (d docker) verifyEngine(ctx context.Context, expected string) error {
	if expected == "" {
		return nil // Legacy observation remains readable; resource cleanup refuses it.
	}
	current, err := d.engineID(ctx)
	if err != nil {
		return err
	}
	if current != expected {
		return ErrIdentity
	}
	return nil
}

type imageInfo struct {
	ID           string `json:"Id"`
	OS           string `json:"Os"`
	Architecture string `json:"Architecture"`
	Config       struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
}

func (d docker) inspectImage(ctx context.Context, ref string) (imageInfo, bool, error) {
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 1 << 20}, "image", "inspect", ref)
	if err != nil {
		return imageInfo{}, false, err
	}
	if missing(result) {
		return imageInfo{}, false, nil
	}
	var images []imageInfo
	if result.ExitCode != 0 || json.Unmarshal(out.Bytes(), &images) != nil || len(images) != 1 ||
		!strings.HasPrefix(images[0].ID, "sha256:") || images[0].OS == "" || images[0].Architecture == "" {
		return imageInfo{}, false, ErrRemoteDocker
	}
	return images[0], true, nil
}

func (d docker) pull(ctx context.Context, ref string) error {
	result, err := d.run(ctx, nil, nil, "pull", "--quiet", ref)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return ErrRemoteDocker
	}
	return nil
}

type containerState struct {
	Status     string    `json:"Status"`
	Running    bool      `json:"Running"`
	OOMKilled  bool      `json:"OOMKilled"`
	ExitCode   int       `json:"ExitCode"`
	StartedAt  time.Time `json:"StartedAt"`
	FinishedAt time.Time `json:"FinishedAt"`
}
type containerInfo struct {
	ID     string         `json:"Id"`
	Name   string         `json:"Name"`
	Image  string         `json:"Image"`
	State  containerState `json:"State"`
	Config struct {
		Labels      map[string]string `json:"Labels"`
		User        string            `json:"User"`
		Cmd         []string          `json:"Cmd"`
		Entrypoint  []string          `json:"Entrypoint"`
		WorkingDir  string            `json:"WorkingDir"`
		Env         []string          `json:"Env"`
		Healthcheck *struct {
			Test []string `json:"Test"`
		} `json:"Healthcheck,omitempty"`
	} `json:"Config"`
	HostConfig struct {
		Mounts        []dockerMountRequest `json:"Mounts"`
		Tmpfs         map[string]string    `json:"Tmpfs"`
		NetworkMode   string               `json:"NetworkMode"`
		PidMode       string               `json:"PidMode"`
		Privileged    bool                 `json:"Privileged"`
		CapDrop       []string             `json:"CapDrop"`
		CapAdd        []string             `json:"CapAdd"`
		SecurityOpt   []string             `json:"SecurityOpt"`
		Binds         []string             `json:"Binds"`
		Memory        int64                `json:"Memory"`
		MemorySwap    int64                `json:"MemorySwap"`
		NanoCPUs      int64                `json:"NanoCpus"`
		PidsLimit     int64                `json:"PidsLimit"`
		RestartPolicy struct {
			Name string `json:"Name"`
		} `json:"RestartPolicy"`
	} `json:"HostConfig"`
	Mounts []dockerMount `json:"Mounts"`
}

type dockerMountRequest struct {
	Type          string `json:"Type"`
	Source        string `json:"Source"`
	Target        string `json:"Target"`
	ReadOnly      bool   `json:"ReadOnly"`
	VolumeOptions struct {
		NoCopy bool `json:"NoCopy"`
	} `json:"VolumeOptions"`
}

type dockerMount struct {
	Type        string `json:"Type"`
	Name        string `json:"Name"`
	Destination string `json:"Destination"`
	RW          bool   `json:"RW"`
}

// Started reports Docker's own evidence that the container process was launched.
func (c containerInfo) Started() bool { return c.State.StartedAt.Year() > 1 }

func (d docker) inspect(ctx context.Context, name string) (containerInfo, bool, error) {
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 1 << 20}, "container", "inspect", name)
	if err != nil {
		return containerInfo{}, false, err
	}
	if missing(result) {
		return containerInfo{}, false, nil
	}
	var containers []containerInfo
	if result.ExitCode != 0 || json.Unmarshal(out.Bytes(), &containers) != nil || len(containers) != 1 ||
		containers[0].ID == "" {
		return containerInfo{}, false, ErrRemoteDocker
	}
	if containerID.MatchString(name) {
		if containers[0].ID != name {
			return containerInfo{}, false, ErrRemoteDocker
		}
	} else if containers[0].Name != "/"+name {
		return containerInfo{}, false, ErrRemoteDocker
	}
	return containers[0], true, nil
}

// containerSpec is the complete hardened create request for one attempt. CPUs and
// MemoryBytes are optional operator limits.
type containerSpec struct {
	SecretTmpfs   bool
	NoHealthcheck bool
	Name          string
	Image         string
	Platform      string
	Labels        map[string]string
	Internet      bool
	CPUs          string
	MemoryBytes   int64
	PIDs          int
	Command       []string
	Mounts        []dockerMountRequest
}

func (s containerSpec) args() []string {
	network := "none"
	if s.Internet {
		network = "bridge"
	}
	args := []string{"container", "create", "--name", s.Name, "--user", "65534:65534", "--network", network,
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", strconv.Itoa(s.PIDs),
		"--restart", "no", "--stop-timeout", "30", "--workdir", "/relay"}
	if s.SecretTmpfs {
		args = append(args, "--tmpfs", secretTmpfsPath+":"+secretTmpfsOptions)
	}
	if s.NoHealthcheck {
		args = append(args, "--no-healthcheck")
	}
	if s.Platform != "" {
		args = append(args, "--platform", s.Platform)
	}
	// Optional limits; without them the engine's own VM or cgroup bounds the attempt.
	if s.MemoryBytes > 0 {
		memory := strconv.FormatInt(s.MemoryBytes, 10)
		args = append(args, "--memory", memory, "--memory-swap", memory)
	}
	if s.CPUs != "" {
		args = append(args, "--cpus", s.CPUs)
	}
	for _, key := range slices.Sorted(maps.Keys(s.Labels)) {
		args = append(args, "--label", key+"="+s.Labels[key])
	}
	for _, mount := range s.Mounts {
		args = append(args, "--mount", "type=volume,source="+mount.Source+",target="+mount.Target+",readonly,volume-nocopy")
	}
	return append(append(args, s.Image), s.Command...)
}

// create returns the new container ID. A name conflict means an earlier create may
// have succeeded; the caller inspects instead of creating again.
func (d docker) create(ctx context.Context, spec containerSpec) (string, error) {
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 4096}, spec.args()...)
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(out.String())
	if result.ExitCode != 0 || len(id) != 64 {
		return "", ErrRemoteDocker
	}
	return id, nil
}

// copyIn extracts a tar stream at the container root, keeping archive ownership.
func (d docker) copyIn(ctx context.Context, name string, archive io.Reader) error {
	result, err := d.run(ctx, archive, nil, "container", "cp", "--archive", "-", name+":/")
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return ErrRemoteDocker
	}
	return nil
}

// copyOut streams a tar archive of one container path. found is false for a missing
// container or path; the caller distinguishes them through inspect.
func (d docker) copyOut(ctx context.Context, name, source string, w io.Writer) (bool, error) {
	result, err := d.run(ctx, nil, w, "container", "cp", name+":"+source, "-")
	if err != nil {
		return false, err
	}
	if result.ExitCode != 0 {
		if missing(result) || strings.Contains(string(result.Stderr), "Could not find the file") {
			return false, nil
		}
		return false, ErrRemoteDocker
	}
	return true, nil
}

func (d docker) simple(ctx context.Context, args ...string) (Result, error) {
	return d.run(ctx, nil, nil, args...)
}

func (d docker) logs(ctx context.Context, name string, w io.Writer) (bool, error) {
	result, err := d.run(ctx, nil, w, "container", "logs", name)
	if err != nil {
		return false, err
	}
	if missing(result) {
		return false, nil
	}
	if result.ExitCode != 0 {
		return false, ErrRemoteDocker
	}
	return true, nil
}

// limitedWriter fails once a bounded response would exceed its limit.
type limitedWriter struct {
	w     io.Writer
	limit int64
	n     int64
}

var errResponseLimit = errors.New("remote response exceeds its bound")

func (l *limitedWriter) Write(p []byte) (int, error) {
	if l.n+int64(len(p)) > l.limit {
		return 0, errResponseLimit
	}
	n, err := l.w.Write(p)
	l.n += int64(n)
	return n, err
}
