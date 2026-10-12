package sshhost

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const fakeImageID = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

type fakeFile struct {
	data []byte
	uid  int
	link bool
}

type fakeContainer struct {
	id, name string
	args     []string
	labels   map[string]string
	status   string
	exit     int
	oom      bool
	started  time.Time
	files    map[string]fakeFile // container paths without the leading slash
	logs     string
}

// fakeHost is an in-memory Docker host behind the executor seam. It understands only
// the fixed argument arrays the adapter issues and records every command.
type fakeHost struct {
	t             *testing.T
	mu            sync.Mutex
	images        map[string]imageInfo
	builds        int
	buildExit     int
	buildInput    []byte
	platform      string
	engineTime    time.Time
	image         bool
	registry      bool // pull succeeds
	containers    map[string]*fakeContainer
	calls         [][]string
	wireCalls     [][]string
	machineID     string
	engineID      string
	dockerHost    string
	starts        int
	created       int
	secretPackets [][]byte
	secretAckLost bool
	closed        int
	fail          func(args []string) (Result, bool)
}

func newFakeHost(t *testing.T) *fakeHost {
	return &fakeHost{t: t, image: true, platform: "linux/amd64", images: map[string]imageInfo{}, containers: map[string]*fakeContainer{},
		machineID: strings.Repeat("a", 32), engineID: "synthetic-engine-01", dockerHost: "unix:///var/run/docker.sock"}
}

func (h *fakeHost) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed++
	return nil
}

func (h *fakeHost) find(ref string) *fakeContainer {
	for _, c := range h.containers {
		if c.name == ref || c.id == ref {
			return c
		}
	}
	return nil
}

func (h *fakeHost) container(t *testing.T) *fakeContainer {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.containers) != 1 {
		t.Fatalf("host has %d containers", len(h.containers))
	}
	for _, c := range h.containers {
		return c
	}
	return nil
}

// finish simulates the runner: it writes work files and exits the container.
func (h *fakeHost) finish(exit int, work map[string]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.containers {
		c.status, c.exit = "exited", exit
		for name, data := range work {
			c.files["relay/work/"+name] = fakeFile{data: []byte(data), uid: nobody}
		}
	}
}

func (h *fakeHost) commands(verb string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, call := range h.calls {
		if len(call) > 2 && call[0] == "/usr/local/bin/docker" && call[1] == "container" && call[2] == verb {
			n++
		}
	}
	return n
}

func noSuch(kind string) Result {
	return Result{ExitCode: 1, Stderr: []byte("Error response from daemon: No such " + kind + ": x")}
}

func (h *fakeHost) Run(ctx context.Context, _ map[string]string, argv []string, stdin io.Reader, stdout io.Writer) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if stdout == nil {
		stdout = io.Discard
	}
	// Archives are read before locking so a streaming writer never blocks the host.
	var archive []byte
	if stdin != nil {
		var err error
		if archive, err = io.ReadAll(stdin); err != nil {
			return Result{}, ErrUnavailable
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.wireCalls = append(h.wireCalls, append([]string(nil), argv...))
	if len(argv) > 0 && argv[0] == "/usr/bin/env" {
		if len(argv) < 9 || !slices.Equal(argv[:5], []string{"/usr/bin/env", "-u", "DOCKER_CONTEXT", "-u", "DOCKER_HOST"}) || argv[6] != "--host" || argv[7] != h.dockerHost {
			h.t.Fatal("Docker command did not pin the local socket", argv)
		}
		argv = append([]string{argv[5]}, argv[8:]...)
	}
	h.calls = append(h.calls, argv)
	switch {
	case slices.Equal(argv, []string{"/usr/bin/uname", "-s"}):
		_, _ = io.WriteString(stdout, "Linux\n")
		return Result{}, nil
	case slices.Equal(argv, []string{"/bin/cat", "/etc/machine-id"}):
		_, _ = io.WriteString(stdout, h.machineID+"\n")
		return Result{}, nil
	case slices.Equal(argv, []string{"/bin/sh", "-c", `printf '%s\n%s\n' "${DOCKER_CONTEXT-}" "${DOCKER_HOST-}"`}):
		_, _ = io.WriteString(stdout, "\n\n")
		return Result{}, nil
	}
	if argv[0] == "test" {
		if argv[2] == "/usr/local/bin/docker" {
			return Result{}, nil
		}
		return Result{ExitCode: 1}, nil
	}
	args := argv[1:]
	if h.fail != nil {
		if result, failed := h.fail(args); failed {
			return result, nil
		}
	}
	switch {
	case slices.Equal(args, []string{"context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"}):
		_ = json.NewEncoder(stdout).Encode(h.dockerHost)
		return Result{}, nil
	case slices.Equal(args, []string{"info", "--format", "{{json .ID}}"}):
		_ = json.NewEncoder(stdout).Encode(h.engineID)
		return Result{}, nil
	case slices.Equal(args, []string{"info", "--format", "{{.SystemTime}}"}):
		_, _ = io.WriteString(stdout, h.engineTime.Format(time.RFC3339Nano))
		return Result{}, nil
	case slices.Equal(args, []string{"version"}):
		return Result{}, nil
	case slices.Equal(args, []string{"version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"}):
		_, _ = io.WriteString(stdout, h.platform)
		return Result{}, nil
	case len(args) > 0 && args[0] == "build":
		h.builds++
		h.buildInput = append([]byte(nil), archive...)
		info := imageInfo{ID: fakeImageID, OS: "linux", Architecture: strings.TrimPrefix(h.platform, "linux/")}
		info.Config.Labels = map[string]string{"io.compute-relay.image-schema": "1"}
		var tags []string
		for i := 1; i < len(args)-1; i++ {
			if args[i] == "--label" {
				k, v, _ := strings.Cut(args[i+1], "=")
				info.Config.Labels[k] = v
			}
			if args[i] == "--tag" {
				tags = append(tags, args[i+1])
			}
		}
		for _, tag := range tags {
			h.images[tag] = info
		}
		h.images[info.ID] = info
		return Result{ExitCode: h.buildExit}, nil
	case len(args) == 3 && args[0] == "image" && args[1] == "inspect":
		if image, exists := h.images[args[2]]; exists {
			raw, _ := json.Marshal([]imageInfo{image})
			_, _ = stdout.Write(raw)
			return Result{}, nil
		}
		if !h.image || args[2] != StandardImage {
			return noSuch("image"), nil
		}
		_, _ = fmt.Fprintf(stdout, `[{"Id":%q,"Os":"linux","Architecture":"amd64"}]`, fakeImageID)
		return Result{}, nil
	case len(args) == 3 && args[0] == "pull" && args[1] == "--quiet" && args[2] == StandardImage:
		if !h.registry {
			return Result{ExitCode: 1, Stderr: []byte("registry unavailable")}, nil
		}
		h.image = true
		return Result{}, nil
	case len(args) < 3 || args[0] != "container":
		h.t.Errorf("unexpected docker command %q", args)
		return Result{ExitCode: 125}, nil
	}
	verb, rest := args[1], args[2:]
	switch verb {
	case "create":
		return h.create(rest, stdout), nil
	case "cp":
		return h.copy(rest, archive, stdout), nil
	case "exec":
		if len(rest) != 13 || !slices.Equal(rest[:3], []string{"-i", "--user", "65534:65534"}) || !slices.Equal(rest[4:], []string{"python3", "-E", "-s", "/relay/runner/run.py", "--receive-secrets", "--manifest", "/relay/manifest.json", "--start-control", startControlPath}) {
			h.t.Error("unexpected secret receiver command")
			return Result{ExitCode: 125}, nil
		}
		c := h.find(rest[3])
		if c == nil || c.status != "running" {
			return noSuch("container"), nil
		}
		h.secretPackets = append(h.secretPackets, append([]byte(nil), archive...))
		if h.secretAckLost {
			return Result{ExitCode: 1}, nil
		}
		return Result{}, nil
	}
	c := h.find(rest[len(rest)-1])
	if c == nil {
		return noSuch("container"), nil
	}
	switch verb {
	case "inspect":
		return h.inspect(c, stdout), nil
	case "start":
		c.status, c.started = "running", time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC)
		h.starts++
	case "stop":
		if c.status == "running" {
			c.status, c.exit = "exited", 143
		}
	case "logs":
		_, _ = io.WriteString(stdout, c.logs)
	case "rm":
		if c.status == "running" {
			return Result{ExitCode: 1, Stderr: []byte("cannot remove a running container")}, nil
		}
		delete(h.containers, c.name)
	default:
		h.t.Errorf("unexpected container verb %q", verb)
		return Result{ExitCode: 125}, nil
	}
	return Result{}, nil
}

func (h *fakeHost) create(args []string, stdout io.Writer) Result {
	c := &fakeContainer{args: args, labels: map[string]string{}, status: "created", files: map[string]fakeFile{}}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name":
			c.name = args[i+1]
		case "--label":
			key, value, _ := strings.Cut(args[i+1], "=")
			c.labels[key] = value
		}
	}
	if h.find(c.name) != nil {
		return Result{ExitCode: 125, Stderr: []byte("Conflict. The container name is already in use")}
	}
	h.created++
	c.id = strings.Repeat(fmt.Sprintf("%x", h.created%16), 64)
	h.containers[c.name] = c
	_, _ = io.WriteString(stdout, c.id+"\n")
	return Result{}
}

func (h *fakeHost) inspect(c *fakeContainer, stdout io.Writer) Result {
	state := map[string]any{"Status": c.status, "Running": c.status == "running", "OOMKilled": c.oom, "ExitCode": c.exit,
		"StartedAt": c.started.Format(time.RFC3339Nano), "FinishedAt": "0001-01-01T00:00:00Z"}
	cfg := map[string]any{"Labels": c.labels}
	host := map[string]any{}
	for i := 0; i+1 < len(c.args); i++ {
		val := c.args[i+1]
		switch c.args[i] {
		case "--tmpfs":
			path, options, _ := strings.Cut(val, ":")
			host["Tmpfs"] = map[string]string{path: options}
		case "--user":
			cfg["User"] = val
		case "--workdir":
			cfg["WorkingDir"] = val
		case "--network":
			host["NetworkMode"] = val
		case "--cap-drop":
			host["CapDrop"] = []string{val}
		case "--security-opt":
			host["SecurityOpt"] = []string{val}
		case "--restart":
			host["RestartPolicy"] = map[string]string{"Name": val}
		case "--memory":
			n, _ := strconv.ParseInt(val, 10, 64)
			host["Memory"] = n
		case "--memory-swap":
			n, _ := strconv.ParseInt(val, 10, 64)
			host["MemorySwap"] = n
		case "--pids-limit":
			n, _ := strconv.ParseInt(val, 10, 64)
			host["PidsLimit"] = n
		case "--cpus":
			n, _ := strconv.ParseFloat(val, 64)
			host["NanoCpus"] = int64(n * 1e9)
		case fakeImageID:
			cfg["Cmd"] = c.args[i+1:]
		}
	}
	raw, _ := json.Marshal([]map[string]any{{"Id": c.id, "Name": "/" + c.name, "Image": fakeImageID, "State": state, "Config": cfg, "HostConfig": host}})
	_, _ = stdout.Write(raw)
	return Result{}
}

func (h *fakeHost) copy(args []string, archive []byte, stdout io.Writer) Result {
	if len(args) == 3 && args[0] == "--archive" && args[1] == "-" {
		ref, dest, _ := strings.Cut(args[2], ":")
		c := h.find(ref)
		if c == nil || dest != "/" {
			return noSuch("container")
		}
		tr := tar.NewReader(bytes.NewReader(archive))
		for {
			header, err := tr.Next()
			if err == io.EOF {
				return Result{}
			}
			if err != nil {
				return Result{ExitCode: 1, Stderr: []byte("unexpected EOF")}
			}
			data, _ := io.ReadAll(tr)
			if header.Typeflag == tar.TypeReg {
				c.files[header.Name] = fakeFile{data: data, uid: header.Uid}
			}
		}
	}
	ref, source, _ := strings.Cut(args[0], ":")
	c := h.find(ref)
	if c == nil {
		return noSuch("container")
	}
	key := strings.TrimPrefix(source, "/")
	tw := tar.NewWriter(stdout)
	write := func(name string, f fakeFile) {
		header := &tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Size: int64(len(f.data)), Uid: f.uid}
		if f.link {
			header = &tar.Header{Typeflag: tar.TypeSymlink, Name: name, Linkname: "/etc/passwd"}
		}
		_ = tw.WriteHeader(header)
		if !f.link {
			_, _ = tw.Write(f.data)
		}
	}
	if f, ok := c.files[key]; ok {
		write(path.Base(key), f)
		_ = tw.Close()
		return Result{}
	}
	found := false
	for _, name := range slices.Sorted(maps.Keys(c.files)) {
		if rest, ok := strings.CutPrefix(name, key+"/"); ok {
			if !found {
				_ = tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: path.Base(key) + "/", Mode: 0o755})
				found = true
			}
			write(path.Base(key)+"/"+rest, c.files[name])
		}
	}
	if !found {
		return Result{ExitCode: 1, Stderr: []byte("Error response from daemon: Could not find the file " + source + " in container " + ref)}
	}
	_ = tw.Close()
	return Result{}
}
