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
	t          *testing.T
	mu         sync.Mutex
	image      bool
	containers map[string]*fakeContainer
	calls      [][]string
	starts     int
	created    int
	closed     int
	fail       func(args []string) (Result, bool)
}

func newFakeHost(t *testing.T) *fakeHost {
	return &fakeHost{t: t, image: true, containers: map[string]*fakeContainer{}}
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
	h.calls = append(h.calls, argv)
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
	case slices.Equal(args, []string{"version"}):
		return Result{}, nil
	case len(args) == 3 && args[0] == "image" && args[1] == "inspect":
		if !h.image || args[2] != StandardImage {
			return noSuch("image"), nil
		}
		_, _ = fmt.Fprintf(stdout, `[{"Id":%q,"Os":"linux","Architecture":"amd64"}]`, fakeImageID)
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
	raw, _ := json.Marshal([]map[string]any{{"Id": c.id, "Name": "/" + c.name, "Image": fakeImageID, "State": state,
		"Config": map[string]any{"Labels": c.labels}}})
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
