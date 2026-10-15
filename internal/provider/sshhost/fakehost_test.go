package sshhost

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/hostimages"
)

const fakeImageID = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

type fakeFile struct {
	data []byte
	uid  int
	link bool
}

type fakeContainer struct {
	id, name      string
	imageID       string
	noHealthcheck bool
	args          []string
	labels        map[string]string
	status        string
	exit          int
	oom           bool
	started       time.Time
	files         map[string]fakeFile // container paths without the leading slash
	logs          string
	mounts        []dockerMountRequest
}

type fakeVolume struct {
	labels   map[string]string
	blob     []byte
	complete bool
}

type fakeApplicationImage struct {
	id      string
	labels  map[string]string
	diffIDs []string
	tags    []string
	digests []string
}

// fakeHost is an in-memory Docker host behind the executor seam. It understands only
// the fixed argument arrays the adapter issues and records every command.
type fakeHost struct {
	t                      *testing.T
	mu                     sync.Mutex
	images                 map[string]imageInfo
	applicationImages      map[string]fakeApplicationImage
	applicationLoads       int
	applicationLoadAckLost bool
	applicationLoadMissing bool
	builds                 int
	buildExit              int
	buildInput             []byte
	platform               string
	engineTime             time.Time
	image                  bool
	registry               bool // pull succeeds
	containers             map[string]*fakeContainer
	volumes                map[string]*fakeVolume
	writes                 int
	volumeVerifications    int
	calls                  [][]string
	wireCalls              [][]string
	machineID              string
	engineID               string
	dockerHost             string
	starts                 int
	created                int
	secretPackets          [][]byte
	secretAckLost          bool
	verifierOutput         [][]byte
	verifierTarFault       func([]byte) []byte
	verifierMutate         func(*fakeContainer)
	verifierProducerStatus int
	verifierOmitStatus     bool
	verifierExit           int
	verifierExtraOutput    []byte
	closed                 int
	fail                   func(args []string) (Result, bool)
}

func newFakeHost(t *testing.T) *fakeHost {
	return &fakeHost{t: t, image: true, platform: "linux/amd64", images: map[string]imageInfo{}, applicationImages: map[string]fakeApplicationImage{}, containers: map[string]*fakeContainer{}, volumes: map[string]*fakeVolume{},
		machineID: strings.Repeat("a", 32), engineID: "synthetic-engine-01", dockerHost: "unix:///var/run/docker.sock"}
}

func decodeFakeApplicationArchive(raw []byte) (fakeApplicationImage, error) {
	files := map[string][]byte{}
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fakeApplicationImage{}, err
		}
		if header.Typeflag == tar.TypeReg {
			files[header.Name], err = io.ReadAll(tr)
			if err != nil {
				return fakeApplicationImage{}, err
			}
		}
	}
	var manifest []struct {
		Config   string   `json:"Config"`
		RepoTags []string `json:"RepoTags"`
	}
	if json.Unmarshal(files["manifest.json"], &manifest) != nil || len(manifest) != 1 {
		return fakeApplicationImage{}, fmt.Errorf("invalid manifest")
	}
	configBytes := files[manifest[0].Config]
	var config struct {
		OS           string `json:"os"`
		Architecture string `json:"architecture"`
		RootFS       struct {
			DiffIDs []string `json:"diff_ids"`
		} `json:"rootfs"`
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"config"`
	}
	if json.Unmarshal(configBytes, &config) != nil || config.OS != "linux" || config.Architecture != "amd64" || len(config.RootFS.DiffIDs) == 0 {
		return fakeApplicationImage{}, fmt.Errorf("invalid config")
	}
	id := sha256.Sum256(configBytes)
	return fakeApplicationImage{id: "sha256:" + hex.EncodeToString(id[:]), labels: config.Config.Labels,
		diffIDs: config.RootFS.DiffIDs, tags: manifest[0].RepoTags}, nil
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

func (h *fakeHost) Run(ctx context.Context, env map[string]string, argv []string, stdin io.Reader, stdout io.Writer) (Result, error) {
	if _, err := shellCommand(env, argv); err != nil {
		return Result{}, err
	}
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
	if len(argv) > 2 && argv[0] == "/bin/sh" && argv[1] == "-c" && argv[2] == hostVerifierShell {
		return h.runVerifier(ctx, argv, stdout), nil
	}
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
	case slices.Equal(args, []string{"info", "--format", "{{.ID}}"}):
		_, _ = io.WriteString(stdout, h.engineID)
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
		if image, exists := h.applicationImages[args[2]]; exists {
			raw, _ := json.Marshal([]map[string]any{{"Id": image.id, "Os": "linux", "Architecture": strings.TrimPrefix(h.platform, "linux/"),
				"RepoTags": image.tags, "RepoDigests": image.digests, "Config": map[string]any{"Labels": image.labels},
				"RootFS": map[string]any{"Layers": image.diffIDs}}})
			_, _ = stdout.Write(raw)
			return Result{}, nil
		}
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
	case len(args) == 8 && slices.Equal(args[:5], []string{"image", "ls", "--all", "--no-trunc", "--filter"}) &&
		slices.Equal(args[6:], []string{"--format", "{{.ID}}"}):
		label, value, ok := strings.Cut(strings.TrimPrefix(args[5], "label="), "=")
		if !strings.HasPrefix(args[5], "label=") || !ok || label != hostimages.LabelInstallation {
			h.t.Errorf("application image discovery was not scoped: %q", args)
			return Result{ExitCode: 125}, nil
		}
		for _, image := range h.applicationImages {
			if image.labels[label] == value {
				_, _ = io.WriteString(stdout, image.id+"\n")
			}
		}
		return Result{}, nil
	case slices.Equal(args, []string{"image", "load", "--quiet"}):
		h.applicationLoads++
		if !h.applicationLoadMissing {
			image, err := decodeFakeApplicationArchive(archive)
			if err != nil {
				h.t.Errorf("invalid normalized application archive: %v", err)
				return Result{ExitCode: 125}, nil
			}
			h.applicationImages[image.id] = image
		}
		if h.applicationLoadAckLost || h.applicationLoadMissing {
			return Result{ExitCode: 1, Stderr: []byte("load reply lost")}, nil
		}
		return Result{}, nil
	case len(args) == 3 && args[0] == "pull" && args[1] == "--quiet" && args[2] == StandardImage:
		if !h.registry {
			return Result{ExitCode: 1, Stderr: []byte("registry unavailable")}, nil
		}
		h.image = true
		return Result{}, nil
	case len(args) > 1 && args[0] == "volume":
		return h.volume(args[1:], stdout), nil
	case len(args) > 2 && args[0] == "container" && args[1] == "run":
		return h.runCacheHelper(args[2:], archive, stdout), nil
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

// runVerifier models the fixed host-local pipe while executing its real Python
// parser. The archive never crosses the fake SSH stdout seam.
func (h *fakeHost) runVerifier(ctx context.Context, argv []string, stdout io.Writer) Result {
	if len(argv) != 14 || argv[3] != "relay-verifier" || argv[4] != "/usr/local/bin/docker" || argv[5] != h.dockerHost || argv[7] != fakeImageID || argv[8] != hostVerifierBootstrap {
		h.t.Errorf("verifier did not pin Docker and the installed image: %q", argv)
		return Result{ExitCode: 125}
	}
	c := h.find(argv[6])
	if c == nil || argv[11] != c.labels[labelPrefix+"installation"] || argv[12] != c.labels[labelPrefix+"attempt"] || argv[13] != c.labels[labelPrefix+"preparation"] {
		h.t.Error("verifier labels do not match frozen target labels")
		return Result{ExitCode: 125}
	}
	source, err := base64.StdEncoding.DecodeString(argv[9])
	if err != nil || string(source) != hostVerifierSource {
		h.t.Error("verifier source changed")
		return Result{ExitCode: 125}
	}
	if h.verifierMutate != nil {
		h.verifierMutate(c)
	}
	var archive bytes.Buffer
	copyResult := h.copy([]string{argv[6] + ":/relay", "-"}, nil, &archive)
	if copyResult.ExitCode != 0 {
		return copyResult
	}
	data := archive.Bytes()
	if h.verifierTarFault != nil {
		data = h.verifierTarFault(data)
	}
	producerStatus := h.verifierProducerStatus
	if !h.verifierOmitStatus {
		if _, err = fmt.Fprintf(stdout, "cp:%d\n", producerStatus); err != nil {
			return Result{ExitCode: 125}
		}
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		h.t.Skip("python3 is required for the fixed verifier contract test")
	}
	command := exec.CommandContext(ctx, python, "-B", "-E", "-s", "-c", hostVerifierBootstrap, argv[9], argv[10])
	command.Stdin = bytes.NewReader(data)
	var verified, diagnostic bytes.Buffer
	command.Stdout, command.Stderr = &verified, &diagnostic
	if err = command.Run(); err != nil {
		h.t.Logf("fixed verifier rejected fake archive: %s", diagnostic.String())
		return Result{ExitCode: 2, Stderr: diagnostic.Bytes()}
	}
	if h.verifierExit != 0 {
		return Result{ExitCode: h.verifierExit}
	}
	h.verifierOutput = append(h.verifierOutput, append([]byte(nil), verified.Bytes()...))
	_, _ = stdout.Write(verified.Bytes())
	if len(h.verifierExtraOutput) > 0 {
		_, _ = stdout.Write(h.verifierExtraOutput)
	}
	return Result{}
}

func (h *fakeHost) create(args []string, stdout io.Writer) Result {
	c := &fakeContainer{args: args, imageID: fakeImageID, labels: map[string]string{}, status: "created", files: map[string]fakeFile{}}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--no-healthcheck":
			c.noHealthcheck = true
		case "--name":
			c.name = args[i+1]
		case "--label":
			key, value, _ := strings.Cut(args[i+1], "=")
			c.labels[key] = value
		case "--mount":
			var mount dockerMountRequest
			for _, part := range strings.Split(args[i+1], ",") {
				key, value, hasValue := strings.Cut(part, "=")
				switch key {
				case "type":
					mount.Type = value
				case "source":
					mount.Source = value
				case "target":
					mount.Target = value
				case "readonly":
					mount.ReadOnly = true
				case "volume-nocopy":
					mount.VolumeOptions.NoCopy = true
				default:
					if !hasValue {
						h.t.Errorf("unknown mount option %q", part)
					}
				}
			}
			c.mounts = append(c.mounts, mount)
		}
		if _, ok := h.applicationImages[args[i]]; ok {
			c.imageID = args[i]
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
	if c.noHealthcheck {
		cfg["Healthcheck"] = map[string]any{"Test": []string{"NONE"}}
	}
	host := map[string]any{}
	host["Mounts"] = c.mounts
	effective := []dockerMount{}
	for _, mount := range c.mounts {
		effective = append(effective, dockerMount{Type: "volume", Name: mount.Source, Destination: mount.Target, RW: !mount.ReadOnly})
	}
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
		default:
			if c.args[i] == c.imageID {
				cfg["Cmd"] = c.args[i+1:]
			}
		}
	}
	raw, _ := json.Marshal([]map[string]any{{"Id": c.id, "Name": "/" + c.name, "Image": c.imageID, "State": state, "Config": cfg, "HostConfig": host, "Mounts": effective}})
	_, _ = stdout.Write(raw)
	return Result{}
}

func (h *fakeHost) volume(args []string, stdout io.Writer) Result {
	if len(args) > 2 && args[0] == "ls" {
		filters := map[string]string{}
		for i := 1; i < len(args)-1; i++ {
			if args[i] != "--filter" {
				continue
			}
			value, ok := strings.CutPrefix(args[i+1], "label=")
			if !ok {
				h.t.Errorf("unlabelled volume discovery: %q", args)
				return Result{ExitCode: 125}
			}
			key, want, ok := strings.Cut(value, "=")
			if !ok {
				h.t.Errorf("invalid volume label filter: %q", args)
				return Result{ExitCode: 125}
			}
			filters[key] = want
		}
		if len(filters) != 8 || !slices.Equal(args[len(args)-2:], []string{"--format", "{{.Name}}"}) {
			h.t.Errorf("incomplete cache volume discovery: %q", args)
			return Result{ExitCode: 125}
		}
		names := []string{}
		for name, volume := range h.volumes {
			match := true
			for key, want := range filters {
				if volume.labels[key] != want {
					match = false
					break
				}
			}
			if match {
				names = append(names, name)
			}
		}
		slices.Sort(names)
		for _, name := range names {
			_, _ = io.WriteString(stdout, name+"\n")
		}
		return Result{}
	}
	if len(args) == 2 && args[0] == "inspect" {
		v := h.volumes[args[1]]
		if v == nil {
			return noSuch("volume")
		}
		raw, _ := json.Marshal([]volumeInfo{{Name: args[1], Driver: "local", Scope: "local", Labels: v.labels, Options: map[string]string{}}})
		_, _ = stdout.Write(raw)
		return Result{}
	}
	if len(args) > 3 && args[0] == "create" {
		name := ""
		labels := map[string]string{}
		for i := 1; i < len(args)-1; i++ {
			switch args[i] {
			case "--name":
				name = args[i+1]
			case "--label":
				key, value, _ := strings.Cut(args[i+1], "=")
				labels[key] = value
			}
		}
		if name == "" {
			h.t.Error("unnamed volume")
			return Result{ExitCode: 125}
		}
		if h.volumes[name] == nil {
			h.volumes[name] = &fakeVolume{labels: labels}
		}
		_, _ = io.WriteString(stdout, name+"\n")
		return Result{}
	}
	h.t.Errorf("unexpected volume command %q", args)
	return Result{ExitCode: 125}
}

func (h *fakeHost) runCacheHelper(args []string, input []byte, stdout io.Writer) Result {
	mount := ""
	readonly := false
	kind := ""
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--mount" {
			for _, part := range strings.Split(args[i+1], ",") {
				key, value, _ := strings.Cut(part, "=")
				if key == "source" {
					mount = value
				}
				if key == "readonly" {
					readonly = true
				}
			}
		}
		if args[i] == "--label" && strings.HasPrefix(args[i+1], labelPrefix+"kind=") {
			kind = strings.TrimPrefix(args[i+1], labelPrefix+"kind=")
		}
	}
	v := h.volumes[mount]
	if v == nil {
		h.t.Errorf("helper mounted missing volume %q", mount)
		return Result{ExitCode: 125}
	}
	switch kind {
	case "cache-writer":
		if readonly || v.complete || len(args) < 2 {
			return Result{ExitCode: 2}
		}
		size, err := strconv.ParseInt(args[len(args)-2], 10, 64)
		digest := sha256.Sum256(input)
		if err != nil || size != int64(len(input)) || hex.EncodeToString(digest[:]) != args[len(args)-1] {
			return Result{ExitCode: 2}
		}
		v.blob = append([]byte(nil), input...)
		v.complete = true
		h.writes++
		return Result{}
	case "verification":
		if !readonly || !v.complete {
			return Result{ExitCode: 2}
		}
		h.volumeVerifications++
		digest := sha256.Sum256(v.blob)
		raw, _ := json.Marshal(verifierInventory{Files: []verifierDigest{{Bytes: int64(len(v.blob)), SHA256: hex.EncodeToString(digest[:])}}})
		_, _ = stdout.Write(raw)
		return Result{}
	default:
		h.t.Errorf("unexpected helper kind %q", kind)
		return Result{ExitCode: 125}
	}
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
