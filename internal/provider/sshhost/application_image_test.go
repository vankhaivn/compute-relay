package sshhost

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
)

type imageInspectExecutor struct {
	listed        string
	inspect       []byte
	inspectResult Result
	calls         [][]string
}

type imageStoreExecutor struct {
	output string
	result Result
	calls  int
}

func (e *imageStoreExecutor) Run(_ context.Context, _ map[string]string, args []string, _ io.Reader, out io.Writer) (Result, error) {
	e.calls++
	if !slices.Equal(args, []string{"/usr/local/bin/docker", "info", "--format", imageStoreFormat}) {
		return Result{}, ErrRequest
	}
	_, err := io.WriteString(out, e.output)
	return e.result, err
}

func TestApplicationImageIdentitySelectsKnownStoreBeforeLoad(t *testing.T) {
	archive := &hostimages.Prepared{ImageID: "sha256:" + strings.Repeat("a", 64), ManifestID: "sha256:" + strings.Repeat("b", 64)}
	for _, tc := range []struct {
		name, output, want string
		result             Result
	}{
		{"containerd", `{"driver":"overlayfs","status":[["driver-type","io.containerd.snapshotter.v1"]]}`, archive.ManifestID, Result{}},
		{"classic-overlay2", `{"driver":"overlay2","status":[["Backing Filesystem","extfs"]]}`, archive.ImageID, Result{}},
		{"classic-vfs", `{"driver":"vfs","status":null}`, archive.ImageID, Result{}},
		{"unknown-driver", `{"driver":"future-driver","status":[]}`, "", Result{}},
		{"missing-containerd-type", `{"driver":"overlayfs","status":[]}`, "", Result{}},
		{"unknown-driver-type", `{"driver":"overlay2","status":[["driver-type","future.store"]]}`, "", Result{}},
		{"duplicate-driver-type", `{"driver":"overlayfs","status":[["driver-type","io.containerd.snapshotter.v1"],["driver-type","io.containerd.snapshotter.v1"]]}`, "", Result{}},
		{"short-status-row", `{"driver":"overlay2","status":[["driver-type"]]}`, "", Result{}},
		{"long-status-row", `{"driver":"overlay2","status":[["driver-type","io.containerd.snapshotter.v1","extra"]]}`, "", Result{}},
		{"unknown-field", `{"driver":"overlay2","status":[],"other":true}`, "", Result{}},
		{"trailing-json", `{"driver":"overlay2","status":[]} {}`, "", Result{}},
		{"invalid-json", `{"driver":`, "", Result{}},
		{"command-failed", `{"driver":"overlay2","status":[]}`, "", Result{ExitCode: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &imageStoreExecutor{output: tc.output, result: tc.result}
			d := docker{exec: e, binary: "/usr/local/bin/docker"}
			got, err := d.applicationImageID(t.Context(), archive)
			if got != tc.want || (tc.want == "") != errors.Is(err, ErrRemoteDocker) || e.calls != 1 {
				t.Fatalf("identity=%q err=%v calls=%d", got, err, e.calls)
			}
		})
	}
}

func (e *imageInspectExecutor) Run(_ context.Context, _ map[string]string, args []string, _ io.Reader, out io.Writer) (Result, error) {
	e.calls = append(e.calls, slices.Clone(args))
	switch {
	case len(args) > 3 && slices.Equal(args[1:3], []string{"image", "ls"}):
		_, _ = io.WriteString(out, e.listed)
	case len(args) == 4 && slices.Equal(args[1:3], []string{"image", "inspect"}):
		_, _ = out.Write(e.inspect)
		return e.inspectResult, nil
	default:
		return Result{}, ErrRemoteDocker
	}
	return Result{}, nil
}

func testImageGeneration() hostimages.Generation {
	return hostimages.Generation{
		ID: "img_fixture", Key: hostimages.Key{InstallationID: "rt_fixture", MachineID: domain.SHA256Digest(strings.Repeat("a", 64)),
			EngineID: domain.SHA256Digest(strings.Repeat("b", 64)), ArchiveSHA256: domain.SHA256Digest(strings.Repeat("c", 64)), ArchiveBytes: 4096},
		Owner:       hostimages.Owner{WorkspaceID: "w", JobID: "job_fixture", AttemptID: "attempt_fixture", ObjectID: "image"},
		OperationID: "imload_fixture", ExpectedImageID: "sha256:" + strings.Repeat("d", 64),
		RootFSDiffIDs: []string{"sha256:" + strings.Repeat("e", 64)}, State: hostimages.StateReady,
		CreatedAt: time.Now().UTC(), LeaseUntil: time.Now().UTC().Add(time.Hour),
	}
}

func imageInspectJSON(g hostimages.Generation, tags []string) []byte {
	data, _ := json.Marshal([]any{map[string]any{
		"Id": g.ExpectedImageID, "Os": "linux", "Architecture": "amd64", "RepoTags": tags,
		"RepoDigests": []string{}, "RootFS": map[string]any{"Layers": g.RootFSDiffIDs},
		"Config": map[string]any{"Labels": map[string]string{
			hostimages.LabelInstallation: string(g.Key.InstallationID), hostimages.LabelKind: "application",
			hostimages.LabelArchive: string(g.Key.ArchiveSHA256), hostimages.LabelSchema: hostimages.ApplicationSchema,
			hostimages.LabelMachine: string(g.Key.MachineID), hostimages.LabelEngine: string(g.Key.EngineID),
		}},
	}})
	return data
}

func TestApplicationImageObservationRequiresFilteredTaglessExactIdentity(t *testing.T) {
	g := testImageGeneration()
	for _, tc := range []struct {
		name    string
		listed  string
		inspect []byte
		result  Result
		present bool
		wantErr error
		calls   int
	}{
		{"missing", "", nil, Result{}, false, nil, 1},
		{"removed-after-list", g.ExpectedImageID + "\n", nil, noSuch("image"), false, nil, 2},
		{"exact", g.ExpectedImageID + "\n", imageInspectJSON(g, nil), Result{}, true, nil, 2},
		{"tagged", g.ExpectedImageID + "\n", imageInspectJSON(g, []string{"foreign/app:latest"}), Result{}, false, ErrIdentity, 2},
		{"unfiltered-id", "sha256:" + strings.Repeat("f", 64) + "\n", nil, Result{}, false, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &imageInspectExecutor{listed: tc.listed, inspect: tc.inspect, inspectResult: tc.result}
			d := docker{exec: e, binary: "/usr/local/bin/docker"}
			present, err := d.observeApplicationImage(context.Background(), g, "amd64")
			if present != tc.present || !errors.Is(err, tc.wantErr) || len(e.calls) != tc.calls {
				t.Fatalf("observation=%v err=%v calls=%v", present, err, e.calls)
			}
			if len(e.calls[0]) < 7 || e.calls[0][1] != "image" || e.calls[0][2] != "ls" ||
				!slices.Contains(e.calls[0], "label="+hostimages.LabelInstallation+"="+string(g.Key.InstallationID)) {
				t.Fatalf("image discovery did not filter installation: %v", e.calls[0])
			}
			if len(e.calls) == 2 && e.calls[1][3] != g.ExpectedImageID {
				t.Fatalf("inspected a non-derived identity: %v", e.calls[1])
			}
		})
	}
}

func TestApplicationImageContainerDisablesInheritedHealthcheckOnlyWhenRequested(t *testing.T) {
	base := containerSpec{Name: "job", Image: "sha256:" + strings.Repeat("d", 64), PIDs: 32}
	if slices.Contains(base.args(), "--no-healthcheck") {
		t.Fatal("standard container unexpectedly changed")
	}
	base.NoHealthcheck = true
	if !slices.Contains(base.args(), "--no-healthcheck") {
		t.Fatal("application container inherited the source healthcheck")
	}
}

func TestLegacyReadinessHashIgnoresNewHealthcheckInspectionField(t *testing.T) {
	info := containerInfo{ID: strings.Repeat("a", 64), Image: fakeImageID}
	info.Config.Labels = map[string]string{labelPrefix + "installation": "installation"}
	info.Config.User = "65534:65534"
	info.Config.Cmd = []string{"python3", "run.py"}
	info.Config.WorkingDir = "/relay"
	plan := domain.SHA256Digest(strings.Repeat("b", 64))
	files := map[string]stagedFile{"manifest.json": {Bytes: 3, SHA256: domain.SHA256Digest(strings.Repeat("c", 64))}}
	legacy := observedPreparationHash(info, plan, files)
	info.Config.Healthcheck = &struct {
		Test []string `json:"Test"`
	}{Test: []string{"CMD-SHELL", "echo inherited"}}
	if got := observedPreparationHash(info, plan, files); got != legacy {
		t.Fatal("new inspect field changed a pre-existing legacy readiness digest")
	}
	info.Config.Labels[labelPrefix+"cache"] = "volumes/v1"
	cached := observedPreparationHash(info, plan, files)
	info.Config.Healthcheck.Test = []string{"NONE"}
	if got := observedPreparationHash(info, plan, files); got == cached {
		t.Fatal("new cached readiness failed to bind the observed healthcheck")
	}
}
