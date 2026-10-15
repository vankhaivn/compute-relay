package sshhost

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type cacheFixtureRepo struct {
	mu          sync.Mutex
	pins        map[hostcache.Attempt][]hostcache.Key
	active      map[hostcache.Key]hostcache.Generation
	count       int
	mounts      []hostcache.MountReceipt
	beginNotify chan struct{}
	resumeBegin chan struct{}
	beginOnce   sync.Once
}

func newCacheFixtureRepo() *cacheFixtureRepo {
	return &cacheFixtureRepo{pins: map[hostcache.Attempt][]hostcache.Key{}, active: map[hostcache.Key]hostcache.Generation{}}
}

func (r *cacheFixtureRepo) pin(id provider.Identity, machine, engine string, inputs provider.InputSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[hostcache.Key]bool{}
	objects := []domain.ObjectMetadata{inputs.Bundle}
	for _, input := range inputs.Inputs {
		objects = append(objects, input.Object)
	}
	for _, object := range objects {
		key := hostcache.Key{InstallationID: id.InstallationID, MachineID: domain.SHA256Digest(machine), EngineID: domain.SHA256Digest(engine), SHA256: object.SHA256, Bytes: object.Bytes}
		if !seen[key] {
			r.pins[cacheAttempt(id)] = append(r.pins[cacheAttempt(id)], key)
			seen[key] = true
		}
	}
}

func (r *cacheFixtureRepo) Pins(_ context.Context, owner hostcache.Attempt) ([]hostcache.Key, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]hostcache.Key(nil), r.pins[owner]...), nil
}
func (r *cacheFixtureRepo) Active(_ context.Context, key hostcache.Key) (*hostcache.Generation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.active[key]
	if !ok {
		return nil, nil
	}
	return &g, nil
}
func (r *cacheFixtureRepo) Inventory(context.Context, domain.RuntimeInstallationID, domain.SHA256Digest, domain.SHA256Digest) ([]hostcache.Generation, error) {
	return nil, nil
}
func (r *cacheFixtureRepo) Claim(_ context.Context, owner hostcache.Attempt, key hostcache.Key) (hostcache.Generation, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.active[key]; ok {
		return g, false, nil
	}
	r.count++
	id := fmt.Sprintf("hcgen_%032x", r.count)
	g := hostcache.Generation{Key: key, ID: id, VolumeName: "relay-cache-" + strings.TrimPrefix(id, "hcgen_"), OperationID: domain.OperationID(fmt.Sprintf("hcop_%032x", r.count)), Owner: owner, State: hostcache.Claimed, WriterPhase: hostcache.WriterIntent, CreatedAt: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)}
	r.active[key] = g
	return g, true, nil
}
func (r *cacheFixtureRepo) BeginWrite(_ context.Context, owner hostcache.Attempt, id string) error {
	r.mu.Lock()
	for k, g := range r.active {
		if g.ID == id {
			if g.Owner != owner || g.WriterPhase != hostcache.WriterIntent {
				r.mu.Unlock()
				return hostcache.ErrUnknown
			}
			g.WriterPhase = hostcache.WriterBegun
			r.active[k] = g
			r.mu.Unlock()
			if r.beginNotify != nil {
				r.beginOnce.Do(func() { close(r.beginNotify); <-r.resumeBegin })
			}
			return nil
		}
	}
	r.mu.Unlock()
	return hostcache.ErrConflict
}
func (r *cacheFixtureRepo) FinishWrite(_ context.Context, owner hostcache.Attempt, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, g := range r.active {
		if g.ID == id {
			if g.Owner != owner || g.WriterPhase != hostcache.WriterBegun {
				return hostcache.ErrConflict
			}
			g.WriterPhase = hostcache.WriterFinished
			r.active[k] = g
			return nil
		}
	}
	return hostcache.ErrConflict
}
func (r *cacheFixtureRepo) Verify(_ context.Context, o hostcache.Observation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, g := range r.active {
		if g.ID == o.GenerationID {
			if !o.Matches(g) {
				return hostcache.ErrConflict
			}
			g.State = hostcache.Verified
			g.WriterPhase = hostcache.WriterFinished
			g.VerifiedAt = time.Date(2026, 10, 15, 0, 1, 0, 0, time.UTC)
			r.active[k] = g
			return nil
		}
	}
	return hostcache.ErrConflict
}
func (r *cacheFixtureRepo) Retire(_ context.Context, o hostcache.Observation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, g := range r.active {
		if g.ID == o.GenerationID {
			if !o.Matches(g) {
				return hostcache.ErrConflict
			}
			delete(r.active, k)
			return nil
		}
	}
	return hostcache.ErrConflict
}
func (r *cacheFixtureRepo) RecordMount(_ context.Context, m hostcache.MountReceipt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !m.Valid() {
		return hostcache.ErrInvalid
	}
	r.mounts = append(r.mounts, m)
	return nil
}

func cacheHarness(t *testing.T) (*harness, *cacheFixtureRepo) {
	t.Helper()
	h := newHarness(t)
	h.adapter.config.MachineID = resourceDigest("linux-machine-id", h.host.machineID)
	h.adapter.config.EngineID = resourceDigest("docker-engine-id", h.host.engineID)
	r := newCacheFixtureRepo()
	r.pin(h.job.Identity, h.adapter.config.MachineID, h.adapter.config.EngineID, *h.job.Inputs)
	h.adapter.WithHostCache(r)
	return h, r
}

func TestCacheMissHitAndExactReadOnlyMount(t *testing.T) {
	h, r := cacheHarness(t)
	ctx := context.Background()
	plan, first := h.prepare(t)
	if h.host.writes != 2 || h.host.starts != 0 || !first.VerificationSHA256.Valid() {
		t.Fatalf("first cache preparation: writes=%d starts=%d receipt=%+v", h.host.writes, h.host.starts, first)
	}
	firstContainer := h.host.find(containerName(h.job.Identity))
	if firstContainer == nil || len(firstContainer.mounts) != 2 || len(firstContainer.files["relay/staging/code.bin"].data) != 0 {
		t.Fatal("cache preparation copied an input to the target")
	}
	for _, mount := range firstContainer.mounts {
		if !mount.ReadOnly || !mount.VolumeOptions.NoCopy || !strings.HasPrefix(mount.Target, "/staging-cache/cache/") {
			t.Fatal("non-immutable mount", mount)
		}
	}
	seen, err := h.adapter.ReconcilePrepare(ctx, plan, testOperation)
	if err != nil || seen.Prepared == nil || *seen.Prepared != first {
		t.Fatal("cached reconciliation", err, seen)
	}
	second := h.job.Clone()
	second.Identity.JobID = "job-two"
	second.Identity.AttemptID = "attempt-two"
	second.Identity.ResourceKey = "cr-" + strings.Repeat("cd", 20)
	second.Identity.Nonce = strings.Repeat("b", 32)
	h.job = second
	r.pin(second.Identity, h.adapter.config.MachineID, h.adapter.config.EngineID, *second.Inputs)
	plan2, receipt := h.prepare(t)
	if h.host.writes != 2 || h.host.starts != 0 || !receipt.VerificationSHA256.Valid() {
		t.Fatalf("cache hit transferred bytes: writes=%d", h.host.writes)
	}
	if observed, err := h.adapter.ReconcilePrepare(ctx, plan2, testOperation); err != nil || observed.Prepared == nil || *observed.Prepared != receipt {
		t.Fatal("hit reconciliation", err, observed)
	}
	if out := h.adapter.Start(ctx, receipt, nil, nil); out.Status != provider.SubmissionAccepted || h.host.starts != 1 {
		t.Fatal("cached start", out)
	}
	if len(r.mounts) < 4 {
		t.Fatal("exact mount receipts missing")
	}
}

func TestCacheChangedVolumeBlocksStartAndFreshGeneration(t *testing.T) {
	h, r := cacheHarness(t)
	_, prepared := h.prepare(t)
	var old string
	for name, v := range h.host.volumes {
		if string(v.blob) == "code" {
			old = name
			v.blob = []byte("bad!")
		}
	}
	if old == "" {
		t.Fatal("missing volume")
	}
	if out := h.adapter.Start(context.Background(), prepared, nil, nil); out.Status != provider.SubmissionRejected || h.host.starts != 0 {
		t.Fatal("changed volume started target", out)
	}
	second := h.job.Clone()
	second.Identity.JobID = "job-two"
	second.Identity.AttemptID = "attempt-two"
	second.Identity.ResourceKey = "cr-" + strings.Repeat("cd", 20)
	second.Identity.Nonce = strings.Repeat("b", 32)
	h.job = second
	r.pin(second.Identity, h.adapter.config.MachineID, h.adapter.config.EngineID, *second.Inputs)
	_, fresh := h.prepare(t)
	if h.host.writes != 3 || h.host.starts != 0 || !fresh.VerificationSHA256.Valid() {
		t.Fatal("corrupt generation was reused or executed")
	}
	for _, mount := range h.host.find(containerName(second.Identity)).mounts {
		if mount.Source == old {
			t.Fatal("new attempt mounted corrupt generation")
		}
	}
}

func TestConcurrentCachePreparationWaitsForSingleWriter(t *testing.T) {
	h, r := cacheHarness(t)
	r.beginNotify = make(chan struct{})
	r.resumeBegin = make(chan struct{})
	first, err := h.adapter.Validate(context.Background(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	second := h.job.Clone()
	second.Identity.JobID = "concurrent-job"
	second.Identity.AttemptID = "concurrent-attempt"
	second.Identity.ResourceKey = "cr-" + strings.Repeat("ef", 20)
	second.Identity.Nonce = strings.Repeat("c", 32)
	r.pin(second.Identity, h.adapter.config.MachineID, h.adapter.config.EngineID, *second.Inputs)
	secondPlan, err := h.adapter.Validate(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	firstResult := make(chan error, 1)
	go func() { _, err := h.adapter.Prepare(ctx, first, testOperation); firstResult <- err }()
	select {
	case <-r.beginNotify:
	case <-ctx.Done():
		t.Fatal("first writer did not begin")
	}
	secondResult := make(chan error, 1)
	go func() { _, err := h.adapter.Prepare(ctx, secondPlan, "prep_concurrent"); secondResult <- err }()
	select {
	case err := <-secondResult:
		t.Fatalf("peer did not wait for first writer: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(r.resumeBegin)
	if err := <-firstResult; err != nil {
		t.Fatal("first preparation", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatal("second preparation", err)
	}
	if h.host.writes != 2 || h.host.created != 2 || h.host.starts != 0 {
		t.Fatalf("concurrent attempts duplicated cache writes or executed: writes=%d created=%d starts=%d", h.host.writes, h.host.created, h.host.starts)
	}
}

func TestFixedVolumeVerifierRejectsUnsafeOrChangedBlob(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	root := t.TempDir()
	blob := filepath.Join(root, "blob")
	if err := os.WriteFile(blob, []byte("code"), 0o444); err != nil {
		t.Fatal(err)
	}
	bootstrap := `import base64,json,sys; ns={'__name__':'cache_test'}; exec(base64.b64decode(sys.argv[1]),ns); print(json.dumps(ns['volume_scan'](sys.argv[2],int(sys.argv[3]))))`
	run := func() (verifierInventory, error) {
		out, err := exec.Command(python, "-B", "-E", "-s", "-c", bootstrap, base64.StdEncoding.EncodeToString([]byte(hostVerifierSource)), root, "4").CombinedOutput()
		if err != nil {
			return verifierInventory{}, err
		}
		var inventory verifierInventory
		if err := json.Unmarshal(out, &inventory); err != nil {
			return verifierInventory{}, err
		}
		return inventory, nil
	}
	good, err := run()
	if err != nil || len(good.Files) != 1 || good.Files[0].SHA256 != string(provider.Digest([]byte("code"))) {
		t.Fatalf("direct RO verification failed: %+v %v", good, err)
	}
	if err := os.WriteFile(filepath.Join(root, "extra"), []byte("x"), 0o444); err != nil {
		t.Fatal(err)
	}
	if _, err := run(); err == nil {
		t.Fatal("verifier accepted extra volume entry")
	}
	if err := os.Remove(filepath.Join(root, "extra")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(blob); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "elsewhere"), blob); err != nil {
		t.Fatal(err)
	}
	if _, err := run(); err == nil {
		t.Fatal("verifier accepted symlink")
	}
}

func TestReadinessDigestPreservesLegacyShapeAndBindsCacheMounts(t *testing.T) {
	var info containerInfo
	info.ID = strings.Repeat("a", 64)
	info.Image = fakeImageID
	info.Config.Labels = map[string]string{labelPrefix + "attempt": "legacy-attempt"}
	info.Config.User = "65534:65534"
	info.Config.WorkingDir = "/relay"
	info.HostConfig.NetworkMode = "none"
	info.HostConfig.CapDrop = []string{"ALL"}
	info.HostConfig.SecurityOpt = []string{"no-new-privileges"}
	info.HostConfig.RestartPolicy.Name = "no"
	plan := provider.Digest([]byte("legacy-plan"))
	files := map[string]stagedFile{"manifest.json": {Bytes: 8, SHA256: provider.Digest([]byte("manifest"))}}
	// This is the exact HostConfig type and top-level marshaling shape from
	// before cache mounts were added to containerInfo.
	oldHost := struct {
		Tmpfs         map[string]string `json:"Tmpfs"`
		NetworkMode   string            `json:"NetworkMode"`
		PidMode       string            `json:"PidMode"`
		Privileged    bool              `json:"Privileged"`
		CapDrop       []string          `json:"CapDrop"`
		CapAdd        []string          `json:"CapAdd"`
		SecurityOpt   []string          `json:"SecurityOpt"`
		Binds         []string          `json:"Binds"`
		Memory        int64             `json:"Memory"`
		MemorySwap    int64             `json:"MemorySwap"`
		NanoCPUs      int64             `json:"NanoCpus"`
		PidsLimit     int64             `json:"PidsLimit"`
		RestartPolicy struct {
			Name string `json:"Name"`
		} `json:"RestartPolicy"`
	}{NetworkMode: "none", CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges"}}
	oldHost.RestartPolicy.Name = "no"
	raw, err := json.Marshal(struct {
		Version    string
		Container  string
		Image      string
		Config     any
		HostConfig any
		Plan       domain.SHA256Digest
		Files      map[string]stagedFile
	}{"ssh-readiness/v1", info.ID, info.Image, info.Config, oldHost, plan, files})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := observedPreparationHash(info, plan, files), provider.Digest(raw); got != want {
		t.Fatalf("legacy readiness changed: got %s want %s", got, want)
	}
	info.Config.Labels[labelPrefix+"cache"] = "volumes/v1"
	info.HostConfig.Mounts = []dockerMountRequest{{Type: "volume", Source: "relay-cache-" + strings.Repeat("b", 32), Target: "/staging-cache/cache/blob", ReadOnly: true}}
	info.HostConfig.Mounts[0].VolumeOptions.NoCopy = true
	info.Mounts = []dockerMount{{Type: "volume", Name: info.HostConfig.Mounts[0].Source, Destination: info.HostConfig.Mounts[0].Target, RW: false}}
	ready := observedPreparationHash(info, plan, files)
	if ready == provider.Digest(raw) {
		t.Fatal("cache readiness reused legacy version")
	}
	info.Mounts[0].RW = true
	if changed := observedPreparationHash(info, plan, files); changed == ready {
		t.Fatal("effective mount mutation did not invalidate cached readiness")
	}
}

func volumeInspectCount(h *fakeHost) int {
	count := 0
	for _, call := range h.calls {
		if len(call) > 2 && call[1] == "volume" && call[2] == "inspect" {
			count++
		}
	}
	return count
}

func TestCacheVolumeOwnershipFilterPrecedesInspect(t *testing.T) {
	h, _ := cacheHarness(t)
	_, prepared := h.prepare(t)
	if volumeInspectCount(h.host) == 0 {
		t.Fatal("owned volume was never inspected")
	}
	before := volumeInspectCount(h.host)
	for _, volume := range h.host.volumes {
		delete(volume.labels, hostcache.LabelPrefix+"machine")
	}
	if out := h.adapter.Start(context.Background(), prepared, nil, nil); out.Status != provider.SubmissionRejected || h.host.starts != 0 {
		t.Fatal("mismatched labels reached Start", out)
	}
	if volumeInspectCount(h.host) != before {
		t.Fatal("foreign/unlabelled volume was inspected")
	}
}

func TestForeignCacheVolumeNameCollisionCannotBeInspectedOrWritten(t *testing.T) {
	h, _ := cacheHarness(t)
	name := "relay-cache-" + fmt.Sprintf("%032x", 1)
	h.host.volumes[name] = &fakeVolume{labels: map[string]string{"foreign": "owner"}, blob: []byte("foreign"), complete: true}
	plan, err := h.adapter.Validate(context.Background(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.adapter.Prepare(context.Background(), plan, testOperation); err == nil {
		t.Fatal("foreign name collision allowed preparation")
	}
	if volumeInspectCount(h.host) != 0 || h.host.writes != 0 || h.host.created != 0 || h.host.starts != 0 {
		t.Fatalf("foreign volume was inspected, written, mounted, or executed: inspect=%d writes=%d created=%d starts=%d", volumeInspectCount(h.host), h.host.writes, h.host.created, h.host.starts)
	}
	v := h.host.volumes[name]
	if len(v.labels) != 1 || v.labels["foreign"] != "owner" || string(v.blob) != "foreign" {
		t.Fatal("foreign volume changed")
	}
}

func TestFixedCacheWriterBootstrapPreservesInputArguments(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	h, _ := cacheHarness(t)
	h.prepare(t)
	var command []string
	for _, call := range h.host.wireCalls {
		for i, arg := range call {
			if arg == cacheWriterBootstrap {
				command = append([]string(nil), call[i:]...)
				break
			}
		}
		if command != nil {
			break
		}
	}
	if len(command) != 4 {
		t.Fatalf("writer command arguments: %d", len(command))
	}
	source, err := base64.StdEncoding.DecodeString(command[1])
	if err != nil || string(source) != cacheWriterSource {
		t.Fatal("writer source changed in transport")
	}
	root := t.TempDir()
	data := []byte("finite cache writer input")
	// Substitute only the container mount path for an isolated local test directory.
	source = bytes.ReplaceAll(source, []byte("/cache"), []byte(root))
	args := []string{"-B", "-E", "-s", "-c", command[0], base64.StdEncoding.EncodeToString(source), fmt.Sprint(len(data)), string(provider.Digest(data))}
	if _, err := shellCommand(nil, append([]string{python}, args...)); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, args...)
	cmd.Stdin = bytes.NewReader(data)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("writer: %v %s", err, out)
	}
	blob, err := os.ReadFile(filepath.Join(root, "blob"))
	if err != nil || !bytes.Equal(blob, data) {
		t.Fatal("writer did not publish exact input", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".writing")); !os.IsNotExist(err) {
		t.Fatal("writer left provisional file")
	}
}
