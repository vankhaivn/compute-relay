package sshhost

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type imageFixtureRepo struct {
	mu      sync.Mutex
	pins    map[string]hostimages.Pin
	active  map[hostimages.Key]hostimages.Generation
	serial  int
	permits int
}

func newImageFixtureRepo() *imageFixtureRepo {
	return &imageFixtureRepo{pins: map[string]hostimages.Pin{}, active: map[hostimages.Key]hostimages.Generation{}}
}

func imageAttemptKey(w domain.WorkspaceID, j domain.JobID, a domain.AttemptID) string {
	return string(w) + "/" + string(j) + "/" + string(a)
}

func (r *imageFixtureRepo) pin(id provider.Identity, key hostimages.Key, object domain.ObjectID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pins[imageAttemptKey(id.WorkspaceID, id.JobID, id.AttemptID)] = hostimages.Pin{Key: key, ObjectID: object}
}

func (r *imageFixtureRepo) PinnedImage(_ context.Context, w domain.WorkspaceID, j domain.JobID, a domain.AttemptID) (*hostimages.Pin, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pin, ok := r.pins[imageAttemptKey(w, j, a)]
	if !ok {
		return nil, nil
	}
	return &pin, nil
}

func (r *imageFixtureRepo) ClaimImage(_ context.Context, owner hostimages.Owner, key hostimages.Key) (hostimages.Generation, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pin, ok := r.pins[imageAttemptKey(owner.WorkspaceID, owner.JobID, owner.AttemptID)]
	if !ok || pin.Released || pin.Key != key || pin.ObjectID != owner.ObjectID {
		return hostimages.Generation{}, false, hostimages.ErrImageState
	}
	if g, ok := r.active[key]; ok {
		return g, false, nil
	}
	r.serial++
	now := time.Now().UTC()
	g := hostimages.Generation{ID: fmt.Sprintf("img_fixture_%d", r.serial), Key: key, Owner: owner,
		OperationID: domain.OperationID(fmt.Sprintf("imload_fixture_%d", r.serial)), State: hostimages.StatePreparing,
		CreatedAt: now, LeaseUntil: now.Add(30 * time.Minute)}
	r.active[key] = g
	return g, true, nil
}

func (r *imageFixtureRepo) ActiveImage(_ context.Context, key hostimages.Key) (*hostimages.Generation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.active[key]
	if !ok {
		return nil, nil
	}
	return &g, nil
}

func (r *imageFixtureRepo) BeginImageLoad(_ context.Context, g hostimages.Generation, id string, diffIDs []string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	active, ok := r.active[g.Key]
	if !ok || active.ID != g.ID || active.State != hostimages.StatePreparing {
		return false, nil
	}
	active.State, active.ExpectedImageID, active.RootFSDiffIDs, active.LoadingAt = hostimages.StateLoading, id, append([]string(nil), diffIDs...), time.Now().UTC()
	r.active[g.Key] = active
	r.permits++
	return true, nil
}

func (r *imageFixtureRepo) RetirePreloadImage(_ context.Context, g hostimages.Generation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if active, ok := r.active[g.Key]; !ok || active.ID != g.ID || active.State != hostimages.StatePreparing {
		return hostimages.ErrImageState
	}
	delete(r.active, g.Key)
	return nil
}

func (r *imageFixtureRepo) MarkImageReady(_ context.Context, g hostimages.Generation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	active, ok := r.active[g.Key]
	if !ok || active.ID != g.ID || active.ExpectedImageID != g.ExpectedImageID || active.State != hostimages.StateLoading && active.State != hostimages.StateUnknown && active.State != hostimages.StateReady {
		return hostimages.ErrImageState
	}
	active.State = hostimages.StateReady
	r.active[g.Key] = active
	return nil
}

func (r *imageFixtureRepo) MarkImageUnknown(_ context.Context, g hostimages.Generation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	active, ok := r.active[g.Key]
	if !ok || active.ID != g.ID || active.ExpectedImageID != g.ExpectedImageID || active.State != hostimages.StateLoading && active.State != hostimages.StateUnknown {
		return hostimages.ErrImageState
	}
	active.State = hostimages.StateUnknown
	r.active[g.Key] = active
	return nil
}

func (r *imageFixtureRepo) RetireMissingImage(_ context.Context, g hostimages.Generation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	active, ok := r.active[g.Key]
	if !ok || active.ID != g.ID || active.State != hostimages.StateReady {
		return hostimages.ErrImageState
	}
	delete(r.active, g.Key)
	return nil
}

func (r *imageFixtureRepo) TouchImage(_ context.Context, g hostimages.Generation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	active, ok := r.active[g.Key]
	if !ok || active.ID != g.ID || active.State != hostimages.StateReady {
		return hostimages.ErrImageState
	}
	return nil
}

func fixtureImageTar(t *testing.T) []byte {
	t.Helper()
	writeTar := func(members map[string][]byte) []byte {
		var out bytes.Buffer
		w := tar.NewWriter(&out)
		for name, data := range members {
			if err := w.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(data); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return out.Bytes()
	}
	layer := writeTar(map[string][]byte{"app.txt": []byte("application image\n")})
	ld := sha256.Sum256(layer)
	config, _ := json.Marshal(map[string]any{"architecture": "amd64", "os": "linux",
		"rootfs": map[string]any{"type": "layers", "diff_ids": []string{"sha256:" + hex.EncodeToString(ld[:])}},
		"config": map[string]any{"Entrypoint": []string{}, "Labels": map[string]string{"org.example.app": "fixture"},
			"Healthcheck": map[string]any{"Test": []string{"CMD-SHELL", "touch /tmp/health-ran"}}}})
	cd := sha256.Sum256(config)
	name := hex.EncodeToString(cd[:]) + ".json"
	manifest, _ := json.Marshal([]map[string]any{{"Config": name, "RepoTags": []string{"foreign/app:latest"}, "Layers": []string{"layer/layer.tar"}}})
	return writeTar(map[string][]byte{name: config, "manifest.json": manifest, "layer/layer.tar": layer})
}

func applicationHarness(t *testing.T) (*harness, *imageFixtureRepo, hostimages.Key) {
	t.Helper()
	h := newHarness(t)
	h.adapter.config.Image = fakeImageID
	h.adapter.config.Architecture = "amd64"
	h.adapter.config.ImageRecipeSHA256 = ImageRecipeDigest()
	h.adapter.config.MachineID = resourceDigest("linux-machine-id", h.host.machineID)
	h.adapter.config.EngineID = resourceDigest("docker-engine-id", h.host.engineID)
	standard := imageInfo{ID: fakeImageID, OS: "linux", Architecture: "amd64"}
	standard.Config.Labels = map[string]string{imageRecipeLabel: ImageRecipeDigest(), "io.compute-relay.image-schema": "1"}
	h.host.images[fakeImageID] = standard
	archive := fixtureImageTar(t)
	digest := sha256.Sum256(archive)
	sha := domain.SHA256Digest(hex.EncodeToString(digest[:]))
	spec := strings.Replace(testSpec, `"execution":`, fmt.Sprintf(`"application_image":{"object_id":"image","sha256":%q,"bytes":%d},"execution":`, sha, len(archive)), 1)
	h.job = testJob(t, spec, h.blobs)
	meta := domain.ObjectMetadata{WorkspaceID: "workspace", ID: "image", SHA256: sha, Bytes: int64(len(archive))}
	h.job.Inputs.ApplicationImage = &meta
	h.blobs["image"] = archive
	if err := h.job.Validate(); err != nil {
		t.Fatal(err)
	}
	key := hostimages.Key{InstallationID: h.job.Identity.InstallationID, MachineID: domain.SHA256Digest(h.adapter.config.MachineID),
		EngineID: domain.SHA256Digest(h.adapter.config.EngineID), ArchiveSHA256: sha, ArchiveBytes: meta.Bytes}
	repo := newImageFixtureRepo()
	repo.pin(h.job.Identity, key, meta.ID)
	h.adapter.WithApplicationImages(repo)
	return h, repo, key
}

func secondImageAttempt(h *harness, repo *imageFixtureRepo, key hostimages.Key) {
	job := h.job.Clone()
	job.Identity.JobID, job.Identity.AttemptID = "job-two", "attempt-two"
	job.Identity.ResourceKey = "cr-" + strings.Repeat("cd", 20)
	job.Identity.Nonce = strings.Repeat("b", 32)
	h.job = job
	repo.pin(job.Identity, key, job.Inputs.ApplicationImage.ID)
}

func TestApplicationImageFirstLoadReuseAndReadOnlyRecovery(t *testing.T) {
	h, repo, key := applicationHarness(t)
	plan, first := h.prepare(t)
	active, err := repo.ActiveImage(context.Background(), key)
	if err != nil || active == nil || active.State != hostimages.StateReady || h.host.applicationLoads != 1 || repo.permits != 1 || h.host.starts != 0 {
		t.Fatalf("first image load: active=%+v loads=%d permits=%d starts=%d err=%v", active, h.host.applicationLoads, repo.permits, h.host.starts, err)
	}
	image := h.host.applicationImages[active.ExpectedImageID]
	if len(image.tags) != 0 || len(image.digests) != 0 || image.labels[hostimages.LabelArchive] != string(key.ArchiveSHA256) ||
		image.labels[hostimages.LabelInstallation] != string(key.InstallationID) || image.labels["org.example.app"] != "fixture" {
		t.Fatalf("source tags or ownership mismatch: %+v", image)
	}
	if got, err := h.adapter.ReconcilePrepare(context.Background(), plan, testOperation); err != nil || got.Prepared == nil || *got.Prepared != first {
		t.Fatalf("read-only preparation recovery: %+v err=%v", got, err)
	}
	firstContainer := h.host.find(containerName(plan.Job.Identity))
	if firstContainer == nil || firstContainer.imageID != active.ExpectedImageID || !firstContainer.noHealthcheck {
		t.Fatalf("app target did not pin derived ID and disable healthcheck: %+v", firstContainer)
	}
	secondImageAttempt(h, repo, key)
	_, second := h.prepare(t)
	if h.host.applicationLoads != 1 || repo.permits != 1 || second.VerificationSHA256 == first.VerificationSHA256 {
		t.Fatalf("reuse loaded again or reused readiness: loads=%d permits=%d", h.host.applicationLoads, repo.permits)
	}
	if out := h.adapter.Start(context.Background(), first, nil, nil); out.Status != provider.SubmissionAccepted {
		t.Fatalf("first application start: %+v", out)
	}
	if out := h.adapter.Start(context.Background(), second, nil, nil); out.Status != provider.SubmissionAccepted {
		t.Fatalf("second application start: %+v", out)
	}
}

func TestApplicationImageClassicAndContainerdFreezeEngineIdentity(t *testing.T) {
	for _, containerd := range []bool{false, true} {
		name := "classic"
		if containerd {
			name = "containerd"
		}
		t.Run(name, func(t *testing.T) {
			h, repo, key := applicationHarness(t)
			h.host.containerdImages = containerd
			plan, first := h.prepare(t)
			g, err := repo.ActiveImage(t.Context(), key)
			if err != nil || g == nil || g.State != hostimages.StateReady || h.host.applicationLoads != 1 || repo.permits != 1 {
				t.Fatalf("successful load not retained: generation=%+v loads=%d permits=%d err=%v", g, h.host.applicationLoads, repo.permits, err)
			}
			if _, ok := h.host.applicationImages[g.ExpectedImageID]; !ok || h.host.container(t).imageID != g.ExpectedImageID {
				t.Fatal("preparation did not freeze the exact engine image ID")
			}
			if seen, err := h.adapter.ReconcilePrepare(t.Context(), plan, testOperation); err != nil || seen.Prepared == nil || *seen.Prepared != first {
				t.Fatal("read-only recovery changed the exact prepared image", err)
			}
			secondImageAttempt(h, repo, key)
			h.prepare(t)
			if h.host.applicationLoads != 1 || repo.permits != 1 {
				t.Fatal("reuse granted another image load")
			}
		})
	}
}

func TestApplicationImageUnavailableStoreDoesNotIssueLoadPermit(t *testing.T) {
	h, repo, key := applicationHarness(t)
	h.host.fail = func(args []string) (Result, bool) {
		if len(args) == 3 && args[0] == "info" && args[2] == imageStoreFormat {
			return Result{ExitCode: 1}, true
		}
		return Result{}, false
	}
	plan, err := h.adapter.Validate(t.Context(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.adapter.Prepare(t.Context(), plan, testOperation); !errors.Is(err, ErrRemoteDocker) {
		t.Fatal("unavailable backend did not block preparation", err)
	}
	g, err := repo.ActiveImage(t.Context(), key)
	if err != nil || g != nil || h.host.applicationLoads != 0 || repo.permits != 0 || h.host.created != 0 {
		t.Fatal("backend refusal retained local preparation or granted a mutation")
	}
}

func TestApplicationImageLegacyUnknownConfigIdentityNeverRewrittenOrReloaded(t *testing.T) {
	h, repo, key := applicationHarness(t)
	h.host.containerdImages = true
	meta := h.job.Inputs.ApplicationImage
	archive, err := hostimages.Normalize(t.Context(), bytes.NewReader(h.blobs[meta.ID]), hostimages.Options{
		ArchiveSHA256: meta.SHA256, ArchiveBytes: meta.Bytes, InstallationID: key.InstallationID,
		MachineID: key.MachineID, EngineID: key.EngineID, Architecture: "amd64", TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	owner := hostimages.Owner{WorkspaceID: h.job.Identity.WorkspaceID, JobID: h.job.Identity.JobID,
		AttemptID: h.job.Identity.AttemptID, ObjectID: meta.ID}
	g, elected, err := repo.ClaimImage(t.Context(), owner, key)
	if err != nil || !elected {
		t.Fatal("legacy generation absent", err)
	}
	if begun, err := repo.BeginImageLoad(t.Context(), g, archive.ImageID, archive.RootFSDiffIDs); err != nil || !begun {
		t.Fatal("legacy load permit absent", err)
	}
	g.State, g.ExpectedImageID, g.RootFSDiffIDs = hostimages.StateLoading, archive.ImageID, archive.RootFSDiffIDs
	if err := repo.MarkImageUnknown(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(archive)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := decodeFakeApplicationArchive(raw, true)
	if err != nil || loaded.id == g.ExpectedImageID {
		t.Fatal("fixture did not preserve the config/manifest mismatch", err)
	}
	h.host.applicationImages[loaded.id] = loaded
	plan, err := h.adapter.Validate(t.Context(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.adapter.Prepare(t.Context(), plan, testOperation); !errors.Is(err, hostimages.ErrImageState) {
		t.Fatal("legacy unknown load gained new mutation authority", err)
	}
	active, err := repo.ActiveImage(t.Context(), key)
	if err != nil || active == nil || active.ID != g.ID || active.OperationID != g.OperationID ||
		active.ExpectedImageID != g.ExpectedImageID || active.State != hostimages.StateUnknown ||
		repo.permits != 1 || h.host.applicationLoads != 0 || h.host.created != 0 {
		t.Fatal("legacy load identity was changed or its mutation repeated")
	}
}

func TestApplicationImageLostLoadReplyResolvesOnlyByExactObservation(t *testing.T) {
	h, repo, _ := applicationHarness(t)
	h.host.applicationLoadAckLost = true
	_, prepared := h.prepare(t)
	if !prepared.VerificationSHA256.Valid() || h.host.applicationLoads != 1 || repo.permits != 1 {
		t.Fatalf("lost acknowledgement did not resolve exact image: %+v", prepared)
	}
}

func TestApplicationImageUnknownAbsenceNeverRetriesLoad(t *testing.T) {
	h, repo, key := applicationHarness(t)
	h.host.applicationLoadMissing = true
	plan, err := h.adapter.Validate(context.Background(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.adapter.Prepare(context.Background(), plan, testOperation); !errors.Is(err, hostimages.ErrImageState) {
		t.Fatalf("unresolved load was accepted: %v", err)
	}
	secondImageAttempt(h, repo, key)
	plan, err = h.adapter.Validate(context.Background(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.adapter.Prepare(context.Background(), plan, testOperation); !errors.Is(err, hostimages.ErrImageState) ||
		h.host.applicationLoads != 1 || repo.permits != 1 {
		t.Fatalf("unknown outcome retried load: err=%v loads=%d permits=%d", err, h.host.applicationLoads, repo.permits)
	}
}

func TestApplicationImageReadyThenObservedAbsentCreatesNewGeneration(t *testing.T) {
	h, repo, key := applicationHarness(t)
	h.prepare(t)
	active, err := repo.ActiveImage(context.Background(), key)
	if err != nil || active == nil {
		t.Fatal(err)
	}
	h.host.mu.Lock()
	delete(h.host.applicationImages, active.ExpectedImageID)
	h.host.mu.Unlock()
	secondImageAttempt(h, repo, key)
	h.prepare(t)
	if h.host.applicationLoads != 2 || repo.permits != 2 {
		t.Fatalf("observed absence failed to create one new generation: loads=%d permits=%d", h.host.applicationLoads, repo.permits)
	}
}

func TestApplicationImageConcurrentFirstUseElectsOneRemoteLoad(t *testing.T) {
	h, repo, key := applicationHarness(t)
	first, err := h.adapter.Validate(context.Background(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	secondImageAttempt(h, repo, key)
	second, err := h.adapter.Validate(context.Background(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	otherVault := &testVault{raw: append([]byte(nil), h.vault.raw...)}
	other, err := NewAdapter(h.adapter.config, h.adapter.binding, otherVault, h.blobs, &testClock{now: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	other.WithApplicationImages(repo)
	other.dial = func(context.Context, Target, Credential) (session, error) { return h.host, nil }
	var got [2]provider.Prepared
	var failures [2]error
	var wg sync.WaitGroup
	for i, task := range []struct {
		adapter *Adapter
		plan    provider.Plan
	}{{h.adapter, first}, {other, second}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i], failures[i] = task.adapter.Prepare(context.Background(), task.plan, testOperation)
		}()
	}
	wg.Wait()
	if failures[0] != nil || failures[1] != nil || got[0].Resource == got[1].Resource ||
		h.host.applicationLoads != 1 || repo.permits != 1 {
		t.Fatalf("concurrent first use: failures=%v receipts=%v loads=%d permits=%d", failures, got, h.host.applicationLoads, repo.permits)
	}
}

func TestApplicationImageControlledReadinessAndStartRecheckFrozenImage(t *testing.T) {
	for _, change := range []string{"none", "healthcheck", "removed-image"} {
		t.Run(change, func(t *testing.T) {
			h, _, key := applicationHarness(t)
			clock := &deadlineClock{at: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)}
			h.adapter.clock, h.host.engineTime = clock, clock.at
			spec := strings.Replace(string(h.job.Specification), `"name":"PRIVATE_LABEL"`, `"name":"PRIVATE_LABEL","execution_control":"prepare_then_start"`, 1)
			h.job.Specification = []byte(spec)
			h.job.SpecificationSHA256 = provider.Digest([]byte(spec))
			h.job.Required = append(h.job.Required, domain.CapabilitySeparateStart)
			if err := h.job.Validate(); err != nil {
				t.Fatal(err)
			}
			plan, prepared := h.prepare(t)
			if !prepared.ReadinessRevision().Valid() || h.host.starts != 0 {
				t.Fatalf("application readiness lacks a stopped proof: %+v", prepared)
			}
			if observed, err := h.adapter.ReconcilePrepare(context.Background(), plan, testOperation); err != nil || observed.Prepared == nil || *observed.Prepared != prepared {
				t.Fatalf("controlled application reconciliation: %+v err=%v", observed, err)
			}
			switch change {
			case "healthcheck":
				c := h.host.find(containerName(h.job.Identity))
				c.noHealthcheck = false
			case "removed-image":
				active, err := h.adapter.images.ActiveImage(context.Background(), key)
				if err != nil || active == nil {
					t.Fatal(err)
				}
				h.host.mu.Lock()
				delete(h.host.applicationImages, active.ExpectedImageID)
				h.host.mu.Unlock()
			}
			if change != "none" {
				if observed, err := h.adapter.ReconcilePrepare(context.Background(), plan, testOperation); err == nil && observed.Prepared != nil && observed.Prepared.Ready {
					t.Fatal("changed application image retained readiness")
				}
			}
			start := startAuthority(prepared, clock)
			out := h.adapter.Start(context.Background(), prepared, &start, nil)
			if change == "none" {
				if out.Status != provider.SubmissionAccepted || h.host.starts != 1 {
					t.Fatalf("valid application start: %+v", out)
				}
			} else if out.Status != provider.SubmissionRejected || h.host.starts != 0 {
				t.Fatalf("invalidated application started: %+v", out)
			}
		})
	}
}
