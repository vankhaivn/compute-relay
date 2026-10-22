package sshhost

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// Preload immutable warm generations without issuing any remote mutation.
func warmCacheHarness(t *testing.T, contents int) (*harness, *cacheFixtureRepo) {
	t.Helper()
	h, repo := cacheHarness(t)
	var spec map[string]any
	if err := json.Unmarshal(h.job.Specification, &spec); err != nil {
		t.Fatal(err)
	}
	inputs := []any{}
	snapshot := provider.InputSnapshot{Bundle: h.job.Inputs.Bundle}
	for i := 0; i < contents-1; i++ {
		name := fmt.Sprintf("input-%03d", i)
		id := domain.ObjectID(name)
		data := []byte("bytes-" + name)
		h.blobs[id] = data
		meta := domain.ObjectMetadata{ID: id, WorkspaceID: h.job.Identity.WorkspaceID, Bytes: int64(len(data)), SHA256: provider.Digest(data)}
		snapshot.Inputs = append(snapshot.Inputs, provider.FrozenInput{Name: name, Target: name + ".bin", Object: meta})
		inputs = append(inputs, map[string]any{"name": name, "target": name + ".bin", "source": map[string]any{"kind": "object", "object_id": id}})
	}
	spec["inputs"] = inputs
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	h.job.Inputs = &snapshot
	h.job.Specification = raw
	h.job.SpecificationSHA256 = provider.Digest(raw)
	h.job.Identity.InputManifestSHA256 = snapshot.InputDigest()
	repo.pins = map[hostcache.Attempt][]hostcache.Key{}
	repo.pin(h.job.Identity, h.adapter.config.MachineID, h.adapter.config.EngineID, snapshot)
	objects := cacheObjects(provider.Plan{Job: h.job})
	for _, key := range repo.pins[cacheAttempt(h.job.Identity)] {
		g, _, err := repo.Claim(t.Context(), cacheAttempt(h.job.Identity), key)
		if err != nil {
			t.Fatal(err)
		}
		g.State = hostcache.Verified
		g.WriterPhase = hostcache.WriterFinished
		g.VerifiedAt = g.CreatedAt
		repo.active[key] = g
		object := objects[struct {
			sha   domain.SHA256Digest
			bytes int64
		}{key.SHA256, key.Bytes}]
		h.host.volumes[g.VolumeName] = &fakeVolume{labels: g.Labels(), blob: slices.Clone(h.blobs[object.ID]), complete: true}
	}
	return h, repo
}

func TestCacheReadinessBatchesHundredsOfExactGenerations(t *testing.T) {
	for _, contents := range []int{64, 65, 200, 257} {
		t.Run(fmt.Sprint(contents), func(t *testing.T) {
			h, _ := warmCacheHarness(t, contents)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			plan, err := h.adapter.Validate(ctx, h.job)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := h.adapter.Prepare(ctx, plan, testOperation)
			if err != nil {
				t.Fatal(err)
			}
			batches := (contents + cacheVerifyBatch - 1) / cacheVerifyBatch
			if h.host.cacheBatchVerifications != 2*batches || h.host.cacheInventoryCalls != 2 || h.host.writes != 0 || h.host.created != 1 {
				t.Fatalf("warm preparation did not batch both proofs: helpers=%d inventories=%d writes=%d creates=%d", h.host.cacheBatchVerifications, h.host.cacheInventoryCalls, h.host.writes, h.host.created)
			}
			before := h.host.cacheBatchVerifications
			observed, err := h.adapter.ReconcilePrepare(ctx, plan, testOperation)
			if err != nil || observed.Prepared == nil || h.host.cacheBatchVerifications-before != batches {
				t.Fatal("read-only readiness did not batch", err)
			}
			before = h.host.cacheBatchVerifications
			out := h.adapter.Start(ctx, prepared, nil, nil)
			if out.Status != provider.SubmissionAccepted || h.host.cacheBatchVerifications-before != batches || h.host.writes != 0 || h.host.created != 1 || h.host.starts != 1 {
				t.Fatalf("start changed pin/mutation behavior: %+v", out)
			}
		})
	}
}

func TestCacheBatchIncompleteEvidenceNeverRetiresOrRecreates(t *testing.T) {
	for _, fault := range []string{"truncated inventory", "warning", "duplicate volume", "foreign scope", "short helper", "duplicate index", "helper failed", "response limit"} {
		t.Run(fault, func(t *testing.T) {
			h, repo := warmCacheHarness(t, 3)
			first := h.host.volumes[slices.Sorted(maps.Keys(h.host.volumes))[0]]
			switch fault {
			case "truncated inventory":
				h.host.cacheInventoryReply = storageHTTPReply(`{"Volumes":[`)
			case "warning":
				h.host.cacheInventoryReply = storageHTTPReply(`{"Volumes":[],"Warnings":["private diagnostic"]}`)
			case "duplicate volume":
				name := slices.Sorted(maps.Keys(h.host.volumes))[0]
				volume := volumeInfo{Name: name, Driver: "local", Scope: "local", Labels: first.labels}
				raw, _ := json.Marshal(map[string]any{"Volumes": []volumeInfo{volume, volume}})
				h.host.cacheInventoryReply = storageHTTPReply(string(raw))
			case "foreign scope":
				h.host.cacheInventoryReply = storageHTTPReply(`{"Volumes":[{"Name":"relay-cache-foreign","Labels":{"io.compute-relay.installation":"foreign"}}]}`)
			case "short helper":
				h.host.cacheBatchOutput = []byte(`{"files":[]}`)
			case "duplicate index":
				h.host.cacheBatchOutput = []byte(`{"files":[{"index":0,"status":"corrupt","bytes":0,"sha256":""},{"index":0,"status":"corrupt","bytes":0,"sha256":""},{"index":2,"status":"corrupt","bytes":0,"sha256":""}]}`)
			case "helper failed":
				h.host.fail = func(args []string) (Result, bool) {
					if len(args) > 2 && args[len(args)-2] == "volumes" {
						return Result{ExitCode: 1}, true
					}
					return Result{}, false
				}
			case "response limit":
				h.host.cacheInventoryReply = storageHTTPReply(strings.Repeat("x", hoststorage.ScanMaxBytes+1))
			}
			plan, err := h.adapter.Validate(t.Context(), h.job)
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.adapter.Prepare(t.Context(), plan, testOperation)
			if err == nil || provider.Reason(err, "") == "" || len(repo.active) != 3 || h.host.writes != 0 || h.host.created != 0 || h.host.starts != 0 {
				t.Fatalf("incomplete evidence changed cache: err=%v generations=%d", err, len(repo.active))
			}
		})
	}
}

func TestCacheBatchPinChangeAndContentMismatchRefuseStart(t *testing.T) {
	for _, fault := range []string{"pin", "content", "missing label"} {
		t.Run(fault, func(t *testing.T) {
			h, repo := warmCacheHarness(t, 65)
			_, prepared := h.prepare(t)
			for key, g := range repo.active {
				switch fault {
				case "pin":
					g.ID = "hcgen_" + strings.Repeat("f", 32)
					repo.active[key] = g
				case "content":
					h.host.volumes[g.VolumeName].blob = []byte("changed")
				case "missing label":
					delete(h.host.volumes[g.VolumeName].labels, labelPrefix+"machine")
				}
				break
			}
			out := h.adapter.Start(t.Context(), prepared, nil, nil)
			if out.Status != provider.SubmissionRejected || h.host.created != 1 || h.host.starts != 0 || h.host.writes != 0 || len(repo.active) != 65 {
				t.Fatal("changed generation reached start or replacement", out)
			}
		})
	}
}

func TestCacheBatchLimitAndCancellationRemainFinite(t *testing.T) {
	h, repo := warmCacheHarness(t, 2)
	generations := slices.Collect(maps.Values(repo.active))
	if _, err := h.adapter.observeVolumes(t.Context(), docker{exec: h.host, binary: "/usr/local/bin/docker"}, make([]hostcache.Generation, 500)); !errors.Is(err, ErrRequest) || len(h.host.calls) != 0 {
		t.Fatal("oversized batch reached host", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := h.adapter.observeVolumes(ctx, docker{exec: h.host, binary: "/usr/local/bin/docker"}, generations); !errors.Is(err, context.Canceled) || h.host.cacheBatchVerifications != 0 {
		t.Fatal("cancelled cache scan ran helpers", err)
	}
}

func TestFixedBatchVerifierHashesReadOnlyFilesAndRejectsUnknownIO(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	root := t.TempDir()
	for i := 0; i < 2; i++ {
		path := filepath.Join(root, fmt.Sprint(i))
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "blob"), []byte("code"), 0444); err != nil {
			t.Fatal(err)
		}
	}
	bootstrap := `import base64,json,sys; ns={'__name__':'batch_test'}; exec(base64.b64decode(sys.argv[1]),ns); print(json.dumps(ns['volume_batch'](sys.argv[2],[4,4])))`
	run := func() ([]byte, error) {
		return exec.Command(python, "-B", "-E", "-s", "-c", bootstrap, base64.StdEncoding.EncodeToString([]byte(hostVerifierSource)), root).CombinedOutput()
	}
	raw, err := run()
	var got cacheBatchInventory
	if err != nil || json.Unmarshal(raw, &got) != nil || len(got.Files) != 2 || got.Files[1].Index != 1 || got.Files[1].SHA256 != string(provider.Digest([]byte("code"))) {
		t.Fatal("batch verifier lost exact content proof", err)
	}
	if err := os.WriteFile(filepath.Join(root, "0", "extra"), []byte("x"), 0400); err != nil {
		t.Fatal(err)
	}
	raw, err = run()
	if err != nil || json.Unmarshal(raw, &got) != nil || got.Files[0].Status != "corrupt" || got.Files[1].Status != "complete" {
		t.Fatal("measured corruption lost per-volume evidence", err)
	}
	if err := os.Remove(filepath.Join(root, "1", "blob")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/unavailable", filepath.Join(root, "1", "blob")); err != nil {
		t.Fatal(err)
	}
	if _, err = run(); err == nil {
		t.Fatal("unknown volume IO became a terminal observation")
	}
}

func TestCacheBatchInventoryAndVerifierOverPinnedSSH(t *testing.T) {
	h, repo := warmCacheHarness(t, 65)
	server := fakeHostOverSSH(t, h.host, "synthetic-password", nil, nil)
	client, err := Dial(t.Context(), server.target(), Credential{Password: []byte("synthetic-password")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	d := docker{exec: client, binary: "/usr/local/bin/docker", host: h.host.dockerHost}
	observations, err := h.adapter.observeVolumes(t.Context(), d, slices.Collect(maps.Values(repo.active)))
	if err != nil || len(observations) != 65 || h.host.cacheInventoryCalls != 1 || h.host.cacheBatchVerifications != 2 {
		t.Fatal("pinned SSH batch failed", err)
	}
	for _, o := range observations {
		if o.Kind != hostcache.ObservedComplete {
			t.Fatal("SSH lost exact volume proof", o.Kind)
		}
	}
}

func TestApplicationImagePeerPreparingWaitUsesCallerDeadline(t *testing.T) {
	h, repo, key := applicationHarness(t)
	meta := h.job.Inputs.ApplicationImage
	owner := hostimages.Owner{WorkspaceID: h.job.Identity.WorkspaceID, JobID: h.job.Identity.JobID, AttemptID: h.job.Identity.AttemptID, ObjectID: meta.ID}
	if _, elected, err := repo.ClaimImage(t.Context(), owner, key); err != nil || !elected {
		t.Fatal("peer claim failed", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	plan, err := h.adapter.Validate(ctx, h.job)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.adapter.Prepare(ctx, plan, testOperation)
	if !errors.Is(err, context.DeadlineExceeded) || provider.Reason(err, "") == "" || h.host.applicationLoads != 0 || repo.permits != 0 || h.host.created != 0 {
		t.Fatal("peer wait exceeded caller or granted another load", err)
	}
}
