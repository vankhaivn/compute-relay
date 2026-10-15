package sshhost

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"slices"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	runnerassets "github.com/vankhaivn/compute-relay/runner"
)

// Only Docker's effective Mounts response changes. Requested mounts and the
// actual volumes, container, marker, policy and staged bytes stay unchanged.
type reorderedMountSession struct{ session }

func (s reorderedMountSession) Run(ctx context.Context, env map[string]string, argv []string, in io.Reader, out io.Writer) (Result, error) {
	i := slices.Index(argv, "container")
	if i < 0 || len(argv) <= i+1 || argv[i+1] != "inspect" {
		return s.session.Run(ctx, env, argv, in, out)
	}
	var captured bytes.Buffer
	result, err := s.session.Run(ctx, env, argv, in, &captured)
	if err != nil || result.ExitCode != 0 {
		_, _ = out.Write(captured.Bytes())
		return result, err
	}
	var containers []map[string]json.RawMessage
	if err := json.Unmarshal(captured.Bytes(), &containers); err != nil {
		return Result{}, err
	}
	for _, container := range containers {
		var mounts []dockerMount
		if err := json.Unmarshal(container["Mounts"], &mounts); err != nil {
			return Result{}, err
		}
		slices.Reverse(mounts)
		container["Mounts"], err = json.Marshal(mounts)
		if err != nil {
			return Result{}, err
		}
	}
	return result, json.NewEncoder(out).Encode(containers)
}

func reverseEffectiveMountOrder(h *harness) {
	original := h.adapter.dial
	h.adapter.dial = func(ctx context.Context, target Target, credential Credential) (session, error) {
		inner, err := original(ctx, target, credential)
		if err != nil {
			return nil, err
		}
		return reorderedMountSession{inner}, nil
	}
}

func mountOrderHarness(t *testing.T, controlled bool) (*harness, *deadlineClock) {
	t.Helper()
	var h *harness
	var clock *deadlineClock
	if controlled {
		h, clock = controlledHarness(t)
	} else {
		h = newHarness(t)
	}
	h.adapter.config.MachineID = resourceDigest("linux-machine-id", h.host.machineID)
	h.adapter.config.EngineID = resourceDigest("docker-engine-id", h.host.engineID)
	repo := newCacheFixtureRepo()
	repo.pin(h.job.Identity, h.adapter.config.MachineID, h.adapter.config.EngineID, *h.job.Inputs)
	h.adapter.WithHostCache(repo)
	return h, clock
}

func TestCachedReadinessIgnoresEffectiveMountOrder(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		name := "automatic"
		if controlled {
			name = "controlled"
		}
		t.Run(name, func(t *testing.T) {
			h, clock := mountOrderHarness(t, controlled)
			plan, prepared := h.prepare(t)
			if len(h.host.container(t).mounts) != 2 {
				t.Fatal("fixture requires bundle plus separate input")
			}
			reverseEffectiveMountOrder(h)
			seen, err := h.adapter.ReconcilePrepare(t.Context(), plan, testOperation)
			if err != nil || seen.Prepared == nil || *seen.Prepared != prepared {
				t.Fatal("effective mount ordering changed readiness", err)
			}
			var authorization *provider.StartAuthorization
			if controlled {
				start := startAuthority(prepared, clock)
				authorization = &start
			}
			if out := h.adapter.Start(t.Context(), prepared, authorization, nil); out.Status != provider.SubmissionAccepted || h.host.starts != 1 {
				t.Fatal("effective mount ordering blocked start", out.Status)
			}
		})
	}
}

func legacyMountProof(t *testing.T, h *harness, plan provider.Plan, prepared provider.Prepared) provider.Prepared {
	t.Helper()
	c := h.host.container(t)
	var raw bytes.Buffer
	h.host.inspect(c, &raw)
	var containers []containerInfo
	if err := json.Unmarshal(raw.Bytes(), &containers); err != nil || len(containers) != 1 {
		t.Fatal("container fixture unavailable", err)
	}
	modules, err := runnerassets.Sources()
	if err != nil {
		t.Fatal(err)
	}
	info := containers[0]
	files := expectedStaging(plan, modules, c.files["relay/manifest.json"].data, info.Config.Labels[labelPrefix+"cache"] == "volumes/v1")
	version := "ssh-readiness/v2"
	if info.Config.Labels[labelPrefix+"application-image"] != "" {
		version = "ssh-readiness/v3"
	}
	// This is the original v2/v3 byte shape, independent of the new hash helper.
	rawProof, err := json.Marshal(struct {
		Version    string
		Container  string
		Image      string
		Config     any
		HostConfig any
		Mounts     any
		Plan       domain.SHA256Digest
		Files      map[string]stagedFile
	}{version, info.ID, info.Image, info.Config, info.HostConfig, info.Mounts, plan.Digest(), files})
	if err != nil {
		t.Fatal(err)
	}
	prepared.VerificationSHA256 = provider.Digest(rawProof)
	var staged marker
	file := c.files["relay/staged.json"]
	if json.Unmarshal(file.data, &staged) != nil || staged.Verification == prepared.VerificationSHA256 {
		t.Fatal("fixture did not distinguish new and legacy proofs")
	}
	staged.Verification = prepared.VerificationSHA256
	file.data, err = json.Marshal(staged)
	if err != nil {
		t.Fatal(err)
	}
	c.files["relay/staged.json"] = file
	return prepared
}

func TestLegacyCachedReadinessKeepsExactOriginalProof(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		for _, reordered := range []bool{false, true} {
			name := "automatic/original"
			if controlled {
				name = "controlled/original"
			}
			if reordered {
				name += "_reordered"
			}
			t.Run(name, func(t *testing.T) {
				h, clock := mountOrderHarness(t, controlled)
				plan, prepared := h.prepare(t)
				prepared = legacyMountProof(t, h, plan, prepared)
				originalRevision := prepared.ReadinessRevision()
				originalMarker := slices.Clone(h.host.container(t).files["relay/staged.json"].data)
				if reordered {
					reverseEffectiveMountOrder(h)
				}
				seen, err := h.adapter.ReconcilePrepare(t.Context(), plan, testOperation)
				if reordered {
					if err == nil {
						t.Fatal("reordered legacy proof was guessed")
					}
				} else if err != nil || seen.Prepared == nil || *seen.Prepared != prepared || seen.Prepared.ReadinessRevision() != originalRevision {
					t.Fatal("original legacy readiness changed", err)
				}
				var authorization *provider.StartAuthorization
				if controlled {
					start := startAuthority(prepared, clock)
					authorization = &start
				}
				out := h.adapter.Start(t.Context(), prepared, authorization, nil)
				if reordered {
					if out.Status != provider.SubmissionRejected || h.host.starts != 0 {
						t.Fatal("unverified legacy proof started", out.Status)
					}
				} else if out.Status != provider.SubmissionAccepted || h.host.starts != 1 {
					t.Fatal("original legacy proof could not start", out.Status)
				}
				if !bytes.Equal(originalMarker, h.host.container(t).files["relay/staged.json"].data) {
					t.Fatal("verification rewrote the original proof")
				}
			})
		}
	}
}

func TestCachedReadinessRejectsChangedMountIdentity(t *testing.T) {
	for _, mutation := range []string{"source", "target", "writable"} {
		t.Run(mutation, func(t *testing.T) {
			h, clock := mountOrderHarness(t, true)
			plan, prepared := h.prepare(t)
			mount := &h.host.container(t).mounts[0]
			switch mutation {
			case "source":
				mount.Source += "-changed"
			case "target":
				mount.Target += "-changed"
			case "writable":
				mount.ReadOnly = false
			}
			reverseEffectiveMountOrder(h)
			if _, err := h.adapter.ReconcilePrepare(t.Context(), plan, testOperation); err == nil {
				t.Fatal("changed mount retained readiness")
			}
			start := startAuthority(prepared, clock)
			if out := h.adapter.Start(t.Context(), prepared, &start, nil); out.Status != provider.SubmissionRejected || h.host.starts != 0 {
				t.Fatal("changed mount started", out.Status)
			}
		})
	}
}

func TestLegacyApplicationReadinessKeepsExactOriginalProof(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, reordered := range []bool{false, true} {
			if !cached && reordered {
				continue
			}
			name := "direct/original"
			if cached {
				name = "cached/original"
			}
			if reordered {
				name += "_reordered"
			}
			t.Run(name, func(t *testing.T) {
				h, _, _ := applicationHarness(t)
				if cached {
					repo := newCacheFixtureRepo()
					repo.pin(h.job.Identity, h.adapter.config.MachineID, h.adapter.config.EngineID, *h.job.Inputs)
					h.adapter.WithHostCache(repo)
				}
				plan, prepared := h.prepare(t)
				prepared = legacyMountProof(t, h, plan, prepared)
				if reordered {
					reverseEffectiveMountOrder(h)
				}
				seen, err := h.adapter.ReconcilePrepare(t.Context(), plan, testOperation)
				if reordered {
					if err == nil {
						t.Fatal("reordered legacy application proof was guessed")
					}
				} else if err != nil || seen.Prepared == nil || *seen.Prepared != prepared {
					t.Fatal("original legacy application proof changed", err)
				}
				out := h.adapter.Start(t.Context(), prepared, nil, nil)
				if reordered {
					if out.Status != provider.SubmissionRejected || h.host.starts != 0 {
						t.Fatal("unverified legacy application proof started", out.Status)
					}
				} else if out.Status != provider.SubmissionAccepted || h.host.starts != 1 {
					t.Fatal("original legacy application proof could not start", out.Status)
				}
				if h.host.applicationLoads != 1 {
					t.Fatal("legacy proof repeated the application load")
				}
			})
		}
	}
}
