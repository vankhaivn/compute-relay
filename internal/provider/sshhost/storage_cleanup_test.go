package sshhost

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

func TestStorageBindingMismatchStopsBeforeCredentialOrRemoteIO(t *testing.T) {
	for _, changed := range []string{"machine", "engine"} {
		t.Run(changed, func(t *testing.T) {
			h := newHarness(t)
			scope := hoststorage.Scope{InstallationID: "installation", MachineID: domain.SHA256Digest(strings.Repeat("a", 64)), EngineID: domain.SHA256Digest(strings.Repeat("b", 64))}
			h.adapter.config.MachineID, h.adapter.config.EngineID = string(scope.MachineID), string(scope.EngineID)
			if changed == "machine" {
				scope.MachineID = domain.SHA256Digest(strings.Repeat("c", 64))
			} else {
				scope.EngineID = domain.SHA256Digest(strings.Repeat("c", 64))
			}
			target := hoststorage.Target{Scope: scope, Class: hoststorage.Volume, ID: "relay-cache-example", Action: hoststorage.OrphanVolume,
				Labels: map[string]string{storageLabel: "installation"}}
			if _, err := h.adapter.ScanStorage(t.Context(), scope); !errors.Is(err, hoststorage.ErrUnverified) {
				t.Fatalf("wrong scope scanned: %v", err)
			}
			if _, err := h.adapter.ObserveStorageTarget(t.Context(), target); !errors.Is(err, hoststorage.ErrUnverified) {
				t.Fatalf("wrong scope inspected: %v", err)
			}
			if _, err := h.adapter.CleanupStorageTarget(t.Context(), target, false); !errors.Is(err, hoststorage.ErrUnverified) {
				t.Fatalf("wrong scope cleaned: %v", err)
			}
			if h.vault.calls != 0 || h.dials != 0 || len(h.host.calls) != 0 {
				t.Fatalf("binding mismatch performed I/O: credentials=%d dials=%d commands=%v", h.vault.calls, h.dials, h.host.calls)
			}
		})
	}
}

// The exact installation listing excludes the target after its labels become
// foreign or absent. Even a retained exact Docker ID cannot authorize inspection.
type storageExcludedSession struct {
	host     *fakeHost
	listings []string
	unsafe   []string
}

func (s *storageExcludedSession) Close() error { return nil }
func (s *storageExcludedSession) Run(ctx context.Context, env map[string]string, argv []string, stdin io.Reader, stdout io.Writer) (Result, error) {
	args := argv[1:]
	if len(argv) >= 9 && argv[0] == "/usr/bin/env" {
		args = argv[8:]
	}
	command := strings.Join(args, " ")
	if len(args) >= 2 && (args[0] == "container" || args[0] == "volume" || args[0] == "image") {
		switch args[1] {
		case "ls":
			s.listings = append(s.listings, command)
			return Result{}, nil
		case "inspect", "rm":
			s.unsafe = append(s.unsafe, command)
			return Result{ExitCode: 1}, nil
		}
	}
	return s.host.Run(ctx, env, argv, stdin, stdout)
}

func TestStorageCleanupNeverInspectsOrDeletesExcludedObjects(t *testing.T) {
	for _, kind := range []hoststorage.Kind{hoststorage.Container, hoststorage.Volume, hoststorage.Image} {
		for _, labels := range []string{"foreign_installation", "unlabelled"} {
			t.Run(string(kind)+"/"+labels, func(t *testing.T) {
				h := newHarness(t)
				scope := hoststorage.Scope{InstallationID: "installation", MachineID: domain.SHA256Digest(resourceDigest("linux-machine-id", h.host.machineID)),
					EngineID: domain.SHA256Digest(resourceDigest("docker-engine-id", h.host.engineID))}
				h.adapter.config.MachineID, h.adapter.config.EngineID = string(scope.MachineID), string(scope.EngineID)
				excluded := &storageExcludedSession{host: h.host}
				h.adapter.dial = func(context.Context, Target, Credential) (session, error) { return excluded, nil }
				target := hoststorage.Target{Scope: scope, Class: kind, ID: strings.Repeat("a", 64), Action: hoststorage.OrphanContainer,
					Labels: map[string]string{storageLabel: "installation"}}
				if kind == hoststorage.Volume {
					target.ID, target.Action = "relay-cache-example", hoststorage.OrphanVolume
				} else if kind == hoststorage.Image {
					target.ID, target.Action = "sha256:"+strings.Repeat("a", 64), hoststorage.OrphanImage
				}
				// A caller cannot submit foreign or unlabelled target evidence.
				invalid := target
				invalid.Labels = map[string]string{}
				if labels == "foreign_installation" {
					invalid.Labels[storageLabel] = "another_installation"
				}
				if _, err := h.adapter.CleanupStorageTarget(t.Context(), invalid, false); !errors.Is(err, hoststorage.ErrUnverified) || h.vault.calls != 0 {
					t.Fatalf("unowned evidence reached I/O: %v, credentials=%d", err, h.vault.calls)
				}
				// Previously valid evidence is rechecked by the exact current label
				// filter; an excluded remote object is never inspected or removed.
				outcome, err := h.adapter.CleanupStorageTarget(t.Context(), target, false)
				if err != nil || outcome != hoststorage.AbsentConfirmed || len(excluded.listings) != 1 || len(excluded.unsafe) != 0 {
					t.Fatalf("excluded object touched: outcome=%s err=%v lists=%v unsafe=%v", outcome, err, excluded.listings, excluded.unsafe)
				}
				if !strings.Contains(excluded.listings[0], "--filter label="+storageLabel+"=installation") {
					t.Fatalf("cleanup omitted exact ownership filter: %v", excluded.listings)
				}
			})
		}
	}
}
