package sshhost

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

func TestResourceIdentityDerivationIsStable(t *testing.T) {
	// Persisted identities are a compatibility contract, including input tags.
	for _, tc := range []struct{ kind, native, digest string }{
		{"linux-machine-id", strings.Repeat("a", 32), "daefae7916e00bdb10f4ef2296059d257e298a24b6bf20b16ff07159b4bbf9b5"},
		{"darwin-platform-uuid", "a1234567-89ab-cdef-0123-456789abcdef", "8a0b5fa97e99402f7d2fc8bf2ede898555b1d3b91ee85fe42229a980be3d6d98"},
		{"docker-engine-id", "synthetic-engine-01", "32f98314e1cb06675fb34e7e86cb0ceee4a3471818a8fc21696da26b79b7e397"},
	} {
		if got := resourceDigest(tc.kind, tc.native); got != tc.digest {
			t.Fatalf("%s identity changed: %s", tc.kind, got)
		}
	}
}

func TestMachineIdentityNormalizesNativeMetadataAndRejectsUncertainty(t *testing.T) {
	for _, tc := range []struct {
		name, system, native, kind, normalized string
	}{
		{"linux", "Linux", strings.Repeat("a", 32) + "\n", "linux-machine-id", strings.Repeat("a", 32)},
		{"darwin", "Darwin", `    "IOPlatformUUID" = "A1234567-89AB-CDEF-0123-456789ABCDEF"` + "\n", "darwin-platform-uuid", "a1234567-89ab-cdef-0123-456789abcdef"},
		{"empty", "Linux", "", "", ""},
		{"zero", "Linux", strings.Repeat("0", 32), "", ""},
		{"uppercase", "Linux", strings.Repeat("A", 32), "", ""},
		{"oversized", "Linux", strings.Repeat("a", 4096), "", ""},
		{"unsupported", "FreeBSD", strings.Repeat("a", 32), "", ""},
		{"darwin missing", "Darwin", `"IOPlatformSerialNumber" = "synthetic"`, "", ""},
		{"darwin duplicate", "Darwin", strings.Repeat(`"IOPlatformUUID" = "a1234567-89ab-cdef-0123-456789abcdef"`+"\n", 2), "", ""},
		{"darwin zero", "Darwin", `"IOPlatformUUID" = "00000000-0000-0000-0000-000000000000"`, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &scriptedExec{answer: func(args []string, _ io.Reader, out io.Writer) (Result, error) {
				value := tc.native
				if slices.Equal(args, []string{"/usr/bin/uname", "-s"}) {
					value = tc.system + "\n"
				}
				_, err := io.WriteString(out, value)
				return Result{}, err
			}}
			id, err := machineIdentity(context.Background(), exec)
			if tc.kind == "" {
				if !errors.Is(err, ErrResourceIdentity) || id != "" {
					t.Fatal("uncertain identity accepted", id, err)
				}
			} else if err != nil || id != resourceDigest(tc.kind, tc.normalized) || strings.Contains(id, tc.normalized) {
				t.Fatal("native identity not privately namespaced", id, err)
			}
		})
	}
}

func TestDockerResourceRequiresAndPinsALocalSocket(t *testing.T) {
	for _, tc := range []struct {
		name, environment, inspected string
		valid                        bool
	}{
		{"default", "\n\n", "unix:///var/run/docker.sock", true},
		{"desktop", "desktop-linux\n\n", "unix:///Users/fixture/.docker/run/docker.sock", true},
		{"explicit socket", "\nunix:///run/user/1000/docker.sock\n", "", true},
		{"context wins", "desktop-linux\ntcp://remote.example:2375\n", "unix:///var/run/docker.sock", true},
		{"remote context", "remote\n\n", "ssh://remote.example", false},
		{"remote host", "\ntcp://remote.example:2375\n", "", false},
		{"relative socket", "\n\n", "unix://relative.sock", false},
		{"unclean socket", "\n\n", "unix:///run/../tmp/docker.sock", false},
		{"multiline environment", "\n\nunexpected\n", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &scriptedExec{answer: func(args []string, _ io.Reader, out io.Writer) (Result, error) {
				switch args[0] {
				case "/bin/sh":
					_, _ = io.WriteString(out, tc.environment)
				case "/usr/local/bin/docker":
					if len(args) < 5 || args[1] != "context" || args[2] != "inspect" {
						t.Fatal("unexpected unpinned Docker call", args)
					}
					_ = json.NewEncoder(out).Encode(tc.inspected)
				case "/usr/bin/env":
					if len(args) != 9 || !slices.Equal(args[:7], []string{"/usr/bin/env", "-u", "DOCKER_CONTEXT", "-u", "DOCKER_HOST", "/usr/local/bin/docker", "--host"}) || args[8] != "version" {
						t.Fatal("call lost frozen context", args)
					}
				default:
					t.Fatal("unexpected command", args)
				}
				return Result{}, nil
			}}
			d, err := (docker{exec: exec, binary: "/usr/local/bin/docker"}).localResource(context.Background())
			if !tc.valid {
				if !errors.Is(err, ErrResourceIdentity) {
					t.Fatal("nonlocal resource accepted", d, err)
				}
				return
			}
			if err != nil || !strings.HasPrefix(d.host, "unix:///") {
				t.Fatal("local socket unavailable", err)
			}
			before := len(exec.calls)
			if again, err := d.localResource(context.Background()); err != nil || again.host != d.host || len(exec.calls) != before {
				t.Fatal("same operation re-resolved its context", err)
			}
			if err := d.ping(context.Background()); err != nil || exec.calls[len(exec.calls)-1][7] != d.host {
				t.Fatal("Docker call was redirected", err)
			}
		})
	}
}

func TestManagedHostResourceIdentityIsSeparateFromTheSSHAccount(t *testing.T) {
	fake, raw := imageHost(t)
	first, err := VerifyManagedHost(context.Background(), raw)
	if err != nil || first.MachineID == "" || first.EngineID == "" {
		t.Fatal("resource identity unavailable", first, err)
	}
	// A second host can share an SSH key and endpoint after reassignment. Its
	// different native machine identity must not be inferred from that key.
	fake.mu.Lock()
	fake.machineID = strings.Repeat("b", 32)
	fake.mu.Unlock()
	other, err := VerifyManagedHost(context.Background(), raw)
	if err != nil || first.Account != other.Account || first.MachineID == other.MachineID {
		t.Fatal("SSH account substituted for scheduling resource", err)
	}
}

func TestResourceChangeStopsBeforeAnyAttemptMutation(t *testing.T) {
	for _, changed := range []string{"machine", "engine"} {
		t.Run(changed, func(t *testing.T) {
			h := newHarness(t)
			h.adapter.config.MachineID = resourceDigest("linux-machine-id", h.host.machineID)
			h.adapter.config.EngineID = resourceDigest("docker-engine-id", h.host.engineID)
			plan, err := h.adapter.Validate(context.Background(), h.job)
			if err != nil {
				t.Fatal(err)
			}
			if changed == "machine" {
				h.host.machineID = strings.Repeat("b", 32)
			} else {
				h.host.engineID = "synthetic-engine-replaced"
			}
			if _, err := h.adapter.Prepare(context.Background(), plan, testOperation); !errors.Is(err, ErrResourceIdentity) || h.host.created != 0 || h.host.starts != 0 || h.host.builds != 0 {
				t.Fatal("changed resource received a mutation", err)
			}
		})
	}
}
