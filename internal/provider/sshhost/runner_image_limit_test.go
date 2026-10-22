package sshhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

func runnerInventory(count int) string {
	var out strings.Builder
	for i := 0; i < count; i++ {
		fmt.Fprintf(&out, "sha256:%064x\n", i+1)
	}
	return out.String()
}

func TestRunnerImageCapacityRequiresCompleteBoundedInventory(t *testing.T) {
	transport := failure(errors.New("synthetic unavailable channel"), "ssh_channel_open_failed")
	for _, test := range []struct {
		name, output, reason string
		result               Result
		cause                error
	}{
		{name: "empty"},
		{name: "below", output: runnerInventory(RunnerImageCountLimit - 1)},
		{name: "tags_count_once", output: strings.Repeat(runnerInventory(1), 100)},
		{name: "at_limit", output: runnerInventory(RunnerImageCountLimit), reason: "runner_image_count_limit"},
		{name: "above_limit", output: runnerInventory(RunnerImageCountLimit + 1), reason: "runner_image_count_limit"},
		{name: "malformed", output: "foreign-id\n", reason: "runner_image_inventory_unknown"},
		{name: "whitespace", output: " \n", reason: "runner_image_inventory_unknown"},
		{name: "blank_row", output: runnerInventory(1) + "\n", reason: "runner_image_inventory_unknown"},
		{name: "partial", output: runnerInventory(1) + "sha256:abc", reason: "runner_image_inventory_unknown"},
		{name: "oversized", output: strings.Repeat(runnerInventory(1), 1000), reason: "docker_image_list_failed"},
		{name: "nonzero", output: runnerInventory(1), result: Result{ExitCode: 1}, reason: "runner_image_inventory_unknown"},
		{name: "transport", output: runnerInventory(1), cause: transport, reason: "ssh_channel_open_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := &scriptedExec{answer: func(argv []string, _ io.Reader, out io.Writer) (Result, error) {
				if !slices.Equal(argv, []string{"/usr/bin/docker", "image", "ls", "--all", "--no-trunc", "--filter", "label=io.compute-relay.image-schema=1", "--format", "{{.ID}}"}) {
					t.Fatalf("unbounded inventory command: %q", argv)
				}
				_, err := io.WriteString(out, test.output)
				return test.result, errors.Join(test.cause, err)
			}}
			err := (docker{exec: executor, binary: "/usr/bin/docker"}).runnerImageCapacity(t.Context())
			if test.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !errors.Is(err, ErrImageInstallationUnknown) || provider.Reason(err, "") != test.reason {
					t.Fatalf("wrong refusal: %v (%s)", err, provider.Reason(err, ""))
				}
				if test.cause != nil && !errors.Is(err, test.cause) {
					t.Fatal("transport cause lost")
				}
			}
			if len(executor.calls) != 1 {
				t.Fatal("inventory retried or mutated images")
			}
		})
	}
}

func TestRunnerImageLimitPrecedesBuildAndPreservesExactRecovery(t *testing.T) {
	for _, count := range []int{RunnerImageCountLimit - 1, RunnerImageCountLimit} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			fake, raw, before := verifiedImageHost(t)
			// Matching legacy/foreign images consume capacity without granting ownership.
			fake.runnerImageInventoryReply = runnerInventory(count)
			_, err := InstallImage(t.Context(), raw, before.Account, before.MachineID, before.Architecture, "op_capacity", false)
			if count == RunnerImageCountLimit {
				if !errors.Is(err, ErrImageInstallationUnknown) || fake.builds != 0 {
					t.Fatal("full inventory allowed a build", err)
				}
				assertFailureReason(t, err, "runner_image_count_limit")
			} else if err != nil || fake.builds != 1 {
				t.Fatal("available capacity did not allow one build", err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			seenInventory := false
			for _, call := range fake.calls {
				joined := strings.Join(call, " ")
				if strings.Contains(joined, "image ls --all") {
					seenInventory = true
				}
				if slices.Contains(call, "build") && !seenInventory {
					t.Fatal("build preceded inventory")
				}
				for _, forbidden := range []string{"rm", "prune", "rmi", "tag"} {
					if slices.Contains(call, forbidden) {
						t.Fatalf("capacity policy mutated an existing image: %q", call)
					}
				}
			}
		})
	}

	fake, raw, before := verifiedImageHost(t)
	installed, err := InstallImage(t.Context(), raw, before.Account, before.MachineID, before.Architecture, "op_exact_recovery", false)
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.runnerImageInventoryReply = "untrusted inventory"
	fake.calls = nil
	fake.mu.Unlock()
	for _, recoverOnly := range []bool{false, true} {
		got, err := InstallImage(t.Context(), raw, before.Account, before.MachineID, before.Architecture, "op_exact_recovery", recoverOnly)
		if err != nil || got != installed || fake.builds != 1 {
			t.Fatal("exact recovery consulted capacity or rebuilt", err)
		}
	}
	if _, err := InstallImage(context.Background(), raw, before.Account, before.MachineID, before.Architecture, "op_absent_recovery", true); !errors.Is(err, ErrImageInstallationUnknown) {
		t.Fatal("absent recovery became fresh installation", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, call := range fake.calls {
		if strings.Contains(strings.Join(call, " "), "image ls --all") || slices.Contains(call, "build") {
			t.Fatalf("recovery issued a capacity read or build: %q", call)
		}
	}
}
