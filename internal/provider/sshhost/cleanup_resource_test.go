package sshhost

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func resourceCleanupRequest(t *testing.T, h *harness) provider.CleanupResourceRequest {
	t.Helper()
	plan, prepared := h.prepare(t)
	return provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{
		Remote:   provider.RemoteReference{Identity: prepared.Identity, Resource: prepared.Resource, Version: "prepared"},
		LedgerID: "res_fixture", CreationOperationID: prepared.PreparationID, Mode: provider.CleanupApply},
		Purpose: "staging", OwnershipOperationID: prepared.PreparationID, PlanSHA256: plan.Digest()}
}

func TestResourceCleanupBindsEngineAndFullOwnership(t *testing.T) {
	for _, changed := range []string{"engine", "missing_on_other_engine", "workspace", "job", "nonce", "plan", "legacy", "creation_operation"} {
		t.Run(changed, func(t *testing.T) {
			h := newHarness(t)
			request := resourceCleanupRequest(t, h)
			container := h.host.container(t)
			switch changed {
			case "engine", "missing_on_other_engine":
				h.host.engineID = "replacement-engine-000000001"
				if changed == "missing_on_other_engine" {
					delete(h.host.containers, container.name)
				}
			case "legacy":
				request.Remote.Resource = encodeRef(container.name, container.id)
			case "creation_operation":
				request.OwnershipOperationID = "prep_foreign"
			default:
				container.labels[labelPrefix+changed] = "foreign"
			}
			out, err := h.adapter.CleanupResource(context.Background(), request)
			if err == nil || out.Deleted || out.AlreadyAbsent || h.host.commands("rm") != 0 {
				t.Fatalf("changed identity reached cleanup: %+v %v", out, err)
			}
			if changed == "legacy" && !errors.Is(err, provider.ErrCleanupUnsupported) {
				t.Fatal("missing historic evidence must be explicit", err)
			}
		})
	}
}

func TestResourceCleanupPreservesUnrelatedDataAndNeverFollowsOutputPaths(t *testing.T) {
	h := newHarness(t)
	request := resourceCleanupRequest(t, h)
	c := h.host.container(t)
	c.files["relay/work/outputs/../../../../etc/sentinel"] = fakeFile{link: true}
	c.files["relay/work/outputs/link-to-home"] = fakeFile{link: true}
	foreign := &fakeContainer{id: strings.Repeat("d", 64), name: "unrelated", status: "created", labels: map[string]string{}, files: map[string]fakeFile{"home/sentinel": {data: []byte("keep")}}}
	h.host.containers[foreign.name] = foreign
	images := len(h.host.images)
	h.host.calls = nil
	out, err := h.adapter.CleanupResource(context.Background(), request)
	if err != nil || !out.Deleted || h.host.containers[foreign.name] != foreign || string(foreign.files["home/sentinel"].data) != "keep" || len(h.host.images) != images {
		t.Fatalf("cleanup touched unrelated data: %+v %v", out, err)
	}
	for _, argv := range h.host.calls {
		if argv[0] == "test" {
			continue
		}
		args := argv[1:]
		allowed := slices.Equal(args, []string{"info", "--format", "{{.ID}}"}) ||
			slices.Equal(args, []string{"container", "inspect", c.id}) || slices.Equal(args, []string{"container", "rm", c.id})
		if !allowed {
			t.Fatalf("cleanup issued a filesystem, volume, image or broad command: %v", args)
		}
	}
}

func TestResourceCleanupRunningRaceAndLostAcknowledgement(t *testing.T) {
	for _, fault := range []string{"started", "lost_response"} {
		t.Run(fault, func(t *testing.T) {
			h := newHarness(t)
			request := resourceCleanupRequest(t, h)
			c := h.host.container(t)
			h.host.fail = func(args []string) (Result, bool) {
				if !reflect.DeepEqual(args, []string{"container", "rm", c.id}) {
					return Result{}, false
				}
				if fault == "started" {
					c.status = "running"
					return Result{}, false
				}
				delete(h.host.containers, c.name)
				return Result{ExitCode: 1, Stderr: []byte("private transport failure")}, true
			}
			out, err := h.adapter.CleanupResource(context.Background(), request)
			if err == nil || out.Deleted || out.AlreadyAbsent {
				t.Fatalf("uncertain call invented success: %+v %v", out, err)
			}
			h.host.fail = nil
			if fault == "started" {
				if h.host.containers[c.name] != c || c.status != "running" {
					t.Fatal("cleanup forced removal of a running container")
				}
			} else if out, err = h.adapter.CleanupResource(context.Background(), request); err != nil || !out.AlreadyAbsent {
				t.Fatalf("original-target recovery did not verify absence: %+v %v", out, err)
			}
		})
	}
}

func TestResourceCleanupRefusesActiveContainerWithValidProblem(t *testing.T) {
	for _, state := range []string{"running", "paused", "restarting"} {
		t.Run(state, func(t *testing.T) {
			h := newHarness(t)
			request := resourceCleanupRequest(t, h)
			container := h.host.container(t)
			container.status = state
			h.host.calls = nil
			out, err := h.adapter.CleanupResource(context.Background(), request)
			var problem domain.Problem
			if !errors.As(err, &problem) || problem.Code != domain.CodeRemoteExecutionUnresolved || problem.Stage != domain.FailureStageOperation {
				t.Fatalf("active cleanup did not return the defined operation problem: %+v, %v", problem, err)
			}
			if out != (provider.CleanupOutcome{}) || h.host.commands("rm") != 0 || h.host.containers[container.name] != container || container.status != state {
				t.Fatal("active cleanup changed the container or claimed success", out)
			}
		})
	}
}

func TestResourceCleanupDescriptionReportsUnknownSize(t *testing.T) {
	h := newHarness(t)
	request := resourceCleanupRequest(t, h)
	description, err := h.adapter.DescribeCleanup(request)
	if err != nil || !description.Valid() || description.Bytes != nil || description.Support != domain.CapabilitySupportSupported || description.Scope != "container_writable_layer" {
		t.Fatal(description, err)
	}
}
