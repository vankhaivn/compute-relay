package sshhost

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestCleanupCannotDeleteReplacedContainerWithCopiedLabels(t *testing.T) {
	h := newHarness(t)
	remote := h.submit(t)
	h.host.finish(0, nil)
	replacement := h.host.container(t)
	replacement.id = strings.Repeat("f", 64)
	req := provider.CleanupRequest{Remote: remote, LedgerID: "ledger_fixture", CreationOperationID: testOperation, ResultsCollected: true, Mode: provider.CleanupApply}
	out, err := h.adapter.Cleanup(context.Background(), req)
	if err != nil && !errors.Is(err, ErrIdentity) || out.Deleted || h.host.commands("rm") != 0 || len(h.host.containers) != 1 {
		t.Fatalf("replacement must survive cleanup: outcome=%+v error=%v", out, err)
	}
}

func TestCleanupRejectsMalformedAndChangedOriginalReferences(t *testing.T) {
	for _, variant := range []string{"invalid", "version", "trailing", "empty"} {
		t.Run(variant, func(t *testing.T) {
			h := newHarness(t)
			remote := h.submit(t)
			h.host.finish(0, nil)
			switch variant {
			case "invalid":
				remote.Resource = `{"container":"../../home","id":"` + strings.Repeat("a", 64) + `"}`
			case "version":
				remote.Version = strings.Repeat("f", 64)
			case "trailing":
				remote.Resource += ` {"ignored":true}`
			case "empty":
				remote.Resource = ""
			}
			req := provider.CleanupRequest{Remote: remote, LedgerID: "ledger_fixture", CreationOperationID: testOperation, Mode: provider.CleanupApply}
			if out, err := h.adapter.Cleanup(context.Background(), req); err == nil || out.Deleted || h.host.commands("rm") != 0 {
				t.Fatalf("invalid reference reached deletion: %+v %v", out, err)
			}
		})
	}
}

func TestCleanupUsesOnlyOriginalIDWithoutForceOrVolumeRemoval(t *testing.T) {
	h := newHarness(t)
	_, prepared := h.prepare(t)
	ref, err := decodeRef(prepared.Resource, prepared.Identity)
	if err != nil {
		t.Fatal(err)
	}
	req := provider.CleanupRequest{Remote: provider.RemoteReference{Identity: prepared.Identity, Resource: prepared.Resource, Version: "prepared"}, LedgerID: "ledger_fixture", CreationOperationID: testOperation, Mode: provider.CleanupDryRun}
	if out, err := h.adapter.Cleanup(context.Background(), req); err != nil || !out.WouldDelete || h.host.commands("rm") != 0 {
		t.Fatalf("preview: %+v %v", out, err)
	}
	req.Mode = provider.CleanupApply
	if out, err := h.adapter.Cleanup(context.Background(), req); err != nil || !out.Deleted {
		t.Fatalf("apply: %+v %v", out, err)
	}
	if out, err := h.adapter.Cleanup(context.Background(), req); err != nil || !out.AlreadyAbsent {
		t.Fatalf("repeat: %+v %v", out, err)
	}
	for _, call := range h.host.calls {
		if len(call) > 2 && call[1] == "container" && call[2] == "rm" && !slices.Equal(call[1:], []string{"container", "rm", ref.ID}) {
			t.Fatalf("cleanup widened its target: %v", call)
		}
	}
}
