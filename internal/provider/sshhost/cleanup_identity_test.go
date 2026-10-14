package sshhost

import (
	"context"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestCleanupCannotDeleteReplacedContainerWithCopiedLabels(t *testing.T) {
	h := newHarness(t)
	remote := h.submit(t)
	h.host.finish(0, nil)
	replacement := h.host.container(t)
	replacement.id = strings.Repeat("f", 64)
	req := provider.CleanupRequest{Remote: remote, LedgerID: "ledger_fixture", CreationOperationID: testOperation, ResultsCollected: true, Mode: provider.CleanupApply}
	out, err := h.adapter.CleanupResource(context.Background(), resourceRequest(req, mustPlan(t, h).Digest()))
	if err != nil && !problemCode(err, domain.CodeRemoteIdentityMismatch) || out.Deleted || h.host.commands("rm") != 0 || len(h.host.containers) != 1 {
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
			if out, err := h.adapter.CleanupResource(context.Background(), resourceRequest(req, mustPlan(t, h).Digest())); err == nil || out.Deleted || h.host.commands("rm") != 0 {
				t.Fatalf("invalid reference reached deletion: %+v %v", out, err)
			}
		})
	}
}
