package kaggle

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestKaggleCleanupRetainsBothResourcesWithoutConditionalDelete(t *testing.T) {
	id := provider.Identity{InstallationID: "rt_fixture", WorkspaceID: "app", JobID: "job_fixture", AttemptID: "att_fixture", InstanceID: "instance", IntentID: "si_fixture", ResourceKey: "fixture", Nonce: strings.Repeat("n", 32), BundleSHA256: domain.SHA256Digest(strings.Repeat("a", 64)), InputManifestSHA256: domain.SHA256Digest(strings.Repeat("b", 64))}
	adapter := &RuntimeAdapter{config: Config{InstanceID: "instance", AccountName: "fixtureowner"}}
	for _, purpose := range []string{"staging", "execution"} {
		t.Run(purpose, func(t *testing.T) {
			request := provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{Remote: provider.RemoteReference{Identity: id}, LedgerID: "res_fixture", CreationOperationID: "prep_fixture", Mode: provider.CleanupApply}, Purpose: purpose, PlanSHA256: domain.SHA256Digest(strings.Repeat("c", 64))}
			var raw []byte
			if purpose == "staging" {
				request.OwnershipOperationID, request.Remote.Version = "prep_fixture", "prepared"
				seed, _ := json.Marshal([]string{string(id.InstallationID), string(id.WorkspaceID), string(id.InstanceID), string(id.JobID), string(id.AttemptID), "prep_fixture"})
				raw, _ = json.Marshal(stagingReference{DatasetID: "12345", Reference: "fixtureowner/crs-" + string(provider.Digest(seed))[:40], Version: 1, Marker: request.PlanSHA256})
			} else {
				request.OwnershipOperationID, request.Remote.Version = "si_fixture", "1"
				seed, _ := json.Marshal([]string{string(id.InstallationID), string(id.WorkspaceID), string(id.InstanceID), string(id.JobID), string(id.AttemptID), string(id.IntentID), id.ResourceKey, id.Nonce})
				raw, _ = json.Marshal(kernelReference{KernelID: "67890", Reference: "fixtureowner/cre-" + string(provider.Digest(seed))[:40], SourceSHA256: request.PlanSHA256})
			}
			request.Remote.Resource = string(raw)
			description, err := adapter.DescribeCleanup(request)
			if err != nil || !description.Valid() || description.Support != domain.CapabilitySupportUnsupported || description.Reason != "conditional_delete_unavailable" || description.Bytes != nil {
				t.Fatal("slug-only deletion was presented as safe", description, err)
			}
			// The adapter has no clients, loader or credential resolver: reaching any
			// authenticated or mutating path would panic instead of passing this check.
			for _, mode := range []provider.CleanupMode{provider.CleanupDryRun, provider.CleanupApply} {
				request.Mode = mode
				if out, err := adapter.CleanupResource(context.Background(), request); !errors.Is(err, provider.ErrCleanupUnsupported) || out.Deleted || out.AlreadyAbsent || out.WouldDelete {
					t.Fatal("unsupported target acquired deletion evidence", out, err)
				}
			}
			for _, changed := range []string{"owner", "version", "nonce", "creation"} {
				foreign := request
				switch changed {
				case "owner":
					foreign.Remote.Resource = strings.Replace(foreign.Remote.Resource, "fixtureowner/", "foreign/", 1)
				case "version":
					foreign.Remote.Version = "2"
				case "nonce":
					foreign.Remote.Identity.AttemptID = "att_foreign"
				case "creation":
					foreign.OwnershipOperationID = "op_foreign"
				}
				if _, err := adapter.DescribeCleanup(foreign); err == nil {
					t.Fatal("changed original resource was accepted", changed)
				}
			}
		})
	}
}
