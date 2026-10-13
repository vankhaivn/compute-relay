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

func TestKaggleCleanupWarnsAndRequiresAcceptanceForBothResources(t *testing.T) {
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
			if err != nil || !description.Valid() || description.Support != domain.CapabilitySupportSupported || !description.NameBasedDelete || description.Warning != nameBasedDeleteWarning || description.Bytes != nil {
				t.Fatal("slug-only deletion was presented as safe", description, err)
			}
			// The adapter has no clients, loader or credential resolver: reaching any
			// authenticated or mutating path would panic instead of passing this check.
			for _, mode := range []provider.CleanupMode{provider.CleanupApply} {
				request.Mode = mode
				if out, err := adapter.CleanupResource(context.Background(), request); !cleanupProblemCode(err, domain.CodeUnsupportedCapability) || out.Deleted || out.AlreadyAbsent || out.WouldDelete {
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

func cleanupProblemCode(err error, code domain.ErrorCode) bool {
	var p domain.Problem
	return errors.As(err, &p) && p.Code == code
}

func TestKaggleCleanupOriginalPlanAndReadOnlyRecovery(t *testing.T) {
	for _, purpose := range []string{"staging", "execution"} {
		t.Run(purpose, func(t *testing.T) {
			e := newExecutionFixture(t)
			p := &RuntimeAdapter{config: e.stager.config, binding: provider.BindingSnapshot{Binding: e.plan.Job.Binding, AccountScope: e.stager.config.AccountName, CredentialRef: string(e.stager.config.CredentialRef)}, stager: e.stager}
			p.load = func(context.Context, provider.Identity) (provider.Plan, provider.Prepared, error) {
				return e.plan, e.prepared, nil
			}
			request := provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{Remote: provider.RemoteReference{Identity: e.plan.Job.Identity, Resource: e.prepared.Resource, Version: "prepared"}, LedgerID: "res_fixture", CreationOperationID: e.prepared.PreparationID, Mode: provider.CleanupApply}, Purpose: purpose, OwnershipOperationID: e.prepared.PreparationID, PlanSHA256: e.plan.Digest(), AcceptNameBasedDelete: true}
			if purpose == "execution" {
				raw, _ := json.Marshal(kernelReference{KernelID: "42", Reference: e.request.Owner + "/" + e.request.Slug, SourceSHA256: e.request.SourceSHA256})
				request.Remote.Resource, request.Remote.Version = string(raw), "1"
				request.OwnershipOperationID = domain.OperationID(e.plan.Job.Identity.IntentID)
			}
			calls := 0
			expectedMode := "apply"
			reply := "deleted"
			p.runCleanup = func(_ context.Context, _ Config, mode string, secret []byte, q cleanupRequest) (cleanupResponse, error) {
				calls++
				if mode != expectedMode || string(secret) != "SYNTHETIC_TOKEN" || !q.AcceptNameBasedDelete || q.ID == "" || !q.SHA256.Valid() {
					t.Fatal("invalid cleanup invocation", mode, q)
				}
				return cleanupResponse{1, reply}, nil
			}
			if out, err := p.CleanupResource(context.Background(), request); err != nil || !out.Deleted {
				t.Fatal(out, err)
			}
			request.RecoverOnly = true
			expectedMode = "recover"
			reply = "unknown"
			if out, err := p.CleanupResource(context.Background(), request); !cleanupProblemCode(err, domain.CodeProviderStateUnknown) || out.Deleted {
				t.Fatal(out, err)
			}
			reply = "deleted" // Recovery cannot turn a malformed helper acknowledgement into success.
			if out, err := p.CleanupResource(context.Background(), request); err == nil || out.Deleted {
				t.Fatal(out, err)
			}
			reply = "absent"
			if out, err := p.CleanupResource(context.Background(), request); err != nil || !out.AlreadyAbsent {
				t.Fatal(out, err)
			}
			request.Mode = provider.CleanupDryRun
			expectedMode = "preview"
			reply = "would_delete"
			if out, err := p.CleanupResource(context.Background(), request); err != nil || !out.WouldDelete {
				t.Fatal(out, err)
			}
			before := calls
			request.PlanSHA256 = provider.Digest([]byte("changed"))
			if _, err := p.CleanupResource(context.Background(), request); !cleanupProblemCode(err, domain.CodeRemoteIdentityMismatch) || calls != before {
				t.Fatal("changed plan reached SDK", err)
			}
		})
	}
}
