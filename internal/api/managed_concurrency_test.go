package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type concurrencyHTTPAdapter struct {
	managedAdapter
	scope           string
	resourceMissing bool
}

func (a *concurrencyHTTPAdapter) Descriptor() connections.Descriptor {
	d := a.managedAdapter.Descriptor()
	d.Capabilities.Concurrency = &connections.ConcurrencyCapability{Support: "supported", Scope: a.scope, Minimum: 1, Maximum: 64, Default: 1}
	if a.scope == "resource" {
		d.Capabilities.Quota = "unsupported"
	}
	return d
}

func (a *concurrencyHTTPAdapter) Verify(ctx context.Context, raw []byte) (connections.Verification, error) {
	v, err := a.managedAdapter.Verify(ctx, raw)
	if a.scope == "resource" {
		v.CanonicalAccount = "private-account-canary-" + string(raw)
		if !a.resourceMissing {
			v.Resource = &connections.ResourceIdentity{Kind: "machine", ID: "resource_" + strings.Repeat("a", 64)}
		}
	}
	return v, err
}

func TestManagedHTTPMissingRequiredResourceCannotPublishIndependentCapacity(t *testing.T) {
	a := &concurrencyHTTPAdapter{scope: "resource", resourceMissing: true}
	f := setupManagedHTTP(t, true, a)
	_, raw := f.call(t, "POST", "/v1/workspaces/a/connections", "manage", "missing-resource-create", managedBody, 202)
	op := readManagedOperation(t, raw)
	if handled, err := f.service.RunOnce(context.Background()); err != nil || !handled {
		t.Fatal("worker did not handle unverified resource", err)
	}
	_, raw = f.call(t, "GET", "/v1/workspaces/a/connection-operations/"+string(op.ID), "manage", "", "", 200)
	op = readManagedOperation(t, raw)
	if op.Status != "failed" || op.Problem == nil || *op.Problem != "provider_unavailable" {
		t.Fatal("missing resource verification succeeded", op)
	}
	_, raw = f.call(t, "GET", "/v1/workspaces/a/connections/"+op.ConnectionID, "read", "", "", 200)
	wireSchema(t, "connection", raw)
	var c connections.Connection
	if json.Unmarshal(raw, &c) != nil || c.Selection != nil || c.AccountID != nil || c.Concurrency == nil || c.Concurrency.EffectiveLimit != nil || c.Concurrency.ActiveAttempts != nil {
		t.Fatal("unknown resource created an independent pool", c)
	}
}

func (*concurrencyHTTPAdapter) ValidateConfiguration(c connections.Configuration) error {
	if c.MaxProcesses != 0 {
		return connections.ErrUnsupported
	}
	return c.Validate()
}

func TestManagedHTTPConcurrencyUsesSharedSchedulerWithoutRemoteReads(t *testing.T) {
	for _, scope := range []string{"account", "resource"} {
		t.Run(scope, func(t *testing.T) {
			ctx := context.Background()
			adapter := &concurrencyHTTPAdapter{scope: scope}
			f := setupManagedHTTP(t, true, adapter)
			settings := scheduler.DefaultSettings()
			settings.MaxWorkers, settings.MaxActivePerAccount = 4, 64
			if err := f.store.ConfigureScheduler(ctx, settings); err != nil {
				t.Fatal(err)
			}
			_, raw := f.call(t, "GET", "/v1/workspaces/a/providers", "read", "", "", 200)
			wireSchema(t, "provider-descriptors", raw)
			var providers struct{ Providers []connections.Descriptor }
			if json.Unmarshal(raw, &providers) != nil || len(providers.Providers) != 1 || providers.Providers[0].Capabilities.Concurrency == nil || *providers.Providers[0].Capabilities.Concurrency != *adapter.Descriptor().Capabilities.Concurrency || adapter.verifies.Load() != 0 {
				t.Fatal("descriptor lost supported local range or contacted provider")
			}
			read := func(workspace, id string) connections.Connection {
				t.Helper()
				_, raw := f.call(t, "GET", "/v1/workspaces/"+workspace+"/connections/"+id, workspace, "", "", 200)
				wireSchema(t, "connection", raw)
				managedSafeResponse(t, raw)
				var c connections.Connection
				if json.Unmarshal(raw, &c) != nil || c.Concurrency == nil || c.Concurrency.Scope != scope {
					t.Fatal("missing concurrency projection")
				}
				if scope == "resource" && len(c.Quotas) != 0 {
					t.Fatal("resource provider reported unsupported quota")
				}
				return c
			}
			create := func(workspace, route string, limit int) connections.Connection {
				t.Helper()
				body := fmt.Sprintf(`{"provider_type":"fixture","label":"Fixture route","credentials":{"api_token":"SYNTHETIC_%s"},"configuration":{"max_remote_wall_seconds":600,"max_active_attempts":%d}}`, route, limit)
				_, raw := f.call(t, "POST", "/v1/workspaces/"+workspace+"/connections", workspace, "connection-"+route, body, 202)
				op := readManagedOperation(t, raw)
				pending := read(workspace, op.ConnectionID).Concurrency
				if pending.ConfiguredLimit != limit || pending.EffectiveLimit != nil || pending.ActiveAttempts != nil || pending.WaitReason == nil || *pending.WaitReason != "connection_unverified" {
					t.Fatal("unverified connection promised capacity", pending)
				}
				if handled, err := f.service.RunOnce(ctx); err != nil || !handled {
					t.Fatal("verification did not complete", err)
				}
				return read(workspace, op.ConnectionID)
			}
			lan := create("a", "LAN", 2)
			vpn := create("a", "VPN", 2)
			var jobs []admission.Receipt
			for i, c := range []connections.Connection{lan, vpn, vpn} {
				body := strings.Replace(jobBody, "default-gpu", c.Selection.Profile, 1)
				_, raw := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", fmt.Sprintf("parallel-job-%d", i), body, 202)
				var job admission.Receipt
				if json.Unmarshal(raw, &job) != nil {
					t.Fatal("invalid admission")
				}
				grant := fmt.Sprintf(`{"attempt_id":%q,"max_remote_wall_seconds":120,"authorize_private_staging":true,"authorize_compute":true}`, job.AttemptID)
				f.call(t, "POST", job.Links.Self+"/authorize", "execute", fmt.Sprintf("parallel-grant-%d", i), grant, 202)
				jobs = append(jobs, job)
			}
			before := read("a", lan.ID).Concurrency
			if *before.ActiveAttempts != 0 || *before.EffectiveLimit != 2 {
				t.Fatal("unclaimed authorizations occupied execution slots", before)
			}
			for i := 0; i < 2; i++ {
				claim, err := f.store.ClaimNext(ctx, "parallel-worker", time.Now().UTC())
				if err != nil || claim.Claim == nil || claim.Claim.JobID != jobs[i].JobID {
					t.Fatal("two independent claims unavailable", claim, err)
				}
			}
			wantReason := "account_capacity"
			if scope == "resource" {
				wantReason = "resource_capacity"
			}
			current := read("a", vpn.ID).Concurrency
			if *current.EffectiveLimit != 2 || *current.ActiveAttempts != 2 || current.WaitReason == nil || *current.WaitReason != wantReason || scope == "resource" && current.Resource == nil {
				t.Fatal("shared reservations were not projected", current)
			}
			_, raw = f.call(t, "GET", jobs[2].Links.Self, "read", "", "", 200)
			wireSchema(t, "job-status", raw)
			var status struct{ Scheduling *admission.SchedulingStatus }
			if json.Unmarshal(raw, &status) != nil || status.Scheduling == nil || status.Scheduling.Reason != wantReason {
				t.Fatal("third job did not expose exact scheduler capacity reason", string(raw))
			}
			f.call(t, "GET", jobs[2].Links.Self, "b", "", "", 403)
			// Another workspace's retained serial alias constrains the same pool.
			create("b", "DNS", 1)
			current = read("a", vpn.ID).Concurrency
			if current.ConfiguredLimit != 2 || *current.EffectiveLimit != 1 || *current.ActiveAttempts != 2 {
				t.Fatal("alias reduction changed accepted work or lost effective limit", current)
			}
			calls := adapter.verifies.Load()
			f.vault.mu.Lock()
			f.vault.fail = credentials.ErrUnavailable
			f.vault.mu.Unlock()
			read("a", vpn.ID)
			f.call(t, "GET", "/v1/workspaces/a/providers", "read", "", "", 200)
			f.call(t, "GET", jobs[2].Links.Self, "read", "", "", 200)
			if adapter.verifies.Load() != calls {
				t.Fatal("cached concurrency read contacted provider")
			}
		})
	}
}
