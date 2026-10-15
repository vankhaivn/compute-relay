package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/hostidentity"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type nativeStoragePreparation struct {
	*probeProvider
	resource string
}

type nativeStorageCleanup struct{ *cleanupProbe }

func (nativeStorageCleanup) DescribeCleanup(request provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	if !request.Valid() {
		return provider.CleanupDescription{}, ErrInvalid
	}
	return provider.CleanupDescription{Kind: "docker_container", ID: "fixture", Name: "relay-job", Version: "prepared",
		Scope: "container_writable_layer", Support: domain.CapabilitySupportSupported, Reason: "immutable_container_id"}, nil
}

func (p nativeStoragePreparation) Prepare(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.Prepared, error) {
	prepared, err := p.probeProvider.Prepare(ctx, plan, operation)
	prepared.Resource = p.resource
	return prepared, err
}

func TestStorageAttemptCleanupReleasesPinsFromNativeEngineReceipt(t *testing.T) {
	f, id, _ := applicationImageFlow(t, false)
	ctx := t.Context()
	if hostidentity.Digest("docker-engine-id", cacheNativeEngine) != cacheEngine || cacheNativeEngine == cacheEngine {
		t.Fatal("fixture must distinguish native and opaque engine identities")
	}
	container := strings.Repeat("e", 64)
	resource := `{"container":"relay-job","id":"` + container + `","engine_id":"` + cacheNativeEngine + `"}`
	binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
	f.registry = provider.NewSnapshotRegistry()
	requireNoError(t, f.registry.Register(binding, nativeStoragePreparation{probeProvider: f.adapter, resource: resource}))
	var err error
	f.engine, err = dispatch.New(f.s, f.registry, f.blobs, f.clock, dispatch.DefaultConfig())
	requireNoError(t, err)
	grantAuthorization(t, f, id)
	for steps := 0; f.journal(t, id).Phase != dispatch.Ready; steps++ {
		if steps > 8 {
			t.Fatal("native-reference attempt never prepared")
		}
		worked, err := gateStep(t, f)
		requireNoError(t, err)
		if !worked {
			t.Fatal("preparation made no progress")
		}
	}
	owner := hostcache.Attempt{WorkspaceID: id.WorkspaceID, JobID: id.JobID, AttemptID: id.AttemptID}
	keys, err := f.s.Pins(ctx, owner)
	requireNoError(t, err)
	if len(keys) == 0 {
		t.Fatal("attempt has no cache pins")
	}
	g, fresh, err := f.s.Claim(ctx, owner, keys[0])
	requireNoError(t, err)
	if !fresh {
		t.Fatal("cache writer was not elected")
	}
	requireNoError(t, f.s.BeginWrite(ctx, owner, g.ID))
	requireNoError(t, f.s.Verify(ctx, completeCacheObservation(g)))
	active, err := f.s.Active(ctx, keys[0])
	requireNoError(t, err)
	if active == nil {
		t.Fatal("verified cache entry missing")
	}
	requireNoError(t, f.s.RecordMount(ctx, hostcache.MountReceipt{Attempt: owner, Generation: *active, ContainerID: container, ReadOnly: true, Labels: active.Labels()}))
	service, actor, _ := controlService(t, f, "a", auth.Operate)
	if cancelled := cancelControl(t, f, service, actor, id); cancelled.Effect != operations.DispatchPrevented {
		t.Fatalf("prepared submission was not prevented: %+v", cancelled)
	}
	pin, err := f.s.PinnedImage(ctx, id.WorkspaceID, id.JobID, id.AttemptID)
	requireNoError(t, err)
	if pin == nil || pin.Released {
		t.Fatal("prepared cancellation prematurely released image")
	}
	controls, principal, receipt := admitCleanup(t, f, id, "native-receipt-cleanup", false)
	probe := &cleanupProbe{probeProvider: f.adapter}
	probe.resourceHook = func(_ context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		var observed struct {
			ID       string `json:"id"`
			EngineID string `json:"engine_id"`
		}
		if json.Unmarshal([]byte(request.Remote.Resource), &observed) != nil || observed.ID != container || observed.EngineID != cacheNativeEngine {
			t.Fatalf("cleanup lost original native receipt: %+v", request.Remote)
		}
		return provider.CleanupOutcome{Deleted: true}, nil
	}
	engine, err := cleanup.New(f.s, cleanupResolver{nativeStorageCleanup{probe}}, cleanup.DefaultConfig())
	requireNoError(t, err)
	runCleanup(t, engine)
	settled, err := controls.Get(ctx, principal, "a", receipt.Operation.ID)
	requireNoError(t, err)
	if settled.Effect != operations.RemoteDeleted || len(probe.requests) != 1 {
		t.Fatalf("native receipt cleanup failed to settle exactly once: %+v, calls=%d", settled, len(probe.requests))
	}
	keys, err = f.s.Pins(ctx, owner)
	requireNoError(t, err)
	pin, err = f.s.PinnedImage(ctx, id.WorkspaceID, id.JobID, id.AttemptID)
	requireNoError(t, err)
	if len(keys) != 0 || pin == nil || !pin.Released {
		t.Fatal("confirmed native-engine cleanup retained cache or image pins")
	}
	var live int
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_reservations WHERE class='attempt'
	 AND owner_workspace_id=? AND owner_job_id=? AND owner_attempt_id=? AND state!='released'`, id.WorkspaceID, id.JobID, id.AttemptID).Scan(&live))
	if live != 0 {
		t.Fatal("confirmed cleanup retained attempt allowance")
	}
}
