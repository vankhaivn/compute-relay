package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type healthStateAdapter struct {
	*resourceStateAdapter
	noImage bool
}

func (a *healthStateAdapter) Descriptor() connections.Descriptor {
	d := a.resourceStateAdapter.Descriptor()
	d.Type = "ssh"
	return d
}

func (a *healthStateAdapter) Verify(ctx context.Context, raw []byte) (connections.Verification, error) {
	v, err := a.resourceStateAdapter.Verify(ctx, raw)
	if err != nil {
		return v, err
	}
	v.SelectionUnavailable = a.noImage
	if !a.noImage {
		v.RuntimeConfig = []byte(fmt.Sprintf(`{"schema_version":3,"image":%q,"architecture":"amd64","recipe_sha256":%q,"machine_id":%q,"engine_id":%q}`,
			"sha256:"+strings.Repeat("1", 64), sshhost.ImageRecipeDigest(), strings.TrimPrefix(v.Resource.ID, "resource_"), strings.Repeat("e", 64)))
	}
	return v, nil
}

func healthStateFixture(t *testing.T) (*managedStateFixture, *healthStateAdapter) {
	t.Helper()
	f, resource := resourceFixture(t)
	adapter := &healthStateAdapter{resourceStateAdapter: resource}
	var err error
	f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{adapter})
	requireNoError(t, err)
	return f, adapter
}

func createHealthConnection(t *testing.T, f *managedStateFixture, w domain.WorkspaceID, key, secret string) connections.Connection {
	t.Helper()
	body := strings.Replace(string(managedCreateBody(secret)), `"fixture"`, `"ssh"`, 1)
	op, err := f.service.Submit(context.Background(), f.principals[w], w, "", key, []byte(body))
	requireNoError(t, err)
	f.run(t)
	profiles, err := f.s.ActiveStorageProfiles(context.Background())
	requireNoError(t, err)
	for _, profile := range profiles {
		requireNoError(t, f.s.RecordStorageScan(context.Background(), hoststorage.Scan{
			Scope: profile.Scope, ObservedAt: f.clock.Now(), Objects: []hoststorage.Object{},
		}))
	}
	return f.get(t, w, op.ConnectionID)
}

func storeHealthSnapshot(resource string, now time.Time) hosthealth.Snapshot {
	return hosthealth.Parse(resource, now, map[hosthealth.Source]hosthealth.Sample{
		hosthealth.Platform:           {Status: hosthealth.Available, Data: []byte("Darwin x86_64")},
		hosthealth.NativeArchitecture: {Status: hosthealth.Available, Data: []byte("7\n")},
		hosthealth.ThermalState:       {Status: hosthealth.Available, Data: []byte("0\n")},
		hosthealth.Sysctl:             {Status: hosthealth.Available, Data: []byte("hw.ncpu: 16\nhw.memsize: 68719476736\nvm.loadavg: { 1.0 2.0 3.0 }\n")},
	})
}

func TestHostHealthStoreAliasesShareHistoryAcrossRestartAndPruneBounds(t *testing.T) {
	f, _ := healthStateFixture(t)
	ctx := context.Background()
	lan := createHealthConnection(t, f, "a", "health-lan", "SYNTHETIC_LAN")
	vpn := createHealthConnection(t, f, "b", "health-vpn", "SYNTHETIC_VPN")
	first, err := f.s.ResolveHostHealthTarget(ctx, "a", f.principals["a"].TokenID(), lan.ID)
	requireNoError(t, err)
	second, err := f.s.ResolveHostHealthTarget(ctx, "b", f.principals["b"].TokenID(), vpn.ID)
	if err != nil || first.ResourceID != second.ResourceID || first.CanonicalAccount == second.CanonicalAccount {
		t.Fatal("aliases did not resolve one verified machine", err)
	}
	for i := range hosthealth.HistoryMaxRows + 5 {
		snapshot := storeHealthSnapshot(first.ResourceID, f.clock.Now().Add(time.Duration(i-hosthealth.HistoryMaxRows-5)*time.Second))
		requireNoError(t, f.s.RecordHostHealth(ctx, snapshot))
	}
	history, err := f.s.ReadHostHealth(ctx, second.ResourceID)
	if err != nil || len(history) != hosthealth.HistoryMaxRows || !history[0].ObservedAt.After(history[len(history)-1].ObservedAt) {
		t.Fatal("history row bound/order failed", len(history), err)
	}
	f.restartManaged(t)
	history, err = f.s.ReadHostHealth(ctx, first.ResourceID)
	if err != nil || len(history) != hosthealth.HistoryMaxRows {
		t.Fatal("restart lost history", len(history), err)
	}
	// An old machine is pruned even when only another machine is observed.
	otherResource := "resource_" + strings.Repeat("b", 64)
	_, err = f.s.db.Exec(`INSERT INTO managed_resource_accounts VALUES('other-health-account','machine',?)`, otherResource)
	requireNoError(t, err)
	f.clock.advance(hosthealth.HistoryMaxAge + time.Second)
	requireNoError(t, f.s.RecordHostHealth(ctx, storeHealthSnapshot(otherResource, f.clock.Now())))
	var oldRows int
	requireNoError(t, f.s.db.QueryRow(`SELECT count(*) FROM host_health_history WHERE resource_id=?`, first.ResourceID).Scan(&oldRows))
	if oldRows != 0 {
		t.Fatal("age pruning retained an idle machine's old rows", oldRows)
	}
	f.clock.advance(hosthealth.HistoryMaxAge + time.Second)
	history, err = f.s.ReadHostHealth(ctx, otherResource)
	if err != nil || len(history) != 0 {
		t.Fatal("idle read did not prune expired history", len(history), err)
	}
}

func TestHostHealthTargetsRequireActorAndVerifiedResourceIncludingNoImage(t *testing.T) {
	f, adapter := healthStateFixture(t)
	ctx := context.Background()
	adapter.noImage = true
	c := createHealthConnection(t, f, "a", "health-before-image", "SYNTHETIC_LAN")
	if c.Selection != nil {
		t.Fatal("fixture unexpectedly has an image selection")
	}
	target, err := f.s.ResolveHostHealthTarget(ctx, "a", f.principals["a"].TokenID(), c.ID)
	if err != nil || !target.Valid() || len(target.RuntimeConfig) != 0 {
		t.Fatal("verified machine requires an execution image", err)
	}
	if _, err = f.s.ResolveHostHealthTarget(ctx, "b", f.principals["a"].TokenID(), c.ID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("cross-workspace target access", err)
	}
	if _, err = f.s.ResolveHostHealthTarget(ctx, "b", f.principals["b"].TokenID(), c.ID); !errors.Is(err, connections.ErrNotFound) {
		t.Fatal("target existence leaked", err)
	}
	op, err := f.service.Submit(ctx, f.principals["a"], "a", "", "health-unverified", []byte(strings.Replace(string(managedCreateBody("SYNTHETIC_DNS")), `"fixture"`, `"ssh"`, 1)))
	requireNoError(t, err)
	if _, err = f.s.ResolveHostHealthTarget(ctx, "a", f.principals["a"].TokenID(), op.ConnectionID); !errors.Is(err, hosthealth.ErrUnverified) {
		t.Fatal("pending connection supplied a target", err)
	}
	if _, err = f.s.db.Exec(`UPDATE managed_connections SET provider_type='kaggle',host_health=NULL WHERE connection_id=?`, c.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.ResolveHostHealthTarget(ctx, "a", f.principals["a"].TokenID(), c.ID); !errors.Is(err, hosthealth.ErrUnsupported) {
		t.Fatal("non-SSH provider supplied a host probe", err)
	}
}

func TestHostHealthPeriodicTargetsIncludeOnlyBusyMachinesAndSurviveDisable(t *testing.T) {
	f, _ := healthStateFixture(t)
	ctx := context.Background()
	lan := createHealthConnection(t, f, "a", "busy-lan", "SYNTHETIC_LAN")
	createHealthConnection(t, f, "b", "busy-vpn", "SYNTHETIC_VPN")
	createHealthConnection(t, f, "a", "idle-other", "SYNTHETIC_OTHER_HOST")
	targets, err := f.s.ActiveHostHealthTargets(ctx)
	if err != nil || len(targets) != 0 {
		t.Fatal("idle machines were selected", len(targets), err)
	}
	receipt, _ := f.admit(t, "a", lan, "health-busy-job")
	id := scheduler.Identity{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID}
	for _, phase := range []domain.OrchestrationState{domain.OrchestrationQueued, domain.OrchestrationPreparing, domain.OrchestrationRunning} {
		state := domain.InitialAttemptState()
		state.Orchestration = phase
		if phase == domain.OrchestrationRunning {
			state.Execution, state.RemoteActivity = domain.ExecutionRunning, domain.RemoteActivityActive
		}
		schedulerSetState(t, f.s, id, state)
		targets, err = f.s.ActiveHostHealthTargets(ctx)
		if err != nil || len(targets) != 1 || targets[0].ResourceID != "resource_"+strings.Repeat("a", 64) {
			t.Fatal("busy machine or aliases selected incorrectly", phase, len(targets), err)
		}
	}
	for _, phase := range []domain.OrchestrationState{domain.OrchestrationDispatching, domain.OrchestrationSubmitted, domain.OrchestrationReconciling, domain.OrchestrationCancelling, domain.OrchestrationNeedsAttention} {
		state := domain.InitialAttemptState()
		state.Orchestration, state.Execution, state.RemoteActivity = phase, domain.ExecutionUnknown, domain.RemoteActivityPossible
		schedulerSetState(t, f.s, id, state)
		targets, err = f.s.ActiveHostHealthTargets(ctx)
		if err != nil || len(targets) != 1 {
			t.Fatal("unresolved active work lost machine observation", phase, err)
		}
	}
	running := domain.InitialAttemptState()
	running.Orchestration, running.Execution, running.RemoteActivity = domain.OrchestrationRunning, domain.ExecutionRunning, domain.RemoteActivityActive
	schedulerSetState(t, f.s, id, running)
	// Remove the other alias, then disable the only remaining connection while
	// its frozen running attempt still needs observation and its engine pin.
	var vpnID string
	requireNoError(t, f.s.db.QueryRow(`SELECT connection_id FROM managed_connections WHERE workspace_id='b'`).Scan(&vpnID))
	vpn := f.get(t, "b", vpnID)
	f.action(t, "b", vpn, "health-remove-idle-alias", "remove", "")
	f.run(t)
	f.action(t, "a", lan, "health-disable-running", "disable", "")
	f.run(t)
	lan = f.get(t, "a", lan.ID)
	targets, err = f.s.ActiveHostHealthTargets(ctx)
	if err != nil || len(targets) != 1 || targets[0].ConnectionID != lan.ID || len(targets[0].RuntimeConfig) == 0 {
		t.Fatal("disable lost active machine evidence", len(targets), err)
	}
	_, err = f.service.Submit(ctx, f.principals["a"], "a", lan.ID, "health-remove-active", managedActionBody("remove", lan.Revision, ""))
	if !errors.Is(err, connections.ErrActiveWork) {
		t.Fatal("active connection unexpectedly removable", err)
	}
	state := domain.InitialAttemptState()
	state.Orchestration, state.Execution, state.RemoteActivity, state.Result = domain.OrchestrationCollecting, domain.ExecutionSucceeded, domain.RemoteActivityInactive, domain.ResultCollecting
	schedulerSetState(t, f.s, id, state)
	targets, err = f.s.ActiveHostHealthTargets(ctx)
	if err != nil || len(targets) != 0 {
		t.Fatal("completed execution still selected", len(targets), err)
	}
}

func TestHostHealthStoreRejectsUnverifiedInvalidFutureAndCorruptSnapshots(t *testing.T) {
	f, _ := healthStateFixture(t)
	ctx := context.Background()
	c := createHealthConnection(t, f, "a", "health-validate", "SYNTHETIC_LAN")
	target, err := f.s.ResolveHostHealthTarget(ctx, "a", f.principals["a"].TokenID(), c.ID)
	requireNoError(t, err)
	snapshot := storeHealthSnapshot(target.ResourceID, f.clock.Now())
	for _, kind := range []string{"unverified", "invalid", "future"} {
		s := snapshot
		want := hosthealth.ErrSnapshot
		switch kind {
		case "unverified":
			s.ResourceID = "resource_" + strings.Repeat("f", 64)
			want = hosthealth.ErrUnverified
		case "invalid":
			s.SchemaVersion = 999
		case "future":
			s.ObservedAt = s.ObservedAt.Add(time.Hour)
		}
		if err := f.s.RecordHostHealth(ctx, s); !errors.Is(err, want) {
			t.Fatal(kind, err)
		}
	}
	requireNoError(t, f.s.RecordHostHealth(ctx, snapshot))
	if _, err = f.s.db.Exec(`UPDATE host_health_history SET observed_ms=observed_ms+1`); err == nil {
		t.Fatal("immutable observation was updated")
	}
	snapshot.ResourceID = "resource_" + strings.Repeat("b", 64)
	raw, err := json.Marshal(snapshot)
	requireNoError(t, err)
	_, err = f.s.db.Exec(`INSERT INTO host_health_history(resource_id,observed_ms,snapshot) VALUES(?,?,?)`, target.ResourceID, snapshot.ObservedAt.UnixMilli(), raw)
	requireNoError(t, err)
	if _, err = f.s.ReadHostHealth(ctx, target.ResourceID); !errors.Is(err, ErrCorrupt) {
		t.Fatal("mismatched persisted machine accepted", err)
	}
}
