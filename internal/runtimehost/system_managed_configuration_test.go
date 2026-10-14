package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestManagedConfigurationConcurrentRevisionHasOneWinner(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	original := f.managedCreate(t, "app", "concurrent-config-create", systemManagedSecret)
	results := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.services.connections.Submit(context.Background(), f.actor, "app", original.ID, fmt.Sprintf("configure-key-%d", i), systemConfigureBody(original.Revision, int64(3600+i)))
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, connections.ErrConflict) {
			t.Fatal(err)
		}
	}
	if winners != 1 || f.managedGet(t, "app", original.ID).Revision != original.Revision+1 || f.adapter.checkCount() != 1 {
		t.Fatal("configuration revision did not serialize", winners)
	}
}

func TestManagedConfigurationCreateAndLifecycleRetainOverride(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	var body map[string]any
	if err := json.Unmarshal(systemManagedCreateBody(systemManagedSecret), &body); err != nil {
		t.Fatal(err)
	}
	body["configuration"] = connections.Configuration{MaxRemoteWallSeconds: 7200}
	raw, _ := json.Marshal(body)
	op, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "configured-create", raw)
	if err != nil {
		t.Fatal(err)
	}
	f.managedRun(t)
	current := f.managedGet(t, "app", op.ConnectionID)
	for _, action := range []string{"check", "replace_credential", "disable", "enable"} {
		if current.Configuration == nil || current.Configuration.MaxRemoteWallSeconds != 7200 || current.Selection != nil && current.Selection.MaxRemoteWallSeconds != 7200 {
			t.Fatal("lifecycle lost explicit configuration", action, current)
		}
		secret := ""
		if action == "replace_credential" {
			secret = systemManagedRotated
		}
		f.managedAction(t, "app", current, "configured-"+action, action, secret)
		f.reopen(t)
		f.managedRun(t)
		current = f.managedGet(t, "app", current.ID)
		if action == "disable" {
			before := f.adapter.checkCount()
			systemConfigure(t, f, current, "configure-disabled", 7200)
			current = f.managedGet(t, "app", current.ID)
			if current.NewWork != "disabled" || current.Selection != nil || f.adapter.checkCount() != before {
				t.Fatal("saving configuration enabled a disabled connection")
			}
		}
	}
	if current.Selection == nil || current.Selection.MaxRemoteWallSeconds != 7200 {
		t.Fatal("enable reverted to startup default", current)
	}
}

func TestManagedConfigurationPendingRejectedAndRemovedConnections(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	op, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "pending-config-create", systemManagedCreateBody(systemManagedSecret))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.connections.Submit(context.Background(), f.actor, "app", op.ConnectionID, "configure-pending", systemConfigureBody(op.ConnectionRevision, 7200)); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("configuration raced pending verification", err)
	}
	f.adapter.verifyError = connections.ErrCredentialRejected
	f.managedRun(t)
	rejected := f.managedGet(t, "app", op.ConnectionID)
	systemConfigure(t, f, rejected, "configure-rejected", 7200)
	current := f.managedGet(t, "app", rejected.ID)
	if current.Authentication != "rejected" || current.Selection != nil || current.CredentialPresent {
		t.Fatal("configuration manufactured authentication", current)
	}
	f.managedAction(t, "app", current, "remove-configured", "remove", "")
	f.managedRun(t)
	removed := f.managedGet(t, "app", current.ID)
	if _, err := f.services.connections.Submit(context.Background(), f.actor, "app", removed.ID, "configure-removed", systemConfigureBody(removed.Revision, 7200)); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("configuration resurrected removed connection", err)
	}
}

func TestManagedConfigurationPreservesFrozenJobsGrantsAndRuntime(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	ctx := context.Background()
	original := f.managedCreate(t, "app", "original-connection", systemManagedSecret)
	beforeCalls := f.adapter.checkCount()
	oldReceipt, oldJob := f.managedAdmit(t, "app", original, "original-job")
	authorizations, err := executionauth.New(f.host.access, f.host.store, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authorizations.Authorize(ctx, f.actor, "app", oldReceipt.JobID, "original-grant", systemAuthorizationRequest(oldReceipt.AttemptID, 10))
	if err != nil {
		t.Fatal(err)
	}
	oldBinding := provider.BindingSnapshot{Binding: oldJob.Profile.Binding, AccountScope: oldJob.Profile.AccountScope, CredentialRef: oldJob.Profile.CredentialRef}
	oldRuntime, err := f.host.store.ReadManagedBinding(ctx, oldBinding)
	if err != nil {
		t.Fatal(err)
	}
	beforeConfiguration := f.managedGet(t, "app", original.ID)
	systemConfigure(t, f, original, "raise-wall-limit", 7200)
	current := f.managedGet(t, "app", original.ID)
	if current.Configuration == nil || current.Configuration.MaxRemoteWallSeconds != 7200 || current.Selection == nil || current.Selection.MaxRemoteWallSeconds != 7200 || current.Selection.Profile == original.Selection.Profile || current.Revision != original.Revision+1 {
		t.Fatal("configuration did not publish a new selection", current)
	}
	if !reflect.DeepEqual(current.Quotas, beforeConfiguration.Quotas) {
		t.Fatal("configuration changed quota observations or reservations")
	}
	_, newJob := f.managedAdmit(t, "app", current, "configured-new-job")
	if newJob.Profile.MaxRemoteWallSeconds != 7200 || oldJob.Profile.MaxRemoteWallSeconds != 1800 {
		t.Fatal("jobs did not freeze distinct limits")
	}
	oldAgain, err := f.host.store.ReadManagedBinding(ctx, oldBinding)
	if err != nil || !reflect.DeepEqual(oldAgain, oldRuntime) {
		t.Fatal("configuration rewrote old binding/runtime", err)
	}
	newBinding := provider.BindingSnapshot{Binding: newJob.Profile.Binding, AccountScope: newJob.Profile.AccountScope, CredentialRef: newJob.Profile.CredentialRef}
	newRuntime, err := f.host.store.ReadManagedBinding(ctx, newBinding)
	if err != nil || string(newRuntime.RuntimeConfig) != string(oldRuntime.RuntimeConfig) || newRuntime.Profile.AccountScope != oldRuntime.Profile.AccountScope {
		t.Fatal("configuration reinterpreted resource or account", err)
	}
	jobs, err := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	retained, err := jobs.Get(ctx, f.actor, "app", oldReceipt.JobID)
	if err != nil || retained.Profile != oldJob.Profile {
		t.Fatal("existing job changed", err)
	}
	replayed, err := authorizations.Authorize(ctx, f.actor, "app", oldReceipt.JobID, "original-grant", systemAuthorizationRequest(oldReceipt.AttemptID, 10))
	if err != nil || replayed.AuthorizationID != grant.AuthorizationID || replayed.MaxRemoteWallSeconds != grant.MaxRemoteWallSeconds || !replayed.Replay {
		t.Fatal("configuration changed the finite grant", err)
	}
	if f.adapter.checkCount() != beforeCalls {
		t.Fatal("configuration made provider calls")
	}
}

func TestManagedConfigurationReplayRevisionAndRestart(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	original := f.managedCreate(t, "app", "configuration-create", systemManagedSecret)
	first := systemConfigure(t, f, original, "configuration-key", 7200)
	current := f.managedGet(t, "app", original.ID)
	systemConfigure(t, f, current, "later-configuration", 3600)
	f.reopen(t)
	replay, err := f.services.connections.Submit(context.Background(), f.actor, "app", original.ID, "configuration-key", systemConfigureBody(original.Revision, 7200))
	if err != nil || !replay.Replay || replay.ID != first.ID || replay.ConnectionRevision != first.ConnectionRevision || replay.Status != "succeeded" {
		t.Fatal("restart replay lost original terminal receipt", replay, err)
	}
	for _, tc := range []struct {
		key  string
		body []byte
	}{
		{"configuration-key", systemConfigureBody(original.Revision, 3600)},
		{"stale-configuration", systemConfigureBody(original.Revision, 7200)},
	} {
		if _, err := f.services.connections.Submit(context.Background(), f.actor, "app", original.ID, tc.key, tc.body); !errors.Is(err, connections.ErrConflict) {
			t.Fatal("conflicting configuration accepted", err)
		}
	}
	current = f.managedGet(t, "app", original.ID)
	if current.Revision != original.Revision+2 || current.Configuration.MaxRemoteWallSeconds != 3600 || current.Selection.MaxRemoteWallSeconds != 3600 {
		t.Fatal("replay or restart rolled back configuration", current)
	}
	if worked, err := f.services.connections.RunOnce(context.Background()); err != nil || worked || f.adapter.checkCount() != 1 {
		t.Fatal("configuration queued hidden provider work", worked, err)
	}
}
