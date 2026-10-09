package sqlite

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

func configureBody(revision, wall int64) []byte {
	return []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":%d}}`, revision, wall))
}

func configureConnection(t testing.TB, f *managedStateFixture, current connections.Connection, key string, wall int64) connections.Operation {
	t.Helper()
	op, err := f.service.Submit(context.Background(), f.principals["a"], "a", current.ID, key, configureBody(current.Revision, wall))
	if err != nil || op.Status != "succeeded" || op.Action != "configure" {
		t.Fatal("configuration did not commit locally", op, err)
	}
	return op
}

type runtimeStateAdapter struct{ *stateAdapter }

func (runtimeStateAdapter) RuntimeConfig() []byte {
	return []byte(`{"version":1,"machine_shape":"fixture_cpu"}`)
}

func TestManagedConfigurationPreservesFrozenJobsGrantsAndRuntime(t *testing.T) {
	f := newManagedStateFixture(t)
	var err error
	f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{runtimeStateAdapter{f.discovery}}, f.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	original := f.create(t, "a", "original-connection", managedStateSecret)
	beforeCalls := f.discovery.count()
	oldReceipt, oldJob := f.admit(t, "a", original, "original-job")
	authorizations, err := executionauth.New(f.access, f.s, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authorizations.Authorize(ctx, f.principals["a"], "a", oldReceipt.JobID, "original-grant", authorizationRequest(oldReceipt.AttemptID, 10))
	if err != nil {
		t.Fatal(err)
	}
	oldBinding := provider.BindingSnapshot{Binding: oldJob.Profile.Binding, AccountScope: oldJob.Profile.AccountScope, CredentialRef: oldJob.Profile.CredentialRef}
	oldRuntime, err := f.s.ReadManagedBinding(ctx, oldBinding)
	if err != nil {
		t.Fatal(err)
	}
	beforeConfiguration := f.get(t, "a", original.ID)
	configureConnection(t, f, original, "raise-wall-limit", 7200)
	current := f.get(t, "a", original.ID)
	if current.Configuration == nil || current.Configuration.MaxRemoteWallSeconds != 7200 || current.Selection == nil || current.Selection.MaxRemoteWallSeconds != 7200 || current.Selection.Profile == original.Selection.Profile || current.Revision != original.Revision+1 {
		t.Fatal("configuration did not publish a new selection", current)
	}
	if !reflect.DeepEqual(current.Quotas, beforeConfiguration.Quotas) {
		t.Fatal("configuration changed quota observations or reservations")
	}
	_, newJob := f.admit(t, "a", current, "configured-new-job")
	if newJob.Profile.MaxRemoteWallSeconds != 7200 || oldJob.Profile.MaxRemoteWallSeconds != 1800 {
		t.Fatal("jobs did not freeze distinct limits")
	}
	oldAgain, err := f.s.ReadManagedBinding(ctx, oldBinding)
	if err != nil || !reflect.DeepEqual(oldAgain, oldRuntime) {
		t.Fatal("configuration rewrote old binding/runtime", err)
	}
	newBinding := provider.BindingSnapshot{Binding: newJob.Profile.Binding, AccountScope: newJob.Profile.AccountScope, CredentialRef: newJob.Profile.CredentialRef}
	newRuntime, err := f.s.ReadManagedBinding(ctx, newBinding)
	if err != nil || string(newRuntime.RuntimeConfig) != string(oldRuntime.RuntimeConfig) || newRuntime.Profile.AccountScope != oldRuntime.Profile.AccountScope {
		t.Fatal("configuration reinterpreted resource or account", err)
	}
	jobs, err := admission.New(f.access, f.s, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	retained, err := jobs.Get(ctx, f.principals["a"], "a", oldReceipt.JobID)
	if err != nil || retained.Profile != oldJob.Profile {
		t.Fatal("existing job changed", err)
	}
	replayed, err := authorizations.Authorize(ctx, f.principals["a"], "a", oldReceipt.JobID, "original-grant", authorizationRequest(oldReceipt.AttemptID, 10))
	if err != nil || replayed.AuthorizationID != grant.AuthorizationID || replayed.MaxRemoteWallSeconds != grant.MaxRemoteWallSeconds || !replayed.Replay {
		t.Fatal("configuration changed the finite grant", err)
	}
	if f.discovery.count() != beforeCalls {
		t.Fatal("configuration made provider calls")
	}
}

func TestManagedConfigurationReplayRevisionAndRestart(t *testing.T) {
	f := newManagedStateFixture(t)
	original := f.create(t, "a", "configuration-create", managedStateSecret)
	first := configureConnection(t, f, original, "configuration-key", 7200)
	current := f.get(t, "a", original.ID)
	configureConnection(t, f, current, "later-configuration", 3600)
	f.restartManaged(t)
	replay, err := f.service.Submit(context.Background(), f.principals["a"], "a", original.ID, "configuration-key", configureBody(original.Revision, 7200))
	if err != nil || !replay.Replay || replay.ID != first.ID || replay.ConnectionRevision != first.ConnectionRevision || replay.Status != "succeeded" {
		t.Fatal("restart replay lost original terminal receipt", replay, err)
	}
	for _, tc := range []struct {
		key  string
		body []byte
	}{
		{"configuration-key", configureBody(original.Revision, 3600)},
		{"stale-configuration", configureBody(original.Revision, 7200)},
	} {
		if _, err := f.service.Submit(context.Background(), f.principals["a"], "a", original.ID, tc.key, tc.body); !errors.Is(err, connections.ErrConflict) {
			t.Fatal("conflicting configuration accepted", err)
		}
	}
	current = f.get(t, "a", original.ID)
	if current.Revision != original.Revision+2 || current.Configuration.MaxRemoteWallSeconds != 3600 || current.Selection.MaxRemoteWallSeconds != 3600 {
		t.Fatal("replay or restart rolled back configuration", current)
	}
	if worked, err := f.service.RunOnce(context.Background()); err != nil || worked || f.discovery.count() != 1 {
		t.Fatal("configuration queued hidden provider work", worked, err)
	}
}

func TestManagedConfigurationConcurrentRevisionHasOneWinner(t *testing.T) {
	f := newManagedStateFixture(t)
	original := f.create(t, "a", "concurrent-config-create", managedStateSecret)
	results := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.service.Submit(context.Background(), f.principals["a"], "a", original.ID, fmt.Sprintf("configure-key-%d", i), configureBody(original.Revision, int64(3600+i)))
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
	if winners != 1 || f.get(t, "a", original.ID).Revision != original.Revision+1 || f.discovery.count() != 1 {
		t.Fatal("configuration revision did not serialize", winners)
	}
}

func TestManagedConfigurationCreateAndLifecycleRetainOverride(t *testing.T) {
	f := newManagedStateFixture(t)
	var body map[string]any
	if err := json.Unmarshal(managedCreateBody(managedStateSecret), &body); err != nil {
		t.Fatal(err)
	}
	body["configuration"] = connections.Configuration{MaxRemoteWallSeconds: 7200}
	raw, _ := json.Marshal(body)
	op, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "configured-create", raw)
	if err != nil {
		t.Fatal(err)
	}
	f.run(t)
	current := f.get(t, "a", op.ConnectionID)
	for _, action := range []string{"check", "replace_credential", "disable", "enable"} {
		if current.Configuration == nil || current.Configuration.MaxRemoteWallSeconds != 7200 || current.Selection != nil && current.Selection.MaxRemoteWallSeconds != 7200 {
			t.Fatal("lifecycle lost explicit configuration", action, current)
		}
		secret := ""
		if action == "replace_credential" {
			secret = managedStateRotated
		}
		f.action(t, "a", current, "configured-"+action, action, secret)
		f.restartManaged(t)
		f.run(t)
		current = f.get(t, "a", current.ID)
		if action == "disable" {
			before := f.discovery.count()
			configureConnection(t, f, current, "configure-disabled", 7200)
			current = f.get(t, "a", current.ID)
			if current.NewWork != "disabled" || current.Selection != nil || f.discovery.count() != before {
				t.Fatal("saving configuration enabled a disabled connection")
			}
		}
	}
	if current.Selection == nil || current.Selection.MaxRemoteWallSeconds != 7200 {
		t.Fatal("enable reverted to startup default", current)
	}
}

func TestManagedConfigurationPendingRejectedAndRemovedConnections(t *testing.T) {
	f := newManagedStateFixture(t)
	op, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "pending-config-create", managedCreateBody(managedStateSecret))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Submit(context.Background(), f.principals["a"], "a", op.ConnectionID, "configure-pending", configureBody(op.ConnectionRevision, 7200)); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("configuration raced pending verification", err)
	}
	f.discovery.fail = connections.ErrCredentialRejected
	f.run(t)
	rejected := f.get(t, "a", op.ConnectionID)
	configureConnection(t, f, rejected, "configure-rejected", 7200)
	current := f.get(t, "a", rejected.ID)
	if current.Authentication != "rejected" || current.Selection != nil || current.CredentialPresent {
		t.Fatal("configuration manufactured authentication", current)
	}
	f.action(t, "a", current, "remove-configured", "remove", "")
	f.run(t)
	removed := f.get(t, "a", current.ID)
	if _, err := f.service.Submit(context.Background(), f.principals["a"], "a", removed.ID, "configure-removed", configureBody(removed.Revision, 7200)); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("configuration resurrected removed connection", err)
	}
}
