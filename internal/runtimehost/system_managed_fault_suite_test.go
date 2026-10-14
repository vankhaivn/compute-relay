package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestManagedCredentialReadSurvivesRotationAndReleasesGuardBeforeCallback(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	initial := f.managedCreate(t, "app", "read-rotation-create", systemManagedSecret)
	f.managedAction(t, "app", initial, "read-rotation-replace", "replace_credential", systemManagedRotated)
	repo := &systemManagedPausedResolution{Store: f.host.store, resolved: make(chan struct{}), resume: make(chan struct{})}
	var err error
	f.services.connections, err = connections.New(f.host.access, repo, f.vault, []connections.Adapter{f.adapter})
	requireNoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	callback, releaseCallback := make(chan struct{}), make(chan struct{})
	var releaseReadOnce, releaseCallbackOnce sync.Once
	releaseRead := func() { releaseReadOnce.Do(func() { close(repo.resume) }) }
	releaseUse := func() { releaseCallbackOnce.Do(func() { close(releaseCallback) }) }
	defer releaseRead()
	defer releaseUse()
	var owned []byte
	readDone := make(chan error, 1)
	go func() {
		readDone <- f.services.connections.WithCredential(ctx, ports.CredentialRef("vault:"+initial.ID), func(value []byte) error {
			if string(value) != systemManagedSecret {
				return fmt.Errorf("selected credential generation changed before callback")
			}
			owned = value
			close(callback)
			select {
			case <-releaseCallback:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-repo.resolved:
	case <-ctx.Done():
		t.Fatal("credential resolution did not pause")
	}
	// Verification and publication of the replacement must not take the read guard.
	if worked, err := f.services.connections.RunOnce(ctx); err != nil || !worked {
		t.Fatal("rotation could not publish while a credential read was pending", err)
	}
	gcDone := make(chan error, 1)
	go func() { _, err := f.services.connections.RunOnce(ctx); gcDone <- err }()
	select {
	case err := <-gcDone:
		t.Fatal("old generation was deleted before its selected read finished", err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseRead()
	select {
	case <-callback:
	case err := <-readDone:
		t.Fatal("credential callback did not run", err)
	case <-ctx.Done():
		t.Fatal("credential callback did not begin")
	}
	select {
	case err := <-gcDone:
		if err != nil {
			t.Fatal("old generation cleanup failed", err)
		}
	case <-ctx.Done():
		t.Fatal("caller callback retained the credential guard")
	}
	if string(owned) != systemManagedSecret || f.vaultEntries() != 2 {
		t.Fatal("cleanup invalidated the callback buffer or retained old generation")
	}
	releaseUse()
	requireNoError(t, <-readDone)
	for _, b := range owned {
		if b != 0 {
			t.Fatal("credential copy survived callback completion")
		}
	}
	if f.managedResolvedSecret(t, "vault:"+initial.ID) != systemManagedRotated {
		t.Fatal("stable reference did not move to replacement")
	}
}

func TestManagedDelayedCredentialWriteCannotOutliveRemoval(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	repo := f.managedCapture(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	staging, resume := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer release()
	f.vault.createHook = func(_ string, written bool) error {
		if !written && held.CompareAndSwap(false, true) {
			close(staging)
			select {
			case <-resume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	raw := systemManagedCreateBody(systemManagedSecret)
	stale := make(chan error, 1)
	go func() {
		_, err := f.services.connections.Submit(ctx, f.actor, "app", "", "delayed-create-key", raw)
		stale <- err
	}()
	select {
	case <-staging:
	case <-ctx.Done():
		t.Fatal("credential staging did not pause")
	}
	finished := make(chan error, 1)
	go func() {
		op, err := f.services.connections.Submit(ctx, f.actor, "app", "", "delayed-create-key", raw)
		if err == nil {
			_, err = f.services.connections.RunOnce(ctx)
		}
		if err == nil {
			_, err = f.services.connections.Submit(ctx, f.actor, "app", op.ConnectionID, "delayed-remove-key", systemManagedActionBody("remove", op.ConnectionRevision, ""))
		}
		if err == nil {
			_, err = f.services.connections.RunOnce(ctx)
		}
		finished <- err
	}()
	var early bool
	select {
	case err := <-finished:
		early = true
		if err != nil {
			t.Fatal("concurrent lifecycle failed", err)
		}
	case <-time.After(100 * time.Millisecond):
	}
	release()
	if err := <-stale; err != nil {
		t.Fatal("delayed credential write failed", err)
	}
	if !early {
		if err := <-finished; err != nil {
			t.Fatal("concurrent lifecycle failed", err)
		}
	}
	if early {
		t.Fatal("removal crossed an unfinished credential write")
	}
	f.vault.mu.Lock()
	remaining := len(f.vault.entries)
	f.vault.mu.Unlock()
	record := repo.record(t)
	current := f.managedGet(t, "app", record.Operation.ConnectionID)
	if remaining != 1 || current.NewWork != "removed" {
		t.Fatal("delayed write left an orphan credential after removal")
	}
}

func TestManagedPartialVaultRemovalKeepsFenceAndRetryIntent(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	initial := f.managedCreate(t, "app", "remove-create-key", systemManagedSecret)
	f.managedAction(t, "app", initial, "remove-rotation-key", "replace_credential", systemManagedRotated)
	f.managedRun(t)
	current := f.managedGet(t, "app", initial.ID)
	if f.vaultEntries() != 3 {
		t.Fatal("fixture needs fingerprint key and two credential generations")
	}
	remove := f.managedAction(t, "app", current, "remove-operation-key", "remove", "")
	f.vault.mu.Lock()
	f.vault.failDeleteAt = f.vault.deletes + 2
	f.vault.mu.Unlock()
	f.managedRun(t)
	failed, err := f.services.connections.Operation(context.Background(), f.actor, "app", remove.ID)
	if err != nil || failed.Status != "failed" || failed.Problem == nil || *failed.Problem != "credential_store_unavailable" {
		t.Fatal("partial deletion reported success", failed, err)
	}
	c := f.managedGet(t, "app", initial.ID)
	if c.NewWork != "disabled" || c.Selection != nil {
		t.Fatal("partial deletion lifted admission fence")
	}
	if _, err = f.services.connections.Submit(context.Background(), f.actor, "app", c.ID, "enable-during-removal", systemManagedActionBody("enable", c.Revision, "")); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("partial deletion admitted another action", err)
	}
	f.reopen(t)
	f.vault.mu.Lock()
	f.vault.failDeleteAt = 0
	f.vault.mu.Unlock()
	f.advance(t, 31*time.Second)
	f.managedRun(t)
	done, err := f.services.connections.Operation(context.Background(), f.actor, "app", remove.ID)
	if err != nil || done.Status != "succeeded" {
		t.Fatal("exact removal did not resume", done, err)
	}
	c = f.managedGet(t, "app", c.ID)
	if c.NewWork != "removed" || c.CredentialPresent || c.Selection != nil {
		t.Fatal("removal not tombstoned")
	}
	if err = f.services.connections.WithCredential(context.Background(), ports.CredentialRef("vault:"+c.ID), func([]byte) error { t.Fatal("removed credential callback called"); return nil }); !errors.Is(err, credentials.ErrUnavailable) {
		t.Fatal("removed slot resolved", err)
	}
	if f.vaultEntries() != 1 {
		t.Fatal("removal retained credential bytes")
	}
	replay, err := f.services.connections.Submit(context.Background(), f.actor, "app", initial.ID, "remove-operation-key", systemManagedActionBody("remove", current.Revision, ""))
	if err != nil || !replay.Replay || replay.Status != "accepted" || replay.ID != remove.ID {
		t.Fatal("remove replay lost original receipt", err)
	}
}

func TestManagedRestartReservedCredentialIntent(t *testing.T) {
	for _, afterWrite := range []bool{false, true} {
		name := "before_vault_write"
		if afterWrite {
			name = "after_vault_write"
		}
		t.Run(name, func(t *testing.T) {
			f := newManagedLifecycleFixture(t)
			repo := f.managedCapture(t, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.vault.createHook = func(_ string, written bool) error {
				if written == afterWrite {
					cancel()
					return context.Canceled
				}
				return nil
			}
			if _, err := f.services.connections.Submit(ctx, f.actor, "app", "", "crash-reserved-key", systemManagedCreateBody(systemManagedSecret)); !errors.Is(err, context.Canceled) {
				t.Fatal("injected interruption lost", err)
			}
			record := repo.record(t)
			opID, connectionID, key := string(record.Operation.ID), record.Operation.ConnectionID, record.CredentialKey
			if record.Stage != "waiting_secret" {
				t.Fatal("reservation did not precede the vault write")
			}
			f.vault.mu.Lock()
			_, present := f.vault.entries[key]
			f.vault.createHook = nil
			f.vault.mu.Unlock()
			if present != afterWrite {
				t.Fatal("wrong simulated cross-resource boundary")
			}
			f.reopen(t)
			if f.adapter.checkCount() != 0 {
				t.Fatal("restart made provider call")
			}
			// An old reservation lacking secret bytes remains recoverable by exact replay.
			// A worker may inspect it, but cannot convert absent credentials into verification.
			f.advance(t, 31*time.Second)
			f.managedRun(t)
			if f.adapter.checkCount() != 0 {
				t.Fatal("reservation recovery invoked provider verification")
			}
			replay, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "crash-reserved-key", systemManagedCreateBody(systemManagedSecret))
			if err != nil || !replay.Replay || string(replay.ID) != opID || replay.ConnectionID != connectionID {
				t.Fatal("replay changed reserved identity", err)
			}
			f.managedRun(t)
			current := f.managedGet(t, "app", connectionID)
			if current.Authentication != "verified" || current.Selection == nil {
				t.Fatal("exact replay did not complete reserved credential write")
			}
		})
	}
}

func TestManagedVaultWriteReadyCommitLostAcknowledgement(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	repo := f.managedCapture(t, true)
	service := f.services.connections
	var err error
	if _, err = service.Submit(context.Background(), f.actor, "app", "", "ready-lost-ack-key", systemManagedCreateBody(systemManagedSecret)); err == nil {
		t.Fatal("fault did not lose acknowledgement")
	}
	record := repo.record(t)
	id := string(record.Operation.ID)
	if repo.ready != record.Operation.ID {
		t.Fatal("ready acknowledgement was not captured")
	}
	f.reopen(t)
	replay, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "ready-lost-ack-key", systemManagedCreateBody(systemManagedSecret))
	if err != nil || !replay.Replay || string(replay.ID) != id {
		t.Fatal("lost acknowledgement created new operation", err)
	}
	f.managedRun(t)
	if f.adapter.checkCount() != 1 {
		t.Fatal("lost acknowledgement duplicated generation")
	}
	if f.managedResolvedSecret(t, "vault:"+replay.ConnectionID) != systemManagedSecret {
		t.Fatal("original staged credential changed")
	}
}

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

func TestManagedRemovalRejectsQueuedAmbiguousAndCollectionDependencies(t *testing.T) {
	for _, phase := range []string{"queued", "ambiguous", "collection"} {
		t.Run(phase, func(t *testing.T) {
			f := newManagedLifecycleFixture(t)
			scenario := fake.DefaultScenario()
			scenario.GPU = domain.CapabilitySupportSupported
			if phase == "ambiguous" {
				scenario.Mode = fake.Unresolved
			}
			f.useFake(t, scenario)
			connection := f.managedCreate(t, "app", "dependent-create-key", systemManagedSecret)
			receipt, _ := f.managedAdmit(t, "app", connection, "dependent-job-key")
			if phase != "queued" {
				f.authorizeJob(t, receipt)
				f.dispatchOnce(t)
				f.dispatchOnce(t)
				if phase == "ambiguous" {
					target, err := f.host.store.ReadLogAttempt(t.Context(), "app", f.actor.TokenID(), receipt.JobID, receipt.AttemptID)
					if err != nil || target.Remote != nil || f.backend.Stats().SubmitCalls != 1 {
						t.Fatal("fixture did not preserve submission uncertainty", err)
					}
				} else {
					f.runToCollection(t, receipt)
					record, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), receipt.JobID)
					if err != nil || record.Attempt.State.Orchestration != domain.OrchestrationCollecting {
						t.Fatal("fixture lacks retained collection dependency", record.Attempt.State, err)
					}
				}
			}
			before := f.vault.deletes
			_, err := f.services.connections.Submit(t.Context(), f.actor, "app", connection.ID, "blocked-remove-key", systemManagedActionBody("remove", connection.Revision, ""))
			if !errors.Is(err, connections.ErrActiveWork) {
				t.Fatal("dependent removal accepted", phase, err)
			}
			current := f.managedGet(t, "app", connection.ID)
			if current.Revision != connection.Revision || current.NewWork != "enabled" || f.vault.deletes != before {
				t.Fatal("rejected removal changed connection")
			}
		})
	}
}
func TestManagedResourceRecheckCannotReassignAcceptedWork(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	base := f.adapter.verifyHook
	f.adapter.descriptorHook = func(d connections.Descriptor) connections.Descriptor {
		d.Capabilities.Concurrency = &connections.ConcurrencyCapability{Support: "supported", Scope: "resource", Minimum: 1, Maximum: 64, Default: 1}
		return d
	}
	changed := false
	f.adapter.verifyHook = func(ctx context.Context, value []byte) (connections.Verification, error) {
		result, err := base(ctx, value)
		native := strings.Repeat("a", 64)
		if changed {
			native = strings.Repeat("c", 64)
		}
		result.Resource = &connections.ResourceIdentity{Kind: "machine", ID: "resource_" + native}
		return result, err
	}
	connection := f.managedCreate(t, "app", "bound-lan", systemManagedSecret)
	receipt, before := f.managedAdmit(t, "app", connection, "bound-job")
	f.authorizeJob(t, receipt)
	changed = true
	op := f.managedAction(t, "app", connection, "changed-native-identity", "check", "")
	f.managedRun(t)
	result, err := f.services.connections.Operation(t.Context(), f.actor, "app", op.ID)
	if err != nil || result.Status != "failed" || result.Problem == nil || *result.Problem != "resource_changed" {
		t.Fatal("recheck silently moved the connection", result, err)
	}
	after, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), receipt.JobID)
	if err != nil || before.Profile != after.Profile || before.Job != after.Job {
		t.Fatal("identity mismatch rewrote accepted work", err)
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "synthetic-state-secret") {
		t.Fatal("credential reached resource failure projection")
	}
}

func TestManagedFailedVaultWriteSanitizedAndNoVerification(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	// Establish the HMAC key independently before denying the credential write.
	f.managedCreate(t, "app", "vault-failure-bootstrap", systemManagedSecret)
	f.vault.mu.Lock()
	f.vault.failCreate = errors.New("synthetic-secret-bearing-error-canary")
	f.vault.mu.Unlock()
	op, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "failed-secret-write", systemManagedCreateBody(systemManagedOther))
	requireNoError(t, err)
	status, err := f.services.connections.Operation(context.Background(), f.actor, "app", op.ID)
	if err != nil || status.Status != "failed" || status.Problem == nil || strings.Contains(*status.Problem, "canary") {
		t.Fatal("failed vault write lost sanitized outcome", status, err)
	}
	before := f.adapter.checkCount()
	_, err = f.services.connections.RunOnce(context.Background())
	requireNoError(t, err)
	if f.adapter.checkCount() != before {
		t.Fatal("absent credential reached account verification")
	}
}
