package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

func TestManagedCredentialReadSurvivesRotationAndReleasesGuardBeforeCallback(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	initial := f.managedCreate(t, "app", "read-rotation-create", systemManagedSecret)
	f.managedAction(t, "app", initial, "read-rotation-replace", "replace_credential", systemManagedRotated)
	repo := &systemManagedPausedResolution{Store: f.host.store, resolved: make(chan struct{}), resume: make(chan struct{})}
	var err error
	f.services.connections, err = connections.New(f.host.access, repo, f.vault, []connections.Adapter{f.adapter})
	if err != nil {
		t.Fatal(err)
	}
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
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
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

func TestManagedFailedVaultWriteSanitizedAndNoVerification(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	// Establish the HMAC key independently before denying the credential write.
	f.managedCreate(t, "app", "vault-failure-bootstrap", systemManagedSecret)
	f.vault.mu.Lock()
	f.vault.failCreate = errors.New("synthetic-secret-bearing-error-canary")
	f.vault.mu.Unlock()
	op, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "failed-secret-write", systemManagedCreateBody(systemManagedOther))
	if err != nil {
		t.Fatal(err)
	}
	status, err := f.services.connections.Operation(context.Background(), f.actor, "app", op.ID)
	if err != nil || status.Status != "failed" || status.Problem == nil || strings.Contains(*status.Problem, "canary") {
		t.Fatal("failed vault write lost sanitized outcome", status, err)
	}
	before := f.adapter.checkCount()
	_, err = f.services.connections.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if f.adapter.checkCount() != before {
		t.Fatal("absent credential reached account verification")
	}
}

func TestManagedMissingFingerprintKeyFailsClosed(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	c := f.managedCreate(t, "app", "fingerprint-original-key", systemManagedSecret)
	if err := f.vault.Delete(context.Background(), "request_fingerprint_v1"); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	before := f.adapter.checkCount()
	for _, key := range []string{"fingerprint-original-key", "fingerprint-new-key"} {
		if _, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", key, systemManagedCreateBody(systemManagedSecret)); !errors.Is(err, connections.ErrVault) {
			t.Fatal("missing HMAC key became fresh operation", err)
		}
	}
	if f.managedGet(t, "app", c.ID).Authentication != "verified" || f.adapter.checkCount() != before {
		t.Fatal("HMAC loss discarded prior verified state")
	}
	f.vault.mu.Lock()
	_, present := f.vault.entries["request_fingerprint_v1"]
	f.vault.mu.Unlock()
	if present {
		t.Fatal("missing HMAC key silently regenerated")
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

func TestManagedReplaySurvivesAdapterChanges(t *testing.T) {
	for _, stage := range []string{"ready", "completed"} {
		for _, change := range []string{"removed", "tightened"} {
			t.Run(stage+"_"+change, func(t *testing.T) {
				f := newManagedLifecycleFixture(t)
				raw := systemManagedCreateBody(systemManagedSecret)
				original, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "adapter-change-key", raw)
				if err != nil {
					t.Fatal(err)
				}
				if stage == "completed" {
					f.managedRun(t)
				}
				changed := systemManagedChangedDescriptor{managedTestAdapter: f.adapter, removed: change == "removed"}
				f.services.connections, err = connections.New(f.host.access, f.host.store, f.vault, []connections.Adapter{changed})
				if err != nil {
					t.Fatal(err)
				}
				replay, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "adapter-change-key", raw)
				original.Replay = true
				if err != nil || !reflect.DeepEqual(replay, original) {
					t.Fatal("adapter change lost original receipt", err)
				}
				if _, err = f.services.connections.Submit(context.Background(), f.actor, "app", "", "adapter-change-key", systemManagedCreateBody(systemManagedRotated)); !errors.Is(err, connections.ErrConflict) {
					t.Fatal("adapter change bypassed keyed fingerprint conflict", err)
				}
				want := connections.ErrRequest
				if change == "removed" {
					want = connections.ErrUnsupported
				}
				if _, err = f.services.connections.Submit(context.Background(), f.actor, "app", "", "adapter-change-fresh-key", raw); !errors.Is(err, want) {
					t.Fatal("fresh request bypassed current adapter policy", err)
				}
				if err = f.host.access.Revoke(context.Background(), f.actor.TokenID()); err != nil {
					t.Fatal(err)
				}
				if _, err = f.services.connections.Submit(context.Background(), f.actor, "app", "", "adapter-change-key", raw); err == nil {
					t.Fatal("saved replay bypassed revoked management authority")
				}
			})
		}
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

func TestManagedWaitingSecretReplayRequiresInstalledAdapter(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	repo := f.managedCapture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	f.vault.createHook = func(_ string, written bool) error {
		if !written {
			cancel()
			return context.Canceled
		}
		return nil
	}
	raw := systemManagedCreateBody(systemManagedSecret)
	if _, err := f.services.connections.Submit(ctx, f.actor, "app", "", "adapter-reserved-key", raw); !errors.Is(err, context.Canceled) {
		t.Fatal("reservation interruption lost", err)
	}
	f.vault.createHook = nil
	changed := systemManagedChangedDescriptor{managedTestAdapter: f.adapter, removed: true}
	service, err := connections.New(f.host.access, f.host.store, f.vault, []connections.Adapter{changed})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Submit(context.Background(), f.actor, "app", "", "adapter-reserved-key", raw); !errors.Is(err, connections.ErrUnsupported) {
		t.Fatal("unstaged credentials bypassed missing adapter", err)
	}
	record := repo.record(t)
	status, err := f.services.connections.Operation(context.Background(), f.actor, "app", record.Operation.ID)
	if err != nil || status.Status != "accepted" || f.adapter.checkCount() != 0 {
		t.Fatal("unsupported replay changed original reserved operation", status, err)
	}
}
