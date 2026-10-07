package sqlite

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
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestManagedRestartReservedCredentialIntent(t *testing.T) {
	for _, afterWrite := range []bool{false, true} {
		name := "before_vault_write"
		if afterWrite {
			name = "after_vault_write"
		}
		t.Run(name, func(t *testing.T) {
			f := newManagedStateFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.vault.createHook = func(_ string, written bool) error {
				if written == afterWrite {
					cancel()
					return context.Canceled
				}
				return nil
			}
			if _, err := f.service.Submit(ctx, f.principals["a"], "a", "", "crash-reserved-key", managedCreateBody(managedStateSecret)); !errors.Is(err, context.Canceled) {
				t.Fatal("injected interruption lost", err)
			}
			var opID, connectionID, stage, key string
			if err := f.s.db.QueryRow("SELECT operation_id,connection_id,stage,credential_key FROM managed_connection_operations").Scan(&opID, &connectionID, &stage, &key); err != nil {
				t.Fatal(err)
			}
			if stage != "waiting_secret" {
				t.Fatal("interruption erased reserved credential intent", stage)
			}
			f.vault.mu.Lock()
			_, present := f.vault.values[key]
			f.vault.createHook = nil
			f.vault.mu.Unlock()
			if present != afterWrite {
				t.Fatal("wrong simulated cross-resource boundary")
			}
			f.restartManaged(t)
			if f.discovery.count() != 0 {
				t.Fatal("restart made provider call")
			}
			// An old reservation lacking secret bytes remains recoverable by exact replay.
			// A worker may inspect it, but cannot convert absent credentials into verification.
			f.clock.advance(31 * time.Second)
			f.run(t)
			if f.discovery.count() != 0 {
				t.Fatal("reservation recovery invoked provider verification")
			}
			replay, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "crash-reserved-key", managedCreateBody(managedStateSecret))
			if err != nil || !replay.Replay || string(replay.ID) != opID || replay.ConnectionID != connectionID {
				t.Fatal("replay changed reserved identity", err)
			}
			f.run(t)
			current := f.get(t, "a", connectionID)
			if current.Authentication != "verified" || current.Selection == nil {
				t.Fatal("exact replay did not complete reserved credential write")
			}
			if controlCount(t, f.s, "SELECT count(*) FROM managed_connections") != 1 || controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 1 {
				t.Fatal("restart created another connection/generation")
			}
		})
	}
}

func TestManagedMissingFingerprintKeyFailsClosed(t *testing.T) {
	f := newManagedStateFixture(t)
	c := f.create(t, "a", "fingerprint-original-key", managedStateSecret)
	if err := f.vault.Delete(context.Background(), "request_fingerprint_v1"); err != nil {
		t.Fatal(err)
	}
	f.restartManaged(t)
	before := f.discovery.count()
	for _, key := range []string{"fingerprint-original-key", "fingerprint-new-key"} {
		if _, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", key, managedCreateBody(managedStateSecret)); !errors.Is(err, connections.ErrVault) {
			t.Fatal("missing HMAC key became fresh operation", err)
		}
	}
	if controlCount(t, f.s, "SELECT count(*) FROM managed_connection_operations") != 1 || controlCount(t, f.s, "SELECT count(*) FROM managed_connections") != 1 {
		t.Fatal("HMAC key loss duplicated state")
	}
	if f.get(t, "a", c.ID).Authentication != "verified" || f.discovery.count() != before {
		t.Fatal("HMAC loss discarded prior verified state")
	}
	f.vault.mu.Lock()
	_, present := f.vault.values["request_fingerprint_v1"]
	f.vault.mu.Unlock()
	if present {
		t.Fatal("missing HMAC key silently regenerated")
	}
}

func TestManagedPartialVaultRemovalKeepsFenceAndRetryIntent(t *testing.T) {
	f := newManagedStateFixture(t)
	initial := f.create(t, "a", "remove-create-key", managedStateSecret)
	f.action(t, "a", initial, "remove-rotation-key", "replace_credential", managedStateRotated)
	f.run(t)
	current := f.get(t, "a", initial.ID)
	if controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 2 {
		t.Fatal("fixture needs two retained generations")
	}
	remove := f.action(t, "a", current, "remove-operation-key", "remove", "")
	f.vault.mu.Lock()
	f.vault.failDeleteAt = f.vault.deletes + 2
	f.vault.mu.Unlock()
	f.run(t)
	failed, err := f.service.Operation(context.Background(), f.principals["a"], "a", remove.ID)
	if err != nil || failed.Status != "failed" || failed.Problem == nil || *failed.Problem != "credential_store_unavailable" {
		t.Fatal("partial deletion reported success", failed, err)
	}
	c := f.get(t, "a", initial.ID)
	if c.NewWork != "disabled" || c.Selection != nil {
		t.Fatal("partial deletion lifted admission fence")
	}
	var pending, stage string
	if err = f.s.db.QueryRow("SELECT pending_operation FROM managed_connections WHERE connection_id=?", initial.ID).Scan(&pending); err != nil || pending != string(remove.ID) {
		t.Fatal("deletion intent lost", err)
	}
	if err = f.s.db.QueryRow("SELECT stage FROM managed_connection_operations WHERE operation_id=?", string(remove.ID)).Scan(&stage); err != nil || stage != "ready" {
		t.Fatal("partial deletion no longer retryable", err)
	}
	if _, err = f.service.Submit(context.Background(), f.principals["a"], "a", c.ID, "enable-during-removal", managedActionBody("enable", c.Revision, "")); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("partial deletion admitted another action", err)
	}
	f.restartManaged(t)
	f.vault.mu.Lock()
	f.vault.failDeleteAt = 0
	f.vault.mu.Unlock()
	f.clock.advance(31 * time.Second)
	f.run(t)
	done, err := f.service.Operation(context.Background(), f.principals["a"], "a", remove.ID)
	if err != nil || done.Status != "succeeded" {
		t.Fatal("exact removal did not resume", done, err)
	}
	c = f.get(t, "a", c.ID)
	if c.NewWork != "removed" || c.CredentialPresent || c.Selection != nil {
		t.Fatal("removal not tombstoned")
	}
	if err = f.service.WithCredential(context.Background(), ports.CredentialRef("vault:"+c.ID), func([]byte) error { t.Fatal("removed credential callback called"); return nil }); !errors.Is(err, credentials.ErrUnavailable) {
		t.Fatal("removed slot resolved", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 0 {
		t.Fatal("removed credential generation retained")
	}
	replay, err := f.service.Submit(context.Background(), f.principals["a"], "a", initial.ID, "remove-operation-key", managedActionBody("remove", current.Revision, ""))
	if err != nil || !replay.Replay || replay.Status != "accepted" || replay.ID != remove.ID {
		t.Fatal("remove replay lost original receipt", err)
	}
}

func TestManagedRemovalRejectsQueuedAmbiguousAndCollectionDependencies(t *testing.T) {
	for _, phase := range []string{"queued", "ambiguous", "collection"} {
		t.Run(phase, func(t *testing.T) {
			f := newManagedStateFixture(t)
			connection := f.create(t, "a", "dependent-create-key", managedStateSecret)
			receipt, record := f.admit(t, "a", connection, "dependent-job-key")
			if phase != "queued" {
				scenario := fake.DefaultScenario()
				scenario.GPU = domain.CapabilitySupportSupported
				if phase == "ambiguous" {
					scenario.Mode = fake.Unresolved
				}
				backend, err := fake.NewBackend(scenario)
				if err != nil {
					t.Fatal(err)
				}
				f.backend = backend
				f.profile = record.Profile
				attachManaged(t, f.dispatchFixture)
				permits, err := executionauth.New(f.access, f.s, f.clock)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = permits.Authorize(context.Background(), f.principals["a"], "a", receipt.JobID, "dependency-permit-key", authorizationRequest(receipt.AttemptID, 10)); err != nil {
					t.Fatal(err)
				}
				step := func() {
					f.clock.advance(30 * time.Second)
					worked, err := f.engine.RunOnce(context.Background(), "dependency_worker")
					if err != nil || !worked {
						t.Fatal("dependency fixture could not progress", worked, err)
					}
				}
				step()
				step()
				id := scheduler.Identity{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID}
				if phase == "ambiguous" {
					if f.journal(t, id).Phase != dispatch.Submitting {
						t.Fatal("fixture did not preserve ambiguity")
					}
				} else {
					for i := 0; i < 2; i++ {
						if err = f.adapter.Advance(*f.journal(t, id).Remote); err != nil {
							t.Fatal(err)
						}
						step()
					}
					if f.journal(t, id).Phase != dispatch.Collectible {
						t.Fatal("fixture lacks retained collection dependency")
					}
				}
			}
			before := f.vault.deletes
			_, err := f.service.Submit(context.Background(), f.principals["a"], "a", connection.ID, "blocked-remove-key", managedActionBody("remove", connection.Revision, ""))
			if !errors.Is(err, connections.ErrActiveWork) {
				t.Fatal("dependent connection removal accepted", phase, err)
			}
			current := f.get(t, "a", connection.ID)
			if current.Revision != connection.Revision || current.NewWork != "enabled" || f.vault.deletes != before {
				t.Fatal("rejected removal changed connection")
			}
		})
	}
}

func TestManagedFailedVaultWriteSanitizedAndNoVerification(t *testing.T) {
	f := newManagedStateFixture(t)
	// Establish the HMAC key independently before denying the credential write.
	f.create(t, "a", "vault-failure-bootstrap", managedStateSecret)
	f.vault.mu.Lock()
	f.vault.failCreate = errors.New("synthetic-secret-bearing-error-canary")
	f.vault.mu.Unlock()
	op, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "failed-secret-write", managedCreateBody(managedStateOther))
	if err != nil {
		t.Fatal(err)
	}
	status, err := f.service.Operation(context.Background(), f.principals["a"], "a", op.ID)
	if err != nil || status.Status != "failed" || status.Problem == nil || strings.Contains(*status.Problem, "canary") {
		t.Fatal("failed vault write lost sanitized outcome", status, err)
	}
	before := f.discovery.count()
	_, err = f.service.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if f.discovery.count() != before {
		t.Fatal("absent credential reached account verification")
	}
}

type managedReadyLostAck struct {
	*Store
	lost bool
}

func (r *managedReadyLostAck) ReadyConnection(ctx context.Context, w domain.WorkspaceID, token string, id domain.OperationID) error {
	if err := r.Store.ReadyConnection(ctx, w, token, id); err != nil {
		return err
	}
	if !r.lost {
		r.lost = true
		return errors.New("synthetic acknowledgement lost after ready commit")
	}
	return nil
}
func TestManagedVaultWriteReadyCommitLostAcknowledgement(t *testing.T) {
	f := newManagedStateFixture(t)
	repo := &managedReadyLostAck{Store: f.s}
	service, err := connections.New(f.access, repo, f.vault, []connections.Adapter{f.discovery}, f.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Submit(context.Background(), f.principals["a"], "a", "", "ready-lost-ack-key", managedCreateBody(managedStateSecret)); err == nil {
		t.Fatal("fault did not lose acknowledgement")
	}
	var id, credential, stage string
	if err = f.s.db.QueryRow("SELECT operation_id,credential_key,stage FROM managed_connection_operations").Scan(&id, &credential, &stage); err != nil || stage != "ready" {
		t.Fatal("ready commit not durable", err)
	}
	f.restartManaged(t)
	replay, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "ready-lost-ack-key", managedCreateBody(managedStateSecret))
	if err != nil || !replay.Replay || string(replay.ID) != id {
		t.Fatal("lost acknowledgement created new operation", err)
	}
	f.run(t)
	if f.discovery.count() != 1 || controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 1 {
		t.Fatal("lost acknowledgement duplicated generation")
	}
	if f.resolvedSecret(t, "vault:"+replay.ConnectionID) != managedStateSecret {
		t.Fatal("original staged credential changed")
	}
}

type managedChangedDescriptor struct {
	*stateAdapter
	removed bool
}

func (a managedChangedDescriptor) Descriptor() connections.Descriptor {
	d := a.stateAdapter.Descriptor()
	if a.removed {
		d.Type = "another_fixture"
	} else {
		d.Fields[0].MaxBytes = 1
	}
	return d
}

func TestManagedReplaySurvivesAdapterChanges(t *testing.T) {
	for _, stage := range []string{"ready", "completed"} {
		for _, change := range []string{"removed", "tightened"} {
			t.Run(stage+"_"+change, func(t *testing.T) {
				f := newManagedStateFixture(t)
				raw := managedCreateBody(managedStateSecret)
				original, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "adapter-change-key", raw)
				if err != nil {
					t.Fatal(err)
				}
				if stage == "completed" {
					f.run(t)
				}
				changed := managedChangedDescriptor{stateAdapter: f.discovery, removed: change == "removed"}
				f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{changed}, f.clock.Now)
				if err != nil {
					t.Fatal(err)
				}
				replay, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "adapter-change-key", raw)
				original.Replay = true
				if err != nil || !reflect.DeepEqual(replay, original) {
					t.Fatal("adapter change lost original receipt", err)
				}
				if _, err = f.service.Submit(context.Background(), f.principals["a"], "a", "", "adapter-change-key", managedCreateBody(managedStateRotated)); !errors.Is(err, connections.ErrConflict) {
					t.Fatal("adapter change bypassed keyed fingerprint conflict", err)
				}
				want := connections.ErrRequest
				if change == "removed" {
					want = connections.ErrUnsupported
				}
				if _, err = f.service.Submit(context.Background(), f.principals["a"], "a", "", "adapter-change-fresh-key", raw); !errors.Is(err, want) {
					t.Fatal("fresh request bypassed current adapter policy", err)
				}
				if controlCount(t, f.s, "SELECT count(*) FROM managed_connection_operations") != 1 {
					t.Fatal("adapter change created another operation")
				}
				if err = f.access.Revoke(context.Background(), f.principals["a"].TokenID()); err != nil {
					t.Fatal(err)
				}
				if _, err = f.service.Submit(context.Background(), f.principals["a"], "a", "", "adapter-change-key", raw); err == nil {
					t.Fatal("saved replay bypassed revoked management authority")
				}
			})
		}
	}
}

func TestManagedWaitingSecretReplayRequiresInstalledAdapter(t *testing.T) {
	f := newManagedStateFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	f.vault.createHook = func(_ string, written bool) error {
		if !written {
			cancel()
			return context.Canceled
		}
		return nil
	}
	raw := managedCreateBody(managedStateSecret)
	if _, err := f.service.Submit(ctx, f.principals["a"], "a", "", "adapter-reserved-key", raw); !errors.Is(err, context.Canceled) {
		t.Fatal("reservation interruption lost", err)
	}
	f.vault.createHook = nil
	changed := managedChangedDescriptor{stateAdapter: f.discovery, removed: true}
	service, err := connections.New(f.access, f.s, f.vault, []connections.Adapter{changed}, f.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Submit(context.Background(), f.principals["a"], "a", "", "adapter-reserved-key", raw); !errors.Is(err, connections.ErrUnsupported) {
		t.Fatal("unstaged credentials bypassed missing adapter", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM managed_connection_operations WHERE stage='waiting_secret'") != 1 || f.discovery.count() != 0 {
		t.Fatal("unsupported replay changed reserved intent")
	}
}

func TestManagedDelayedCredentialWriteCannotOutliveRemoval(t *testing.T) {
	f := newManagedStateFixture(t)
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
	raw := managedCreateBody(managedStateSecret)
	stale := make(chan error, 1)
	go func() {
		_, err := f.service.Submit(ctx, f.principals["a"], "a", "", "delayed-create-key", raw)
		stale <- err
	}()
	select {
	case <-staging:
	case <-ctx.Done():
		t.Fatal("credential staging did not pause")
	}
	finished := make(chan error, 1)
	go func() {
		op, err := f.service.Submit(ctx, f.principals["a"], "a", "", "delayed-create-key", raw)
		if err == nil {
			_, err = f.service.RunOnce(ctx)
		}
		if err == nil {
			_, err = f.service.Submit(ctx, f.principals["a"], "a", op.ConnectionID, "delayed-remove-key", managedActionBody("remove", op.ConnectionRevision, ""))
		}
		if err == nil {
			_, err = f.service.RunOnce(ctx)
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
	remaining := len(f.vault.values)
	f.vault.mu.Unlock()
	if remaining != 1 || controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 0 || controlCount(t, f.s, "SELECT count(*) FROM managed_connections WHERE new_work='removed'") != 1 {
		t.Fatal("delayed write left an orphan credential after removal")
	}
}

type managedPausedResolution struct {
	*Store
	resolved chan struct{}
	resume   chan struct{}
	once     sync.Once
}

func (r *managedPausedResolution) ResolveConnectionCredential(ctx context.Context, id string) (string, error) {
	key, err := r.Store.ResolveConnectionCredential(ctx, id)
	if err == nil {
		r.once.Do(func() {
			close(r.resolved)
			select {
			case <-r.resume:
			case <-ctx.Done():
			}
		})
	}
	return key, err
}

func TestManagedCredentialReadSurvivesRotationAndReleasesGuardBeforeCallback(t *testing.T) {
	f := newManagedStateFixture(t)
	initial := f.create(t, "a", "read-rotation-create", managedStateSecret)
	f.action(t, "a", initial, "read-rotation-replace", "replace_credential", managedStateRotated)
	repo := &managedPausedResolution{Store: f.s, resolved: make(chan struct{}), resume: make(chan struct{})}
	var err error
	f.service, err = connections.New(f.access, repo, f.vault, []connections.Adapter{f.discovery}, f.clock.Now)
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
		readDone <- f.service.WithCredential(ctx, ports.CredentialRef("vault:"+initial.ID), func(value []byte) error {
			if string(value) != managedStateSecret {
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
	if worked, err := f.service.RunOnce(ctx); err != nil || !worked {
		t.Fatal("rotation could not publish while a credential read was pending", err)
	}
	gcDone := make(chan error, 1)
	go func() { _, err := f.service.RunOnce(ctx); gcDone <- err }()
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
	if string(owned) != managedStateSecret || controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 1 {
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
	if f.resolvedSecret(t, "vault:"+initial.ID) != managedStateRotated {
		t.Fatal("stable reference did not move to replacement")
	}
}
