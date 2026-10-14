package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestManagedReservedIntentIdentityAndGenerationRowsSurviveRestart(t *testing.T) {
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
			// An old reservation lacking secret bytes remains recoverable by exact replay.
			// A worker may inspect it, but cannot convert absent credentials into verification.
			f.clock.advance(31 * time.Second)
			f.run(t)
			replay, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "crash-reserved-key", managedCreateBody(managedStateSecret))
			if err != nil || !replay.Replay || string(replay.ID) != opID || replay.ConnectionID != connectionID {
				t.Fatal("replay changed reserved identity", err)
			}
			f.run(t)
			if controlCount(t, f.s, "SELECT count(*) FROM managed_connections") != 1 || controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 1 {
				t.Fatal("restart created another connection/generation")
			}
		})
	}
}

func TestManagedMissingFingerprintPreservesInitializedStateAndRows(t *testing.T) {
	f := newManagedStateFixture(t)
	f.create(t, "a", "fingerprint-original-key", managedStateSecret)
	if err := f.vault.Delete(context.Background(), "request_fingerprint_v1"); err != nil {
		t.Fatal(err)
	}
	f.restartManaged(t)
	for _, key := range []string{"fingerprint-original-key", "fingerprint-new-key"} {
		if _, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", key, managedCreateBody(managedStateSecret)); !errors.Is(err, connections.ErrVault) {
			t.Fatal("missing HMAC key became fresh operation", err)
		}
	}
	if controlCount(t, f.s, "SELECT count(*) FROM managed_connection_operations") != 1 || controlCount(t, f.s, "SELECT count(*) FROM managed_connections") != 1 {
		t.Fatal("HMAC key loss duplicated state")
	}
	f.vault.mu.Lock()
	_, present := f.vault.values["request_fingerprint_v1"]
	f.vault.mu.Unlock()
	if present {
		t.Fatal("missing HMAC key silently regenerated")
	}
}

func TestManagedPartialRemovalKeepsDurableIntentAndGenerationRows(t *testing.T) {
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
	var pending, stage string
	if err = f.s.db.QueryRow("SELECT pending_operation FROM managed_connections WHERE connection_id=?", initial.ID).Scan(&pending); err != nil || pending != string(remove.ID) {
		t.Fatal("deletion intent lost", err)
	}
	if err = f.s.db.QueryRow("SELECT stage FROM managed_connection_operations WHERE operation_id=?", string(remove.ID)).Scan(&stage); err != nil || stage != "ready" {
		t.Fatal("partial deletion no longer retryable", err)
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
	if controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 0 {
		t.Fatal("removed credential generation retained")
	}

	if controlCount(t, f.s, "SELECT count(*) FROM managed_connections WHERE new_work='removed'") != 1 {
		t.Fatal("removed connection tombstone was not retained")
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
func TestManagedReadyCommitKeepsOriginalCredentialGeneration(t *testing.T) {
	f := newManagedStateFixture(t)
	repo := &managedReadyLostAck{Store: f.s}
	service, err := connections.New(f.access, repo, f.vault, []connections.Adapter{f.discovery})
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
	if controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 1 {
		t.Fatal("ready acknowledgement loss duplicated generations")
	}
	active, err := f.s.ResolveConnectionCredential(context.Background(), replay.ConnectionID)
	if err != nil || active != credential {
		t.Fatal("ready acknowledgement loss replaced the original generation", err)
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

func TestManagedReceiptRowsSurviveAdapterPolicyChanges(t *testing.T) {
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
				f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{changed})
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
			})
		}
	}
}

func TestManagedWaitingSecretRowsRemainReservedWithoutAdapter(t *testing.T) {
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
	service, err := connections.New(f.access, f.s, f.vault, []connections.Adapter{changed})
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

func TestManagedRemovedConnectionRejectsLateCredentialReady(t *testing.T) {
	f := newManagedStateFixture(t)
	op, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "late-ready-create", managedCreateBody(managedStateSecret))
	if err != nil {
		t.Fatal(err)
	}
	f.run(t)
	current := f.get(t, "a", op.ConnectionID)
	f.action(t, "a", current, "late-ready-remove", "remove", "")
	f.run(t)
	if err := f.s.ReadyConnection(context.Background(), "a", f.principals["a"].TokenID(), op.ID); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("late ready resurrected completed credential intent", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 0 || controlCount(t, f.s, "SELECT count(*) FROM managed_connections WHERE new_work='removed'") != 1 {
		t.Fatal("late ready left an orphan generation or lost the tombstone")
	}
}

func TestManagedSecretDeletionCannotRemoveActiveGeneration(t *testing.T) {
	f := newManagedStateFixture(t)
	initial := f.create(t, "a", "guard-generation-create", managedStateSecret)
	oldKey, err := f.s.ResolveConnectionCredential(context.Background(), initial.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.action(t, "a", initial, "guard-generation-replace", "replace_credential", managedStateRotated)
	f.run(t)
	activeKey, err := f.s.ResolveConnectionCredential(context.Background(), initial.ID)
	if err != nil || activeKey == oldKey {
		t.Fatal("rotation did not freeze distinct credential generations", err)
	}
	if err := f.s.CompleteSecretDeletion(context.Background(), activeKey); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("active generation deletion accepted", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 2 {
		t.Fatal("refused deletion changed generations")
	}
	key, found, err := f.s.NextSecretDeletion(context.Background())
	if err != nil || !found || key != oldKey {
		t.Fatal("deletion selected the active or unrelated generation", key, err)
	}
	if err := f.s.CompleteSecretDeletion(context.Background(), oldKey); err != nil {
		t.Fatal(err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM managed_credential_generations") != 1 {
		t.Fatal("old generation deletion did not preserve the current row")
	}
	retained, err := f.s.ResolveConnectionCredential(context.Background(), initial.ID)
	if err != nil || retained != activeKey {
		t.Fatal("deletion moved active credential ownership", err)
	}
}
