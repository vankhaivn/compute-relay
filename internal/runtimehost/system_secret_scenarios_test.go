package runtimehost

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

func TestSystemSecretRestartRequiresExplicitResupplyAndConsumesOnce(t *testing.T) {
	ctx := context.Background()
	f, _, trace, id, gate, p, ready, secrets, pool := systemSecretFixture(t, fake.AcceptLoseResponse)
	if ready.Secrets == nil || ready.Secrets.Status != "missing" {
		t.Fatal("missing slots not exposed")
	}
	if _, err := gate.Start(ctx, p, "app", id.JobID, "no-material-start", systemStartGrantRaw(f, id, ready)); !errors.Is(err, workloadsecret.ErrUnavailable) {
		t.Fatal("start without slots", err)
	}
	raw := systemSecretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	startRaw := systemSecretStartRequest(f, id, ready, receipt.AuthorizationID)
	if _, err = gate.Start(ctx, p, "app", id.JobID, "start-original", startRaw); err != nil {
		t.Fatal(err)
	}
	assertSystemSecretSanitized(t, f, id)
	pool.Close()
	f.reopen(t)
	gate, p = systemAuthorizationService(t, f, "app", auth.Read, auth.Execute)
	secrets, pool = f.services.secrets, f.host.secretPool
	if _, err = systemGateStep(t, f); err != nil || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("memory loss started payload", err)
	}
	state, err := gate.Readiness(ctx, p, "app", id.JobID)
	if err != nil || state.Secrets.Status != "material_lost" {
		t.Fatal("material loss invisible", err)
	}
	replay, err := secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw)
	if err != nil || !replay.Replay || replay.AuthorizationID != receipt.AuthorizationID {
		t.Fatal("explicit resupply failed", err)
	}
	if _, err = systemGateStep(t, f); err != nil {
		t.Fatal(err)
	}
	if trace.journal(t, id).Phase != dispatch.Submitting || f.backend.Stats().Executions != 1 {
		t.Fatal("lost ack was not retained")
	}
	if pool.Available(workloadsecret.Key{WorkspaceID: "app", JobID: id.JobID, AttemptID: id.AttemptID}, receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("used material retained")
	}
	current, err := secrets.Get(ctx, p, "app", id.JobID, receipt.AuthorizationID)
	if err != nil || current.Status != "consumed" {
		t.Fatal("missing consumption", err)
	}
	if _, err = secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw); err != nil {
		t.Fatal(err)
	}
	if pool.Available(current.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("used replay restored material")
	}
	pool.Close()
	f.reopen(t)
	if _, err = systemGateStep(t, f); err != nil || f.backend.Stats().SubmitCalls != 1 || trace.journal(t, id).Phase != dispatch.Submitted {
		t.Fatal("recovery repeated delivery/start", err)
	}
	assertSystemSecretSanitized(t, f, id)
}

func TestSystemSecretRevokeDuringRemoteVerificationPreventsStart(t *testing.T) {
	ctx := context.Background()
	f, effects, trace, id, gate, p, ready, secrets, pool := systemSecretFixture(t, fake.Accept)
	raw := systemSecretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Start(ctx, p, "app", id.JobID, "start-original", systemSecretStartRequest(f, id, ready, receipt.AuthorizationID)); err != nil {
		t.Fatal(err)
	}
	revokeRaw, _ := json.Marshal(map[string]any{"attempt_id": id.AttemptID})
	effects.afterPreparationRead = func() {
		effects.afterPreparationRead = nil
		if _, err := secrets.Revoke(ctx, p, "app", id.JobID, receipt.AuthorizationID, "revoke-original", revokeRaw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = systemGateStep(t, f); err != nil || f.backend.Stats().SubmitCalls != 0 || trace.journal(t, id).SubmitStarted {
		t.Fatal("revocation lost start race", err)
	}
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("revoked bytes retained")
	}
	replay, err := secrets.Revoke(ctx, p, "app", id.JobID, receipt.AuthorizationID, "revoke-original", revokeRaw)
	if err != nil || !replay.Replay || replay.Status != "revoked" {
		t.Fatal("revocation replay", err)
	}
	if _, err = secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw); err != nil || pool.Available(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("revoked replay rearmed", err)
	}
	assertSystemSecretSanitized(t, f, id)
}

func TestSystemSecretSupplyConflictScopesAndExpiry(t *testing.T) {
	ctx := context.Background()
	f, _, _, id, gate, p, ready, secrets, pool := systemSecretFixture(t, fake.Accept)
	raw := systemSecretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(raw, []byte(base64.StdEncoding.EncodeToString([]byte(systemWorkloadCanary))), []byte(base64.StdEncoding.EncodeToString([]byte("changed-value"))), 1)
	if _, err = secrets.Supply(ctx, p, "app", id.JobID, "secret-original", changed); !errors.Is(err, workloadsecret.ErrConflict) {
		t.Fatal("changed replay", err)
	}
	if _, err = secrets.Supply(ctx, p, "app", id.JobID, "secret-new-key", raw); !errors.Is(err, workloadsecret.ErrConflict) {
		t.Fatal("new supply rearmed", err)
	}
	_, read := systemAuthorizationService(t, f, "app", auth.Read)
	if _, err = secrets.Supply(ctx, read, "app", id.JobID, "secret-original", raw); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope supplied values", err)
	}
	if _, err = secrets.Get(ctx, read, "app", id.JobID, receipt.AuthorizationID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope read write-only receipt", err)
	}
	if _, err = secrets.Supply(ctx, p, "other", id.JobID, "secret-original", raw); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("foreign workspace", err)
	}
	f.advance(t, time.Minute)
	if _, err = gate.Start(ctx, p, "app", id.JobID, "expired-material", systemSecretStartRequest(f, id, ready, receipt.AuthorizationID)); !errors.Is(err, workloadsecret.ErrUnavailable) {
		t.Fatal("expired supply usable", err)
	}
	if _, err = secrets.Supply(ctx, p, "app", id.JobID, "secret-original", raw); err != nil || pool.Available(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("expired replay refreshed material", err)
	}
	assertSystemSecretSanitized(t, f, id)
}

func TestSystemSecretCrashAfterConsumptionNeverRestoresOrSends(t *testing.T) {
	ctx := context.Background()
	f, _, trace, id, gate, p, ready, secrets, pool := systemSecretFixture(t, fake.Accept)
	raw := systemSecretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "app", id.JobID, "crash-secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Start(ctx, p, "app", id.JobID, "crash-start-original", systemSecretStartRequest(f, id, ready, receipt.AuthorizationID)); err != nil {
		t.Fatal(err)
	}
	f.advance(t, 10*time.Millisecond)
	claim, err := f.host.store.ClaimRecovery(ctx, "secret_crash_worker")
	if err != nil || claim == nil {
		t.Fatal("missing claim", err)
	}
	work, err := f.host.store.LoadDispatch(ctx, *claim)
	if err != nil {
		t.Fatal(err)
	}
	material, err := pool.Claim(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now(), func() error {
		_, err := f.host.store.CommitDispatch(ctx, work.Handle, dispatch.Action{Kind: dispatch.BeginSubmission, SecretsAvailable: true})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	material.Clear()
	// Crash before the unique provider invocation leaves intent uncertain, not reusable.
	pool.Close()
	f.reopen(t)
	gate, p = systemAuthorizationService(t, f, "app", auth.Read, auth.Execute)
	secrets, pool = f.services.secrets, f.host.secretPool
	f.advance(t, time.Minute)
	if _, err = systemGateStep(t, f); err == nil || !strings.HasPrefix(err.Error(), "PROVIDER_SUBMISSION_UNKNOWN:") {
		t.Fatal("crash lost uncertainty", err)
	}
	current, err := secrets.Get(ctx, p, "app", id.JobID, receipt.AuthorizationID)
	if err != nil || current.Status != "consumed" {
		t.Fatal("consumption lost", err)
	}
	if replay, err := secrets.Supply(ctx, p, "app", id.JobID, "crash-secret-original", raw); err != nil || !replay.Replay {
		t.Fatal("original replay lost", err)
	}
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) || f.backend.Stats().SubmitCalls != 0 || !trace.journal(t, id).SubmitStarted || f.dispatchState(t, id).RemoteActivity != domain.RemoteActivityPossible {
		t.Fatal("crash rearmed material or execution")
	}
	assertSystemSecretSanitized(t, f, id)
}

func TestSystemSecretCancellationClearsMaterialAndBlocksExactResupply(t *testing.T) {
	ctx := context.Background()
	f, _, _, id, _, p, ready, secrets, pool := systemSecretFixture(t, fake.Accept)
	raw := systemSecretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "app", id.JobID, "cancel-secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	control, op, _ := systemControlService(t, f, "app", auth.Operate)
	systemCancelControl(t, f, control, op, id)
	pool.Sweep(f.fixtureClock().Now(), func(key workloadsecret.Key, authID domain.OperationID) bool {
		usable, err := f.host.store.WorkloadSecretsUsable(ctx, key, authID)
		return err == nil && usable
	})
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("cancelled bytes retained")
	}
	if _, err = secrets.Supply(ctx, p, "app", id.JobID, "cancel-secret-original", raw); err != nil {
		t.Fatal("cancelled original receipt lost", err)
	}
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.fixtureClock().Now()) || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("cancelled replay restored bytes")
	}
	assertSystemSecretSanitized(t, f, id)
}

func TestSystemSecretConcurrentSupplyCoalescesBeforeOneStart(t *testing.T) {
	ctx := context.Background()
	f, _, _, id, gate, p, ready, secrets, pool := systemSecretFixture(t, fake.Accept)
	raw := systemSecretRequest(f, id, ready)
	var wg sync.WaitGroup
	receipts := make(chan workloadsecret.Receipt, 8)
	failures := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := secrets.Supply(ctx, p, "app", id.JobID, "concurrent-secret-original", raw)
			receipts <- r
			failures <- err
		}()
	}
	wg.Wait()
	close(receipts)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first workloadsecret.Receipt
	originals := 0
	for receipt := range receipts {
		if first.AuthorizationID == "" {
			first = receipt
		}
		if receipt.AuthorizationID != first.AuthorizationID {
			t.Fatal("multiple secret authorities")
		}
		if !receipt.Replay {
			originals++
		}
	}
	if originals != 1 {
		t.Fatal("multiple original receipts")
	}
	if _, err := gate.Start(ctx, p, "app", id.JobID, "concurrent-secret-start", systemSecretStartRequest(f, id, ready, first.AuthorizationID)); err != nil {
		t.Fatal(err)
	}
	if _, err := systemGateStep(t, f); err != nil {
		t.Fatal(err)
	}
	if f.backend.Stats().SubmitCalls != 1 || pool.Available(first.Key(), first.AuthorizationID, f.fixtureClock().Now()) {
		t.Fatal("concurrent supply repeated start or retained bytes")
	}
	assertSystemSecretSanitized(t, f, id)
}
