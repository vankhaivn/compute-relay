package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestManagedConcurrentReplayAndRevisionCAS(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	const count = 12
	receipts := make(chan connections.Operation, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "concurrent-create-key", systemManagedCreateBody(systemManagedSecret))
			receipts <- r
			errs <- err
		}()
	}
	wg.Wait()
	close(receipts)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	fresh := 0
	var first connections.Operation
	for r := range receipts {
		if first.ID == "" {
			first = r
		}
		if r.ID != first.ID {
			t.Fatal("concurrent replay changed operation")
		}
		if !r.Replay {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatal("concurrent create duplicated", fresh)
	}
	if _, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "concurrent-create-key", systemManagedCreateBody(systemManagedOther)); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("changed secret reused key", err)
	}
	if f.adapter.checkCount() != 0 {
		t.Fatal("POST invoked discovery")
	}
	f.managedRun(t)
	current := f.managedGet(t, "app", first.ConnectionID)
	errs = make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := f.services.connections.Submit(context.Background(), f.actor, "app", current.ID, fmt.Sprintf("revision-race-key-%d", i), systemManagedActionBody("check", current.Revision, ""))
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else if !errors.Is(err, connections.ErrConflict) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatal("revision had multiple winners", wins)
	}
	f.managedRun(t)
	latest := f.managedGet(t, "app", current.ID)
	if latest.Revision != current.Revision+1 || latest.Selection.Profile == current.Selection.Profile {
		t.Fatal("revision did not publish distinct alias")
	}
	before := f.adapter.checkCount()
	f.reopen(t)
	replay, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", "concurrent-create-key", systemManagedCreateBody(systemManagedSecret))
	if err != nil || !replay.Replay || replay.Status != "accepted" || replay.ID != first.ID {
		t.Fatal("original receipt lost after restart", err)
	}
	if f.adapter.checkCount() != before {
		t.Fatal("restart or replay reverified provider")
	}
}

func TestManagedCredentialRotationKeepsFrozenBindingAndRejectsAccountChange(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	initial := f.managedCreate(t, "app", "initial-connection-key", systemManagedSecret)
	receipt, job := f.managedAdmit(t, "app", initial, "original-managed-job")
	binding := provider.BindingSnapshot{Binding: job.Profile.Binding, AccountScope: job.Profile.AccountScope, CredentialRef: job.Profile.CredentialRef}
	if f.managedResolvedSecret(t, binding.CredentialRef) != systemManagedSecret {
		t.Fatal("initial resolver failed")
	}
	op := f.managedAction(t, "app", initial, "rotate-same-account", "replace_credential", systemManagedRotated)
	f.managedRun(t)
	rotated := f.managedGet(t, "app", initial.ID)
	if rotated.Selection == nil || rotated.Selection.Profile == initial.Selection.Profile || f.managedResolvedSecret(t, binding.CredentialRef) != systemManagedRotated {
		t.Fatal("rotation did not preserve stable slot")
	}
	current, err := f.host.store.ReadJob(context.Background(), "app", f.actor.TokenID(), receipt.JobID)
	if err != nil || !reflect.DeepEqual(current.Profile, job.Profile) {
		t.Fatal("rotation rewrote frozen profile", err)
	}
	frozen, err := f.host.store.ReadManagedBinding(context.Background(), binding)
	if err != nil || frozen.Profile.Binding != binding.Binding || frozen.CanonicalAccount != "fixture_account_alpha" {
		t.Fatal("old binding no longer resolves", err)
	}
	status, err := f.services.connections.Operation(context.Background(), f.actor, "app", op.ID)
	if err != nil || status.Status != "succeeded" {
		t.Fatal("rotation outcome", status, err)
	}
	f.advance(t, time.Millisecond)
	bad := f.managedAction(t, "app", rotated, "rotate-other-account", "replace_credential", systemManagedOther)
	f.managedRun(t)
	failed, err := f.services.connections.Operation(context.Background(), f.actor, "app", bad.ID)
	if err != nil || failed.Status != "failed" || failed.Problem == nil || *failed.Problem != "account_changed" {
		t.Fatal("account replacement not rejected", failed, err)
	}
	if f.managedResolvedSecret(t, binding.CredentialRef) != systemManagedRotated {
		t.Fatal("failed rotation replaced active generation")
	}
	if _, err = f.host.store.ReadManagedBinding(context.Background(), binding); err != nil {
		t.Fatal("failed rotation broke original binding", err)
	}
	retained := f.managedGet(t, "app", initial.ID)
	f.managedAction(t, "app", retained, "disable-old-binding", "disable", "")
	f.managedRun(t)
	if f.managedResolvedSecret(t, binding.CredentialRef) != systemManagedRotated {
		t.Fatal("disable discarded old credential access")
	}
	if _, err = f.host.store.ReadManagedBinding(context.Background(), binding); err != nil {
		t.Fatal("disable discarded frozen binding", err)
	}
}

func TestManagedDuplicateAccountsShareQuotaAndWorkspaceOwnership(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	a := f.managedCreate(t, "app", "connection-a-key", systemManagedSecret)
	b := f.managedCreate(t, "other", "connection-b-key", systemManagedRotated)
	if a.AccountID == nil || b.AccountID == nil || *a.AccountID != *b.AccountID || a.ID == b.ID {
		t.Fatal("duplicate canonical account multiplied identity")
	}
	receipt, _ := f.managedAdmit(t, "app", a, "shared-quota-job")
	permit, err := executionauth.New(f.host.access, f.host.store, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = permit.Authorize(context.Background(), f.actor, "app", receipt.JobID, "shared-quota-grant", systemAuthorizationRequest(receipt.AttemptID, 10)); err != nil {
		t.Fatal(err)
	}
	ap, bp := f.managedGet(t, "app", a.ID), f.managedGet(t, "other", b.ID)
	if len(ap.Quotas) != 1 || len(bp.Quotas) != 1 || ap.Quotas[0].Remaining == nil || bp.Quotas[0].Remaining == nil || *ap.Quotas[0].Remaining != 50 || *bp.Quotas[0].Remaining != 50 || ap.ActiveAttempts != 1 || bp.ActiveAttempts != 1 {
		t.Fatal("duplicate accounts expose independent capacity")
	}
	for _, view := range []connections.Connection{ap, bp} {
		quota := view.Quotas[0]
		if quota.Limit == nil || *quota.Limit != 60 || quota.Used == nil || *quota.Used != 0 || quota.LocalReserved == nil || *quota.LocalReserved != 10 || quota.ResetAt != nil {
			t.Fatal("quota presentation lost observed totals or local reservations", quota)
		}
	}
	jobs, _ := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	raw := string(systemJSON(t, f.jobSpecFor(t, f.actor, "app", a)))
	if _, err = jobs.Submit(context.Background(), f.managedPrincipal(t, "other"), "other", "foreign-alias-job", []byte(raw)); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("foreign workspace admitted managed alias", err)
	}
	f.managedAction(t, "app", a, "stale-selection-check", "check", "")
	if _, err = jobs.Submit(context.Background(), f.actor, "app", "stale-alias-job", []byte(raw)); err == nil {
		t.Fatal("pending mutation admitted stale alias")
	}
}

func TestManagedPublicProjectionOmitsCredentialMaterial(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	c := f.managedCreate(t, "app", "canary-connection-key", systemManagedSecret)
	op := f.managedAction(t, "app", c, "canary-rotation-key", "replace_credential", systemManagedRotated)
	f.managedRun(t)
	list, err := f.services.connections.List(context.Background(), f.actor, "app")
	if err != nil {
		t.Fatal(err)
	}
	operation, err := f.services.connections.Operation(context.Background(), f.actor, "app", op.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(struct {
		Connections []connections.Connection
		Operation   connections.Operation
	}{list, operation})
	for _, canary := range []string{systemManagedSecret, systemManagedRotated, "fixture_account_alpha", "vault:", "credential_key", "fingerprint"} {
		if bytes.Contains(raw, []byte(canary)) {
			t.Fatal("public projection exposed private data")
		}
	}
}

func TestManagedConcurrentChangedPayloadPreservesOneFingerprint(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	second, err := connections.New(f.host.access, f.host.store, f.vault, []connections.Adapter{f.adapter})
	if err != nil {
		t.Fatal(err)
	}
	const count = 12
	type result struct {
		operation connections.Operation
		err       error
	}
	results := make(chan result, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			service, secret := f.services.connections, systemManagedSecret
			if i%2 == 1 {
				service, secret = second, systemManagedOther
			}
			op, err := service.Submit(context.Background(), f.actor, "app", "", "mixed-payload-key", systemManagedCreateBody(secret))
			results <- result{op, err}
		}(i)
	}
	wg.Wait()
	close(results)
	fresh, replay, conflicts := 0, 0, 0
	var id domain.OperationID
	for r := range results {
		if errors.Is(r.err, connections.ErrConflict) {
			conflicts++
			continue
		}
		if r.err != nil {
			t.Fatal(r.err)
		}
		if id == "" {
			id = r.operation.ID
		}
		if r.operation.ID != id {
			t.Fatal("changed payload created another operation")
		}
		if r.operation.Replay {
			replay++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replay != count/2-1 || conflicts != count/2 {
		t.Fatal("changed-payload concurrency lost one fingerprint", fresh, replay, conflicts)
	}
	f.managedRun(t)
	if f.adapter.checkCount() != 1 {
		t.Fatal("multiple original requests reached verification")
	}
}

func TestManagedQuotaPresentationPreservesUnitsFreshnessAndResetEvidence(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	connection := f.managedCreate(t, "app", "quota-details-key", systemManagedSecret)
	f.advance(t, time.Second)
	limit, used, left := 2.0001, 0.2501, 1.5
	reset := f.clock.Now().Add(24 * time.Hour)
	observation := provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "gpu", Unit: "hours", Limit: &limit, Used: &used, Remaining: &left, ResetAt: &reset, ObservedAt: f.clock.Now(), Source: "synthetic", Precision: "lower_bound"}
	if err := f.host.store.RecordQuota(context.Background(), *connection.AccountID, "gpu", observation); err != nil {
		t.Fatal(err)
	}
	calls := f.adapter.checkCount()
	quota := f.managedGet(t, "app", connection.ID).Quotas[0]
	if quota.Limit == nil || *quota.Limit != 7200 || quota.Used == nil || *quota.Used != 901 || quota.Remaining == nil || *quota.Remaining != 5400 || quota.ResetAt == nil || !quota.ResetAt.Equal(reset) {
		t.Fatal("quota metadata did not retain observed units/reset", quota)
	}
	f.advance(t, 6*time.Minute)
	quota = f.managedGet(t, "app", connection.ID).Quotas[0]
	if quota.Status != "stale" || quota.Remaining != nil || quota.Limit == nil || *quota.Limit != 7200 || quota.Precision != "unknown" {
		t.Fatal("stale metadata became fresh capacity", quota)
	}
	f.advance(t, time.Second)
	unknown := provider.QuotaObservation{Status: provider.QuotaUnknown, Resource: "gpu", Unit: "seconds", ObservedAt: f.clock.Now(), Source: "synthetic", Precision: "unknown"}
	if err := f.host.store.RecordQuota(context.Background(), *connection.AccountID, "gpu", unknown); err != nil {
		t.Fatal(err)
	}
	quota = f.managedGet(t, "app", connection.ID).Quotas[0]
	if quota.Limit != nil || quota.Used != nil || quota.LocalReserved != nil || quota.ResetAt != nil || quota.Remaining != nil {
		t.Fatal("unknown quota invented metadata", quota)
	}
	if f.adapter.checkCount() != calls {
		t.Fatal("quota reads invoked provider")
	}
}
