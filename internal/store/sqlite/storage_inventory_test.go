package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func storageManager(t *testing.T, f admissionFixture) string {
	t.Helper()
	secret, _, err := f.access.Issue(t.Context(), "a", []auth.Scope{auth.Read, auth.Manage}, time.Time{})
	requireNoError(t, err)
	p, err := f.access.Authenticate(t.Context(), secret.Reveal())
	requireNoError(t, err)
	return p.TokenID()
}

func storageFixtureScope(t *testing.T, s *Store) hoststorage.Scope {
	t.Helper()
	info, err := s.Info(t.Context())
	requireNoError(t, err)
	return hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(info.InstallationID), MachineID: cacheMachine, EngineID: cacheEngine}
}

func storageOrphanVolume(scope hoststorage.Scope) hoststorage.Object {
	generation := strings.Repeat("7", 32)
	return hoststorage.Object{Kind: hoststorage.Volume, ID: "relay-cache-" + generation, SizeBytes: 7, SizeKnown: true,
		Labels: map[string]string{
			storageLabelPrefix + "installation": string(scope.InstallationID),
			storageLabelPrefix + "machine":      string(scope.MachineID),
			storageLabelPrefix + "engine":       string(scope.EngineID),
			storageLabelPrefix + "kind":         "cache",
			storageLabelPrefix + "generation":   "hcgen_" + generation,
			storageLabelPrefix + "sha256":       strings.Repeat("e", 64),
			storageLabelPrefix + "bytes":        "7",
			storageLabelPrefix + "operation":    "op_orphan",
		}}
}

func TestStorageRetentionViewScopesMachineWorkspaceAndFutureDeadlines(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := t.Context()
	now := time.Now().UTC().Add(time.Second)
	f.s.options.Clock = func() time.Time { return now }
	scope := storageFixtureScope(t, f.s)
	first, err := f.service.Submit(ctx, f.p, "a", "storage-retention-due", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	future, err := f.service.Submit(ctx, f.p, "a", "storage-retention-future", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	foreign, err := f.service.Submit(ctx, f.other, "b", "storage-retention-foreign", cacheJob("cache-host-b", "", ""))
	requireNoError(t, err)
	for _, item := range []struct {
		workspace string
		receipt   admission.Receipt
		due       time.Time
	}{{"a", first, now.Add(-time.Second)}, {"a", future, now.Add(time.Hour)}, {"b", foreign, now.Add(-time.Second)}} {
		_, err := f.s.db.ExecContext(ctx, `INSERT INTO remote_retention_deadlines(workspace_id,job_id,attempt_id,kind,anchor_ms,due_ms)
		 VALUES(?,?,?,'uncollected',?,?)`, item.workspace, item.receipt.JobID, item.receipt.AttemptID, now.Add(-time.Minute).UnixMilli(), item.due.UnixMilli())
		requireNoError(t, err)
	}
	view, err := f.s.ConnectionStorage(ctx, "a", f.p.TokenID(), "con_admission_a")
	requireNoError(t, err)
	if len(view.Retention) != 2 || view.Retention[0].JobID != first.JobID || !view.Retention[0].Pending || view.Retention[0].Status != "due" ||
		view.Retention[1].JobID != future.JobID || view.Retention[1].Pending || view.Retention[1].Status != "scheduled" ||
		!view.Retention[1].DueAt.Equal(now.Add(time.Hour).Truncate(time.Millisecond)) {
		t.Fatalf("retention projection lost deadlines or exposed another workspace: %+v", view.Retention)
	}
	for _, other := range []hoststorage.Scope{
		{InstallationID: scope.InstallationID, MachineID: domain.SHA256Digest(strings.Repeat("1", 64)), EngineID: scope.EngineID},
		{InstallationID: scope.InstallationID, MachineID: scope.MachineID, EngineID: domain.SHA256Digest(strings.Repeat("2", 64))},
	} {
		var otherView hoststorage.View
		requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, at time.Time) error {
			return projectStorageRetention(ctx, tx, other, "a", at, &otherView)
		}))
		if len(otherView.Retention) != 0 {
			t.Fatalf("another machine/engine inherited retention deadlines: %+v", otherView.Retention)
		}
	}
}

func TestStorageRetentionViewSwitchesPolicyAfterVerifiedCollection(t *testing.T) {
	f, _, p, blobs := terminalPolicyFixture(t, `{"after_collection_seconds":3600,"uncollected_max_seconds":60}`)
	ctx := t.Context()
	scope := storageFixtureScope(t, f.s)
	_, err := f.s.db.ExecContext(ctx, `INSERT INTO managed_profile_runtime(profile,revision,config) VALUES(?,?,?)`,
		f.profile.Binding.Profile, f.profile.Binding.ConfigurationRevision,
		[]byte(`{"schema_version":3,"machine_id":"`+cacheMachine+`","engine_id":"`+cacheEngine+`"}`))
	requireNoError(t, err)
	project := func() hoststorage.View {
		t.Helper()
		var view hoststorage.View
		requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
			return projectStorageRetention(ctx, tx, scope, "a", now, &view)
		}))
		return view
	}
	before := project()
	if len(before.Retention) != 1 || before.Retention[0].Status != "scheduled" {
		t.Fatalf("uncollected policy missing before publication: %+v", before.Retention)
	}
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	after := project()
	due := remoteDeadlineAt(t, f, "collected")
	if len(after.Retention) != 1 || after.Retention[0].DueAt.UnixMilli() != due || after.Retention[0].Status != "scheduled" || after.Retention[0].Pending {
		t.Fatalf("publication left superseded uncollected policy pending: %+v", after.Retention)
	}
	var deadlines int
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM remote_retention_deadlines`).Scan(&deadlines))
	if deadlines != 2 {
		t.Fatal("projection removed original retention history")
	}
}

func TestStorageOrphanPreviewApplyFreshnessAndExactReplay(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := t.Context()
	now := time.Now().UTC().Add(time.Second)
	f.s.options.Clock = func() time.Time { return now }
	token := storageManager(t, f)
	scope := storageFixtureScope(t, f.s)
	object := storageOrphanVolume(scope)
	record := func() string {
		t.Helper()
		requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: now, Objects: []hoststorage.Object{object}}))
		view, err := f.s.ConnectionStorage(ctx, "a", token, "con_admission_a")
		requireNoError(t, err)
		if len(view.Orphans) != 1 || !view.Orphans[0].Eligible {
			t.Fatalf("credible orphan not offered: %+v", view.Orphans)
		}
		return view.Orphans[0].ID
	}
	orphan := record()
	candidate, err := f.s.FindStorageOrphan(ctx, "a", token, "con_admission_a", orphan)
	requireNoError(t, err)
	if _, err := f.s.AdmitStorageCleanup(ctx, "a", token, "con_admission_a", orphan, strings.Repeat("f", 64), "without-preview"); !errors.Is(err, hoststorage.ErrIneligible) {
		t.Fatalf("apply without preview was accepted: %v", err)
	}
	observed := hoststorage.TargetObservation{Present: true, Eligible: true, SizeKnown: true, SizeBytes: 7, Reason: "zero_references", ObservedAt: now}
	preview, err := f.s.RecordStoragePreview(ctx, "a", token, candidate, observed)
	requireNoError(t, err)
	op, err := f.s.AdmitStorageCleanup(ctx, "a", token, "con_admission_a", orphan, preview.Fingerprint, "orphan-apply")
	requireNoError(t, err)
	if op.Status != "accepted" || op.Result != "pending" {
		t.Fatalf("wrong accepted receipt: %+v", op)
	}
	now = now.Add(3 * time.Minute)
	newOrphan := record()
	if orphan == newOrphan {
		t.Fatal("later inventory reused an opaque orphan identity")
	}
	replay, err := f.s.AdmitStorageCleanup(ctx, "a", token, "con_admission_a", orphan, preview.Fingerprint, "orphan-apply")
	requireNoError(t, err)
	if replay != op {
		t.Fatalf("exact replay changed original receipt: %+v", replay)
	}
	for _, changed := range []struct{ orphan, connection, fingerprint string }{
		{newOrphan, "con_admission_a", preview.Fingerprint},
		{orphan, "con_admission_b", preview.Fingerprint},
		{orphan, "con_admission_a", strings.Repeat("e", 64)},
	} {
		if _, err := f.s.AdmitStorageCleanup(ctx, "a", token, changed.connection, changed.orphan, changed.fingerprint, "orphan-apply"); !errors.Is(err, admission.ErrConflict) {
			t.Fatalf("changed apply replay was accepted: %v", err)
		}
	}
	if _, err := f.s.AdmitStorageCleanup(ctx, "a", token, "con_admission_a", newOrphan, preview.Fingerprint, "expired-preview"); !errors.Is(err, hoststorage.ErrIneligible) {
		t.Fatalf("expired/replaced preview was accepted: %v", err)
	}
	now = now.Add(hoststorage.ScanFreshness + time.Second)
	if _, err := f.s.FindStorageOrphan(ctx, "a", token, "con_admission_a", newOrphan); !errors.Is(err, hoststorage.ErrUnknown) {
		t.Fatalf("stale scan offered cleanup authority: %v", err)
	}
	var count int
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_cleanup_operations`).Scan(&count))
	if count != 1 {
		t.Fatalf("preview/replay checks created %d mutations", count)
	}
}

func TestStorageOrphanProjectionAllowsStoppedContainerMountsAndKeepsOtherFences(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := t.Context()
	scope := storageFixtureScope(t, f.s)
	container := hoststorage.Object{Kind: hoststorage.Container, ID: strings.Repeat("a", 64), SizeKnown: true, SizeBytes: 7, References: 1,
		Labels: map[string]string{
			storageLabelPrefix + "installation": string(scope.InstallationID), storageLabelPrefix + "workspace": "a",
			storageLabelPrefix + "job": "job_orphan", storageLabelPrefix + "attempt": "attempt_orphan", storageLabelPrefix + "instance": "instance_orphan",
			storageLabelPrefix + "preparation": "prep_orphan", storageLabelPrefix + "plan": strings.Repeat("e", 64),
			storageLabelPrefix + "nonce": strings.Repeat("f", 64), storageLabelPrefix + "engine": "NATIVEENGINE1234567890",
		}}
	running := container
	running.ID, running.Running = strings.Repeat("b", 64), true
	volume := storageOrphanVolume(scope)
	volume.References = 1
	image := hoststorage.Object{Kind: hoststorage.Image, ID: "sha256:" + strings.Repeat("c", 64), SizeKnown: true, SizeBytes: 77, References: 1,
		Labels: map[string]string{
			storageLabelPrefix + "installation": string(scope.InstallationID), storageLabelPrefix + "image-kind": "application",
			storageLabelPrefix + "image-schema": "application/1", storageLabelPrefix + "image-machine": string(scope.MachineID),
			storageLabelPrefix + "image-engine": string(scope.EngineID), storageLabelPrefix + "archive-sha256": strings.Repeat("d", 64),
		}}
	objects := []hoststorage.Object{container, running, volume, image}
	requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: objects}))
	view, err := f.s.ConnectionStorage(ctx, "a", f.p.TokenID(), "con_admission_a")
	requireNoError(t, err)
	if len(view.Orphans) != len(objects) {
		t.Fatalf("orphan projection lost owned objects: %+v", view.Orphans)
	}
	var sequence int64
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT max(scan_seq) FROM host_storage_scans`).Scan(&sequence))
	wantEligible := opaqueOrphanID(scope, sequence, container.Kind, container.ID)
	for _, orphan := range view.Orphans {
		if orphan.Eligible != (orphan.ID == wantEligible) {
			t.Fatalf("class-specific preview eligibility mismatch: %+v", orphan)
		}
	}
}

type storageRecoveryProvider struct {
	provider.Provider
	binding          provider.BindingSnapshot
	recoveryCalls    int
	newDeletionCalls int
}

func (p *storageRecoveryProvider) Verify(_ context.Context, binding provider.BindingSnapshot) error {
	if binding != p.binding {
		return hoststorage.ErrUnverified
	}
	return nil
}
func (*storageRecoveryProvider) ObserveStorageTarget(context.Context, hoststorage.Target) (hoststorage.TargetObservation, error) {
	return hoststorage.TargetObservation{Present: true, Eligible: true}, nil
}
func (p *storageRecoveryProvider) CleanupStorageTarget(_ context.Context, _ hoststorage.Target, recoverOnly bool) (hoststorage.DeleteOutcome, error) {
	if recoverOnly {
		p.recoveryCalls++
		return hoststorage.DeleteUnknown, nil
	}
	p.newDeletionCalls++
	return hoststorage.DeleteConfirmed, nil
}

type storageRecoveryResolver struct{ p *storageRecoveryProvider }

func (r storageRecoveryResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	return r.p, nil
}

func TestStorageOrphanRestartAfterIntentRecoversWithoutSecondDelete(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := t.Context()
	now := time.Now().UTC().Add(time.Second)
	f.s.options.Clock = func() time.Time { return now }
	token := storageManager(t, f)
	scope := storageFixtureScope(t, f.s)
	object := storageOrphanVolume(scope)
	requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: now, Objects: []hoststorage.Object{object}}))
	view, err := f.s.ConnectionStorage(ctx, "a", token, "con_admission_a")
	requireNoError(t, err)
	if len(view.Orphans) != 1 {
		t.Fatalf("expected one orphan, got %+v", view.Orphans)
	}
	orphan := view.Orphans[0].ID
	candidate, err := f.s.FindStorageOrphan(ctx, "a", token, "con_admission_a", orphan)
	requireNoError(t, err)
	preview, err := f.s.RecordStoragePreview(ctx, "a", token, candidate,
		hoststorage.TargetObservation{Present: true, Eligible: true, Reason: "zero_references", ObservedAt: now})
	requireNoError(t, err)
	op, err := f.s.AdmitStorageCleanup(ctx, "a", token, "con_admission_a", orphan, preview.Fingerprint, "restart-orphan")
	requireNoError(t, err)
	work, err := f.s.ClaimStorageCleanup(ctx, time.Minute)
	requireNoError(t, err)
	if work == nil {
		t.Fatal("accepted orphan was not claimable")
	}
	requireNoError(t, f.s.BeginStorageCleanup(ctx, *work))
	requireNoError(t, f.s.RecordStorageCleanupReason(ctx, *work, "storage_delete_verification_failed"))
	options := f.s.options
	requireNoError(t, f.s.Close())
	now = now.Add(2 * time.Minute)
	reopened, err := Open(ctx, f.root, options)
	requireNoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	saved, err := reopened.ReadStorageCleanup(ctx, "a", token, op.OperationID)
	requireNoError(t, err)
	if saved.Status != "running" || saved.Reason != "storage_delete_verification_failed" {
		t.Fatal("lost unresolved cleanup reason", saved)
	}
	adapter := &storageRecoveryProvider{binding: candidate.Binding}
	engine, err := cleanup.New(reopened, storageRecoveryResolver{adapter}, cleanup.DefaultConfig())
	requireNoError(t, err)
	engine.WithStorage(reopened)
	worked, err := engine.RunOnce(ctx)
	requireNoError(t, err)
	if !worked || adapter.recoveryCalls != 1 || adapter.newDeletionCalls != 0 {
		t.Fatalf("restart repeated deletion: worked=%v recovery=%d delete=%d", worked, adapter.recoveryCalls, adapter.newDeletionCalls)
	}
	receipt, err := reopened.ReadStorageCleanup(ctx, "a", token, op.OperationID)
	requireNoError(t, err)
	if receipt.Status != "failed" || receipt.Result != "unknown" {
		t.Fatalf("ambiguous original deletion did not stay unknown: %+v", receipt)
	}
	worked, err = engine.RunOnce(ctx)
	requireNoError(t, err)
	if worked || adapter.recoveryCalls != 1 || adapter.newDeletionCalls != 0 {
		t.Fatal("settled unknown deletion was retried")
	}
	replay, err := reopened.AdmitStorageCleanup(ctx, "a", token, "con_admission_a", orphan, preview.Fingerprint, "restart-orphan")
	requireNoError(t, err)
	if replay != receipt {
		t.Fatalf("restart lost original unknown receipt: %+v", replay)
	}
}

func TestStorageScanCauseSurvivesRestartAndRejectsUnsafeText(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := t.Context()
	scope := storageFixtureScope(t, f.s)
	for _, reason := range []string{"", "private /path", "A", strings.Repeat("x", 65)} {
		if err := f.s.RecordIncompleteStorageScan(ctx, scope, reason); !errors.Is(err, hoststorage.ErrInvalid) {
			t.Fatal("accepted unsafe reason", err)
		}
	}
	requireNoError(t, f.s.RecordIncompleteStorageScan(ctx, scope, "storage_volumes_list_failed"))
	options := f.s.options
	requireNoError(t, f.s.Close())
	reopened, err := Open(ctx, f.root, options)
	requireNoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	view, err := reopened.ConnectionStorage(ctx, "a", f.p.TokenID(), "con_admission_a")
	requireNoError(t, err)
	if view.ScanState != "incomplete" || view.Reason != "storage_volumes_list_failed" {
		t.Fatalf("lost durable scan reason: %+v", view)
	}
	err = reopened.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		_, err := freshStorageChargeInTx(ctx, tx, scope, now)
		return err
	})
	if !errors.Is(err, hoststorage.ErrUnknown) || provider.Reason(err, "") != view.Reason {
		t.Fatal("admission discarded scan cause", err)
	}
}
