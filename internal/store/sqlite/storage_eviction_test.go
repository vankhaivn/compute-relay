package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

func TestStorageEvictionReceiptDischargesChargeAndNewScanRequeues(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	receipt, err := f.service.Submit(ctx, f.p, "a", "storage-eviction", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	owner := cacheOwner("a", receipt)
	pins, err := f.s.Pins(ctx, owner)
	requireNoError(t, err)
	if len(pins) != 1 {
		t.Fatalf("expected one pinned cache key, got %d", len(pins))
	}
	generation, fresh, err := f.s.Claim(ctx, owner, pins[0])
	requireNoError(t, err)
	if !fresh {
		t.Fatal("first cache writer did not create a generation")
	}
	requireNoError(t, f.s.BeginWrite(ctx, owner, generation.ID))
	requireNoError(t, f.s.Verify(ctx, completeCacheObservation(generation)))
	requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if err := releaseHostCachePinsWithoutPreparation(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now); err != nil {
			return err
		}
		return releaseAttemptStorageResourcesInTx(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now)
	}))
	_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=6,storage_hard_bytes=10
	 WHERE connection_id='con_admission_a'`)
	requireNoError(t, err)
	profile := f.profile
	profile.Binding.Profile = "cache-host-a"
	profile.Binding.ConfigurationRevision = "rev_cache_a"
	profile.CredentialRef = "vault:con_admission_a"
	profile.ResourceScope = "resource_" + cacheMachine
	profile.CostClass = admission.CostOperatorManaged
	info, err := f.s.Info(ctx)
	requireNoError(t, err)
	scope := hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(info.InstallationID), MachineID: cacheMachine, EngineID: cacheEngine}
	observed := hoststorage.Object{Kind: hoststorage.Volume, ID: generation.VolumeName, SizeBytes: 7, SizeKnown: true, Labels: generation.Labels()}
	record := func() {
		t.Helper()
		requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: []hoststorage.Object{observed}}))
	}
	charge := func() int64 {
		t.Helper()
		var bytes int64
		requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
			observation, err := latestStorageObservation(ctx, tx, scope)
			if err != nil {
				return err
			}
			result, err := storageChargeInTx(ctx, tx, scope, observation, "a")
			bytes = result.view.Machine.ChargedBytes
			return err
		}))
		return bytes
	}
	cleanupCount := func() int {
		t.Helper()
		var count int
		requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_cleanup_operations`).Scan(&count))
		return count
	}
	record()
	if got := charge(); got != 7 {
		t.Fatalf("expected exact seven-byte charge, got %d", got)
	}
	requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
	if got := cleanupCount(); got != 1 {
		t.Fatalf("soft-limit scheduler created %d operations", got)
	}
	work, err := f.s.ClaimStorageCleanup(ctx, time.Minute)
	requireNoError(t, err)
	if work == nil || work.Target.Action != hoststorage.EvictCache || work.Target.ID != generation.VolumeName {
		t.Fatalf("wrong exact eviction work: %+v", work)
	}
	requireNoError(t, f.s.BeginStorageCleanup(ctx, *work))
	requireNoError(t, f.s.CompleteStorageCleanup(ctx, *work, hoststorage.DeleteConfirmed, "deleted"))
	if got := charge(); got != 0 {
		t.Fatalf("confirmed deletion retained %d charged bytes", got)
	}
	var state, reservation string
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT state FROM host_cache_generations WHERE generation_id=?`, generation.ID).Scan(&state))
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT state FROM host_storage_reservations WHERE mutation_operation_id=?`, generation.OperationID).Scan(&reservation))
	if state != "retired" || reservation != "released" {
		t.Fatalf("exact deletion did not retire ledger and reservation: %s %s", state, reservation)
	}
	// A later complete observation of the same deterministic identity makes it
	// chargeable again and permits a fresh exact cleanup operation.
	record()
	if got := charge(); got != 7 {
		t.Fatalf("new observation did not recharge identity: %d", got)
	}
	requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
	if got := cleanupCount(); got != 2 {
		t.Fatalf("new scan did not permit a new exact cleanup: %d", got)
	}
}

func TestStorageEvictionBoundsCacheVolumeCountBelowByteLimit(t *testing.T) {
	old := cacheVolumeLimit
	cacheVolumeLimit = 0
	t.Cleanup(func() { cacheVolumeLimit = old })
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	receipt, err := f.service.Submit(ctx, f.p, "a", "storage-count-eviction", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	owner := cacheOwner("a", receipt)
	pins, err := f.s.Pins(ctx, owner)
	requireNoError(t, err)
	generation, _, err := f.s.Claim(ctx, owner, pins[0])
	requireNoError(t, err)
	requireNoError(t, f.s.BeginWrite(ctx, owner, generation.ID))
	requireNoError(t, f.s.Verify(ctx, completeCacheObservation(generation)))
	requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if err := releaseHostCachePinsWithoutPreparation(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now); err != nil {
			return err
		}
		return releaseAttemptStorageResourcesInTx(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now)
	}))
	profile := f.profile
	profile.Binding.Profile = "cache-host-a"
	profile.Binding.ConfigurationRevision = "rev_cache_a"
	profile.CredentialRef = "vault:con_admission_a"
	profile.ResourceScope = "resource_" + cacheMachine
	profile.CostClass = admission.CostOperatorManaged
	info, err := f.s.Info(ctx)
	requireNoError(t, err)
	scope := hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(info.InstallationID), MachineID: cacheMachine, EngineID: cacheEngine}
	observed := hoststorage.Object{Kind: hoststorage.Volume, ID: generation.VolumeName, SizeBytes: 7, SizeKnown: true, Labels: generation.Labels()}
	requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: []hoststorage.Object{observed}}))
	requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
	var count int
	var reason string
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*),COALESCE(max(reason),'') FROM host_storage_cleanup_operations`).Scan(&count, &reason))
	if count != 1 || reason != "count_limit_lru" {
		t.Fatalf("idle cache volume over the count limit was not evicted: %d %q", count, reason)
	}
}

func TestStorageHardLimitSharesBudgetAcrossConnectionAliases(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	soft, hard := int64(5<<30), int64(6<<30)
	_, err := f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=?,storage_hard_bytes=?
	 WHERE connection_id='con_admission_a'`, soft, hard)
	requireNoError(t, err)
	first, err := f.service.Submit(ctx, f.p, "a", "storage-alias-first", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	var reserved int64
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(bytes),0) FROM host_storage_reservations
	 WHERE state!='released'`).Scan(&reserved))
	if reserved != hoststorage.AttemptAllowanceBytes+7 {
		t.Fatalf("first admission reserved %d bytes", reserved)
	}
	_, err = f.service.Submit(ctx, f.other, "b", "storage-alias-second", cacheJob("cache-host-b", "", ""))
	if !errors.Is(err, hoststorage.ErrHardLimit) {
		t.Fatalf("second alias bypassed shared hard limit: %v", err)
	}
	if got := tableCount(t, f.s, "jobs"); got != 1 {
		t.Fatalf("refused alias changed jobs: %d", got)
	}
	replayed, err := f.service.Submit(ctx, f.p, "a", "storage-alias-first", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	if !replayed.Replay || replayed.JobID != first.JobID {
		t.Fatal("hard limit broke exact admission replay")
	}
}

func TestStorageAdmissionRefusesExactHardLimitBeforeCreatingWork(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := t.Context()
	_, err := f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=1,storage_hard_bytes=?
	 WHERE connection_id='con_admission_a'`, hoststorage.AttemptAllowanceBytes+7)
	requireNoError(t, err)
	_, err = f.service.Submit(ctx, f.p, "a", "storage-exact-hard-limit", cacheJob("cache-host-a", "", ""))
	if !errors.Is(err, hoststorage.ErrHardLimit) {
		t.Fatalf("exact-full admission created unpreparable work: %v", err)
	}
	var reservations int
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_reservations`).Scan(&reservations))
	if tableCount(t, f.s, "jobs") != 0 || reservations != 0 {
		t.Fatal("hard-limit refusal retained work or reservations")
	}
}

func TestStorageEvictionUsesOldestEligibleContentAndFencesNewPins(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	first, err := f.service.Submit(ctx, f.p, "a", "storage-lru-first", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	firstOwner := cacheOwner("a", first)
	firstPins, err := f.s.Pins(ctx, firstOwner)
	requireNoError(t, err)
	if len(firstPins) != 1 {
		t.Fatalf("first content has %d keys", len(firstPins))
	}
	older, fresh, err := f.s.Claim(ctx, firstOwner, firstPins[0])
	requireNoError(t, err)
	if !fresh {
		t.Fatal("first writer was not fresh")
	}
	requireNoError(t, f.s.BeginWrite(ctx, firstOwner, older.ID))
	requireNoError(t, f.s.Verify(ctx, completeCacheObservation(older)))
	requireNoError(t, f.s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: "a", ID: "second", SHA256: domain.SHA256Digest(strings.Repeat("f", 64)), Bytes: 7}))
	secondRequest := strings.Replace(string(cacheJob("cache-host-a", "", "")), `"object_id":"data"`, `"object_id":"second"`, 1)
	second, err := f.service.Submit(ctx, f.p, "a", "storage-lru-second", []byte(secondRequest))
	requireNoError(t, err)
	secondOwner := cacheOwner("a", second)
	secondPins, err := f.s.Pins(ctx, secondOwner)
	requireNoError(t, err)
	if len(secondPins) != 2 {
		t.Fatalf("second content has %d keys", len(secondPins))
	}
	var newer hostcache.Generation
	for _, pin := range secondPins {
		if pin.SHA256 != domain.SHA256Digest(strings.Repeat("f", 64)) {
			continue
		}
		newer, fresh, err = f.s.Claim(ctx, secondOwner, pin)
		requireNoError(t, err)
		if !fresh {
			t.Fatal("second writer was not fresh")
		}
		requireNoError(t, f.s.BeginWrite(ctx, secondOwner, newer.ID))
		requireNoError(t, f.s.Verify(ctx, completeCacheObservation(newer)))
	}
	if newer.ID == "" {
		t.Fatal("second generation missing")
	}
	for _, owner := range []hostcache.Attempt{firstOwner, secondOwner} {
		requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
			if err := releaseHostCachePinsWithoutPreparation(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now); err != nil {
				return err
			}
			return releaseAttemptStorageResourcesInTx(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now)
		}))
	}
	_, err = f.s.db.ExecContext(ctx, `UPDATE host_cache_generations SET last_used_at=? WHERE generation_id=?`, "2026-10-10T00:00:00Z", older.ID)
	requireNoError(t, err)
	_, err = f.s.db.ExecContext(ctx, `UPDATE host_cache_generations SET last_used_at=? WHERE generation_id=?`, "2026-10-11T00:00:00Z", newer.ID)
	requireNoError(t, err)
	_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=8,storage_hard_bytes=30
	 WHERE connection_id='con_admission_a'`)
	requireNoError(t, err)
	info, err := f.s.Info(ctx)
	requireNoError(t, err)
	scope := hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(info.InstallationID), MachineID: cacheMachine, EngineID: cacheEngine}
	objects := []hoststorage.Object{
		{Kind: hoststorage.Volume, ID: older.VolumeName, SizeBytes: 7, SizeKnown: true, Labels: older.Labels()},
		{Kind: hoststorage.Volume, ID: newer.VolumeName, SizeBytes: 7, SizeKnown: true, Labels: newer.Labels()},
	}
	requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: objects}))
	profile := f.profile
	profile.Binding.Profile = "cache-host-a"
	profile.Binding.ConfigurationRevision = "rev_cache_a"
	profile.CredentialRef = "vault:con_admission_a"
	profile.ResourceScope = "resource_" + cacheMachine
	profile.CostClass = admission.CostOperatorManaged
	requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
	var id string
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT object_id FROM host_storage_cleanup_operations WHERE status='accepted'`).Scan(&id))
	if id != older.VolumeName {
		t.Fatalf("LRU chose %q instead of oldest %q", id, older.VolumeName)
	}
	if _, err := f.service.Submit(ctx, f.p, "a", "storage-fenced-new", cacheJob("cache-host-a", "", "")); !errors.Is(err, hoststorage.ErrIneligible) {
		t.Fatalf("new pin crossed accepted deletion fence: %v", err)
	}
	work, err := f.s.ClaimStorageCleanup(ctx, time.Minute)
	requireNoError(t, err)
	if work == nil {
		t.Fatal("queued cleanup was not claimable")
	}
	if err := f.s.BeginStorageCleanup(ctx, *work); err != nil {
		t.Fatalf("zero-pin target could not begin: %v", err)
	}
}

func TestStorageImageEvictionFencesNewPinsAndRequeuesAfterReappearance(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	digest := domain.SHA256Digest(strings.Repeat("e", 64))
	requireNoError(t, f.s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: "a", ID: "image", SHA256: digest, Bytes: 77}))
	request := imageAdmissionJob("cache-host-a", "image", string(digest), 77)
	receipt, err := f.service.Submit(ctx, f.p, "a", "storage-image-first", request)
	requireNoError(t, err)
	pin, err := f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
	requireNoError(t, err)
	if pin == nil {
		t.Fatal("image admission did not pin the archive")
	}
	owner := hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: "image"}
	generation, fresh, err := f.s.ClaimImage(ctx, owner, pin.Key)
	requireNoError(t, err)
	if !fresh {
		t.Fatal("image writer was not fresh")
	}
	imageID := "sha256:" + strings.Repeat("d", 64)
	_, err = f.s.BeginImageLoad(ctx, generation, imageID, []string{"sha256:" + strings.Repeat("f", 64)})
	requireNoError(t, err)
	active, err := f.s.ActiveImage(ctx, pin.Key)
	requireNoError(t, err)
	if active == nil {
		t.Fatal("loading image lost its generation")
	}
	requireNoError(t, f.s.MarkImageReady(ctx, *active))
	requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if err := releaseHostCachePinsWithoutPreparation(ctx, tx, "a", receipt.JobID, receipt.AttemptID, now); err != nil {
			return err
		}
		if err := releaseHostApplicationImagePins(ctx, tx, "a", receipt.JobID, receipt.AttemptID, now); err != nil {
			return err
		}
		return releaseAttemptStorageResourcesInTx(ctx, tx, "a", receipt.JobID, receipt.AttemptID, now)
	}))
	_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=?,storage_hard_bytes=?
	 WHERE connection_id='con_admission_a'`, int64(3<<30), int64(6<<30))
	requireNoError(t, err)
	info, err := f.s.Info(ctx)
	requireNoError(t, err)
	scope := hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(info.InstallationID), MachineID: cacheMachine, EngineID: cacheEngine}
	labels := map[string]string{
		hostimages.LabelInstallation: string(scope.InstallationID),
		hostimages.LabelKind:         "application",
		hostimages.LabelSchema:       hostimages.ApplicationSchema,
		hostimages.LabelArchive:      string(digest),
		hostimages.LabelMachine:      string(scope.MachineID),
		hostimages.LabelEngine:       string(scope.EngineID),
	}
	observed := hoststorage.Object{Kind: hoststorage.Image, ID: imageID, SizeKnown: true, SizeBytes: 4 << 30, Labels: labels}
	record := func() {
		t.Helper()
		requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: []hoststorage.Object{observed}}))
	}
	profile := f.profile
	profile.Binding.Profile = "cache-host-a"
	profile.Binding.ConfigurationRevision = "rev_cache_a"
	profile.CredentialRef = "vault:con_admission_a"
	profile.ResourceScope = "resource_" + cacheMachine
	profile.CostClass = admission.CostOperatorManaged
	record()
	requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
	work, err := f.s.ClaimStorageCleanup(ctx, time.Minute)
	requireNoError(t, err)
	if work == nil || work.Target.Action != hoststorage.EvictImage || work.Target.ID != imageID {
		t.Fatalf("wrong image eviction: %+v", work)
	}
	if _, err := f.service.Submit(ctx, f.p, "a", "storage-image-fenced", request); !errors.Is(err, hoststorage.ErrIneligible) {
		t.Fatalf("image pin crossed accepted deletion fence: %v", err)
	}
	requireNoError(t, f.s.BeginStorageCleanup(ctx, *work))
	requireNoError(t, f.s.CompleteStorageCleanup(ctx, *work, hoststorage.DeleteConfirmed, "deleted"))
	var state, reservation string
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT state FROM host_application_images WHERE generation_id=?`, generation.ID).Scan(&state))
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT state FROM host_storage_reservations WHERE mutation_operation_id=?`, generation.OperationID).Scan(&reservation))
	if state != "retired" || reservation != "released" {
		t.Fatalf("image deletion did not retire ledger and reservation: %s %s", state, reservation)
	}
	record()
	requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
	var count int
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_cleanup_operations WHERE class='image'`).Scan(&count))
	if count != 2 {
		t.Fatalf("later exact image observation was not requeued: %d", count)
	}
}

func TestStorageEvictionKeepsLivePinnedContent(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	receipt, err := f.service.Submit(ctx, f.p, "a", "storage-pinned", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	owner := cacheOwner("a", receipt)
	pins, err := f.s.Pins(ctx, owner)
	requireNoError(t, err)
	generation, fresh, err := f.s.Claim(ctx, owner, pins[0])
	requireNoError(t, err)
	if !fresh {
		t.Fatal("cache generation was not fresh")
	}
	requireNoError(t, f.s.BeginWrite(ctx, owner, generation.ID))
	requireNoError(t, f.s.Verify(ctx, completeCacheObservation(generation)))
	_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=6,storage_hard_bytes=?
	 WHERE connection_id='con_admission_a'`, int64(6<<30))
	requireNoError(t, err)
	info, err := f.s.Info(ctx)
	requireNoError(t, err)
	scope := hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(info.InstallationID), MachineID: cacheMachine, EngineID: cacheEngine}
	object := hoststorage.Object{Kind: hoststorage.Volume, ID: generation.VolumeName, SizeKnown: true, SizeBytes: 7, Labels: generation.Labels()}
	requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: []hoststorage.Object{object}}))
	profile := f.profile
	profile.Binding.Profile = "cache-host-a"
	profile.Binding.ConfigurationRevision = "rev_cache_a"
	profile.CredentialRef = "vault:con_admission_a"
	profile.ResourceScope = "resource_" + cacheMachine
	profile.CostClass = admission.CostOperatorManaged
	requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
	var count int
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_cleanup_operations`).Scan(&count))
	if count != 0 {
		t.Fatalf("live-pinned content was queued for deletion: %d", count)
	}
	// The pin survives preparation and running work until an exact cleanup
	// receipt (or a durable no-preparation proof) releases it.
	requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if err := releaseHostCachePinsWithoutPreparation(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now); err != nil {
			return err
		}
		return releaseAttemptStorageResourcesInTx(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now)
	}))
	requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_cleanup_operations`).Scan(&count))
	if count != 1 {
		t.Fatalf("released content was not scheduled: %d", count)
	}
}

func TestStorageScanRecordsExactAttemptAbsenceWithoutCleanupReceipt(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	receipt, err := f.service.Submit(ctx, f.p, "a", "storage-attempt-absence", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	info, err := f.s.Info(ctx)
	requireNoError(t, err)
	scope := hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(info.InstallationID), MachineID: cacheMachine, EngineID: cacheEngine}
	containerID := strings.Repeat("9", 64)
	identity := `{"InstallationID":"` + string(scope.InstallationID) + `"}`
	resource := `{"id":"` + containerID + `"}`
	_, err = f.s.db.ExecContext(ctx, `INSERT INTO provider_resources
	 (resource_id,workspace_id,job_id,attempt_id,instance_id,account_scope,purpose,operation_id,resource_key,identity,plan_sha256,resource_ref,created_at)
	 VALUES('res_storage_absence','a',?,?,'instance_1',?,'staging','op_storage_absence','key',?,?,?,?)`,
		receipt.JobID, receipt.AttemptID, f.profile.AccountScope, identity, strings.Repeat("a", 64), resource, time.Now().UTC().Format(time.RFC3339Nano))
	requireNoError(t, err)
	requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: []hoststorage.Object{}}))
	var first, last int64
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT first_scan_seq,last_scan_seq FROM host_storage_absences
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND class='container' AND object_id=?`,
		scope.InstallationID, scope.MachineID, scope.EngineID, containerID).Scan(&first, &last))
	if first == 0 || first != last {
		t.Fatalf("exact attempt absence was not retained: %d/%d", first, last)
	}
	var receipts int
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_cleanup_operations`).Scan(&receipts))
	if receipts != 0 {
		t.Fatal("scan absence manufactured a deletion receipt")
	}
}

func TestStorageEvictionKeepsCacheAndImagePinsAcrossActiveStates(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := t.Context()
	digest := domain.SHA256Digest(strings.Repeat("e", 64))
	requireNoError(t, f.s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: "a", ID: "image", SHA256: digest, Bytes: 77}))
	receipt, err := f.service.Submit(ctx, f.p, "a", "storage-active-pins", imageAdmissionJob("cache-host-a", "image", string(digest), 77))
	requireNoError(t, err)
	owner := cacheOwner("a", receipt)
	keys, err := f.s.Pins(ctx, owner)
	requireNoError(t, err)
	if len(keys) != 1 {
		t.Fatalf("expected one cache key, got %d", len(keys))
	}
	cache, fresh, err := f.s.Claim(ctx, owner, keys[0])
	requireNoError(t, err)
	if !fresh {
		t.Fatal("cache writer was not fresh")
	}
	requireNoError(t, f.s.BeginWrite(ctx, owner, cache.ID))
	requireNoError(t, f.s.Verify(ctx, completeCacheObservation(cache)))
	pin, err := f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
	requireNoError(t, err)
	if pin == nil {
		t.Fatal("application image was not pinned")
	}
	image, fresh, err := f.s.ClaimImage(ctx, hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: "image"}, pin.Key)
	requireNoError(t, err)
	if !fresh {
		t.Fatal("image loader was not fresh")
	}
	imageID := "sha256:" + strings.Repeat("d", 64)
	_, err = f.s.BeginImageLoad(ctx, image, imageID, []string{"sha256:" + strings.Repeat("f", 64)})
	requireNoError(t, err)
	active, err := f.s.ActiveImage(ctx, pin.Key)
	requireNoError(t, err)
	if active == nil {
		t.Fatal("loading image lost its generation")
	}
	requireNoError(t, f.s.MarkImageReady(ctx, *active))
	scope := storageFixtureScope(t, f.s)
	objects := []hoststorage.Object{
		{Kind: hoststorage.Volume, ID: cache.VolumeName, SizeKnown: true, SizeBytes: 7, Labels: cache.Labels()},
		{Kind: hoststorage.Image, ID: imageID, SizeKnown: true, SizeBytes: 128, Labels: map[string]string{
			hostimages.LabelInstallation: string(scope.InstallationID), hostimages.LabelKind: "application", hostimages.LabelSchema: hostimages.ApplicationSchema,
			hostimages.LabelArchive: string(digest), hostimages.LabelMachine: string(scope.MachineID), hostimages.LabelEngine: string(scope.EngineID),
		}},
	}
	requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: objects}))
	_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=1,storage_hard_bytes=? WHERE connection_id='con_admission_a'`, int64(20<<30))
	requireNoError(t, err)
	profile := f.profile
	profile.Binding.Profile, profile.Binding.ConfigurationRevision = "cache-host-a", "rev_cache_a"
	profile.CredentialRef, profile.ResourceScope, profile.CostClass = "vault:con_admission_a", "resource_"+cacheMachine, admission.CostOperatorManaged
	for _, orchestration := range []domain.OrchestrationState{domain.OrchestrationQueued, domain.OrchestrationPreparing, domain.OrchestrationRunning} {
		state := domain.InitialAttemptState()
		state.Orchestration = orchestration
		if orchestration == domain.OrchestrationRunning {
			state.Execution, state.RemoteActivity = domain.ExecutionRunning, domain.RemoteActivityActive
		}
		requireNoError(t, state.Validate())
		raw, err := json.Marshal(state)
		requireNoError(t, err)
		_, err = f.s.db.ExecContext(ctx, `UPDATE attempts SET state=?,orchestration=? WHERE workspace_id='a' AND job_id=? AND attempt_id=?`, string(raw), orchestration, receipt.JobID, receipt.AttemptID)
		requireNoError(t, err)
		requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
		var count int
		requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_cleanup_operations`).Scan(&count))
		if count != 0 {
			t.Fatalf("%s content was queued for eviction: %d", orchestration, count)
		}
		var after string
		requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT state FROM attempts WHERE workspace_id='a' AND job_id=? AND attempt_id=?`, receipt.JobID, receipt.AttemptID).Scan(&after))
		if after != string(raw) {
			t.Fatalf("budget scheduler changed %s attempt state", orchestration)
		}
		keys, err = f.s.Pins(ctx, owner)
		requireNoError(t, err)
		pin, err = f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
		requireNoError(t, err)
		if len(keys) != 1 || pin == nil || pin.Released {
			t.Fatalf("%s attempt lost cache/image pins", orchestration)
		}
	}
}
