package sqlite

import (
	"context"
	"database/sql"
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

func TestStorageCacheReplacementHasSeparateMutationReservation(t *testing.T) {
	for _, kind := range []hostcache.ObservationKind{hostcache.ObservedMissing, hostcache.ObservedCorrupt} {
		t.Run(string(kind), func(t *testing.T) {
			f := cacheAdmissionFixture(t)
			ctx := t.Context()
			receipt, err := f.service.Submit(ctx, f.p, "a", "cache-repair-reservation", cacheJob("cache-host-a", "", ""))
			requireNoError(t, err)
			owner := cacheOwner("a", receipt)
			keys, err := f.s.Pins(ctx, owner)
			requireNoError(t, err)
			original, fresh, err := f.s.Claim(ctx, owner, keys[0])
			requireNoError(t, err)
			if !fresh {
				t.Fatal("first generation was not elected")
			}
			requireNoError(t, f.s.BeginWrite(ctx, owner, original.ID))
			requireNoError(t, f.s.Verify(ctx, completeCacheObservation(original)))
			requireNoError(t, f.s.Retire(ctx, hostcache.Observation{GenerationID: original.ID, VolumeName: original.VolumeName,
				OperationID: original.OperationID, Key: original.Key, Kind: kind, WriterInactive: true}))
			replacement, fresh, err := f.s.Claim(ctx, owner, keys[0])
			requireNoError(t, err)
			if !fresh || replacement.ID == original.ID {
				t.Fatal("repair did not elect a separate generation")
			}
			_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=1,storage_hard_bytes=? WHERE connection_id='con_admission_a'`, hoststorage.AttemptAllowanceBytes+13)
			requireNoError(t, err)
			if err := f.s.BeginWrite(ctx, owner, replacement.ID); !errors.Is(err, hoststorage.ErrHardLimit) {
				t.Fatalf("repair bypassed new physical-byte budget: %v", err)
			}
			var phase string
			requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT writer_phase FROM host_cache_generations WHERE generation_id=?`, replacement.ID).Scan(&phase))
			var failedReservations int
			requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_reservations WHERE mutation_operation_id=?`, replacement.OperationID).Scan(&failedReservations))
			if phase != string(hostcache.WriterIntent) || failedReservations != 0 {
				t.Fatal("refused repair retained a permit or reservation")
			}
			_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_hard_bytes=? WHERE connection_id='con_admission_a'`, hoststorage.DefaultHardBytes)
			requireNoError(t, err)
			requireNoError(t, f.s.BeginWrite(ctx, owner, replacement.ID))
			var reservations int
			requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_reservations WHERE class='cache' AND state='committed'
			 AND mutation_operation_id IN (?,?)`, original.OperationID, replacement.OperationID).Scan(&reservations))
			if reservations != 2 {
				t.Fatalf("repair reused an old mutation reservation: %d", reservations)
			}
			scope := storageFixtureScope(t, f.s)
			var charge storageCharge
			requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
				observation, err := latestStorageObservation(ctx, tx, scope)
				if err != nil {
					return err
				}
				charge, err = storageChargeInTx(ctx, tx, scope, observation, "a")
				return err
			}))
			if charge.view.Machine.CacheBytes != 14 {
				t.Fatalf("repair lost conservative old/new physical charges: %d", charge.view.Machine.CacheBytes)
			}
		})
	}
}

func TestStorageImageReplacementAndRepeatedExactDeletionReleaseReservations(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := t.Context()
	digest := domain.SHA256Digest(strings.Repeat("e", 64))
	requireNoError(t, f.s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: "a", ID: "image", SHA256: digest, Bytes: 77}))
	receipt, err := f.service.Submit(ctx, f.p, "a", "image-repair-reservation", imageAdmissionJob("cache-host-a", "image", string(digest), 77))
	requireNoError(t, err)
	pin, err := f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
	requireNoError(t, err)
	if pin == nil {
		t.Fatal("image not pinned")
	}
	owner := hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: "image"}
	imageID := "sha256:" + strings.Repeat("d", 64)
	loads := 0
	load := func() hostimages.Generation {
		t.Helper()
		g, fresh, err := f.s.ClaimImage(ctx, owner, pin.Key)
		requireNoError(t, err)
		if !fresh {
			t.Fatal("replacement did not elect an image loader")
		}
		loads++
		if loads == 2 {
			_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=1,storage_hard_bytes=? WHERE connection_id='con_admission_a'`, int64(8<<30))
			requireNoError(t, err)
			begun, err := f.s.BeginImageLoad(ctx, g, imageID, []string{"sha256:" + strings.Repeat("f", 64)})
			if begun || !errors.Is(err, hoststorage.ErrHardLimit) {
				t.Fatalf("image repair bypassed bounded capacity: %v %v", begun, err)
			}
			var state string
			requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT state FROM host_application_images WHERE generation_id=?`, g.ID).Scan(&state))
			var count int
			requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_reservations WHERE mutation_operation_id=?`, g.OperationID).Scan(&count))
			if state != "preparing" || count != 0 {
				t.Fatal("refused image repair retained a load permit or reservation")
			}
			_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_hard_bytes=? WHERE connection_id='con_admission_a'`, hoststorage.DefaultHardBytes)
			requireNoError(t, err)
		}
		begun, err := f.s.BeginImageLoad(ctx, g, imageID, []string{"sha256:" + strings.Repeat("f", 64)})
		requireNoError(t, err)
		if !begun {
			t.Fatal("elected image did not begin loading")
		}
		active, err := f.s.ActiveImage(ctx, pin.Key)
		requireNoError(t, err)
		if active == nil {
			t.Fatal("loading image missing")
		}
		requireNoError(t, f.s.MarkImageReady(ctx, *active))
		active, err = f.s.ActiveImage(ctx, pin.Key)
		requireNoError(t, err)
		return *active
	}
	first := load()
	requireNoError(t, f.s.RetireMissingImage(ctx, first))
	second := load()
	if second.OperationID == first.OperationID {
		t.Fatal("missing image replayed original load")
	}
	scope := storageFixtureScope(t, f.s)
	profile := f.profile
	profile.Binding.Profile, profile.Binding.ConfigurationRevision = "cache-host-a", "rev_cache_a"
	profile.CredentialRef, profile.ResourceScope, profile.CostClass = "vault:con_admission_a", "resource_"+cacheMachine, admission.CostOperatorManaged
	remove := func() {
		t.Helper()
		requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
			if err := releaseHostCachePinsWithoutPreparation(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now); err != nil {
				return err
			}
			if err := releaseHostApplicationImagePins(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now); err != nil {
				return err
			}
			return releaseAttemptStorageResourcesInTx(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now)
		}))
		_, err := f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=?,storage_hard_bytes=? WHERE connection_id='con_admission_a'`, int64(3<<30), int64(6<<30))
		requireNoError(t, err)
		object := hoststorage.Object{Kind: hoststorage.Image, ID: imageID, SizeKnown: true, SizeBytes: 4 << 30,
			Labels: map[string]string{hostimages.LabelInstallation: string(scope.InstallationID), hostimages.LabelKind: "application",
				hostimages.LabelSchema: hostimages.ApplicationSchema, hostimages.LabelArchive: string(digest),
				hostimages.LabelMachine: string(scope.MachineID), hostimages.LabelEngine: string(scope.EngineID)}}
		requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: []hoststorage.Object{object}}))
		requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
		work, err := f.s.ClaimStorageCleanup(ctx, time.Minute)
		requireNoError(t, err)
		if work == nil || work.Target.Action != hoststorage.EvictImage || work.Target.ID != imageID {
			t.Fatalf("wrong image eviction: %+v", work)
		}
		requireNoError(t, f.s.BeginStorageCleanup(ctx, *work))
		requireNoError(t, f.s.CompleteStorageCleanup(ctx, *work, hoststorage.DeleteConfirmed, "deleted"))
		var live int
		requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_reservations WHERE class='image' AND state!='released'`).Scan(&live))
		if live != 0 {
			t.Fatalf("exact image deletion retained %d live generation reservations", live)
		}
		var charged int64
		requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
			observation, err := latestStorageObservation(ctx, tx, scope)
			if err != nil {
				return err
			}
			charge, err := storageChargeInTx(ctx, tx, scope, observation, "a")
			charged = charge.view.Machine.ImageSizeUpperBound
			return err
		}))
		if charged != 0 {
			t.Fatalf("exact deleted image retained %d charged bytes", charged)
		}
	}
	remove()
	_, err = f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=?,storage_hard_bytes=? WHERE connection_id='con_admission_a'`, hoststorage.DefaultSoftBytes, hoststorage.DefaultHardBytes)
	requireNoError(t, err)
	requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: []hoststorage.Object{}}))
	receipt, err = f.service.Submit(ctx, f.p, "a", "image-after-first-eviction", imageAdmissionJob("cache-host-a", "image", string(digest), 77))
	requireNoError(t, err)
	pin, err = f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
	requireNoError(t, err)
	if pin == nil {
		t.Fatal("reloaded image was not pinned")
	}
	owner = hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: "image"}
	third := load()
	if third.OperationID == second.OperationID {
		t.Fatal("deleted image did not get a fresh load operation")
	}
	remove()
}

func TestStorageAbsenceSettlesOnlyFinishedRetiredCache(t *testing.T) {
	for _, mode := range []string{"retired_absent", "corrupt_present", "old_scan", "begun_unknown"} {
		t.Run(mode, func(t *testing.T) {
			f := cacheAdmissionFixture(t)
			ctx := t.Context()
			now := time.Now().UTC().Add(time.Second)
			f.s.options.Clock = func() time.Time { return now }
			receipt, err := f.service.Submit(ctx, f.p, "a", "absence-reservation", cacheJob("cache-host-a", "", ""))
			requireNoError(t, err)
			owner := cacheOwner("a", receipt)
			keys, err := f.s.Pins(ctx, owner)
			requireNoError(t, err)
			g, _, err := f.s.Claim(ctx, owner, keys[0])
			requireNoError(t, err)
			requireNoError(t, f.s.BeginWrite(ctx, owner, g.ID))
			oldScanAt := now
			if mode != "begun_unknown" {
				requireNoError(t, f.s.Verify(ctx, completeCacheObservation(g)))
				now = now.Add(time.Second)
				kind := hostcache.ObservedMissing
				if mode == "corrupt_present" {
					kind = hostcache.ObservedCorrupt
				}
				requireNoError(t, f.s.Retire(ctx, hostcache.Observation{GenerationID: g.ID, VolumeName: g.VolumeName, OperationID: g.OperationID,
					Key: g.Key, Kind: kind, WriterInactive: true}))
			}
			now = now.Add(time.Second)
			scan := hoststorage.Scan{Scope: storageFixtureScope(t, f.s), ObservedAt: now, Objects: []hoststorage.Object{}}
			if mode == "corrupt_present" {
				scan.Objects = []hoststorage.Object{{Kind: hoststorage.Volume, ID: g.VolumeName, SizeKnown: true, SizeBytes: 7, Labels: g.Labels()}}
			} else if mode == "old_scan" {
				scan.ObservedAt = oldScanAt
			}
			requireNoError(t, f.s.RecordStorageScan(ctx, scan))
			var charge storageCharge
			requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
				observation, err := latestStorageObservation(ctx, tx, scan.Scope)
				if err != nil {
					return err
				}
				charge, err = storageChargeInTx(ctx, tx, scan.Scope, observation, "a")
				return err
			}))
			want := int64(7)
			if mode == "retired_absent" {
				want = 0
			}
			if charge.view.Machine.CacheBytes != want {
				t.Fatalf("%s charge=%d, want %d", mode, charge.view.Machine.CacheBytes, want)
			}
			var receipts int
			requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_cleanup_operations`).Scan(&receipts))
			if receipts != 0 {
				t.Fatal("inventory absence fabricated a deletion receipt")
			}
		})
	}
}

func TestStorageAbsenceSettlesOnlyRetiredReadyImage(t *testing.T) {
	for _, mode := range []string{"retired_absent", "retired_present", "old_scan", "load_unknown"} {
		t.Run(mode, func(t *testing.T) {
			f := cacheAdmissionFixture(t)
			ctx := t.Context()
			now := time.Now().UTC().Add(time.Second)
			f.s.options.Clock = func() time.Time { return now }
			digest := domain.SHA256Digest(strings.Repeat("e", 64))
			requireNoError(t, f.s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: "a", ID: "image", SHA256: digest, Bytes: 77}))
			receipt, err := f.service.Submit(ctx, f.p, "a", "image-absence", imageAdmissionJob("cache-host-a", "image", string(digest), 77))
			requireNoError(t, err)
			pin, err := f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
			requireNoError(t, err)
			if pin == nil {
				t.Fatal("image not pinned")
			}
			owner := hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: "image"}
			g, _, err := f.s.ClaimImage(ctx, owner, pin.Key)
			requireNoError(t, err)
			imageID := "sha256:" + strings.Repeat("d", 64)
			begun, err := f.s.BeginImageLoad(ctx, g, imageID, []string{"sha256:" + strings.Repeat("f", 64)})
			requireNoError(t, err)
			if !begun {
				t.Fatal("image did not begin loading")
			}
			active, err := f.s.ActiveImage(ctx, pin.Key)
			requireNoError(t, err)
			if active == nil {
				t.Fatal("loading image missing")
			}
			oldScanAt := now
			if mode == "load_unknown" {
				requireNoError(t, f.s.MarkImageUnknown(ctx, *active))
			} else {
				requireNoError(t, f.s.MarkImageReady(ctx, *active))
				active, err = f.s.ActiveImage(ctx, pin.Key)
				requireNoError(t, err)
				now = now.Add(time.Second)
				requireNoError(t, f.s.RetireMissingImage(ctx, *active))
			}
			now = now.Add(time.Second)
			scope := storageFixtureScope(t, f.s)
			scan := hoststorage.Scan{Scope: scope, ObservedAt: now, Objects: []hoststorage.Object{}}
			if mode == "retired_present" {
				scan.Objects = []hoststorage.Object{{Kind: hoststorage.Image, ID: imageID, SizeKnown: true, SizeBytes: 77,
					Labels: map[string]string{hostimages.LabelInstallation: string(scope.InstallationID), hostimages.LabelKind: "application",
						hostimages.LabelSchema: hostimages.ApplicationSchema, hostimages.LabelArchive: string(digest),
						hostimages.LabelMachine: string(scope.MachineID), hostimages.LabelEngine: string(scope.EngineID)}}}
			} else if mode == "old_scan" {
				scan.ObservedAt = oldScanAt
			}
			requireNoError(t, f.s.RecordStorageScan(ctx, scan))
			var charge storageCharge
			requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
				observation, err := latestStorageObservation(ctx, tx, scope)
				if err != nil {
					return err
				}
				charge, err = storageChargeInTx(ctx, tx, scope, observation, "a")
				return err
			}))
			want := int64(4 << 30)
			if mode == "retired_absent" {
				want = 0
			}
			if charge.view.Machine.ImageSizeUpperBound != want {
				t.Fatalf("%s image charge=%d, want %d", mode, charge.view.Machine.ImageSizeUpperBound, want)
			}
			var receipts int
			requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM host_storage_cleanup_operations`).Scan(&receipts))
			if receipts != 0 {
				t.Fatal("image absence fabricated a deletion receipt")
			}
		})
	}
}

func TestStorageExpiredImageNormalizerCannotConsumeReservation(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := t.Context()
	now := time.Now().UTC().Add(time.Second)
	f.s.options.Clock = func() time.Time { return now }
	digest := domain.SHA256Digest(strings.Repeat("e", 64))
	requireNoError(t, f.s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: "a", ID: "image", SHA256: digest, Bytes: 77}))
	receipt, err := f.service.Submit(ctx, f.p, "a", "image-expired-normalizer", imageAdmissionJob("cache-host-a", "image", string(digest), 77))
	requireNoError(t, err)
	pin, err := f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
	requireNoError(t, err)
	if pin == nil {
		t.Fatal("image not pinned")
	}
	owner := hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: "image"}
	stale, _, err := f.s.ClaimImage(ctx, owner, pin.Key)
	requireNoError(t, err)
	now = now.Add(31 * time.Minute)
	current, fresh, err := f.s.ClaimImage(ctx, owner, pin.Key)
	requireNoError(t, err)
	if !fresh || current.ID == stale.ID {
		t.Fatal("expired normalization was not replaced")
	}
	begun, err := f.s.BeginImageLoad(ctx, stale, "sha256:"+strings.Repeat("d", 64), []string{"sha256:" + strings.Repeat("f", 64)})
	requireNoError(t, err)
	if begun {
		t.Fatal("expired normalizer acquired a load permit")
	}
	var state string
	var operation sql.NullString
	requireNoError(t, f.s.db.QueryRowContext(ctx, `SELECT state,mutation_operation_id FROM host_storage_reservations WHERE class='image'`).Scan(&state, &operation))
	if state != "provisional" || operation.Valid {
		t.Fatalf("expired normalizer consumed current reservation: %s/%s", state, operation.String)
	}
	begun, err = f.s.BeginImageLoad(ctx, current, "sha256:"+strings.Repeat("d", 64), []string{"sha256:" + strings.Repeat("f", 64)})
	requireNoError(t, err)
	if !begun {
		t.Fatal("current normalizer could not consume its original reservation")
	}
}
