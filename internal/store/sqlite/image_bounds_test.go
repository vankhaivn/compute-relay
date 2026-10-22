package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

func imageBoundFixture(t *testing.T) admissionFixture {
	t.Helper()
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	for _, workspace := range []string{"a", "b"} {
		p := f.profile
		p.Binding.Profile = "cache-host-" + workspace
		p.Binding.ConfigurationRevision = "rev_count_" + workspace
		p.CredentialRef = "vault:con_admission_" + workspace
		p.ResourceScope = "resource_" + cacheMachine
		p.CostClass = admission.CostOperatorManaged
		requireNoError(t, f.s.PutProfile(ctx, p, true))
		config := fmt.Sprintf(`{"schema_version":3,"image":"sha256:%064x","architecture":"amd64","recipe_sha256":"%064x","machine_id":%q,"engine_id":%q,"storage_budget":{"soft_bytes":%d,"hard_bytes":%d}}`, 1, 2, cacheMachine, cacheEngine, int64(256<<30), int64(512<<30))
		_, err := f.s.db.ExecContext(ctx, `INSERT INTO managed_profile_runtime(profile,revision,config)VALUES(?,?,?)`, p.Binding.Profile, p.Binding.ConfigurationRevision, []byte(config))
		requireNoError(t, err)
	}
	_, err := f.s.db.ExecContext(ctx, `UPDATE managed_connections SET storage_soft_bytes=?,storage_hard_bytes=?`, int64(256<<30), int64(512<<30))
	requireNoError(t, err)
	return f
}

func boundImageRequest(t *testing.T, f admissionFixture, index int) []byte {
	t.Helper()
	id := domain.ObjectID(fmt.Sprintf("bounded-image-%d", index))
	digest := domain.SHA256Digest(fmt.Sprintf("%064x", index+1))
	requireNoError(t, f.s.CommitObject(context.Background(), domain.ObjectMetadata{WorkspaceID: "a", ID: id, SHA256: digest, Bytes: 77}))
	return imageAdmissionJob("cache-host-a", string(id), string(digest), 77)
}
func TestImageCountAdmissionSerializesDistinctReservationsAndAllowsReuse(t *testing.T) {
	f := imageBoundFixture(t)
	ctx := context.Background()
	requests := make([][]byte, hostimages.ImageCountLimit+1)
	for i := range requests {
		requests[i] = boundImageRequest(t, f, i)
	}
	var workers sync.WaitGroup
	results := make(chan error, len(requests))
	for i, request := range requests {
		workers.Add(1)
		go func(index int, raw []byte) {
			defer workers.Done()
			_, err := f.service.Submit(ctx, f.p, "a", fmt.Sprintf("image-count-%d", index), raw)
			results <- err
		}(i, request)
	}
	workers.Wait()
	close(results)
	accepted, refused := 0, 0
	for err := range results {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, hostimages.ErrCountLimit):
			refused++
			var p domain.Problem
			if !errors.As(err, &p) || p.PublicDetails()[domain.ReasonDetail] != "host_image_count_limit" {
				t.Fatal("missing closed refusal", err)
			}
		default:
			t.Fatal("unexpected admission failure", err)
		}
	}
	if accepted != hostimages.ImageCountLimit || refused != 1 || tableCount(t, f.s, "jobs") != hostimages.ImageCountLimit || controlCount(t, f.s, "SELECT count(*) FROM host_application_images") != 0 {
		t.Fatal("image slots oversubscribed", accepted, refused)
	}
	var object, digest string
	var size int64
	requireNoError(t, f.s.db.QueryRow(`SELECT object_id,archive_sha256,archive_bytes FROM host_application_image_pins LIMIT 1`).Scan(&object, &digest, &size))
	_, err := f.service.Submit(ctx, f.p, "a", "image-count-reuse", imageAdmissionJob("cache-host-a", object, digest, size))
	requireNoError(t, err)
}

func TestImageCountUnknownAndPinnedGenerationsKeepTheirSlots(t *testing.T) {
	f := imageBoundFixture(t)
	ctx := context.Background()
	var existing hostimages.Generation
	for i := range hostimages.ImageCountLimit {
		receipt, err := f.service.Submit(ctx, f.p, "a", fmt.Sprintf("unknown-image-%d", i), boundImageRequest(t, f, i))
		requireNoError(t, err)
		pin, err := f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
		requireNoError(t, err)
		owner := hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: pin.ObjectID}
		g, _, err := f.s.ClaimImage(ctx, owner, pin.Key)
		requireNoError(t, err)
		begun, err := f.s.BeginImageLoad(ctx, g, fmt.Sprintf("sha256:%064x", i+200), []string{fmt.Sprintf("sha256:%064x", i+300)})
		requireNoError(t, err)
		if !begun {
			t.Fatal("load permit absent")
		}
		g, err = readImageGenerationForTest(ctx, f.s, g.Key)
		requireNoError(t, err)
		requireNoError(t, f.s.MarkImageUnknown(ctx, g))
		existing = g
	}
	// Releasing a pin does not establish that an uncertain load vanished.
	requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		_, err := tx.ExecContext(ctx, `UPDATE host_application_image_pins SET released_at=?`, now.Format(time.RFC3339Nano))
		return err
	}))
	_, err := f.service.Submit(ctx, f.p, "a", "unknown-new-image", boundImageRequest(t, f, 99))
	if !errors.Is(err, hostimages.ErrCountLimit) {
		t.Fatal("unknown images lost their slots", err)
	}
	// Existing frozen keys remain reusable despite the full host.
	request := imageAdmissionJob("cache-host-a", string(existing.Owner.ObjectID), string(existing.Key.ArchiveSHA256), existing.Key.ArchiveBytes)
	_, err = f.service.Submit(ctx, f.p, "a", "unknown-existing-image", request)
	requireNoError(t, err)
}
func readImageGenerationForTest(ctx context.Context, s *Store, key hostimages.Key) (hostimages.Generation, error) {
	g, err := s.ActiveImage(ctx, key)
	if err != nil {
		return hostimages.Generation{}, err
	}
	if g == nil {
		return hostimages.Generation{}, ErrInvalid
	}
	return *g, nil
}

func TestImageCountLegacyOverLimitRefusesNewClaimsBeforeIntent(t *testing.T) {
	f := imageBoundFixture(t)
	ctx := context.Background()
	var first hostimages.Owner
	var firstKey hostimages.Key
	for i := range hostimages.ImageCountLimit + 1 {
		receipt, err := f.service.Submit(ctx, f.p, "a", fmt.Sprintf("legacy-image-%d", i), cacheJob("cache-host-a", "", ""))
		requireNoError(t, err)
		info, err := f.s.Info(ctx)
		requireNoError(t, err)
		key := hostimages.Key{InstallationID: domain.RuntimeInstallationID(info.InstallationID), MachineID: cacheMachine, EngineID: cacheEngine, ArchiveSHA256: domain.SHA256Digest(fmt.Sprintf("%064x", i+1)), ArchiveBytes: 7}
		owner := hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: "code"}
		_, err = f.s.db.ExecContext(ctx, `INSERT INTO host_application_image_pins(workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,archive_sha256,archive_bytes,object_id,created_at)VALUES(?,?,?,?,?,?,?,?,?,?)`, owner.WorkspaceID, owner.JobID, owner.AttemptID, key.InstallationID, key.MachineID, key.EngineID, key.ArchiveSHA256, key.ArchiveBytes, owner.ObjectID, time.Now().Format(time.RFC3339Nano))
		requireNoError(t, err)
		if i == 0 {
			first, firstKey = owner, key
			_, _, err = f.s.ClaimImage(ctx, owner, key)
			requireNoError(t, err)
		}
		if i == hostimages.ImageCountLimit {
			if _, created, err := f.s.ClaimImage(ctx, owner, key); !errors.Is(err, hostimages.ErrCountLimit) || created {
				t.Fatal("legacy overflow created load intent", created, err)
			}
			err = f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
				record, err := readJobRecord(ctx, tx, "a", receipt.JobID)
				if err != nil {
					return err
				}
				return checkApplicationImageNewWork(ctx, tx, record, now)
			})
			if !errors.Is(err, hostimages.ErrCountLimit) {
				t.Fatal("legacy overflow crossed preparation gate", err)
			}
		}
	}
	if _, created, err := f.s.ClaimImage(ctx, first, firstKey); err != nil || created {
		t.Fatal("legacy active key blocked", created, err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM host_application_images") != 1 {
		t.Fatal("legacy overflow added a generation")
	}
}

func TestImageCountHeadroomEvictsOnlyExactUnpinnedImages(t *testing.T) {
	for _, outcome := range []hoststorage.DeleteOutcome{hoststorage.DeleteConfirmed, hoststorage.DeleteUnknown} {
		t.Run(string(outcome), func(t *testing.T) {
			f := imageBoundFixture(t)
			ctx := context.Background()
			objects := make([]hoststorage.Object, 0, hostimages.ImageCountLimit)
			var scope hoststorage.Scope
			for i := range hostimages.ImageCountLimit {
				receipt, err := f.service.Submit(ctx, f.p, "a", fmt.Sprintf("headroom-image-%d", i), boundImageRequest(t, f, i))
				requireNoError(t, err)
				pin, err := f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
				requireNoError(t, err)
				owner := hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: pin.ObjectID}
				g, _, err := f.s.ClaimImage(ctx, owner, pin.Key)
				requireNoError(t, err)
				id := fmt.Sprintf("sha256:%064x", i+200)
				_, err = f.s.BeginImageLoad(ctx, g, id, []string{fmt.Sprintf("sha256:%064x", i+300)})
				requireNoError(t, err)
				g, err = readImageGenerationForTest(ctx, f.s, g.Key)
				requireNoError(t, err)
				requireNoError(t, f.s.MarkImageReady(ctx, g))
				_, err = f.s.db.ExecContext(ctx, `UPDATE host_application_images SET last_used_at=? WHERE generation_id=?`, time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC).Format(time.RFC3339Nano), g.ID)
				requireNoError(t, err)
				scope = hoststorage.Scope{InstallationID: g.Key.InstallationID, MachineID: g.Key.MachineID, EngineID: g.Key.EngineID}
				labels := map[string]string{hostimages.LabelInstallation: string(scope.InstallationID), hostimages.LabelKind: "application", hostimages.LabelSchema: hostimages.ApplicationSchema, hostimages.LabelArchive: string(g.Key.ArchiveSHA256), hostimages.LabelMachine: string(scope.MachineID), hostimages.LabelEngine: string(scope.EngineID)}
				object := hoststorage.Object{Kind: hoststorage.Image, ID: id, SizeKnown: true, SizeBytes: 77, Labels: labels}
				if i == 0 {
					object.References = 1
				} // A tag or container mount blocks oldest image.
				objects = append(objects, object)
				if i > 1 {
					requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
						return releaseHostApplicationImagePins(ctx, tx, "a", receipt.JobID, receipt.AttemptID, now)
					}))
				}
			}
			profile := f.profile
			profile.Binding.Profile = "cache-host-a"
			profile.Binding.ConfigurationRevision = "rev_count_a"
			profile.CredentialRef = "vault:con_admission_a"
			profile.ResourceScope = "resource_" + cacheMachine
			profile.CostClass = admission.CostOperatorManaged
			record := func() {
				requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: time.Now().UTC(), Objects: objects}))
			}
			record()
			requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
			var id, reason string
			requireNoError(t, f.s.db.QueryRow(`SELECT object_id,reason FROM host_storage_cleanup_operations WHERE class='image'`).Scan(&id, &reason))
			if id != objects[2].ID || reason != "image_count_limit_lru" {
				t.Fatal("headroom evicted pinned or referenced image", id, reason)
			}
			// Repeated queueing cannot issue a second delete for the same observation.
			requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
			if controlCount(t, f.s, "SELECT count(*) FROM host_storage_cleanup_operations") != 1 {
				t.Fatal("count eviction repeated intent")
			}
			work, err := f.s.ClaimStorageCleanup(ctx, time.Minute)
			requireNoError(t, err)
			if work == nil {
				t.Fatal("no cleanup claim")
			}
			requireNoError(t, f.s.BeginStorageCleanup(ctx, *work))
			requireNoError(t, f.s.CompleteStorageCleanup(ctx, *work, outcome, "cleanup_outcome"))

			if outcome == hoststorage.DeleteUnknown {
				record()
				requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
				if controlCount(t, f.s, "SELECT count(*) FROM host_storage_cleanup_operations") != 1 {
					t.Fatal("unknown deletion repeated or over-evicted")
				}
				_, err = f.service.Submit(ctx, f.p, "a", "unknown-cleanup-no-slot", boundImageRequest(t, f, 99))
				if !errors.Is(err, hostimages.ErrCountLimit) {
					t.Fatal("unknown cleanup freed a slot", err)
				}
				return
			}
			// A confirmed exact receipt frees a slot despite the older scan.
			requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
				view, err := imageAllocationsInTx(ctx, tx, hostimages.Key{InstallationID: scope.InstallationID, MachineID: scope.MachineID, EngineID: scope.EngineID, ArchiveSHA256: domain.SHA256Digest(fmt.Sprintf("%064x", 999)), ArchiveBytes: 77})
				if err == nil && view.count != hostimages.ImageRetentionTarget {
					t.Fatalf("receipt did not free slot: %+v", view)
				}
				return err
			}))
			record()
			requireNoError(t, f.s.QueueStorageEvictions(ctx, scope, "a", profile))
			if controlCount(t, f.s, "SELECT count(*) FROM host_storage_cleanup_operations") != 2 {
				t.Fatal("new exact reappearance was not requeued")
			}
		})
	}
}

func TestImageCountRetryCannotReintroduceReleasedDistinctKey(t *testing.T) {
	f := imageBoundFixture(t)
	ctx := context.Background()
	old, err := f.service.Submit(ctx, f.p, "a", "released-image", boundImageRequest(t, f, 100))
	requireNoError(t, err)
	requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		return releaseHostApplicationImagePins(ctx, tx, "a", old.JobID, old.AttemptID, now)
	}))
	for i := range hostimages.ImageCountLimit {
		_, err := f.service.Submit(ctx, f.p, "a", fmt.Sprintf("retry-full-%d", i), boundImageRequest(t, f, i))
		requireNoError(t, err)
	}
	err = f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		return pinHostApplicationImageRetry(ctx, tx, "a", old.JobID, old.AttemptID, "new_attempt", now)
	})
	if !errors.Is(err, hostimages.ErrCountLimit) {
		t.Fatal("retry bypassed image slots", err)
	}
}
