package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostidentity"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

var _ hostimages.Repository = (*Store)(nil)

const imageGenerationColumns = `generation_id,installation_id,machine_id,engine_id,archive_sha256,archive_bytes,source_workspace_id,source_object_id,owner_job_id,owner_attempt_id,operation_id,expected_image_id,rootfs_diff_ids,state,created_at,prepare_lease_until,loading_at,last_used_at`

func (s *Store) PinnedImage(ctx context.Context, workspace domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID) (*hostimages.Pin, error) {
	if !workspace.Valid() || !job.Valid() || !attempt.Valid() {
		return nil, ErrInvalid
	}
	return read(ctx, s, func(ctx context.Context) (*hostimages.Pin, error) {
		var pin hostimages.Pin
		var released string
		err := s.db.QueryRowContext(ctx, `SELECT installation_id,machine_id,engine_id,archive_sha256,archive_bytes,object_id,released_at
		 FROM host_application_image_pins WHERE workspace_id=? AND job_id=? AND attempt_id=?`, workspace, job, attempt).
			Scan(&pin.Key.InstallationID, &pin.Key.MachineID, &pin.Key.EngineID, &pin.Key.ArchiveSHA256,
				&pin.Key.ArchiveBytes, &pin.ObjectID, &released)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, dbError(err)
		}
		pin.Released = released != ""
		if !pin.Valid() {
			return nil, ErrCorrupt
		}
		return &pin, nil
	})
}

func scanImageGeneration(row interface{ Scan(...any) error }) (hostimages.Generation, error) {
	var g hostimages.Generation
	var created, lease, loading, used, diffs string
	err := row.Scan(&g.ID, &g.Key.InstallationID, &g.Key.MachineID, &g.Key.EngineID, &g.Key.ArchiveSHA256,
		&g.Key.ArchiveBytes, &g.Owner.WorkspaceID, &g.Owner.ObjectID, &g.Owner.JobID, &g.Owner.AttemptID,
		&g.OperationID, &g.ExpectedImageID, &diffs, &g.State, &created, &lease, &loading, &used)
	if err != nil {
		return g, err
	}
	g.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return g, ErrCorrupt
	}
	g.LeaseUntil, err = time.Parse(time.RFC3339Nano, lease)
	if err != nil {
		return g, ErrCorrupt
	}
	if loading != "" {
		g.LoadingAt, err = time.Parse(time.RFC3339Nano, loading)
		if err != nil {
			return g, ErrCorrupt
		}
	}
	if used != "" {
		g.LastUsedAt, err = time.Parse(time.RFC3339Nano, used)
		if err != nil {
			return g, ErrCorrupt
		}
	}
	if diffs != "" && json.Unmarshal([]byte(diffs), &g.RootFSDiffIDs) != nil {
		return g, ErrCorrupt
	}
	if !g.Valid() {
		return g, ErrCorrupt
	}
	return g, nil
}

func imageByKey(ctx context.Context, q queryer, key hostimages.Key) (*hostimages.Generation, error) {
	g, err := scanImageGeneration(q.QueryRowContext(ctx, `SELECT `+imageGenerationColumns+` FROM host_application_images
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND archive_sha256=? AND archive_bytes=?
	 AND state IN ('preparing','loading','ready','unknown')`, key.InstallationID, key.MachineID, key.EngineID, key.ArchiveSHA256, key.ArchiveBytes))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	return &g, nil
}

func (s *Store) ActiveImage(ctx context.Context, key hostimages.Key) (*hostimages.Generation, error) {
	if !key.Valid() {
		return nil, ErrInvalid
	}
	return read(ctx, s, func(ctx context.Context) (*hostimages.Generation, error) {
		return imageByKey(ctx, s.db, key)
	})
}

func (s *Store) ClaimImage(ctx context.Context, owner hostimages.Owner, key hostimages.Key) (hostimages.Generation, bool, error) {
	if !owner.Valid() || !key.Valid() {
		return hostimages.Generation{}, false, ErrInvalid
	}
	var result hostimages.Generation
	var created bool
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var pinned bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_application_image_pins
		 WHERE workspace_id=? AND job_id=? AND attempt_id=? AND installation_id=? AND machine_id=? AND engine_id=?
		 AND archive_sha256=? AND archive_bytes=? AND object_id=? AND released_at='')`,
			owner.WorkspaceID, owner.JobID, owner.AttemptID, key.InstallationID, key.MachineID, key.EngineID,
			key.ArchiveSHA256, key.ArchiveBytes, owner.ObjectID).Scan(&pinned); err != nil {
			return dbError(err)
		}
		if !pinned {
			return hostimages.ErrImageState
		}
		active, err := imageByKey(ctx, tx, key)
		if err != nil {
			return err
		}
		if active != nil && active.State == hostimages.StatePreparing && !now.Before(active.LeaseUntil) {
			_, err = tx.ExecContext(ctx, `UPDATE host_application_images SET state='retired',retired_at=?,retired_reason='preload_expired'
			 WHERE generation_id=? AND state='preparing'`, now.Format(time.RFC3339Nano), active.ID)
			if err != nil {
				return dbError(err)
			}
			active = nil
		}
		if active != nil {
			result = *active
			return nil
		}
		id, err := randomID("img_", 16)
		if err != nil {
			return err
		}
		op, err := randomID("imload_", 16)
		if err != nil {
			return err
		}
		lease := now.Add(30 * time.Minute)
		result = hostimages.Generation{ID: id, Key: key, Owner: owner, OperationID: domain.OperationID(op),
			State: hostimages.StatePreparing, CreatedAt: now, LeaseUntil: lease}
		_, err = tx.ExecContext(ctx, `INSERT INTO host_application_images
		 (generation_id,installation_id,machine_id,engine_id,archive_sha256,archive_bytes,source_workspace_id,source_object_id,
		 owner_job_id,owner_attempt_id,operation_id,state,created_at,prepare_lease_until)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, key.InstallationID, key.MachineID, key.EngineID, key.ArchiveSHA256,
			key.ArchiveBytes, owner.WorkspaceID, owner.ObjectID, owner.JobID, owner.AttemptID, op, result.State,
			now.Format(time.RFC3339Nano), lease.Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
		created = true
		return nil
	})
	return result, created, err
}

func (s *Store) BeginImageLoad(ctx context.Context, g hostimages.Generation, imageID string, diffIDs []string) (bool, error) {
	if !g.Valid() || g.State != hostimages.StatePreparing || !strings.HasPrefix(imageID, "sha256:") ||
		!domain.SHA256Digest(strings.TrimPrefix(imageID, "sha256:")).Valid() {
		return false, ErrInvalid
	}
	candidate := g
	candidate.State, candidate.ExpectedImageID, candidate.RootFSDiffIDs = hostimages.StateLoading, imageID, diffIDs
	if !candidate.Valid() {
		return false, ErrInvalid
	}
	encodedDiffs, err := json.Marshal(diffIDs)
	if err != nil {
		return false, ErrInvalid
	}
	var begun bool
	err = s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var elected bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_application_images
		 WHERE generation_id=? AND operation_id=? AND state='preparing' AND prepare_lease_until>?)`,
			g.ID, g.OperationID, now.Format(time.RFC3339Nano)).Scan(&elected); err != nil {
			return dbError(err)
		}
		if !elected {
			return nil
		}
		fenced, err := storageContentFencedInTx(ctx, tx, hoststorage.Scope{InstallationID: g.Key.InstallationID,
			MachineID: g.Key.MachineID, EngineID: g.Key.EngineID}, "image", string(g.Key.ArchiveSHA256), g.Key.ArchiveBytes)
		if err != nil {
			return err
		}
		if fenced {
			return hoststorage.ErrIneligible
		}
		if err := commitStorageReservationInTx(ctx, tx, hoststorage.Scope{InstallationID: g.Key.InstallationID,
			MachineID: g.Key.MachineID, EngineID: g.Key.EngineID}, "image", g.Key.ArchiveSHA256, g.Key.ArchiveBytes, g.OperationID, now); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE host_application_images SET state='loading',expected_image_id=?,rootfs_diff_ids=?,loading_at=?
		 WHERE generation_id=? AND operation_id=? AND state='preparing' AND prepare_lease_until>?`,
			imageID, string(encodedDiffs), now.Format(time.RFC3339Nano), g.ID, g.OperationID, now.Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		if count != 1 {
			return nil
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO host_application_image_loads(operation_id,generation_id,expected_image_id,begun_at)
		 VALUES(?,?,?,?)`, g.OperationID, g.ID, imageID, now.Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
		begun = true
		return nil
	})
	return begun, err
}

func (s *Store) RetirePreloadImage(ctx context.Context, g hostimages.Generation) error {
	if !g.Valid() || g.State != hostimages.StatePreparing {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		result, err := tx.ExecContext(ctx, `UPDATE host_application_images SET state='retired',retired_at=?,retired_reason='preload_expired'
		 WHERE generation_id=? AND operation_id=? AND state='preparing'`, now.Format(time.RFC3339Nano), g.ID, g.OperationID)
		if err != nil {
			return dbError(err)
		}
		return requireImageRow(result)
	})
}

func (s *Store) MarkImageReady(ctx context.Context, g hostimages.Generation) error {
	if !g.Valid() || g.ExpectedImageID == "" {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		result, err := tx.ExecContext(ctx, `UPDATE host_application_images SET state='ready',ready_at=CASE WHEN ready_at='' THEN ? ELSE ready_at END,last_used_at=?
		 WHERE generation_id=? AND operation_id=? AND expected_image_id=? AND state IN ('loading','unknown','ready')`,
			now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), g.ID, g.OperationID, g.ExpectedImageID)
		if err != nil {
			return dbError(err)
		}
		return requireImageRow(result)
	})
}

func (s *Store) MarkImageUnknown(ctx context.Context, g hostimages.Generation) error {
	if !g.Valid() || g.ExpectedImageID == "" {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		result, err := tx.ExecContext(ctx, `UPDATE host_application_images SET state='unknown'
		 WHERE generation_id=? AND operation_id=? AND expected_image_id=? AND state IN ('loading','unknown')`,
			g.ID, g.OperationID, g.ExpectedImageID)
		if err != nil {
			return dbError(err)
		}
		return requireImageRow(result)
	})
}

func (s *Store) RetireMissingImage(ctx context.Context, g hostimages.Generation) error {
	if !g.Valid() || g.State != hostimages.StateReady {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		result, err := tx.ExecContext(ctx, `UPDATE host_application_images SET state='retired',retired_at=?,retired_reason='observed_missing'
		 WHERE generation_id=? AND operation_id=? AND expected_image_id=? AND state='ready'`,
			now.Format(time.RFC3339Nano), g.ID, g.OperationID, g.ExpectedImageID)
		if err != nil {
			return dbError(err)
		}
		return requireImageRow(result)
	})
}

func (s *Store) TouchImage(ctx context.Context, g hostimages.Generation) error {
	if !g.Valid() || g.State != hostimages.StateReady {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		result, err := tx.ExecContext(ctx, `UPDATE host_application_images SET last_used_at=?
		 WHERE generation_id=? AND operation_id=? AND expected_image_id=? AND state='ready'`,
			now.Format(time.RFC3339Nano), g.ID, g.OperationID, g.ExpectedImageID)
		if err != nil {
			return dbError(err)
		}
		return requireImageRow(result)
	})
}

func requireImageRow(result sql.Result) error {
	n, err := result.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if n != 1 {
		return hostimages.ErrImageState
	}
	return nil
}

func pinHostApplicationImageAdmission(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, profile admission.Profile, refs []admission.FrozenObject, now time.Time) error {
	for _, ref := range refs {
		if ref.Role != "application_image" {
			continue
		}
		target, err := cacheTargetInTx(ctx, tx, w, profile)
		if err != nil {
			return err
		}
		if target == nil {
			return admission.ErrRequirements
		}
		fenced, err := storageContentFencedInTx(ctx, tx, storageScope(target), "image", string(ref.Object.SHA256), ref.Object.Bytes)
		if err != nil {
			return err
		}
		if fenced {
			return hoststorage.ErrIneligible
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO host_application_image_pins
		 (workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,archive_sha256,archive_bytes,object_id,created_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`, w, job, attempt, target.installation, target.machine, target.engine,
			ref.Object.SHA256, ref.Object.Bytes, ref.Object.ID, now.Format(time.RFC3339Nano))
		return dbError(err)
	}
	return nil
}

func pinHostApplicationImageRetry(ctx context.Context, tx *sql.Tx, oldWorkspace domain.WorkspaceID, oldJob domain.JobID, oldAttempt, newAttempt domain.AttemptID, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO host_application_image_pins
	 (workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,archive_sha256,archive_bytes,object_id,created_at)
	 SELECT workspace_id,job_id,?,installation_id,machine_id,engine_id,archive_sha256,archive_bytes,object_id,?
	 FROM host_application_image_pins WHERE workspace_id=? AND job_id=? AND attempt_id=?`,
		newAttempt, now.Format(time.RFC3339Nano), oldWorkspace, oldJob, oldAttempt)
	return dbError(err)
}

func releaseHostApplicationImagePins(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE host_application_image_pins SET released_at=?
	 WHERE workspace_id=? AND job_id=? AND attempt_id=? AND released_at=''`, now.Format(time.RFC3339Nano), w, job, attempt)
	return dbError(err)
}

// A pin becomes evictable only after the exact staging container is confirmed
// deleted or absent by the aggregate cleanup operation.
func releaseHostApplicationImagePinsOnCleanup(ctx context.Context, tx *sql.Tx, op domain.Operation, resources []cleanup.ResourceWork, now time.Time) error {
	var hasPins bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_application_image_pins
	 WHERE workspace_id=? AND job_id=? AND attempt_id=? AND released_at='')`, op.WorkspaceID, op.JobID, op.AttemptID).Scan(&hasPins); err != nil {
		return dbError(err)
	}
	if !hasPins {
		return nil
	}
	var staging *cleanup.ResourceWork
	for i := range resources {
		if resources[i].Result.Purpose == "staging" {
			if staging != nil {
				return ErrCorrupt
			}
			staging = &resources[i]
		}
	}
	if staging == nil || !staging.Result.State.Confirmed() {
		return ErrCorrupt
	}
	var remote struct {
		ID       string `json:"id"`
		EngineID string `json:"engine_id"`
	}
	if json.Unmarshal([]byte(staging.Resource.Request.Remote.Resource), &remote) != nil ||
		!domain.SHA256Digest(remote.ID).Valid() || !storageNativeEngine.MatchString(remote.EngineID) ||
		!staging.Resource.Request.Remote.Identity.InstallationID.Valid() {
		return ErrCorrupt
	}
	var mismatch bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_application_image_pins
	 WHERE workspace_id=? AND job_id=? AND attempt_id=? AND released_at=''
	 AND (installation_id!=? OR engine_id!=?))`, op.WorkspaceID, op.JobID, op.AttemptID,
		staging.Resource.Request.Remote.Identity.InstallationID, hostidentity.Digest("docker-engine-id", remote.EngineID)).Scan(&mismatch); err != nil {
		return dbError(err)
	}
	if mismatch {
		return ErrCorrupt
	}
	return releaseHostApplicationImagePins(ctx, tx, op.WorkspaceID, op.JobID, op.AttemptID, now)
}
