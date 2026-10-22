package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
)

type imageAllocationView struct {
	count  int
	reused bool
}

func imageCountRefusal() error {
	problem, _ := domain.NewProblem(domain.CodeResourceRequirementUnsatisfied, "The host's application image slots are occupied; reuse an existing image or wait for exact cleanup.", domain.FailureStageValidation)
	return problem.WithDetail(domain.ReasonDetail, "host_image_count_limit").WithCause(hostimages.ErrCountLimit)
}

// Count distinct archive identities across resident/uncertain generations and
// pending pins. A complete later observation can also reintroduce a retired image.
func imageAllocationsInTx(ctx context.Context, tx *sql.Tx, key hostimages.Key) (imageAllocationView, error) {
	var scan int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(max(scan_seq),0) FROM host_storage_scans
  WHERE installation_id=? AND machine_id=? AND engine_id=? AND status='complete'`, key.InstallationID, key.MachineID, key.EngineID).Scan(&scan); err != nil {
		return imageAllocationView{}, dbError(err)
	}
	var view imageAllocationView
	err := tx.QueryRowContext(ctx, `WITH present AS (
  SELECT o.object_id FROM host_storage_objects o WHERE o.scan_seq=? AND o.class='image'
  AND json_extract(o.labels_json,'$."io.compute-relay.image-kind"')='application'
  AND json_extract(o.labels_json,'$."io.compute-relay.image-machine"')=?
  AND json_extract(o.labels_json,'$."io.compute-relay.image-engine"')=?
  AND NOT EXISTS(SELECT 1 FROM host_storage_cleanup_operations d
   WHERE d.installation_id=? AND d.machine_id=? AND d.engine_id=? AND d.class='image' AND d.object_id=o.object_id
   AND d.result IN ('deleted','absent') AND d.confirmed_scan_seq>=o.scan_seq)
 ), allocated AS (
  SELECT archive_sha256,archive_bytes FROM host_application_images
   WHERE installation_id=? AND machine_id=? AND engine_id=?
   AND (state IN ('preparing','loading','ready','unknown') OR expected_image_id IN (SELECT object_id FROM present))
  UNION
  SELECT archive_sha256,archive_bytes FROM host_application_image_pins
   WHERE installation_id=? AND machine_id=? AND engine_id=? AND released_at=''
 )
 SELECT (SELECT count(*) FROM allocated)+(SELECT count(*) FROM present p
  WHERE NOT EXISTS(SELECT 1 FROM host_application_images g
   WHERE g.installation_id=? AND g.machine_id=? AND g.engine_id=? AND g.expected_image_id=p.object_id)),
 EXISTS(SELECT 1 FROM allocated WHERE archive_sha256=? AND archive_bytes=?)`,
		scan, key.MachineID, key.EngineID, key.InstallationID, key.MachineID, key.EngineID,
		key.InstallationID, key.MachineID, key.EngineID, key.InstallationID, key.MachineID, key.EngineID,
		key.InstallationID, key.MachineID, key.EngineID, key.ArchiveSHA256, key.ArchiveBytes).Scan(&view.count, &view.reused)
	return view, dbError(err)
}

func admitApplicationImageKeyInTx(ctx context.Context, tx *sql.Tx, key hostimages.Key) error {
	view, err := imageAllocationsInTx(ctx, tx, key)
	if err != nil {
		return err
	}
	if !view.reused && view.count >= hostimages.ImageCountLimit {
		return imageCountRefusal()
	}
	return nil
}

func claimApplicationImageKeyInTx(ctx context.Context, tx *sql.Tx, key hostimages.Key) error {
	view, err := imageAllocationsInTx(ctx, tx, key)
	if err != nil {
		return err
	}
	// The current attempt's pin is already included. Existing active generations
	// are returned before this check, so old observed/uncertain work can continue.
	if view.count > hostimages.ImageCountLimit {
		return imageCountRefusal()
	}
	return nil
}

func checkApplicationImageNewWork(ctx context.Context, tx *sql.Tx, record admission.Record, _ time.Time) error {
	var key hostimages.Key
	err := tx.QueryRowContext(ctx, `SELECT installation_id,machine_id,engine_id,archive_sha256,archive_bytes
 FROM host_application_image_pins WHERE workspace_id=? AND job_id=? AND attempt_id=? AND released_at=''`, record.Job.WorkspaceID, record.Job.ID, record.Attempt.ID).
		Scan(&key.InstallationID, &key.MachineID, &key.EngineID, &key.ArchiveSHA256, &key.ArchiveBytes)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return dbError(err)
	}
	active, err := imageByKey(ctx, tx, key)
	if err != nil || active != nil {
		return err
	}
	return claimApplicationImageKeyInTx(ctx, tx, key)
}
