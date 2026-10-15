package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

func storageKnownObject(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, object hoststorage.Object) (bool, error) {
	var known bool
	switch object.Kind {
	case hoststorage.Volume:
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_generations WHERE installation_id=? AND machine_id=? AND engine_id=? AND volume_name=?)`,
			scope.InstallationID, scope.MachineID, scope.EngineID, object.ID).Scan(&known)
		return known, dbError(err)
	case hoststorage.Image:
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_application_images WHERE installation_id=? AND machine_id=? AND engine_id=?
		 AND (expected_image_id=? OR generation_id=?))`, scope.InstallationID, scope.MachineID, scope.EngineID,
			object.ID, object.Labels[storageLabelPrefix+"image-generation"]).Scan(&known)
		return known, dbError(err)
	case hoststorage.Container:
		// provider_resources includes both staging and execution intents; a
		// credible ledger reference sends the object to attempt cleanup instead.
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_resources r
		 WHERE json_valid(r.identity) AND json_extract(r.identity,'$.InstallationID')=?
		 AND json_valid(r.resource_ref) AND (json_extract(r.resource_ref,'$.id')=?
		 OR (json_valid(json_extract(r.resource_ref,'$.resource')) AND json_extract(json_extract(r.resource_ref,'$.resource'),'$.id')=?)))`,
			scope.InstallationID, object.ID, object.ID).Scan(&known)
		if err != nil {
			return false, dbError(err)
		}
		if known {
			return true, nil
		}
		operation := object.Labels[storageLabelPrefix+"cache-operation"]
		if operation == "" {
			operation = object.Labels[storageLabelPrefix+"operation"]
		}
		if operation == "" {
			return false, nil
		}
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_generations WHERE installation_id=? AND machine_id=? AND engine_id=? AND operation_id=?)`,
			scope.InstallationID, scope.MachineID, scope.EngineID, operation).Scan(&known)
		return known, dbError(err)
	}
	return false, ErrCorrupt
}

func projectStorageOrphans(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, observation storageObservation,
	caller domain.WorkspaceID, view *hoststorage.View) error {
	if observation.state != "complete" {
		return nil
	}
	for _, object := range observation.objects {
		known, err := storageKnownObject(ctx, tx, scope, object)
		if err != nil {
			return err
		}
		if known {
			continue
		}
		if object.Kind == hoststorage.Container {
			// Another workspace's attempt labels are not exposed, even as an
			// opaque orphan row, to the current connection manager.
			if other := object.Labels[storageLabelPrefix+"workspace"]; other != "" && other != string(caller) {
				continue
			}
		}
		if len(view.Orphans) >= 64 {
			view.Truncated = true
			break
		}
		eligible := !object.Running
		if object.Kind != hoststorage.Container {
			eligible = eligible && object.References == 0
		}
		orphan := hoststorage.Orphan{ID: opaqueOrphanID(scope, observation.seq, object.Kind, object.ID), Class: object.Kind,
			ChargedBytes: object.SizeBytes, Eligible: credibleStorageOrphan(scope, object, caller) && eligible}
		view.Orphans = append(view.Orphans, orphan)
	}
	return nil
}

func projectStorageRetention(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, caller domain.WorkspaceID, now time.Time, view *hoststorage.View) error {
	rows, err := tx.QueryContext(ctx, `SELECT d.job_id,d.attempt_id,d.due_ms,
	 CASE WHEN o.status IS NOT NULL THEN o.status WHEN d.due_ms<=? THEN 'due' ELSE 'scheduled' END,
	 COALESCE(d.operation_id,'')
	 FROM remote_retention_deadlines d JOIN jobs j ON j.workspace_id=d.workspace_id AND j.job_id=d.job_id
	 JOIN managed_profile_runtime m ON m.profile=j.profile AND m.revision=j.profile_revision
	 LEFT JOIN operations o ON o.workspace_id=d.workspace_id AND o.operation_id=d.operation_id
	 WHERE d.workspace_id=? AND json_extract(m.config,'$.schema_version')=3
	 AND json_extract(m.config,'$.machine_id')=? AND json_extract(m.config,'$.engine_id')=?
	 AND (d.operation_id IS NULL OR o.status!='succeeded')
	 AND (d.operation_id IS NOT NULL OR (
	 ((d.kind='collected' AND EXISTS(SELECT 1 FROM collection_publications p WHERE p.workspace_id=d.workspace_id AND p.job_id=d.job_id AND p.attempt_id=d.attempt_id))
	 OR (d.kind='uncollected' AND NOT EXISTS(SELECT 1 FROM collection_publications p WHERE p.workspace_id=d.workspace_id AND p.job_id=d.job_id AND p.attempt_id=d.attempt_id)))
	 AND NOT EXISTS(SELECT 1 FROM operations c WHERE c.kind='cleanup' AND c.workspace_id=d.workspace_id AND c.job_id=d.job_id AND c.attempt_id=d.attempt_id AND c.status IN ('accepted','running','succeeded'))
	 AND NOT EXISTS(SELECT 1 FROM retired_explicit_attempts r WHERE r.workspace_id=d.workspace_id AND r.job_id=d.job_id AND r.attempt_id=d.attempt_id)))
	 ORDER BY d.due_ms,d.job_id,d.attempt_id LIMIT 33`, now.UnixMilli(), caller, scope.MachineID, scope.EngineID)
	if err != nil {
		return dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item hoststorage.RetentionItem
		var ms int64
		if err := rows.Scan(&item.JobID, &item.AttemptID, &ms, &item.Status, &item.OperationID); err != nil {
			return dbError(err)
		}
		if ms <= 0 {
			return ErrCorrupt
		}
		item.DueAt = time.UnixMilli(ms).UTC()
		item.Pending = ms <= now.UnixMilli() && item.Status != "succeeded"
		if len(view.Retention) < 32 {
			view.Retention = append(view.Retention, item)
		} else {
			view.Truncated = true
		}
	}
	return dbError(rows.Err())
}

func projectStorageOtherReferences(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, caller domain.WorkspaceID, view *hoststorage.View) error {
	var cache, images int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(sum(bytes),0) FROM host_cache_pins
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND workspace_id!=? AND released_at=''`,
		scope.InstallationID, scope.MachineID, scope.EngineID, caller).Scan(&cache); err != nil {
		return dbError(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(sum(archive_bytes),0) FROM host_application_image_pins
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND workspace_id!=? AND released_at=''`,
		scope.InstallationID, scope.MachineID, scope.EngineID, caller).Scan(&images); err != nil {
		return dbError(err)
	}
	if err := addStorageCharge(&view.OtherWorkspacesReferencedBytes, cache); err != nil {
		return err
	}
	return addStorageCharge(&view.OtherWorkspacesReferencedBytes, images)
}

func projectStorageExtras(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, observation storageObservation,
	caller domain.WorkspaceID, now time.Time, view *hoststorage.View) error {
	if err := projectStorageOtherReferences(ctx, tx, scope, caller, view); err != nil {
		return err
	}
	if err := projectStorageOrphans(ctx, tx, scope, observation, caller, view); err != nil {
		return err
	}
	if err := projectStorageRetention(ctx, tx, scope, caller, now, view); err != nil {
		return err
	}
	return nil
}
