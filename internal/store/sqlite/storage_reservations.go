package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

func contentReservationKey(sha domain.SHA256Digest, bytes int64) string {
	return string(sha) + ":" + strconv.FormatInt(bytes, 10)
}

func commitStorageReservationInTx(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, class string,
	sha domain.SHA256Digest, bytes int64, operation domain.OperationID, now time.Time) error {
	if !scope.Valid() || !sha.Valid() || bytes < 0 || !operation.Valid() || (class != "cache" && class != "image") {
		return ErrInvalid
	}
	var committed bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_storage_reservations
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND class=? AND resource_key=?
	 AND mutation_operation_id=? AND state='committed')`, scope.InstallationID, scope.MachineID, scope.EngineID,
		class, contentReservationKey(sha, bytes), operation).Scan(&committed)
	if err != nil {
		return dbError(err)
	}
	if committed {
		return nil
	}
	var id string
	err = tx.QueryRowContext(ctx, `SELECT reservation_id FROM host_storage_reservations
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND class=? AND resource_key=? AND state='provisional'`,
		scope.InstallationID, scope.MachineID, scope.EngineID, class, contentReservationKey(sha, bytes)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return reserveReplacementStorageInTx(ctx, tx, scope, class, sha, bytes, operation, now)
	}
	if err != nil {
		return dbError(err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE host_storage_reservations SET state='committed',mutation_operation_id=?
	 WHERE reservation_id=? AND state='provisional' AND mutation_operation_id IS NULL`, operation, id)
	if err != nil {
		return dbError(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if count != 1 {
		return ErrCorrupt
	}
	return nil
}

// Repair may create another physical generation while the retired object's
// reservation remains charged. Each mutation keeps its own immutable bound.
func reserveReplacementStorageInTx(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, class string,
	sha domain.SHA256Digest, bytes int64, operation domain.OperationID, now time.Time) error {
	key := contentReservationKey(sha, bytes)
	var tracked bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_storage_reservations
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND class=? AND resource_key=?)`,
		scope.InstallationID, scope.MachineID, scope.EngineID, class, key).Scan(&tracked); err != nil {
		return dbError(err)
	}
	if !tracked {
		// Historical attempts without a storage reservation retain their frozen
		// permit; their immutable generation ledger supplies the charge.
		return nil
	}
	var workspace domain.WorkspaceID
	var job domain.JobID
	var attempt domain.AttemptID
	query := `SELECT owner_workspace_id,owner_job_id,owner_attempt_id FROM host_cache_generations WHERE operation_id=?`
	amount := bytes
	if class == "image" {
		query = `SELECT source_workspace_id,owner_job_id,owner_attempt_id FROM host_application_images WHERE operation_id=?`
		amount = 4 << 30
	}
	if err := tx.QueryRowContext(ctx, query, operation).Scan(&workspace, &job, &attempt); err != nil {
		return dbError(err)
	}
	id, err := randomID("stres_", 16)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO host_storage_reservations
	 (reservation_id,installation_id,machine_id,engine_id,class,resource_key,bytes,owner_workspace_id,owner_job_id,owner_attempt_id,mutation_operation_id,state,created_at)
	 VALUES(?,?,?,?,?,?,?,?,?,?,?,'committed',?)`, id, scope.InstallationID, scope.MachineID, scope.EngineID,
		class, key, amount, workspace, job, attempt, operation, now.Format(time.RFC3339Nano))
	if err != nil {
		return dbError(err)
	}
	charge, err := freshStorageChargeInTx(ctx, tx, scope, now)
	if err != nil {
		return err
	}
	record, err := readJobRecord(ctx, tx, workspace, job)
	if err != nil {
		return err
	}
	configured, err := frozenStorageBudgetInTx(ctx, tx, record.Profile)
	if err != nil {
		return err
	}
	effective, err := effectiveStorageBudget(ctx, tx, scope, configured)
	if err != nil {
		return err
	}
	if charge.view.Machine.ChargedBytes >= effective.HardBytes {
		return hoststorage.ErrHardLimit
	}
	return nil
}

// A complete absence after retirement can settle a finished writer's bound.
// It records no deletion receipt and cannot settle a still-possible mutation.
func releaseAbsentStorageReservationsInTx(ctx context.Context, tx *sql.Tx, scan hoststorage.Scan, present map[string]bool) error {
	scope := scan.Scope
	rows, err := tx.QueryContext(ctx, `SELECT 'cache',volume_name,operation_id,retired_at FROM host_cache_generations
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND state='retired' AND writer_phase='finished'
	 UNION ALL SELECT 'image',expected_image_id,operation_id,retired_at FROM host_application_images
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND state='retired' AND retired_reason='observed_missing' AND ready_at!=''`,
		scope.InstallationID, scope.MachineID, scope.EngineID, scope.InstallationID, scope.MachineID, scope.EngineID)
	if err != nil {
		return dbError(err)
	}
	type candidate struct{ class, id, operation, retired string }
	items := []candidate{}
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.class, &item.id, &item.operation, &item.retired); err != nil {
			rows.Close()
			return dbError(err)
		}
		items = append(items, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return dbError(err)
	}
	for _, item := range items {
		retired, err := time.Parse(time.RFC3339Nano, item.retired)
		if err != nil {
			return ErrCorrupt
		}
		kind := hoststorage.Volume
		if item.class == "image" {
			kind = hoststorage.Image
		}
		if present[storageObjectKey(kind, item.id)] || scan.ObservedAt.Before(retired) {
			continue
		}
		_, err = tx.ExecContext(ctx, `UPDATE host_storage_reservations SET state='released',released_at=?
		 WHERE installation_id=? AND machine_id=? AND engine_id=? AND class=? AND mutation_operation_id=? AND state='committed'`,
			scan.ObservedAt.Format(time.RFC3339Nano), scope.InstallationID, scope.MachineID, scope.EngineID, item.class, item.operation)
		if err != nil {
			return dbError(err)
		}
	}
	return nil
}

func releaseAttemptStorageReservationInTx(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID,
	job domain.JobID, attempt domain.AttemptID, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE host_storage_reservations SET state='released',released_at=?
	 WHERE owner_workspace_id=? AND owner_job_id=? AND owner_attempt_id=? AND class='attempt' AND state!='released'`,
		now.Format(time.RFC3339Nano), w, job, attempt)
	return dbError(err)
}

// Terminal local-only work, or confirmed cleanup of its exact remote
// resources, discharges the attempt allowance. It also drops provisional
// content claims that have neither a live pin nor a writer generation.
func releaseAttemptStorageResourcesInTx(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID,
	job domain.JobID, attempt domain.AttemptID, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT installation_id,machine_id,engine_id FROM host_storage_reservations
	 WHERE owner_workspace_id=? AND owner_job_id=? AND owner_attempt_id=? AND state!='released'`, w, job, attempt)
	if err != nil {
		return dbError(err)
	}
	scopes := []hoststorage.Scope{}
	for rows.Next() {
		var scope hoststorage.Scope
		if err := rows.Scan(&scope.InstallationID, &scope.MachineID, &scope.EngineID); err != nil {
			rows.Close()
			return dbError(err)
		}
		scopes = append(scopes, scope)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return dbError(err)
	}
	if err := releaseAttemptStorageReservationInTx(ctx, tx, w, job, attempt, now); err != nil {
		return err
	}
	for _, scope := range scopes {
		if err := releaseUnusedContentReservationsInTx(ctx, tx, scope, now); err != nil {
			return err
		}
	}
	return nil
}

// A provisional content reservation can be discharged without host I/O only
// when no attempt remains pinned and no generation has ever been claimed for
// that key. Committed writer reservations wait for exact absence/deletion.
func releaseUnusedContentReservationsInTx(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `SELECT reservation_id,class,resource_key FROM host_storage_reservations
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND state='provisional' AND class IN ('cache','image')`,
		scope.InstallationID, scope.MachineID, scope.EngineID)
	if err != nil {
		return dbError(err)
	}
	type item struct{ id, class, key string }
	items := []item{}
	for rows.Next() {
		var value item
		if err := rows.Scan(&value.id, &value.class, &value.key); err != nil {
			rows.Close()
			return dbError(err)
		}
		items = append(items, value)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return dbError(err)
	}
	for _, value := range items {
		sha, count, ok := strings.Cut(value.key, ":")
		if !ok || !domain.SHA256Digest(sha).Valid() {
			return ErrCorrupt
		}
		bytes, err := strconv.ParseInt(count, 10, 64)
		if err != nil || bytes < 0 || strconv.FormatInt(bytes, 10) != count {
			return ErrCorrupt
		}
		var used bool
		if value.class == "cache" {
			err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_pins WHERE installation_id=? AND machine_id=? AND engine_id=?
			 AND sha256=? AND bytes=? AND released_at='') OR EXISTS(SELECT 1 FROM host_cache_generations
			 WHERE installation_id=? AND machine_id=? AND engine_id=? AND sha256=? AND bytes=?)`,
				scope.InstallationID, scope.MachineID, scope.EngineID, sha, bytes,
				scope.InstallationID, scope.MachineID, scope.EngineID, sha, bytes).Scan(&used)
		} else {
			err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_application_image_pins WHERE installation_id=? AND machine_id=? AND engine_id=?
			 AND archive_sha256=? AND archive_bytes=? AND released_at='') OR EXISTS(SELECT 1 FROM host_application_images
			 WHERE installation_id=? AND machine_id=? AND engine_id=? AND archive_sha256=? AND archive_bytes=?)`,
				scope.InstallationID, scope.MachineID, scope.EngineID, sha, bytes,
				scope.InstallationID, scope.MachineID, scope.EngineID, sha, bytes).Scan(&used)
		}
		if err != nil {
			return dbError(err)
		}
		if !used {
			_, err = tx.ExecContext(ctx, `UPDATE host_storage_reservations SET state='released',released_at=? WHERE reservation_id=? AND state='provisional'`,
				now.Format(time.RFC3339Nano), value.id)
			if err != nil {
				return dbError(err)
			}
		}
	}
	return nil
}
