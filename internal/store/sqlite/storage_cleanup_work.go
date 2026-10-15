package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

var _ cleanup.StorageRepository = (*Store)(nil)

func scanStorageWork(ctx context.Context, tx *sql.Tx, id domain.OperationID, lease cleanup.Lease) (cleanup.StorageWork, error) {
	var work cleanup.StorageWork
	var labels, binding string
	err := tx.QueryRowContext(ctx, `SELECT installation_id,machine_id,engine_id,action,class,object_id,labels_json,binding_json,result
	 FROM host_storage_cleanup_operations WHERE operation_id=? AND workspace_id=? AND status='running'`, id, lease.WorkspaceID).
		Scan(&work.Target.Scope.InstallationID, &work.Target.Scope.MachineID, &work.Target.Scope.EngineID,
			&work.Target.Action, &work.Target.Class, &work.Target.ID, &labels, &binding, &work.State)
	if err != nil {
		return work, dbError(err)
	}
	if json.Unmarshal([]byte(labels), &work.Target.Labels) != nil || json.Unmarshal([]byte(binding), &work.Binding) != nil {
		return work, ErrCorrupt
	}
	work.Lease = lease
	if !work.Valid() {
		return work, ErrCorrupt
	}
	return work, nil
}

func (s *Store) ClaimStorageCleanup(ctx context.Context, ttl time.Duration) (*cleanup.StorageWork, error) {
	if ttl < time.Second || ttl > 31*time.Minute {
		return nil, ErrInvalid
	}
	var result *cleanup.StorageWork
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var w domain.WorkspaceID
		var id domain.OperationID
		var generation int64
		err := tx.QueryRowContext(ctx, `SELECT o.workspace_id,o.operation_id,COALESCE(l.generation,0)
		 FROM host_storage_cleanup_operations o LEFT JOIN host_storage_cleanup_leases l ON l.operation_id=o.operation_id
		 WHERE o.status IN ('accepted','running') AND (l.operation_id IS NULL OR l.until_ms<=?)
		 ORDER BY o.created_at,o.operation_id LIMIT 1`, now.UnixMilli()).Scan(&w, &id, &generation)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		if generation == int64(^uint64(0)>>1) {
			return ErrCorrupt
		}
		fence, err := randomID("", 32)
		if err != nil {
			return err
		}
		until := now.Add(ttl)
		_, err = tx.ExecContext(ctx, `INSERT INTO host_storage_cleanup_leases(operation_id,generation,fence,until_ms)
		 VALUES(?,?,?,?) ON CONFLICT(operation_id) DO UPDATE SET generation=excluded.generation,fence=excluded.fence,until_ms=excluded.until_ms`,
			id, generation+1, fence, until.UnixMilli())
		if err != nil {
			return dbError(err)
		}
		_, err = tx.ExecContext(ctx, `UPDATE host_storage_cleanup_operations SET status='running',updated_at=? WHERE operation_id=? AND status IN ('accepted','running')`, now.Format(time.RFC3339Nano), id)
		if err != nil {
			return dbError(err)
		}
		lease := cleanup.Lease{WorkspaceID: w, OperationID: id, Generation: generation + 1, Fence: fence, Until: until}
		work, err := scanStorageWork(ctx, tx, id, lease)
		if err != nil {
			return err
		}
		result = &work
		return nil
	})
	return result, err
}

func storageLeaseCurrent(ctx context.Context, tx *sql.Tx, work cleanup.StorageWork, now time.Time) (bool, error) {
	var current bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_storage_cleanup_leases l
	 JOIN host_storage_cleanup_operations o ON o.operation_id=l.operation_id
	 WHERE l.operation_id=? AND o.workspace_id=? AND o.status='running'
	 AND l.generation=? AND l.fence=? AND l.until_ms>?)`, work.Lease.OperationID,
		work.Lease.WorkspaceID, work.Lease.Generation, work.Lease.Fence, now.UnixMilli()).Scan(&current)
	return current, dbError(err)
}

func (s *Store) RenewStorageCleanup(ctx context.Context, work cleanup.StorageWork, ttl time.Duration) (time.Time, error) {
	if !work.Valid() || ttl < time.Second || ttl > 31*time.Minute {
		return time.Time{}, ErrInvalid
	}
	var until time.Time
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		current, err := storageLeaseCurrent(ctx, tx, work, now)
		if err != nil {
			return err
		}
		if !current {
			return cleanup.ErrLeaseLost
		}
		until = now.Add(ttl)
		_, err = tx.ExecContext(ctx, `UPDATE host_storage_cleanup_leases SET until_ms=? WHERE operation_id=? AND generation=? AND fence=?`,
			until.UnixMilli(), work.Lease.OperationID, work.Lease.Generation, work.Lease.Fence)
		return dbError(err)
	})
	return until, err
}

func storageEligibleInTx(ctx context.Context, tx *sql.Tx, target hoststorage.Target) (bool, error) {
	if !target.Valid() {
		return false, ErrInvalid
	}
	if target.Action == hoststorage.OrphanContainer || target.Action == hoststorage.OrphanVolume || target.Action == hoststorage.OrphanImage {
		known, err := storageKnownObject(ctx, tx, target.Scope, hoststorage.Object{Kind: target.Class, ID: target.ID, Labels: target.Labels})
		return !known, err
	}
	var eligible bool
	var err error
	switch target.Action {
	case hoststorage.EvictCache:
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_generations g
		 WHERE g.installation_id=? AND g.machine_id=? AND g.engine_id=? AND g.volume_name=?
		 AND g.state IN ('verified','retired') AND g.writer_phase='finished'
		 AND NOT EXISTS(SELECT 1 FROM host_cache_pins p WHERE p.installation_id=g.installation_id AND p.machine_id=g.machine_id
		 AND p.engine_id=g.engine_id AND p.sha256=g.sha256 AND p.bytes=g.bytes AND p.released_at=''))`,
			target.Scope.InstallationID, target.Scope.MachineID, target.Scope.EngineID, target.ID).Scan(&eligible)
	case hoststorage.EvictImage:
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_application_images g
		 WHERE g.installation_id=? AND g.machine_id=? AND g.engine_id=? AND g.expected_image_id=? AND g.state IN ('ready','retired')
		 AND NOT EXISTS(SELECT 1 FROM host_application_image_pins p WHERE p.installation_id=g.installation_id AND p.machine_id=g.machine_id
		 AND p.engine_id=g.engine_id AND p.archive_sha256=g.archive_sha256 AND p.archive_bytes=g.archive_bytes AND p.released_at=''))`,
			target.Scope.InstallationID, target.Scope.MachineID, target.Scope.EngineID, target.ID).Scan(&eligible)
	default:
		return false, ErrInvalid
	}
	return eligible, dbError(err)
}

func (s *Store) BeginStorageCleanup(ctx context.Context, work cleanup.StorageWork) error {
	if !work.Valid() || work.State != "pending" {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		current, err := storageLeaseCurrent(ctx, tx, work, now)
		if err != nil {
			return err
		}
		if !current {
			return cleanup.ErrLeaseLost
		}
		eligible, err := storageEligibleInTx(ctx, tx, work.Target)
		if err != nil {
			return err
		}
		if !eligible {
			return hoststorage.ErrIneligible
		}
		result, err := tx.ExecContext(ctx, `UPDATE host_storage_cleanup_operations SET result='running',reason='delete_intent_recorded',updated_at=?
		 WHERE operation_id=? AND status='running' AND result='pending'`, now.Format(time.RFC3339Nano), work.Lease.OperationID)
		if err != nil {
			return dbError(err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		if count != 1 {
			return cleanup.ErrLeaseLost
		}
		return nil
	})
}

func (s *Store) CompleteStorageCleanup(ctx context.Context, work cleanup.StorageWork, outcome hoststorage.DeleteOutcome) error {
	if !work.Valid() || (outcome != hoststorage.DeleteConfirmed && outcome != hoststorage.AbsentConfirmed &&
		outcome != hoststorage.DeleteRetained && outcome != hoststorage.DeleteUnknown) {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		current, err := storageLeaseCurrent(ctx, tx, work, now)
		if err != nil {
			return err
		}
		if !current {
			return cleanup.ErrLeaseLost
		}
		status := "failed"
		if outcome == hoststorage.DeleteConfirmed || outcome == hoststorage.AbsentConfirmed {
			status = "succeeded"
		}
		prior := "pending"
		if work.State == "running" {
			prior = "running"
		}
		var confirmedScan int64
		if status == "succeeded" {
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(max(scan_seq),0) FROM host_storage_scans
			 WHERE installation_id=? AND machine_id=? AND engine_id=? AND status='complete'`,
				work.Target.Scope.InstallationID, work.Target.Scope.MachineID, work.Target.Scope.EngineID).Scan(&confirmedScan); err != nil {
				return dbError(err)
			}
		}
		result, err := tx.ExecContext(ctx, `UPDATE host_storage_cleanup_operations
		 SET status=?,result=?,reason=?,updated_at=?,confirmed_scan_seq=?
		 WHERE operation_id=? AND status='running' AND result IN (?, 'running')`, status, outcome,
			string(outcome), now.Format(time.RFC3339Nano), confirmedScan, work.Lease.OperationID, prior)
		if err != nil {
			return dbError(err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		if count != 1 {
			return cleanup.ErrLeaseLost
		}
		if status == "succeeded" {
			if err := releaseDeletedStorageContentInTx(ctx, tx, work.Target, now); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM host_storage_cleanup_leases WHERE operation_id=? AND generation=? AND fence=?`,
			work.Lease.OperationID, work.Lease.Generation, work.Lease.Fence)
		return dbError(err)
	})
}

func releaseDeletedStorageContentInTx(ctx context.Context, tx *sql.Tx, target hoststorage.Target, now time.Time) error {
	var operation, sha string
	var bytes int64
	scope := target.Scope
	stamp := now.Format(time.RFC3339Nano)
	switch target.Action {
	case hoststorage.EvictCache:
		err := tx.QueryRowContext(ctx, `SELECT operation_id,sha256,bytes FROM host_cache_generations
		 WHERE installation_id=? AND machine_id=? AND engine_id=? AND volume_name=?`,
			scope.InstallationID, scope.MachineID, scope.EngineID, target.ID).Scan(&operation, &sha, &bytes)
		if err != nil {
			return dbError(err)
		}
		_, err = tx.ExecContext(ctx, `UPDATE host_cache_generations
		 SET state='retired',retired_at=?,retired_reason='missing'
		 WHERE installation_id=? AND machine_id=? AND engine_id=? AND volume_name=? AND state='verified'`,
			stamp, scope.InstallationID, scope.MachineID, scope.EngineID, target.ID)
		if err != nil {
			return dbError(err)
		}
	case hoststorage.EvictImage:
		_, err := tx.ExecContext(ctx, `UPDATE host_application_images
		 SET state='retired',retired_at=?,retired_reason='observed_missing'
		 WHERE installation_id=? AND machine_id=? AND engine_id=? AND expected_image_id=? AND state='ready'`,
			stamp, scope.InstallationID, scope.MachineID, scope.EngineID, target.ID)
		if err != nil {
			return dbError(err)
		}
		// A deterministic image ID may belong to several historical load
		// generations. Exact physical deletion settles every matching bound.
		_, err = tx.ExecContext(ctx, `UPDATE host_storage_reservations SET state='released',released_at=?
		 WHERE installation_id=? AND machine_id=? AND engine_id=? AND class='image' AND state='committed'
		 AND mutation_operation_id IN (SELECT operation_id FROM host_application_images
		 WHERE installation_id=? AND machine_id=? AND engine_id=? AND expected_image_id=?)`,
			stamp, scope.InstallationID, scope.MachineID, scope.EngineID,
			scope.InstallationID, scope.MachineID, scope.EngineID, target.ID)
		return dbError(err)
	default:
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE host_storage_reservations SET state='released',released_at=?
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND class=? AND resource_key=?
	 AND mutation_operation_id=? AND state='committed'`, stamp, scope.InstallationID, scope.MachineID,
		scope.EngineID, "cache", contentReservationKey(domain.SHA256Digest(sha), bytes), operation)
	return dbError(err)
}
