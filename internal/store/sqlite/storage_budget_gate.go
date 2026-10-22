package sqlite

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type storageReservation struct {
	class, key string
	bytes      int64
}

func frozenStorageBudgetInTx(ctx context.Context, tx *sql.Tx, profile admission.Profile) (hoststorage.Budget, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT config FROM managed_profile_runtime WHERE profile=? AND revision=?`,
		profile.Binding.Profile, profile.Binding.ConfigurationRevision).Scan(&raw); err != nil {
		return hoststorage.Budget{}, dbError(err)
	}
	budget, err := hoststorage.BudgetFromRuntime(raw)
	if err != nil {
		return budget, ErrCorrupt
	}
	return budget, nil
}

func freshStorageChargeInTx(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, now time.Time) (storageCharge, error) {
	observation, err := latestStorageObservation(ctx, tx, scope)
	if err != nil {
		return storageCharge{}, err
	}
	if observation.state != "complete" || observation.at.After(now) || now.Sub(observation.at) > hoststorage.ScanFreshness {
		return storageCharge{}, provider.WithReason(hoststorage.ErrUnknown, storageObservationReason(observation, now))
	}
	charge, err := storageChargeInTx(ctx, tx, scope, observation, "")
	if err != nil {
		return charge, err
	}
	if charge.unknown {
		return charge, provider.WithReason(hoststorage.ErrUnknown, "storage_charge_unknown")
	}
	return charge, nil
}

func storageAdmissionInTx(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID,
	profile admission.Profile, refs []admission.FrozenObject, now time.Time) error {
	target, err := cacheTargetInTx(ctx, tx, w, profile)
	if err != nil || target == nil {
		return err
	}
	scope := storageScope(target)
	charge, err := freshStorageChargeInTx(ctx, tx, scope, now)
	if err != nil {
		return err
	}
	configured, err := frozenStorageBudgetInTx(ctx, tx, profile)
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
	proposed := []storageReservation{{class: "attempt", key: string(attempt), bytes: hoststorage.AttemptAllowanceBytes}}
	seen := map[string]bool{}
	for _, ref := range refs {
		class := "cache"
		if ref.Role == "application_image" {
			class = "image"
		} else if ref.Role != "bundle" && !strings.HasPrefix(ref.Role, "input:") {
			continue
		}
		key := string(ref.Object.SHA256) + ":" + strconv.FormatInt(ref.Object.Bytes, 10)
		if seen[class+":"+key] {
			continue
		}
		seen[class+":"+key] = true
		var exists bool
		if class == "cache" {
			err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_generations WHERE installation_id=? AND machine_id=? AND engine_id=?
			 AND sha256=? AND bytes=? AND state IN ('claimed','verified'))`, scope.InstallationID, scope.MachineID, scope.EngineID, ref.Object.SHA256, ref.Object.Bytes).Scan(&exists)
		} else {
			err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_application_images WHERE installation_id=? AND machine_id=? AND engine_id=?
			 AND archive_sha256=? AND archive_bytes=? AND state IN ('preparing','loading','ready','unknown'))`, scope.InstallationID, scope.MachineID, scope.EngineID, ref.Object.SHA256, ref.Object.Bytes).Scan(&exists)
		}
		if err != nil {
			return dbError(err)
		}
		if exists {
			continue
		}
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_storage_reservations WHERE installation_id=? AND machine_id=? AND engine_id=?
		 AND class=? AND resource_key=? AND state='provisional')`, scope.InstallationID, scope.MachineID, scope.EngineID, class, key).Scan(&exists); err != nil {
			return dbError(err)
		}
		if exists {
			continue
		}
		bytes := ref.Object.Bytes
		if class == "image" {
			bytes = 4 << 30
		} // bounded normalized tar + expansion
		proposed = append(proposed, storageReservation{class, key, bytes})
	}
	total := charge.view.Machine.ChargedBytes
	for _, reservation := range proposed {
		total, err = hoststorage.AddChecked(total, reservation.bytes)
		if err != nil {
			return hoststorage.ErrUnknown
		}
	}
	if total >= effective.HardBytes {
		return hoststorage.ErrHardLimit
	}
	for _, reservation := range proposed {
		id, err := randomID("stres_", 16)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO host_storage_reservations
		 (reservation_id,installation_id,machine_id,engine_id,class,resource_key,bytes,owner_workspace_id,owner_job_id,owner_attempt_id,state,created_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?,'provisional',?)`, id, scope.InstallationID, scope.MachineID, scope.EngineID,
			reservation.class, reservation.key, reservation.bytes, w, job, attempt, now.Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
	}
	return nil
}

func checkStorageNewWork(ctx context.Context, tx *sql.Tx, record admission.Record, now time.Time) error {
	target, err := cacheTargetInTx(ctx, tx, record.Job.WorkspaceID, record.Profile)
	if err != nil || target == nil {
		return err
	}
	scope := storageScope(target)
	charge, err := freshStorageChargeInTx(ctx, tx, scope, now)
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

func (s *Store) StorageScopeForProfile(ctx context.Context, w domain.WorkspaceID, profile admission.Profile) (hoststorage.Scope, error) {
	var scope hoststorage.Scope
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		target, err := cacheTargetInTx(ctx, tx, w, profile)
		if err != nil {
			return err
		}
		if target != nil {
			scope = storageScope(target)
		}
		return nil
	})
	return scope, err
}

func storageObservationReason(observation storageObservation, now time.Time) string {
	if domain.ValidReason(observation.reason) {
		return observation.reason
	}
	if observation.at.IsZero() {
		return "storage_not_observed"
	}
	if observation.at.After(now) {
		return "storage_clock_changed"
	}
	if now.Sub(observation.at) > hoststorage.ScanFreshness {
		return "storage_scan_stale"
	}
	return "storage_scan_incomplete"
}
