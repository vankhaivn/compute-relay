package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/hostidentity"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

var _ hostcache.Repository = (*Store)(nil)

type frozenCacheTarget struct {
	installation domain.RuntimeInstallationID
	machine      domain.SHA256Digest
	engine       domain.SHA256Digest
}

// cacheTargetInTx reads the same immutable profile revision frozen on the job.
// Old SSH revisions without a verified machine and engine retain legacy
// staging. A non-SSH provider never acquires SSH cache pins.
func cacheTargetInTx(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, profile admission.Profile) (*frozenCacheTarget, error) {
	id, ok := credentials.VaultReference(ports.CredentialRef(profile.CredentialRef))
	if !ok {
		return nil, ErrCorrupt
	}
	var providerType string
	err := tx.QueryRowContext(ctx, `SELECT provider_type FROM managed_connections WHERE workspace_id=? AND connection_id=?`, string(w), id).Scan(&providerType)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCorrupt
	}
	if err != nil {
		return nil, dbError(err)
	}
	if providerType != "ssh" {
		return nil, nil
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT config FROM managed_profile_runtime WHERE profile=? AND revision=?`, profile.Binding.Profile, profile.Binding.ConfigurationRevision).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCorrupt
	}
	if err != nil {
		return nil, dbError(err)
	}
	var identity struct {
		Version   int    `json:"schema_version"`
		MachineID string `json:"machine_id"`
		EngineID  string `json:"engine_id"`
	}
	if len(raw) > 8192 || json.Unmarshal(raw, &identity) != nil {
		return nil, ErrCorrupt
	}
	if identity.Version == 1 || identity.Version == 2 {
		return nil, nil
	}
	if identity.Version != 3 || !domain.SHA256Digest(identity.MachineID).Valid() || !domain.SHA256Digest(identity.EngineID).Valid() ||
		profile.ResourceScope != "resource_"+identity.MachineID {
		return nil, ErrCorrupt
	}
	var installation domain.RuntimeInstallationID
	if err = tx.QueryRowContext(ctx, `SELECT installation_id FROM runtime_installation WHERE singleton=1`).Scan(&installation); err != nil {
		return nil, dbError(err)
	}
	if !installation.Valid() {
		return nil, ErrCorrupt
	}
	return &frozenCacheTarget{installation, domain.SHA256Digest(identity.MachineID), domain.SHA256Digest(identity.EngineID)}, nil
}

func pinHostCacheAdmission(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, profile admission.Profile, refs []admission.FrozenObject, now time.Time) error {
	target, err := cacheTargetInTx(ctx, tx, w, profile)
	if err != nil || target == nil {
		return err
	}
	seen := map[struct {
		sha   domain.SHA256Digest
		bytes int64
	}]bool{}
	for _, ref := range refs {
		if ref.Role != "bundle" && !strings.HasPrefix(ref.Role, "input:") {
			continue
		}
		key := struct {
			sha   domain.SHA256Digest
			bytes int64
		}{ref.Object.SHA256, ref.Object.Bytes}
		if seen[key] {
			continue
		}
		seen[key] = true
		fenced, err := storageContentFencedInTx(ctx, tx, storageScope(target), "cache", string(key.sha), key.bytes)
		if err != nil {
			return err
		}
		if fenced {
			return hoststorage.ErrIneligible
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO host_cache_pins(workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,sha256,bytes,created_at)
		 VALUES(?,?,?,?,?,?,?,?,?)`, w, job, attempt, target.installation, target.machine, target.engine, key.sha, key.bytes, now.Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
	}
	return nil
}

// Retry inherits the original attempt's exact key set in the same transaction
// as the new attempt. New profile aliases and current host state are irrelevant.
func pinHostCacheRetry(ctx context.Context, tx *sql.Tx, oldAttempt, newAttempt hostcache.Attempt, now time.Time) error {
	if !oldAttempt.Valid() || !newAttempt.Valid() || oldAttempt.WorkspaceID != newAttempt.WorkspaceID || oldAttempt.JobID != newAttempt.JobID {
		return hostcache.ErrInvalid
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO host_cache_pins(workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,sha256,bytes,created_at)
	 SELECT workspace_id,job_id,?,installation_id,machine_id,engine_id,sha256,bytes,?
	 FROM host_cache_pins WHERE workspace_id=? AND job_id=? AND attempt_id=?`, newAttempt.AttemptID, now.Format(time.RFC3339Nano), oldAttempt.WorkspaceID, oldAttempt.JobID, oldAttempt.AttemptID)
	return dbError(err)
}

// A queue that never crossed its preparation barrier has no possible remote
// cache mount. This is the only no-container proof that releases pins without
// a confirmed exact staging-resource cleanup receipt.
func releaseHostCachePinsWithoutPreparation(ctx context.Context, tx *sql.Tx, workspace domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, now time.Time) error {
	var barrier, hasResource bool
	if err := tx.QueryRowContext(ctx, `SELECT dispatch_barrier FROM scheduler_queue WHERE workspace_id=? AND job_id=? AND attempt_id=?`, workspace, job, attempt).Scan(&barrier); err != nil {
		return dbError(err)
	}
	if barrier {
		return hostcache.ErrUnknown
	}
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_resources WHERE workspace_id=? AND job_id=? AND attempt_id=?)`, workspace, job, attempt).Scan(&hasResource); err != nil {
		return dbError(err)
	}
	if hasResource {
		return hostcache.ErrUnknown
	}
	_, err := tx.ExecContext(ctx, `UPDATE host_cache_pins SET released_at=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND released_at=''`, now.Format(time.RFC3339Nano), workspace, job, attempt)
	return dbError(err)
}

// A successful cleanup has already collected exact per-resource deleted/absent
// receipts under its fenced worker lease. Release only when its staging target
// is confirmed and every observed cache mount names that same exact container.
// The receipts remain immutable history; released pins cease blocking eviction.
func releaseHostCachePinsOnCleanup(ctx context.Context, tx *sql.Tx, op domain.Operation, resources []cleanup.ResourceWork, now time.Time) error {
	var hasPins bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_pins WHERE workspace_id=? AND job_id=? AND attempt_id=? AND released_at='')`,
		op.WorkspaceID, op.JobID, op.AttemptID).Scan(&hasPins); err != nil {
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
		!domain.SHA256Digest(remote.ID).Valid() || !storageNativeEngine.MatchString(remote.EngineID) {
		return ErrCorrupt
	}
	rows, err := tx.QueryContext(ctx, `SELECT installation_id,engine_id FROM host_cache_pins
	 WHERE workspace_id=? AND job_id=? AND attempt_id=? AND released_at=''`, op.WorkspaceID, op.JobID, op.AttemptID)
	if err != nil {
		return dbError(err)
	}
	for rows.Next() {
		var installation, engine string
		if err := rows.Scan(&installation, &engine); err != nil {
			_ = rows.Close()
			return dbError(err)
		}
		if installation != string(staging.Resource.Request.Remote.Identity.InstallationID) || engine != hostidentity.Digest("docker-engine-id", remote.EngineID) {
			_ = rows.Close()
			return ErrCorrupt
		}
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return dbError(err)
	}
	var mismatchedMount bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_mount_receipts
	 WHERE workspace_id=? AND job_id=? AND attempt_id=? AND container_id!=?)`, op.WorkspaceID, op.JobID, op.AttemptID, remote.ID).Scan(&mismatchedMount); err != nil {
		return dbError(err)
	}
	if mismatchedMount {
		return ErrCorrupt
	}
	_, err = tx.ExecContext(ctx, `UPDATE host_cache_pins SET released_at=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND released_at=''`,
		now.Format(time.RFC3339Nano), op.WorkspaceID, op.JobID, op.AttemptID)
	return dbError(err)
}

const cacheGenerationColumns = `generation_id,installation_id,machine_id,engine_id,sha256,bytes,volume_name,operation_id,owner_workspace_id,owner_job_id,owner_attempt_id,state,writer_phase,created_at,verified_at,last_used_at`

type cacheScanner interface{ Scan(...any) error }

func scanCacheGeneration(row cacheScanner) (hostcache.Generation, error) {
	var g hostcache.Generation
	var created, verified, used string
	err := row.Scan(&g.ID, &g.Key.InstallationID, &g.Key.MachineID, &g.Key.EngineID, &g.Key.SHA256, &g.Key.Bytes,
		&g.VolumeName, &g.OperationID, &g.Owner.WorkspaceID, &g.Owner.JobID, &g.Owner.AttemptID, &g.State, &g.WriterPhase, &created, &verified, &used)
	if err != nil {
		return g, err
	}
	g.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return g, ErrCorrupt
	}
	if verified != "" {
		g.VerifiedAt, err = time.Parse(time.RFC3339Nano, verified)
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
	if !g.Valid() {
		return g, ErrCorrupt
	}
	return g, nil
}

func activeCacheInTx(ctx context.Context, tx *sql.Tx, key hostcache.Key) (*hostcache.Generation, error) {
	g, err := scanCacheGeneration(tx.QueryRowContext(ctx, `SELECT `+cacheGenerationColumns+` FROM host_cache_generations
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND sha256=? AND bytes=? AND state IN ('claimed','verified')`,
		key.InstallationID, key.MachineID, key.EngineID, key.SHA256, key.Bytes))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	return &g, nil
}

func (s *Store) Pins(ctx context.Context, owner hostcache.Attempt) ([]hostcache.Key, error) {
	if !owner.Valid() {
		return nil, hostcache.ErrInvalid
	}
	return read(ctx, s, func(ctx context.Context) ([]hostcache.Key, error) {
		rows, err := s.db.QueryContext(ctx, `SELECT installation_id,machine_id,engine_id,sha256,bytes FROM host_cache_pins
		 WHERE workspace_id=? AND job_id=? AND attempt_id=? AND released_at=''
		 ORDER BY installation_id,machine_id,engine_id,sha256,bytes`, owner.WorkspaceID, owner.JobID, owner.AttemptID)
		if err != nil {
			return nil, dbError(err)
		}
		defer rows.Close()
		keys := []hostcache.Key{}
		for rows.Next() {
			var key hostcache.Key
			if err := rows.Scan(&key.InstallationID, &key.MachineID, &key.EngineID, &key.SHA256, &key.Bytes); err != nil {
				return nil, dbError(err)
			}
			if !key.Valid() {
				return nil, ErrCorrupt
			}
			keys = append(keys, key)
		}
		return keys, dbError(rows.Err())
	})
}

func (s *Store) Active(ctx context.Context, key hostcache.Key) (*hostcache.Generation, error) {
	if !key.Valid() {
		return nil, hostcache.ErrInvalid
	}
	return read(ctx, s, func(ctx context.Context) (*hostcache.Generation, error) {
		g, err := scanCacheGeneration(s.db.QueryRowContext(ctx, `SELECT `+cacheGenerationColumns+` FROM host_cache_generations
		 WHERE installation_id=? AND machine_id=? AND engine_id=? AND sha256=? AND bytes=? AND state IN ('claimed','verified')`,
			key.InstallationID, key.MachineID, key.EngineID, key.SHA256, key.Bytes))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, dbError(err)
		}
		return &g, nil
	})
}

func (s *Store) Claim(ctx context.Context, owner hostcache.Attempt, key hostcache.Key) (hostcache.Generation, bool, error) {
	if !owner.Valid() || !key.Valid() {
		return hostcache.Generation{}, false, hostcache.ErrInvalid
	}
	var result hostcache.Generation
	var created bool
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var pinned bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_pins WHERE workspace_id=? AND job_id=? AND attempt_id=? AND installation_id=? AND machine_id=? AND engine_id=? AND sha256=? AND bytes=? AND released_at='')`,
			owner.WorkspaceID, owner.JobID, owner.AttemptID, key.InstallationID, key.MachineID, key.EngineID, key.SHA256, key.Bytes).Scan(&pinned); err != nil {
			return dbError(err)
		}
		if !pinned {
			return hostcache.ErrInvalid
		}
		active, err := activeCacheInTx(ctx, tx, key)
		if err != nil {
			return err
		}
		if active != nil {
			result = *active
			return nil
		}
		id, err := randomID("hcgen_", 16)
		if err != nil {
			return err
		}
		op, err := randomID("hcop_", 16)
		if err != nil {
			return err
		}
		result = hostcache.Generation{Key: key, ID: id, VolumeName: "relay-cache-" + strings.TrimPrefix(id, "hcgen_"),
			OperationID: domain.OperationID(op), Owner: owner, State: hostcache.Claimed, WriterPhase: hostcache.WriterIntent, CreatedAt: now}
		_, err = tx.ExecContext(ctx, `INSERT INTO host_cache_generations(generation_id,installation_id,machine_id,engine_id,sha256,bytes,volume_name,operation_id,owner_workspace_id,owner_job_id,owner_attempt_id,state,writer_phase,created_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, result.ID, key.InstallationID, key.MachineID, key.EngineID, key.SHA256, key.Bytes, result.VolumeName,
			result.OperationID, owner.WorkspaceID, owner.JobID, owner.AttemptID, result.State, result.WriterPhase, now.Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
		created = true
		return nil
	})
	if err != nil {
		return hostcache.Generation{}, false, err
	}
	return result, created, nil
}

func cacheGenerationInTx(ctx context.Context, tx *sql.Tx, id string) (hostcache.Generation, error) {
	g, err := scanCacheGeneration(tx.QueryRowContext(ctx, `SELECT `+cacheGenerationColumns+` FROM host_cache_generations WHERE generation_id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return g, hostcache.ErrConflict
	}
	if err != nil {
		return g, dbError(err)
	}
	return g, nil
}

// BeginWrite is the only one-shot authorization for a remote cache mutation.
// Claim by itself is not permission to create the volume or run its writer.
func (s *Store) BeginWrite(ctx context.Context, owner hostcache.Attempt, generationID string) error {
	if !owner.Valid() {
		return hostcache.ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		g, err := cacheGenerationInTx(ctx, tx, generationID)
		if err != nil {
			return err
		}
		if g.Owner != owner || g.State != hostcache.Claimed {
			return hostcache.ErrConflict
		}
		if g.WriterPhase != hostcache.WriterIntent {
			return hostcache.ErrUnknown
		}
		fenced, err := storageContentFencedInTx(ctx, tx, hoststorage.Scope{InstallationID: g.Key.InstallationID,
			MachineID: g.Key.MachineID, EngineID: g.Key.EngineID}, "cache", string(g.Key.SHA256), g.Key.Bytes)
		if err != nil {
			return err
		}
		if fenced {
			return hoststorage.ErrIneligible
		}
		if err := commitStorageReservationInTx(ctx, tx, hoststorage.Scope{InstallationID: g.Key.InstallationID,
			MachineID: g.Key.MachineID, EngineID: g.Key.EngineID}, "cache", g.Key.SHA256, g.Key.Bytes, g.OperationID, now); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE host_cache_generations SET writer_phase='begun' WHERE generation_id=? AND state='claimed' AND writer_phase='intent'`, g.ID)
		return dbError(err)
	})
}

// FinishWrite is reserved for a known-terminated one-shot host call. A timeout,
// dropped connection or lease expiry is not proof that the writer cannot later
// send or finish, so callers must leave such generations begun and unknown.
func (s *Store) FinishWrite(ctx context.Context, owner hostcache.Attempt, generationID string) error {
	if !owner.Valid() {
		return hostcache.ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		g, err := cacheGenerationInTx(ctx, tx, generationID)
		if err != nil {
			return err
		}
		if g.Owner != owner {
			return hostcache.ErrConflict
		}
		if g.WriterPhase == hostcache.WriterFinished {
			return nil
		}
		if g.State != hostcache.Claimed || g.WriterPhase != hostcache.WriterBegun {
			return hostcache.ErrConflict
		}
		_, err = tx.ExecContext(ctx, `UPDATE host_cache_generations SET writer_phase='finished' WHERE generation_id=? AND state='claimed' AND writer_phase='begun'`, g.ID)
		return dbError(err)
	})
}

func (s *Store) Verify(ctx context.Context, observation hostcache.Observation) error {
	if observation.Kind != hostcache.ObservedComplete {
		return hostcache.ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		g, err := cacheGenerationInTx(ctx, tx, observation.GenerationID)
		if err != nil {
			return err
		}
		if !observation.Matches(g) {
			return hostcache.ErrConflict
		}
		if g.State == hostcache.Verified {
			return nil
		}
		if g.State != hostcache.Claimed {
			return hostcache.ErrConflict
		}
		if g.WriterPhase == hostcache.WriterIntent {
			return hostcache.ErrUnknown
		}
		// Exact, sealed content plus a provably inactive original writer is
		// sufficient even if the local FinishWrite acknowledgement was lost.
		// The fixed writer must never replace an already published blob.
		query := `UPDATE host_cache_generations SET state='verified',verified_at=? WHERE generation_id=? AND state='claimed' AND writer_phase='finished'`
		if g.WriterPhase == hostcache.WriterBegun {
			query = `UPDATE host_cache_generations SET state='verified',writer_phase='finished',verified_at=? WHERE generation_id=? AND state='claimed' AND writer_phase='begun'`
		}
		_, err = tx.ExecContext(ctx, query, now.Format(time.RFC3339Nano), g.ID)
		return dbError(err)
	})
}

func (s *Store) Retire(ctx context.Context, observation hostcache.Observation) error {
	if observation.Kind != hostcache.ObservedMissing && observation.Kind != hostcache.ObservedCorrupt {
		return hostcache.ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		g, err := cacheGenerationInTx(ctx, tx, observation.GenerationID)
		if err != nil {
			return err
		}
		if !observation.Matches(g) {
			return hostcache.ErrConflict
		}
		if g.State == hostcache.Retired {
			return nil
		}
		if g.State == hostcache.Claimed && g.WriterPhase != hostcache.WriterFinished {
			return hostcache.ErrUnknown
		}
		_, err = tx.ExecContext(ctx, `UPDATE host_cache_generations SET state='retired',retired_at=?,retired_reason=? WHERE generation_id=? AND state IN ('claimed','verified')`,
			now.Format(time.RFC3339Nano), observation.Kind, g.ID)
		return dbError(err)
	})
}

func (s *Store) RecordMount(ctx context.Context, mount hostcache.MountReceipt) error {
	if !mount.Valid() {
		return hostcache.ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		g, err := cacheGenerationInTx(ctx, tx, mount.Generation.ID)
		if err != nil {
			return err
		}
		if g.State != hostcache.Verified || g.Key != mount.Generation.Key || g.VolumeName != mount.Generation.VolumeName ||
			g.OperationID != mount.Generation.OperationID || g.Owner != mount.Generation.Owner {
			return hostcache.ErrConflict
		}
		var pinned bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_pins WHERE workspace_id=? AND job_id=? AND attempt_id=? AND installation_id=? AND machine_id=? AND engine_id=? AND sha256=? AND bytes=? AND released_at='')`,
			mount.Attempt.WorkspaceID, mount.Attempt.JobID, mount.Attempt.AttemptID, g.Key.InstallationID, g.Key.MachineID, g.Key.EngineID, g.Key.SHA256, g.Key.Bytes).Scan(&pinned); err != nil {
			return dbError(err)
		}
		if !pinned {
			return hostcache.ErrConflict
		}
		var oldID, oldContainer, oldVolume string
		err = tx.QueryRowContext(ctx, `SELECT generation_id,container_id,volume_name FROM host_cache_mount_receipts
		 WHERE workspace_id=? AND job_id=? AND attempt_id=? AND installation_id=? AND machine_id=? AND engine_id=? AND sha256=? AND bytes=?`,
			mount.Attempt.WorkspaceID, mount.Attempt.JobID, mount.Attempt.AttemptID, g.Key.InstallationID, g.Key.MachineID, g.Key.EngineID, g.Key.SHA256, g.Key.Bytes).Scan(&oldID, &oldContainer, &oldVolume)
		if err == nil {
			if oldID != g.ID || oldContainer != mount.ContainerID || oldVolume != g.VolumeName {
				return hostcache.ErrConflict
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO host_cache_mount_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			mount.Attempt.WorkspaceID, mount.Attempt.JobID, mount.Attempt.AttemptID, g.Key.InstallationID, g.Key.MachineID, g.Key.EngineID, g.Key.SHA256, g.Key.Bytes,
			g.ID, mount.ContainerID, g.VolumeName, now.Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
		_, err = tx.ExecContext(ctx, `UPDATE host_cache_generations SET last_used_at=? WHERE generation_id=?`, now.Format(time.RFC3339Nano), g.ID)
		return dbError(err)
	})
}

// Inventory counts every claimed or unremoved generation's bytes once. Pins
// are key-wide, so a retired generation still cannot be evicted while an old
// attempt may hold its exact volume. Claimed rows include unknown outcomes.
func (s *Store) Inventory(ctx context.Context, installation domain.RuntimeInstallationID, machine, engine domain.SHA256Digest) ([]hostcache.Generation, error) {
	if !installation.Valid() || !machine.Valid() || !engine.Valid() {
		return nil, hostcache.ErrInvalid
	}
	return read(ctx, s, func(ctx context.Context) ([]hostcache.Generation, error) {
		rows, err := s.db.QueryContext(ctx, `SELECT `+cacheGenerationColumns+`,
		 (SELECT count(*) FROM host_cache_pins p WHERE p.installation_id=g.installation_id AND p.machine_id=g.machine_id AND p.engine_id=g.engine_id AND p.sha256=g.sha256 AND p.bytes=g.bytes AND p.released_at=''),
		 (SELECT count(*) FROM host_cache_mount_receipts m JOIN host_cache_pins p
		  ON p.workspace_id=m.workspace_id AND p.job_id=m.job_id AND p.attempt_id=m.attempt_id
		  AND p.installation_id=m.installation_id AND p.machine_id=m.machine_id AND p.engine_id=m.engine_id AND p.sha256=m.sha256 AND p.bytes=m.bytes
		  WHERE m.generation_id=g.generation_id AND p.released_at='')
		 FROM host_cache_generations g WHERE installation_id=? AND machine_id=? AND engine_id=?`, installation, machine, engine)
		if err != nil {
			return nil, dbError(err)
		}
		defer rows.Close()
		result := []hostcache.Generation{}
		for rows.Next() {
			var g hostcache.Generation
			var created, verified, used string
			if err := rows.Scan(&g.ID, &g.Key.InstallationID, &g.Key.MachineID, &g.Key.EngineID, &g.Key.SHA256, &g.Key.Bytes,
				&g.VolumeName, &g.OperationID, &g.Owner.WorkspaceID, &g.Owner.JobID, &g.Owner.AttemptID, &g.State, &g.WriterPhase, &created, &verified, &used, &g.Pins, &g.Mounts); err != nil {
				return nil, dbError(err)
			}
			g.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
			if err != nil {
				return nil, ErrCorrupt
			}
			if verified != "" {
				g.VerifiedAt, err = time.Parse(time.RFC3339Nano, verified)
				if err != nil {
					return nil, ErrCorrupt
				}
			}
			if used != "" {
				g.LastUsedAt, err = time.Parse(time.RFC3339Nano, used)
				if err != nil {
					return nil, ErrCorrupt
				}
			}
			if !g.Valid() || g.Pins < 0 || g.Mounts < 0 {
				return nil, ErrCorrupt
			}
			result = append(result, g)
		}
		if err := rows.Err(); err != nil {
			return nil, dbError(err)
		}
		sort.Slice(result, func(i, j int) bool {
			if result[i].CreatedAt.Equal(result[j].CreatedAt) {
				return result[i].ID < result[j].ID
			}
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		})
		return result, nil
	})
}
