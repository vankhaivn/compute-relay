package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type evictionCandidate struct {
	class      hoststorage.Kind
	id, used   string
	bytes      int64
	generation string
	operation  string
	sha        string
}

// QueueStorageEvictions creates durable exact-target work only after a fresh,
// complete scan observes a machine above its shared soft threshold. Candidates
// are chosen by oldest use across cache volumes and application images.
// cacheVolumeLimit is the number of cache volumes a machine keeps before the oldest
// unpinned ones are evicted, independent of the byte budget.
var cacheVolumeLimit = 64

func (s *Store) QueueStorageEvictions(ctx context.Context, scope hoststorage.Scope, w domain.WorkspaceID, profile admission.Profile) error {
	if !scope.Valid() || !w.Valid() || profile.Validate() != nil {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		target, err := cacheTargetInTx(ctx, tx, w, profile)
		if err != nil || target == nil || storageScope(target) != scope {
			return hoststorage.ErrUnverified
		}
		connection, ok := credentials.VaultReference(ports.CredentialRef(profile.CredentialRef))
		if !ok {
			return ErrCorrupt
		}
		var enabled bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM managed_connections c JOIN profiles p ON p.profile=c.current_profile
		 WHERE c.workspace_id=? AND c.connection_id=? AND c.new_work='enabled' AND c.authentication='verified'
		 AND c.pending_operation='' AND p.enabled=1 AND p.profile=? AND p.revision=?)`,
			w, connection, profile.Binding.Profile, profile.Binding.ConfigurationRevision).Scan(&enabled); err != nil {
			return dbError(err)
		}
		if !enabled {
			return nil
		}
		observation, err := latestStorageObservation(ctx, tx, scope)
		if err != nil {
			return err
		}
		if observation.state != "complete" || observation.at.After(now) || now.Sub(observation.at) > hoststorage.ScanFreshness {
			return hoststorage.ErrUnknown
		}
		charge, err := storageChargeInTx(ctx, tx, scope, observation, w)
		if err != nil {
			return err
		}
		if charge.unknown {
			return hoststorage.ErrUnknown
		}
		configured, err := frozenStorageBudgetInTx(ctx, tx, profile)
		if err != nil {
			return err
		}
		effective, err := effectiveStorageBudget(ctx, tx, scope, configured)
		if err != nil {
			return err
		}
		// Idle cache volumes are also bounded by count: small volumes never reach the
		// byte limit, yet each one adds work to every storage scan.
		volumes, images := 0, 0
		for _, object := range observation.objects {
			if object.Kind == hoststorage.Volume && object.Labels[storageLabelPrefix+"kind"] == "cache" {
				volumes++
			}
			if object.Kind == hoststorage.Image && object.Labels[hostimages.LabelKind] == "application" && object.Labels[hostimages.LabelMachine] == string(scope.MachineID) && object.Labels[hostimages.LabelEngine] == string(scope.EngineID) {
				images++
			}
		}
		excess := volumes - cacheVolumeLimit
		imageExcess := images - hostimages.ImageRetentionTarget
		if imageExcess > 0 {
			var scheduled int
			if err := tx.QueryRowContext(ctx, `SELECT count(DISTINCT object_id) FROM host_storage_cleanup_operations
			 WHERE installation_id=? AND machine_id=? AND engine_id=? AND class='image'
			 AND object_id IN (SELECT object_id FROM host_storage_objects WHERE scan_seq=? AND class='image')
			 AND (status IN ('accepted','running') OR result='unknown'
			 OR (result IN ('deleted','absent') AND confirmed_scan_seq>=?))`,
				scope.InstallationID, scope.MachineID, scope.EngineID, observation.seq, observation.seq).Scan(&scheduled); err != nil {
				return dbError(err)
			}
			imageExcess -= scheduled
		}
		if charge.view.Machine.ChargedBytes <= effective.SoftBytes && excess <= 0 && imageExcess <= 0 {
			return nil
		}
		const eligible = `SELECT 'volume',g.volume_name,g.bytes,g.last_used_at,g.generation_id,g.operation_id,g.sha256
		 FROM host_cache_generations g WHERE g.installation_id=? AND g.machine_id=? AND g.engine_id=?
		 AND g.state IN ('verified','retired') AND g.writer_phase='finished'
		 AND NOT EXISTS(SELECT 1 FROM host_cache_pins p WHERE p.installation_id=g.installation_id AND p.machine_id=g.machine_id
		 AND p.engine_id=g.engine_id AND p.sha256=g.sha256 AND p.bytes=g.bytes AND p.released_at='')
		 UNION ALL
		 SELECT 'image',g.expected_image_id,g.archive_bytes,g.last_used_at,g.generation_id,g.operation_id,g.archive_sha256
		 FROM host_application_images g WHERE g.installation_id=? AND g.machine_id=? AND g.engine_id=?
		 AND g.state IN ('ready','retired') AND g.expected_image_id!=''
		 AND NOT EXISTS(SELECT 1 FROM host_application_image_pins p WHERE p.installation_id=g.installation_id AND p.machine_id=g.machine_id
		 AND p.engine_id=g.engine_id AND p.archive_sha256=g.archive_sha256 AND p.archive_bytes=g.archive_bytes AND p.released_at='')
		 ORDER BY 4,2 LIMIT 2049`
		rows, err := tx.QueryContext(ctx, eligible, scope.InstallationID, scope.MachineID, scope.EngineID,
			scope.InstallationID, scope.MachineID, scope.EngineID)
		if err != nil {
			return dbError(err)
		}
		candidates := []evictionCandidate{}
		for rows.Next() {
			var item evictionCandidate
			if err := rows.Scan(&item.class, &item.id, &item.bytes, &item.used, &item.generation, &item.operation, &item.sha); err != nil {
				rows.Close()
				return dbError(err)
			}
			candidates = append(candidates, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return dbError(err)
		}
		if len(candidates) > 2048 {
			return hoststorage.ErrUnknown
		}
		objects := map[string]hoststorage.Object{}
		for _, object := range observation.objects {
			objects[storageObjectKey(object.Kind, object.ID)] = object
		}
		binding := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
		bindingJSON, err := json.Marshal(binding)
		if err != nil || len(bindingJSON) > 8192 {
			return ErrCorrupt
		}
		remaining := charge.view.Machine.ChargedBytes - effective.SoftBytes
		for _, item := range candidates {
			if remaining <= 0 && excess <= 0 && imageExcess <= 0 {
				break
			}
			reason := "soft_limit_lru"
			if remaining <= 0 {
				switch {
				case item.class == hoststorage.Volume && excess > 0:
					reason = "count_limit_lru"
				case item.class == hoststorage.Image && imageExcess > 0:
					reason = "image_count_limit_lru"
				default:
					continue
				}
			}
			object, present := objects[storageObjectKey(item.class, item.id)]
			if !present || !object.SizeKnown || object.References != 0 || item.bytes < 0 {
				continue
			}
			labels := object.Labels
			if item.class == hoststorage.Volume {
				if labels[storageLabelPrefix+"generation"] != item.generation || labels[storageLabelPrefix+"operation"] != item.operation ||
					labels[storageLabelPrefix+"sha256"] != item.sha || labels[storageLabelPrefix+"bytes"] != strconv.FormatInt(item.bytes, 10) ||
					!credibleStorageOrphan(scope, object, w) {
					continue
				}
			} else if labels[storageLabelPrefix+"archive-sha256"] != item.sha ||
				labels[storageLabelPrefix+"image-kind"] != "application" || labels[storageLabelPrefix+"image-schema"] != "application/1" ||
				labels[storageLabelPrefix+"image-machine"] != string(scope.MachineID) || labels[storageLabelPrefix+"image-engine"] != string(scope.EngineID) {
				continue
			}
			var blocked bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_storage_cleanup_operations
			 WHERE installation_id=? AND machine_id=? AND engine_id=? AND class=? AND object_id=?
			 AND (status IN ('accepted','running') OR result='unknown'
			 OR (result IN ('deleted','absent') AND confirmed_scan_seq>=?)))`,
				scope.InstallationID, scope.MachineID, scope.EngineID, item.class, item.id,
				observation.seq).Scan(&blocked); err != nil {
				return dbError(err)
			}
			if blocked {
				continue
			}
			action := hoststorage.EvictCache
			if item.class == hoststorage.Image {
				action = hoststorage.EvictImage
			}
			target := hoststorage.Target{Scope: scope, Class: item.class, ID: item.id, Labels: labels, Action: action}
			if !target.Valid() {
				continue
			}
			labelsJSON, err := json.Marshal(labels)
			if err != nil || len(labelsJSON) > 4096 {
				return ErrCorrupt
			}
			id, err := randomID("stev_", 16)
			if err != nil {
				return err
			}
			fingerprint := sha256.Sum256([]byte(string(scope.InstallationID) + "\x00" + string(scope.MachineID) + "\x00" +
				string(scope.EngineID) + "\x00" + string(item.class) + "\x00" + item.id + "\x00" + strconv.FormatInt(observation.seq, 10)))
			hash := hex.EncodeToString(fingerprint[:])
			_, err = tx.ExecContext(ctx, `INSERT INTO host_storage_cleanup_operations
			 (operation_id,workspace_id,connection_id,preview_id,installation_id,machine_id,engine_id,action,class,object_id,
			 labels_json,binding_json,fingerprint,key_sha256,actor_token_id,status,result,reason,created_at,updated_at)
			 VALUES(?,?,?,NULL,?,?,?,?,?,?,?,?,?,?,'system','accepted','pending',?,?,?)`,
				id, w, connection, scope.InstallationID, scope.MachineID, scope.EngineID, action, item.class, item.id,
				string(labelsJSON), string(bindingJSON), hash, hash, reason, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
			if err != nil {
				return dbError(err)
			}
			freed := charge.objects[storageObjectKey(item.class, item.id)]
			remaining -= freed
			if item.class == hoststorage.Volume {
				excess--
			} else if item.class == hoststorage.Image {
				imageExcess--
			}
		}
		return nil
	})
}
