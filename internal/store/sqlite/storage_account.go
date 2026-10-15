package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

const storageLabelPrefix = "io.compute-relay."

type storageObservation struct {
	seq     int64
	at      time.Time
	state   string
	foreign hoststorage.Foreign
	objects []hoststorage.Object
}

func storageScope(t *frozenCacheTarget) hoststorage.Scope {
	return hoststorage.Scope{InstallationID: t.installation, MachineID: t.machine, EngineID: t.engine}
}

// RecordStorageScan commits a complete cohort atomically. An incomplete scan
// records its failure but cannot replace the latest complete observation for
// absence inference or admission freshness.
func (s *Store) RecordStorageScan(ctx context.Context, scan hoststorage.Scan) error {
	if !scan.Valid() {
		return hoststorage.ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if scan.ObservedAt.After(now) || now.Sub(scan.ObservedAt) > hoststorage.ScanFreshness {
			return hoststorage.ErrInvalid
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO host_storage_scans(installation_id,machine_id,engine_id,observed_ms,status,foreign_containers,foreign_volumes,foreign_images)
		 VALUES(?,?,?,?,'complete',?,?,?)`, scan.Scope.InstallationID, scan.Scope.MachineID, scan.Scope.EngineID,
			scan.ObservedAt.UnixMilli(), scan.Foreign.Containers, scan.Foreign.Volumes, scan.Foreign.Images)
		if err != nil {
			return dbError(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			return dbError(err)
		}
		for _, o := range scan.Objects {
			labels, err := json.Marshal(o.Labels)
			if err != nil || len(labels) > 4096 {
				return hoststorage.ErrInvalid
			}
			var size any
			if o.SizeKnown {
				size = o.SizeBytes
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO host_storage_objects(scan_seq,class,object_id,size_bytes,labels_json,running,references_count)
			 VALUES(?,?,?,?,?,?,?)`, seq, o.Kind, o.ID, size, string(labels), o.Running, o.References)
			if err != nil {
				return dbError(err)
			}
		}
		present := make(map[string]bool, len(scan.Objects))
		for _, o := range scan.Objects {
			present[storageObjectKey(o.Kind, o.ID)] = true
		}
		ledger, err := tx.QueryContext(ctx, `SELECT 'volume',volume_name FROM host_cache_generations
		 WHERE installation_id=? AND machine_id=? AND engine_id=?
		 UNION ALL SELECT 'image',expected_image_id FROM host_application_images
		 WHERE installation_id=? AND machine_id=? AND engine_id=? AND expected_image_id!=''`,
			scan.Scope.InstallationID, scan.Scope.MachineID, scan.Scope.EngineID,
			scan.Scope.InstallationID, scan.Scope.MachineID, scan.Scope.EngineID)
		if err != nil {
			return dbError(err)
		}
		type missingObject struct{ class, id string }
		missing := []missingObject{}
		for ledger.Next() {
			var item missingObject
			if err := ledger.Scan(&item.class, &item.id); err != nil {
				ledger.Close()
				return dbError(err)
			}
			if !present[item.class+"\x00"+item.id] {
				missing = append(missing, item)
			}
		}
		err = ledger.Err()
		ledger.Close()
		if err != nil {
			return dbError(err)
		}
		// Attempt containers have independent creation ledgers. Their exact
		// observed absence is retained even after cleanup while the job and
		// operation histories remain immutable.
		containers, err := tx.QueryContext(ctx, `SELECT COALESCE(json_extract(r.resource_ref,'$.id'),
		 json_extract(json_extract(r.resource_ref,'$.resource'),'$.id'))
		 FROM provider_resources r JOIN jobs j ON j.workspace_id=r.workspace_id AND j.job_id=r.job_id
		 JOIN managed_profile_runtime m ON m.profile=j.profile AND m.revision=j.profile_revision
		 WHERE json_valid(r.resource_ref) AND json_valid(r.identity)
		 AND json_extract(r.identity,'$.InstallationID')=?
		 AND json_extract(m.config,'$.machine_id')=? AND json_extract(m.config,'$.engine_id')=?`,
			scan.Scope.InstallationID, scan.Scope.MachineID, scan.Scope.EngineID)
		if err != nil {
			return dbError(err)
		}
		for containers.Next() {
			var id sql.NullString
			if err := containers.Scan(&id); err != nil {
				containers.Close()
				return dbError(err)
			}
			if id.Valid && (hoststorage.Object{Kind: hoststorage.Container, ID: id.String}).Valid() && !present[storageObjectKey(hoststorage.Container, id.String)] {
				missing = append(missing, missingObject{"container", id.String})
			}
		}
		err = containers.Err()
		containers.Close()
		if err != nil {
			return dbError(err)
		}
		for _, item := range missing {
			_, err = tx.ExecContext(ctx, `INSERT INTO host_storage_absences(installation_id,machine_id,engine_id,class,object_id,first_scan_seq,last_scan_seq)
			 VALUES(?,?,?,?,?,?,?) ON CONFLICT(installation_id,machine_id,engine_id,class,object_id)
			 DO UPDATE SET last_scan_seq=excluded.last_scan_seq`, scan.Scope.InstallationID, scan.Scope.MachineID,
				scan.Scope.EngineID, item.class, item.id, seq, seq)
			if err != nil {
				return dbError(err)
			}
		}
		return releaseAbsentStorageReservationsInTx(ctx, tx, scan, present)
	})
}

func (s *Store) RecordIncompleteStorageScan(ctx context.Context, scope hoststorage.Scope, reason string) error {
	if !scope.Valid() || len(reason) == 0 || len(reason) > 64 {
		return hoststorage.ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO host_storage_scans(installation_id,machine_id,engine_id,observed_ms,status,reason)
		 VALUES(?,?,?,?,'incomplete',?)`, scope.InstallationID, scope.MachineID, scope.EngineID, now.UnixMilli(), reason)
		return dbError(err)
	})
}

func latestStorageObservation(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope) (storageObservation, error) {
	var o storageObservation
	var millis int64
	err := tx.QueryRowContext(ctx, `SELECT scan_seq,observed_ms,status,foreign_containers,foreign_volumes,foreign_images
	 FROM host_storage_scans WHERE installation_id=? AND machine_id=? AND engine_id=? ORDER BY scan_seq DESC LIMIT 1`,
		scope.InstallationID, scope.MachineID, scope.EngineID).Scan(&o.seq, &millis, &o.state,
		&o.foreign.Containers, &o.foreign.Volumes, &o.foreign.Images)
	if errors.Is(err, sql.ErrNoRows) {
		return o, nil
	}
	if err != nil {
		return o, dbError(err)
	}
	o.at = time.UnixMilli(millis).UTC()
	if o.state != "complete" {
		return o, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT class,object_id,size_bytes,labels_json,running,references_count
	 FROM host_storage_objects WHERE scan_seq=? ORDER BY class,object_id LIMIT ?`, o.seq, hoststorage.ScanMaxObjects*3+1)
	if err != nil {
		return o, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var object hoststorage.Object
		var labels string
		var size sql.NullInt64
		if err := rows.Scan(&object.Kind, &object.ID, &size, &labels, &object.Running, &object.References); err != nil {
			return o, dbError(err)
		}
		if len(labels) > 4096 || json.Unmarshal([]byte(labels), &object.Labels) != nil {
			return o, ErrCorrupt
		}
		object.SizeKnown, object.SizeBytes = size.Valid, size.Int64
		if !object.Valid() || object.Labels[storageLabelPrefix+"installation"] != string(scope.InstallationID) {
			return o, ErrCorrupt
		}
		o.objects = append(o.objects, object)
	}
	if err := rows.Err(); err != nil {
		return o, dbError(err)
	}
	if len(o.objects) > hoststorage.ScanMaxObjects*3 {
		return o, ErrCorrupt
	}
	return o, nil
}

func effectiveStorageBudget(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, configured hoststorage.Budget) (hoststorage.Budget, error) {
	effective := configured
	rows, err := tx.QueryContext(ctx, `SELECT COALESCE(c.storage_soft_bytes,?),COALESCE(c.storage_hard_bytes,?)
	 FROM managed_connections c JOIN profiles p ON p.profile=c.current_profile
	 JOIN managed_profile_runtime m ON m.profile=p.profile AND m.revision=p.revision
	 WHERE c.provider_type='ssh' AND c.authentication='verified' AND c.new_work='enabled'
	 AND c.pending_operation='' AND p.enabled=1 AND json_extract(m.config,'$.schema_version')=3
	 AND json_extract(m.config,'$.machine_id')=? AND json_extract(m.config,'$.engine_id')=?`,
		hoststorage.DefaultSoftBytes, hoststorage.DefaultHardBytes, scope.MachineID, scope.EngineID)
	if err != nil {
		return effective, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var b hoststorage.Budget
		if err := rows.Scan(&b.SoftBytes, &b.HardBytes); err != nil {
			return effective, dbError(err)
		}
		if !b.Valid() {
			return effective, ErrCorrupt
		}
		effective = hoststorage.MinBudget(effective, b)
	}
	return effective, dbError(rows.Err())
}

type storageCharge struct {
	view    hoststorage.View
	objects map[string]int64
	unknown bool
}

func storageObjectKey(kind hoststorage.Kind, id string) string { return string(kind) + "\x00" + id }

func addStorageCharge(total *int64, bytes int64) error {
	n, err := hoststorage.AddChecked(*total, bytes)
	if err != nil {
		return ErrCorrupt
	}
	*total = n
	return nil
}

// A confirmed exact deletion supersedes the scan that supplied its target.
// A later complete scan revokes that accounting shortcut if the same Docker
// identity is observed again, so deterministic image IDs can be charged and
// evicted in a new generation.
func storageDeletedSinceScan(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, observation storageObservation) (map[string]bool, error) {
	deleted := map[string]bool{}
	if observation.state != "complete" || observation.seq == 0 {
		return deleted, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT class,object_id FROM host_storage_cleanup_operations
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND result IN ('deleted','absent')
	 AND confirmed_scan_seq>=?`, scope.InstallationID, scope.MachineID, scope.EngineID, observation.seq)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var class, id string
		if err := rows.Scan(&class, &id); err != nil {
			return nil, dbError(err)
		}
		deleted[class+"\x00"+id] = true
	}
	return deleted, dbError(rows.Err())
}

func storageChargeInTx(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, observation storageObservation, caller domain.WorkspaceID) (storageCharge, error) {
	c := storageCharge{objects: map[string]int64{}}
	c.view.SchemaVersion = 1
	c.view.Scope.ResourceID = "resource_" + string(scope.MachineID)
	c.view.ScanState = "unknown"
	c.view.Precision = "upper_bound"
	c.view.Workspaces = []hoststorage.WorkspaceReference{}
	c.view.Retention = []hoststorage.RetentionItem{}
	c.view.Orphans = []hoststorage.Orphan{}
	if observation.seq != 0 {
		c.view.ScanState = observation.state
		if observation.state == "complete" {
			at := observation.at
			c.view.ObservedAt = &at
			c.view.Foreign = observation.foreign
		}
	}
	deleted, err := storageDeletedSinceScan(ctx, tx, scope, observation)
	if err != nil {
		return c, err
	}
	accountedObjects := make([]hoststorage.Object, 0, len(observation.objects))
	observedKeys := make(map[string]bool, len(observation.objects))
	for _, object := range observation.objects {
		key := storageObjectKey(object.Kind, object.ID)
		if deleted[key] {
			continue
		}
		accountedObjects = append(accountedObjects, object)
		observedKeys[key] = true
	}
	const ledger = `SELECT 'volume',volume_name,bytes,state FROM host_cache_generations
	 WHERE installation_id=? AND machine_id=? AND engine_id=?
	 UNION ALL SELECT 'image',CASE WHEN expected_image_id='' THEN 'generation:'||generation_id ELSE expected_image_id END,archive_bytes,state
	 FROM host_application_images WHERE installation_id=? AND machine_id=? AND engine_id=?`
	rows, err := tx.QueryContext(ctx, ledger, scope.InstallationID, scope.MachineID, scope.EngineID,
		scope.InstallationID, scope.MachineID, scope.EngineID)
	if err != nil {
		return c, dbError(err)
	}
	for rows.Next() {
		var class, id, state string
		var bytes int64
		if err := rows.Scan(&class, &id, &bytes, &state); err != nil {
			rows.Close()
			return c, dbError(err)
		}
		key := class + "\x00" + id
		if deleted[key] {
			continue
		}
		// Retirement is an application state, not physical deletion. Only a
		// complete exact absence may discharge a retired generation.
		if state == "retired" && observation.state == "complete" && !observedKeys[key] {
			continue
		}
		c.objects[key] = max(c.objects[key], bytes)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return c, dbError(err)
	}
	observedAttempts := map[string]int64{}
	for _, object := range accountedObjects {
		key := storageObjectKey(object.Kind, object.ID)
		if object.Kind == hoststorage.Volume && (object.Labels[storageLabelPrefix+"machine"] != string(scope.MachineID) ||
			object.Labels[storageLabelPrefix+"engine"] != string(scope.EngineID) || object.Labels[storageLabelPrefix+"kind"] != "cache") ||
			object.Kind == hoststorage.Image && (object.Labels[storageLabelPrefix+"image-machine"] != string(scope.MachineID) ||
				object.Labels[storageLabelPrefix+"image-engine"] != string(scope.EngineID) || object.Labels[storageLabelPrefix+"image-kind"] != "application") {
			// A known ledger amount cannot make an unverified Docker object a
			// trustworthy bound for the current machine/engine.
			c.unknown = true
		}
		if object.SizeKnown {
			c.objects[key] = max(c.objects[key], object.SizeBytes)
		} else if _, known := c.objects[key]; !known {
			// A cache volume's declared immutable size remains a conservative
			// charge even though Docker volume inspect has no size field.
			c.unknown = true
		}
		if object.Kind == hoststorage.Container {
			attempt := object.Labels[storageLabelPrefix+"attempt"]
			if attempt != "" && object.SizeKnown {
				amount := observedAttempts[attempt]
				if err := addStorageCharge(&amount, object.SizeBytes); err != nil {
					return c, err
				}
				observedAttempts[attempt] = amount
			}
		}
	}
	// Active reservation keys are globally unique across aliases. The maximum
	// of observed and reserved bytes for one object prevents double charging.
	rows, err = tx.QueryContext(ctx, `SELECT class,resource_key,bytes,owner_attempt_id,mutation_operation_id FROM host_storage_reservations
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND state!='released'`,
		scope.InstallationID, scope.MachineID, scope.EngineID)
	if err != nil {
		return c, dbError(err)
	}
	type reservedObject struct {
		class, key, attempt string
		bytes               int64
		operation           sql.NullString
	}
	reservations := []reservedObject{}
	for rows.Next() {
		var item reservedObject
		if err := rows.Scan(&item.class, &item.key, &item.bytes, &item.attempt, &item.operation); err != nil {
			rows.Close()
			return c, dbError(err)
		}
		reservations = append(reservations, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return c, dbError(err)
	}
	for _, item := range reservations {
		class, key, attempt, bytes := item.class, item.key, item.attempt, item.bytes
		if err := addStorageCharge(&c.view.Budget.ReservedBytes, bytes); err != nil {
			return c, err
		}
		if class == "attempt" {
			c.objects["attempt\x00"+attempt] = max(c.objects["attempt\x00"+attempt], bytes)
		} else {
			// A writer claim transfers a provisional content reservation to a
			// ledger identity. Charge the larger amount once while retaining the
			// immutable reservation for auditing and lost-ack recovery.
			physical := "reservation:" + class + "\x00" + key
			sha, count, cut := strings.Cut(key, ":")
			if cut {
				var id string
				var lookupErr error
				if class == "cache" && item.operation.Valid {
					lookupErr = tx.QueryRowContext(ctx, `SELECT volume_name FROM host_cache_generations
					 WHERE installation_id=? AND machine_id=? AND engine_id=? AND operation_id=?`,
						scope.InstallationID, scope.MachineID, scope.EngineID, item.operation.String).Scan(&id)
					if lookupErr == nil {
						physical = "volume\x00" + id
					}
				} else if class == "image" && item.operation.Valid {
					lookupErr = tx.QueryRowContext(ctx, `SELECT CASE WHEN expected_image_id='' THEN 'generation:'||generation_id ELSE expected_image_id END
					 FROM host_application_images WHERE installation_id=? AND machine_id=? AND engine_id=? AND operation_id=?`,
						scope.InstallationID, scope.MachineID, scope.EngineID, item.operation.String).Scan(&id)
					if lookupErr == nil {
						physical = "image\x00" + id
					}
				} else if class == "cache" {
					lookupErr = tx.QueryRowContext(ctx, `SELECT volume_name FROM host_cache_generations
					 WHERE installation_id=? AND machine_id=? AND engine_id=? AND sha256=? AND bytes=? AND state!='retired'
					 ORDER BY created_at DESC LIMIT 1`, scope.InstallationID, scope.MachineID, scope.EngineID, sha, count).Scan(&id)
					if lookupErr == nil {
						physical = "volume\x00" + id
					}
				} else if class == "image" {
					lookupErr = tx.QueryRowContext(ctx, `SELECT CASE WHEN expected_image_id='' THEN 'generation:'||generation_id ELSE expected_image_id END
					 FROM host_application_images WHERE installation_id=? AND machine_id=? AND engine_id=? AND archive_sha256=? AND archive_bytes=? AND state!='retired'
					 ORDER BY created_at DESC LIMIT 1`, scope.InstallationID, scope.MachineID, scope.EngineID, sha, count).Scan(&id)
					if lookupErr == nil {
						physical = "image\x00" + id
					}
				}
				if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
					return c, dbError(lookupErr)
				}
			}
			c.objects[physical] = max(c.objects[physical], bytes)
		}
	}
	for attempt := range observedAttempts {
		key := "attempt\x00" + attempt
		c.objects[key] = max(c.objects[key], observedAttempts[attempt])
	}
	for key, bytes := range c.objects {
		class, id, _ := strings.Cut(key, "\x00")
		if class == "attempt" {
			bytes = max(bytes, observedAttempts[id])
			if err := addStorageCharge(&c.view.Machine.AttemptWritableBytes, bytes); err != nil {
				return c, err
			}
		} else if class == "volume" {
			if err := addStorageCharge(&c.view.Machine.CacheBytes, bytes); err != nil {
				return c, err
			}
		} else if class == "image" || class == "reservation:image" {
			if err := addStorageCharge(&c.view.Machine.ImageSizeUpperBound, bytes); err != nil {
				return c, err
			}
		} else if class == "reservation:cache" {
			if err := addStorageCharge(&c.view.Machine.CacheBytes, bytes); err != nil {
				return c, err
			}
		} else if class == "container" {
			if ledgerAttempt := func() string {
				for _, object := range accountedObjects {
					if object.Kind == hoststorage.Container && object.ID == id {
						return object.Labels[storageLabelPrefix+"attempt"]
					}
				}
				return ""
			}(); ledgerAttempt != "" {
				continue
			}
			// Container size is represented by its attempt reservation when
			// possible; unlabelled helper layers remain an explicit charge.
			if err := addStorageCharge(&c.view.Machine.OrphanBytes, bytes); err != nil {
				return c, err
			}
		}
	}
	for _, part := range []int64{c.view.Machine.CacheBytes, c.view.Machine.ImageSizeUpperBound,
		c.view.Machine.AttemptWritableBytes, c.view.Machine.OrphanBytes} {
		if err := addStorageCharge(&c.view.Machine.ChargedBytes, part); err != nil {
			return c, err
		}
	}
	c.view.Budget.ChargedBytes = c.view.Machine.ChargedBytes
	// Logical references are deliberately independent of the physical charge.
	var ownCache, ownImages int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(sum(bytes),0),count(*) FROM host_cache_pins
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND workspace_id=? AND released_at=''`,
		scope.InstallationID, scope.MachineID, scope.EngineID, caller).Scan(&ownCache, &c.view.Cache.LivePinCount); err != nil {
		return c, dbError(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(sum(archive_bytes),0) FROM host_application_image_pins
	 WHERE installation_id=? AND machine_id=? AND engine_id=? AND workspace_id=? AND released_at=''`,
		scope.InstallationID, scope.MachineID, scope.EngineID, caller).Scan(&ownImages); err != nil {
		return c, dbError(err)
	}
	var refs int64
	if err := addStorageCharge(&refs, ownCache); err != nil {
		return c, err
	}
	if err := addStorageCharge(&refs, ownImages); err != nil {
		return c, err
	}
	c.view.Workspaces = append(c.view.Workspaces, hoststorage.WorkspaceReference{WorkspaceID: caller, ReferencedBytes: refs})
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM host_cache_generations WHERE installation_id=? AND machine_id=? AND engine_id=? AND state!='retired'`,
		scope.InstallationID, scope.MachineID, scope.EngineID).Scan(&c.view.Cache.GenerationCount); err != nil {
		return c, dbError(err)
	}
	c.view.Cache.ChargedBytes = c.view.Machine.CacheBytes
	return c, nil
}

func opaqueOrphanID(scope hoststorage.Scope, seq int64, class hoststorage.Kind, objectID string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%s\x00%s", scope.InstallationID, scope.MachineID, scope.EngineID, seq, class, objectID)))
	return "orphan_" + hex.EncodeToString(sum[:])
}

func (s *Store) ConnectionStorage(ctx context.Context, w domain.WorkspaceID, token, id string) (hoststorage.View, error) {
	var view hoststorage.View
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if err := managedReadActor(ctx, tx, w, token); err != nil {
			return err
		}
		record, err := loadManaged(ctx, tx, w, id)
		if err != nil {
			return err
		}
		if record.view.ProviderType != "ssh" {
			return hoststorage.ErrUnverified
		}
		if record.view.Authentication != "verified" || record.profile == "" {
			return hoststorage.ErrUnverified
		}
		var raw string
		if err := tx.QueryRowContext(ctx, `SELECT p.snapshot FROM profiles x JOIN profile_revisions p ON p.profile=x.profile AND p.revision=x.revision WHERE x.profile=?`, record.profile).Scan(&raw); err != nil {
			return dbError(err)
		}
		var profile admission.Profile
		if json.Unmarshal([]byte(raw), &profile) != nil || profile.Validate() != nil {
			return ErrCorrupt
		}
		target, err := cacheTargetInTx(ctx, tx, w, profile)
		if err != nil {
			return err
		}
		if target == nil {
			return hoststorage.ErrUnverified
		}
		scope := storageScope(target)
		observation, err := latestStorageObservation(ctx, tx, scope)
		if err != nil {
			return err
		}
		charge, err := storageChargeInTx(ctx, tx, scope, observation, w)
		if err != nil {
			return err
		}
		view = charge.view
		view.Scope.ConnectionRevision = record.view.Revision
		view.Budget.Configured = hoststorage.DefaultBudget()
		if record.view.Configuration != nil && record.view.Configuration.StorageBudget != nil {
			view.Budget.Configured = *record.view.Configuration.StorageBudget
		}
		view.Budget.Effective, err = effectiveStorageBudget(ctx, tx, scope, view.Budget.Configured)
		if err != nil {
			return err
		}
		if err := projectStorageExtras(ctx, tx, scope, observation, w, now, &view); err != nil {
			return err
		}
		view.Budget.State = "unknown"
		if observation.state == "complete" && now.Sub(observation.at) <= hoststorage.ScanFreshness && !charge.unknown {
			view.Budget.State = "below_soft"
			if view.Machine.ChargedBytes >= view.Budget.Effective.HardBytes {
				view.Budget.State = "hard_refusal"
			} else if view.Machine.ChargedBytes >= view.Budget.Effective.SoftBytes {
				view.Budget.State = "above_soft"
			}
		}
		return nil
	})
	return view, err
}
