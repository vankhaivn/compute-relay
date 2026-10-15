package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var _ hoststorage.CleanupRepository = (*Store)(nil)

func storageConnectionInTx(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id string) (hoststorage.Scope, provider.BindingSnapshot, error) {
	record, err := loadManaged(ctx, tx, w, id)
	if err != nil {
		return hoststorage.Scope{}, provider.BindingSnapshot{}, err
	}
	if record.view.ProviderType != "ssh" || record.view.Authentication != "verified" || record.view.NewWork == "removed" || record.profile == "" {
		return hoststorage.Scope{}, provider.BindingSnapshot{}, hoststorage.ErrUnverified
	}
	var raw string
	err = tx.QueryRowContext(ctx, `SELECT p.snapshot FROM profiles x JOIN profile_revisions p ON p.profile=x.profile AND p.revision=x.revision WHERE x.profile=?`, record.profile).Scan(&raw)
	if err != nil {
		return hoststorage.Scope{}, provider.BindingSnapshot{}, dbError(err)
	}
	var profile admission.Profile
	if json.Unmarshal([]byte(raw), &profile) != nil || profile.Validate() != nil {
		return hoststorage.Scope{}, provider.BindingSnapshot{}, ErrCorrupt
	}
	target, err := cacheTargetInTx(ctx, tx, w, profile)
	if err != nil {
		return hoststorage.Scope{}, provider.BindingSnapshot{}, err
	}
	if target == nil {
		return hoststorage.Scope{}, provider.BindingSnapshot{}, hoststorage.ErrUnverified
	}
	binding := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
	if !binding.Valid() {
		return hoststorage.Scope{}, provider.BindingSnapshot{}, ErrCorrupt
	}
	return storageScope(target), binding, nil
}

func storageObjectFromScan(ctx context.Context, tx *sql.Tx, seq int64, kind hoststorage.Kind, id string) (hoststorage.Object, error) {
	var object hoststorage.Object
	object.Kind, object.ID = kind, id
	var labels string
	var size sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT size_bytes,labels_json,running,references_count FROM host_storage_objects
	 WHERE scan_seq=? AND class=? AND object_id=?`, seq, kind, id).Scan(&size, &labels, &object.Running, &object.References)
	if err != nil {
		return object, dbError(err)
	}
	if json.Unmarshal([]byte(labels), &object.Labels) != nil {
		return object, ErrCorrupt
	}
	object.SizeKnown, object.SizeBytes = size.Valid, size.Int64
	if !object.Valid() {
		return object, ErrCorrupt
	}
	return object, nil
}

func credibleStorageOrphan(scope hoststorage.Scope, o hoststorage.Object, caller domain.WorkspaceID) bool {
	labels := o.Labels
	if labels[storageLabelPrefix+"installation"] != string(scope.InstallationID) {
		return false
	}
	switch o.Kind {
	case hoststorage.Container:
		return labels[storageLabelPrefix+"workspace"] == string(caller) &&
			domain.JobID(labels[storageLabelPrefix+"job"]).Valid() &&
			domain.AttemptID(labels[storageLabelPrefix+"attempt"]).Valid() &&
			domain.ProviderInstanceID(labels[storageLabelPrefix+"instance"]).Valid() &&
			domain.OperationID(labels[storageLabelPrefix+"preparation"]).Valid() &&
			domain.SHA256Digest(labels[storageLabelPrefix+"plan"]).Valid() &&
			storageLowerHex(labels[storageLabelPrefix+"nonce"], 64) &&
			storageNativeEngine.MatchString(labels[storageLabelPrefix+"engine"])
	case hoststorage.Volume:
		bytes, err := strconv.ParseInt(labels[storageLabelPrefix+"bytes"], 10, 64)
		generation := labels[storageLabelPrefix+"generation"]
		return strings.HasPrefix(generation, "hcgen_") && storageLowerHex(strings.TrimPrefix(generation, "hcgen_"), 32) &&
			o.ID == "relay-cache-"+strings.TrimPrefix(generation, "hcgen_") &&
			labels[storageLabelPrefix+"machine"] == string(scope.MachineID) && labels[storageLabelPrefix+"engine"] == string(scope.EngineID) &&
			labels[storageLabelPrefix+"kind"] == "cache" && domain.SHA256Digest(labels[storageLabelPrefix+"sha256"]).Valid() &&
			bytes >= 0 && err == nil && strconv.FormatInt(bytes, 10) == labels[storageLabelPrefix+"bytes"] &&
			domain.OperationID(labels[storageLabelPrefix+"operation"]).Valid()
	case hoststorage.Image:
		return labels[storageLabelPrefix+"image-kind"] == "application" &&
			labels[storageLabelPrefix+"image-schema"] == "application/1" &&
			labels[storageLabelPrefix+"image-machine"] == string(scope.MachineID) && labels[storageLabelPrefix+"image-engine"] == string(scope.EngineID) &&
			domain.SHA256Digest(labels[storageLabelPrefix+"archive-sha256"]).Valid()
	}
	return false
}

var storageNativeEngine = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:-]{15,127}$`)

func storageLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for i := range value {
		if value[i] < '0' || value[i] > '9' && value[i] < 'a' || value[i] > 'f' {
			return false
		}
	}
	return true
}

func findStorageOrphanInTx(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, connection, orphan string, now time.Time) (hoststorage.Candidate, error) {
	var candidate hoststorage.Candidate
	scope, binding, err := storageConnectionInTx(ctx, tx, w, connection)
	if err != nil {
		return candidate, err
	}
	latest, err := latestStorageObservation(ctx, tx, scope)
	if err != nil {
		return candidate, err
	}
	if latest.state != "complete" || latest.at.After(now) || now.Sub(latest.at) > hoststorage.ScanFreshness {
		return candidate, hoststorage.ErrUnknown
	}
	for _, object := range latest.objects {
		if opaqueOrphanID(scope, latest.seq, object.Kind, object.ID) != orphan {
			continue
		}
		known, err := storageKnownObject(ctx, tx, scope, object)
		if err != nil {
			return candidate, err
		}
		if known {
			return candidate, hoststorage.ErrIneligible
		}
		if !credibleStorageOrphan(scope, object, w) {
			return candidate, hoststorage.ErrUnverified
		}
		action := hoststorage.OrphanContainer
		if object.Kind == hoststorage.Volume {
			action = hoststorage.OrphanVolume
		} else if object.Kind == hoststorage.Image {
			action = hoststorage.OrphanImage
		}
		candidate = hoststorage.Candidate{WorkspaceID: w, ConnectionID: connection, OrphanID: orphan, ScanSeq: latest.seq,
			Target: hoststorage.Target{Scope: scope, Class: object.Kind, ID: object.ID, Labels: object.Labels, Action: action}, Binding: binding}
		if !candidate.Target.Valid() {
			return hoststorage.Candidate{}, hoststorage.ErrUnverified
		}
		return candidate, nil
	}
	return candidate, connections.ErrNotFound
}

func (s *Store) FindStorageOrphan(ctx context.Context, w domain.WorkspaceID, token, connection, orphan string) (hoststorage.Candidate, error) {
	var candidate hoststorage.Candidate
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Manage); err != nil {
			return err
		}
		var err error
		candidate, err = findStorageOrphanInTx(ctx, tx, w, connection, orphan, now)
		return err
	})
	return candidate, err
}

func storageFingerprint(candidate hoststorage.Candidate, observed hoststorage.TargetObservation) (string, error) {
	raw, err := json.Marshal(struct {
		Scope      hoststorage.Scope
		Target     hoststorage.Target
		Binding    provider.BindingSnapshot
		ScanSeq    int64
		ObservedAt time.Time
	}{
		candidate.Target.Scope, candidate.Target, candidate.Binding, candidate.ScanSeq, observed.ObservedAt})
	if err != nil {
		return "", ErrInvalid
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Store) RecordStoragePreview(ctx context.Context, w domain.WorkspaceID, token string, candidate hoststorage.Candidate, observed hoststorage.TargetObservation) (hoststorage.Preview, error) {
	var preview hoststorage.Preview
	if candidate.WorkspaceID != w || !candidate.Target.Valid() || !candidate.Binding.Valid() || observed.ObservedAt.IsZero() ||
		len(observed.Reason) < 1 || len(observed.Reason) > 64 {
		return preview, ErrInvalid
	}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Manage); err != nil {
			return err
		}
		if observed.ObservedAt.After(now) || now.Sub(observed.ObservedAt) > hoststorage.ScanFreshness {
			return hoststorage.ErrUnknown
		}
		current, err := findStorageOrphanInTx(ctx, tx, w, candidate.ConnectionID, candidate.OrphanID, now)
		if err != nil {
			return err
		}
		if current.ScanSeq != candidate.ScanSeq || current.Binding != candidate.Binding || current.Target.ID != candidate.Target.ID {
			return hoststorage.ErrIneligible
		}
		fingerprint, err := storageFingerprint(candidate, observed)
		if err != nil {
			return err
		}
		id, err := randomID("stprev_", 16)
		if err != nil {
			return err
		}
		until := now.Add(2 * time.Minute)
		_, err = tx.ExecContext(ctx, `INSERT INTO host_storage_previews(preview_id,workspace_id,connection_id,scan_seq,class,object_id,fingerprint,eligible,reason,expires_ms,created_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id, w, candidate.ConnectionID, candidate.ScanSeq, candidate.Target.Class, candidate.Target.ID, fingerprint,
			observed.Eligible, observed.Reason, until.UnixMilli(), now.Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
		preview = hoststorage.Preview{OrphanID: candidate.OrphanID, Class: candidate.Target.Class, ChargedBytes: observed.SizeBytes,
			Eligible: observed.Eligible, Reason: observed.Reason, Fingerprint: fingerprint, ExpiresAt: until}
		return nil
	})
	return preview, err
}

func readStorageOperation(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id domain.OperationID) (hoststorage.Operation, error) {
	var op hoststorage.Operation
	var created, updated string
	err := tx.QueryRowContext(ctx, `SELECT operation_id,status,result,reason,created_at,updated_at FROM host_storage_cleanup_operations
	 WHERE operation_id=? AND workspace_id=?`, id, w).Scan(&op.OperationID, &op.Status, &op.Result, &op.Reason, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return op, connections.ErrNotFound
	}
	if err != nil {
		return op, dbError(err)
	}
	op.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return op, ErrCorrupt
	}
	op.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	if err != nil {
		return op, ErrCorrupt
	}
	return op, nil
}

func (s *Store) ReadStorageCleanup(ctx context.Context, w domain.WorkspaceID, token string, id domain.OperationID) (hoststorage.Operation, error) {
	var op hoststorage.Operation
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		if err := managedReadActor(ctx, tx, w, token); err != nil {
			return err
		}
		var err error
		op, err = readStorageOperation(ctx, tx, w, id)
		return err
	})
	return op, err
}

func (s *Store) AdmitStorageCleanup(ctx context.Context, w domain.WorkspaceID, token, connection, orphan, fingerprint, key string) (hoststorage.Operation, error) {
	var op hoststorage.Operation
	hash, err := admission.KeyDigest(key)
	if err != nil || !domain.SHA256Digest(fingerprint).Valid() {
		return op, ErrInvalid
	}
	err = s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Manage); err != nil {
			return err
		}
		var priorID domain.OperationID
		var priorFingerprint, priorConnection, priorInstallation, priorMachine, priorEngine, priorClass, priorObject string
		var priorPreview sql.NullString
		var priorSeq sql.NullInt64
		err := tx.QueryRowContext(ctx, `SELECT o.operation_id,o.fingerprint,o.connection_id,o.preview_id,
		 o.installation_id,o.machine_id,o.engine_id,o.class,o.object_id,p.scan_seq
		 FROM host_storage_cleanup_operations o LEFT JOIN host_storage_previews p ON p.preview_id=o.preview_id
		 WHERE o.workspace_id=? AND o.key_sha256=?`, w, hash).
			Scan(&priorID, &priorFingerprint, &priorConnection, &priorPreview, &priorInstallation,
				&priorMachine, &priorEngine, &priorClass, &priorObject, &priorSeq)
		if err == nil {
			priorScope := hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(priorInstallation),
				MachineID: domain.SHA256Digest(priorMachine), EngineID: domain.SHA256Digest(priorEngine)}
			if priorFingerprint != fingerprint || priorConnection != connection || !priorPreview.Valid || !priorSeq.Valid ||
				opaqueOrphanID(priorScope, priorSeq.Int64, hoststorage.Kind(priorClass), priorObject) != orphan {
				return admission.ErrConflict
			}
			op, err = readStorageOperation(ctx, tx, w, priorID)
			return err
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		candidate, err := findStorageOrphanInTx(ctx, tx, w, connection, orphan, now)
		if err != nil {
			return err
		}
		var previewID string
		var previewSeq, expires int64
		var eligible bool
		err = tx.QueryRowContext(ctx, `SELECT preview_id,scan_seq,eligible,expires_ms FROM host_storage_previews
		 WHERE workspace_id=? AND connection_id=? AND class=? AND object_id=? AND fingerprint=?
		 ORDER BY created_at DESC LIMIT 1`, w, connection, candidate.Target.Class, candidate.Target.ID, fingerprint).
			Scan(&previewID, &previewSeq, &eligible, &expires)
		if errors.Is(err, sql.ErrNoRows) {
			return hoststorage.ErrIneligible
		}
		if err != nil {
			return dbError(err)
		}
		if !eligible || expires <= now.UnixMilli() || previewSeq != candidate.ScanSeq {
			return hoststorage.ErrIneligible
		}
		labels, err := json.Marshal(candidate.Target.Labels)
		if err != nil || len(labels) > 4096 {
			return ErrCorrupt
		}
		binding, err := json.Marshal(candidate.Binding)
		if err != nil || len(binding) > 8192 {
			return ErrCorrupt
		}
		id, err := randomID("stop_", 16)
		if err != nil {
			return err
		}
		created := now.Format(time.RFC3339Nano)
		_, err = tx.ExecContext(ctx, `INSERT INTO host_storage_cleanup_operations
		 (operation_id,workspace_id,connection_id,preview_id,installation_id,machine_id,engine_id,action,class,object_id,
		 labels_json,binding_json,fingerprint,key_sha256,actor_token_id,status,result,reason,created_at,updated_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'accepted','pending','awaiting_worker',?,?)`, id, w, connection, previewID,
			candidate.Target.Scope.InstallationID, candidate.Target.Scope.MachineID, candidate.Target.Scope.EngineID, candidate.Target.Action,
			candidate.Target.Class, candidate.Target.ID, string(labels), string(binding), fingerprint, hash, token, created, created)
		if err != nil {
			return dbError(err)
		}
		op = hoststorage.Operation{OperationID: domain.OperationID(id), Status: "accepted", Result: "pending", Reason: "awaiting_worker", CreatedAt: now, UpdatedAt: now}
		return nil
	})
	return op, err
}
