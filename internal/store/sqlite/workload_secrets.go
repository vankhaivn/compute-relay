package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

type secretRecord struct{ original, current workloadsecret.Receipt }

func loadSecret(ctx context.Context, tx *sql.Tx, key workloadsecret.Key, now time.Time) (secretRecord, error) {
	var raw, metadata string
	var expires int64
	var consumed, revoked, revocation sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT r.receipt,g.metadata,g.expires_at_ms,g.consumed_at,g.revoked_at,v.receipt FROM attempt_grants g JOIN receipts r ON r.workspace_id=g.workspace_id AND r.namespace='grant' AND r.scope=g.kind AND r.key_sha256=g.key_sha256 LEFT JOIN receipts v ON v.workspace_id=g.workspace_id AND v.namespace='revoke' AND v.scope=g.authorization_id AND v.key_sha256=g.revocation_key_sha256 WHERE g.kind='secret' AND g.workspace_id=? AND g.job_id=? AND g.attempt_id=?`, string(key.WorkspaceID), string(key.JobID), string(key.AttemptID)).Scan(&raw, &metadata, &expires, &consumed, &revoked, &revocation)
	if errors.Is(err, sql.ErrNoRows) {
		return secretRecord{}, workloadsecret.ErrNotFound
	}
	if err != nil {
		return secretRecord{}, dbError(err)
	}
	var record secretRecord
	if json.Unmarshal([]byte(raw), &record.original) != nil {
		return record, ErrCorrupt
	}
	canonical, _ := json.Marshal(record.original)
	if raw != string(canonical) {
		return record, ErrCorrupt
	}
	r := record.original
	until, ok := workloadsecret.CanonicalTime(r.ExpiresAt)
	expectedMetadata, _ := json.Marshal(r.Metadata)
	if r.Validate() != nil || r.Key() != key || r.Status != "granted" || r.Replay || !ok || expires != until.UnixMilli() || metadata != string(expectedMetadata) || !until.After(r.CreatedAt) || until.After(r.CreatedAt.Add(workloadsecret.MaxLifetime)) || consumed.Valid && revoked.Valid {
		return record, ErrCorrupt
	}
	if now.Before(r.CreatedAt) {
		return record, ErrCorrupt
	}
	if consumed.Valid {
		at, err := time.Parse(time.RFC3339Nano, consumed.String)
		if err != nil {
			return record, ErrCorrupt
		}
		if now.Before(at) {
			return record, ErrCorrupt
		}
		r.Status, r.ConsumedAt = "consumed", &at
	} else if revoked.Valid {
		at, err := time.Parse(time.RFC3339Nano, revoked.String)
		if err != nil {
			return record, ErrCorrupt
		}
		if now.Before(at) {
			return record, ErrCorrupt
		}
		r.Status, r.RevokedAt = "revoked", &at
		expected, _ := json.Marshal(r)
		if !revocation.Valid || revocation.String != string(expected) {
			return record, ErrCorrupt
		}
	} else if !now.Before(until) {
		r.Status = "expired"
	}
	if r.Validate() != nil {
		return record, ErrCorrupt
	}
	record.current = r
	return record, nil
}

func matchSecretSlots(slots []workloadsecret.Slot, metadata workloadsecret.Metadata) bool {
	names := make([]string, len(slots))
	for i, slot := range slots {
		names[i] = slot.Name
	}
	slices.Sort(names)
	return len(names) > 0 && workloadsecret.ValidSlots(slots) && slices.Equal(names, metadata.Names)
}

// This check observes the original attempt only. It neither grants a permit nor
// reads material, and is also used to discard cached bytes after local controls.
func secretUsable(ctx context.Context, tx *sql.Tx, key workloadsecret.Key, id domain.OperationID, now time.Time) (bool, error) {
	secret, err := loadSecret(ctx, tx, key, now)
	if errors.Is(err, workloadsecret.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if secret.current.AuthorizationID != id || secret.current.Status != "granted" {
		return false, nil
	}
	r, err := readJobRecord(ctx, tx, key.WorkspaceID, key.JobID)
	if err != nil {
		return false, err
	}
	if r.Attempt.ID != key.AttemptID || !r.Request.Spec().SeparateStart() || r.Request.Spec().Expired(now) || r.Attempt.State.Cancellation != domain.CancellationNotRequested || !scheduler.LocalOnly(r.Attempt.State) {
		return false, nil
	}
	g, err := loadExecutionGate(ctx, tx, r, now)
	if err != nil {
		return false, err
	}
	if g == nil || g.Expired(now) || g.ReadyRevision != secret.current.ReadinessRevision || !matchSecretSlots(r.Request.Spec().WorkloadSecrets, secret.current.Metadata) {
		return false, nil
	}
	work, err := controlWork(ctx, tx, r)
	if err != nil {
		return false, err
	}
	if work.Journal.Phase != dispatch.Ready || work.Journal.SubmitStarted {
		return false, nil
	}
	var blocked bool
	err = tx.QueryRowContext(ctx, `SELECT storage_released OR EXISTS(SELECT 1 FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind='cancel' AND status IN ('accepted','running')) FROM attempt_grants WHERE kind='prepare' AND workspace_id=? AND job_id=? AND attempt_id=?`, string(key.WorkspaceID), string(key.JobID), string(key.AttemptID), string(key.WorkspaceID), string(key.JobID), string(key.AttemptID)).Scan(&blocked)
	return err == nil && !blocked, dbError(err)
}

func (s *Store) SupplyWorkloadSecrets(ctx context.Context, c workloadsecret.Command, metadata workloadsecret.Metadata, fingerprint string) (workloadsecret.Receipt, bool, error) {
	if c.Validate() != nil || metadata.Validate() != nil || !domain.SHA256Digest(fingerprint).Valid() {
		return workloadsecret.Receipt{}, false, workloadsecret.ErrRequest
	}
	var receipt workloadsecret.Receipt
	usable := false
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, c.WorkspaceID, c.TokenID, auth.Execute); err != nil {
			return err
		}
		receiptKey := receiptKey{c.WorkspaceID, "grant", "secret", c.KeyDigest, fingerprint, "", workloadsecret.ErrConflict}
		prior, found, err := replayOrCreate(ctx, tx, receiptKey, nil)
		if err != nil {
			return err
		}
		if found {
			if prior.job != c.JobID || prior.attempt != metadata.AttemptID {
				return workloadsecret.ErrConflict
			}
			key := workloadsecret.Key{WorkspaceID: c.WorkspaceID, JobID: c.JobID, AttemptID: metadata.AttemptID}
			original, err := loadSecret(ctx, tx, key, now)
			if err != nil {
				return err
			}
			receipt = original.original
			receipt.Replay = true
			usable, err = secretUsable(ctx, tx, key, receipt.AuthorizationID, now)
			return err
		}
		gateCommand := executionauth.GateCommand{WorkspaceID: c.WorkspaceID, TokenID: c.TokenID, JobID: c.JobID, KeyDigest: c.KeyDigest}
		r, err := gateRecord(ctx, tx, now, gateCommand, metadata.AttemptID)
		if err != nil {
			return err
		}
		if !matchSecretSlots(r.Request.Spec().WorkloadSecrets, metadata) {
			return workloadsecret.ErrRequest
		}
		g, err := loadExecutionGate(ctx, tx, r, now)
		if err != nil {
			return err
		}
		if g == nil || g.ReadyAt == nil || g.ReadyRevision != metadata.ReadinessRevision || g.Expired(now) {
			return workloadsecret.ErrState
		}
		work, err := controlWork(ctx, tx, r)
		if err != nil {
			return err
		}
		if work.Journal.Phase != dispatch.Ready || work.Journal.SubmitStarted {
			return workloadsecret.ErrState
		}
		until, _ := workloadsecret.CanonicalTime(metadata.ExpiresAt)
		if !now.Before(until) || until.After(now.Add(workloadsecret.MaxLifetime)) {
			return workloadsecret.ErrRequest
		}
		var exists bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attempt_grants WHERE kind='secret' AND workspace_id=? AND job_id=? AND attempt_id=?)`, string(c.WorkspaceID), string(c.JobID), string(metadata.AttemptID)).Scan(&exists); err != nil {
			return dbError(err)
		}
		if exists {
			return workloadsecret.ErrConflict
		}
		id, err := randomID("secret_", 16)
		if err != nil {
			return err
		}
		receipt = workloadsecret.Receipt{AuthorizationID: domain.OperationID(id), WorkspaceID: c.WorkspaceID, JobID: c.JobID, Metadata: metadata, Status: "granted", CreatedAt: now.UTC()}
		encoded, _ := json.Marshal(receipt)
		meta, _ := json.Marshal(metadata)
		_, err = tx.ExecContext(ctx, `INSERT INTO attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,metadata,expires_at_ms) VALUES(?,'secret',?,?,?,?,'granted',?,?)`, string(c.WorkspaceID), id, string(c.JobID), string(metadata.AttemptID), c.KeyDigest, string(meta), until.UnixMilli())
		if err != nil {
			return dbError(err)
		}
		_, _, err = replayOrCreate(ctx, tx, receiptKey, &savedReceipt{id, c.JobID, metadata.AttemptID, string(encoded)})
		usable = err == nil
		return err
	})
	if err != nil {
		return workloadsecret.Receipt{}, false, err
	}
	return receipt, usable, nil
}

func (s *Store) RevokeWorkloadSecrets(ctx context.Context, c workloadsecret.Command, attempt domain.AttemptID, id domain.OperationID) (workloadsecret.Receipt, error) {
	if c.Validate() != nil || !attempt.Valid() || !id.Valid() {
		return workloadsecret.Receipt{}, workloadsecret.ErrRequest
	}
	var receipt workloadsecret.Receipt
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, c.WorkspaceID, c.TokenID, auth.Execute); err != nil {
			return err
		}
		record, err := loadSecret(ctx, tx, workloadsecret.Key{WorkspaceID: c.WorkspaceID, JobID: c.JobID, AttemptID: attempt}, now)
		if err != nil {
			return err
		}
		if record.current.AuthorizationID != id {
			return workloadsecret.ErrNotFound
		}
		receipt = record.current
		var supplyKey string
		if err = tx.QueryRowContext(ctx, `SELECT key_sha256 FROM attempt_grants WHERE kind='secret' AND workspace_id=? AND authorization_id=?`, c.WorkspaceID, id).Scan(&supplyKey); err != nil {
			return dbError(err)
		}
		key := receiptKey{c.WorkspaceID, "revoke", string(id), c.KeyDigest, supplyKey, "", workloadsecret.ErrConflict}
		prior, found, err := replayOrCreate(ctx, tx, key, nil)
		if err != nil {
			return err
		}
		if found {
			if receipt.Status != "revoked" || json.Unmarshal([]byte(prior.raw), &receipt) != nil || receipt.Validate() != nil {
				return ErrCorrupt
			}
			receipt.Replay = true
			return nil
		}
		if receipt.Status == "revoked" {
			return workloadsecret.ErrConflict
		}
		if receipt.Status == "consumed" {
			return workloadsecret.ErrState
		}
		receipt.Status, receipt.RevokedAt = "revoked", &now
		raw, _ := json.Marshal(receipt)
		result, err := tx.ExecContext(ctx, `UPDATE attempt_grants SET status='revoked',revoked_at=?,revocation_key_sha256=? WHERE kind='secret' AND workspace_id=? AND authorization_id=? AND consumed_at IS NULL AND revoked_at IS NULL`, now.UTC().Format(time.RFC3339Nano), c.KeyDigest, string(c.WorkspaceID), string(id))
		if err != nil {
			return dbError(err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		if count != 1 {
			return workloadsecret.ErrState
		}
		_, _, err = replayOrCreate(ctx, tx, key, &savedReceipt{string(id), c.JobID, attempt, string(raw)})
		return err
	})
	if err != nil {
		return workloadsecret.Receipt{}, err
	}
	return receipt, nil
}

func (s *Store) ReadWorkloadSecrets(ctx context.Context, workspace domain.WorkspaceID, token string, job domain.JobID, id domain.OperationID) (workloadsecret.Receipt, error) {
	if !workspace.Valid() || !job.Valid() || !id.Valid() {
		return workloadsecret.Receipt{}, workloadsecret.ErrNotFound
	}
	var receipt workloadsecret.Receipt
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, workspace, token, auth.Execute); err != nil {
			return err
		}
		var attempt string
		err := tx.QueryRowContext(ctx, `SELECT attempt_id FROM attempt_grants WHERE kind='secret' AND workspace_id=? AND job_id=? AND authorization_id=?`, string(workspace), string(job), string(id)).Scan(&attempt)
		if errors.Is(err, sql.ErrNoRows) {
			return workloadsecret.ErrNotFound
		}
		if err != nil {
			return dbError(err)
		}
		record, err := loadSecret(ctx, tx, workloadsecret.Key{WorkspaceID: workspace, JobID: job, AttemptID: domain.AttemptID(attempt)}, now)
		receipt = record.current
		return err
	})
	return receipt, err
}

func (s *Store) WorkloadSecretsUsable(ctx context.Context, key workloadsecret.Key, id domain.OperationID) (bool, error) {
	if !key.Valid() || !id.Valid() {
		return false, workloadsecret.ErrRequest
	}
	var usable bool
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var err error
		usable, err = secretUsable(ctx, tx, key, id, now)
		return err
	})
	return usable, err
}

func (s *Store) HasWorkloadSecretAuthorizations(ctx context.Context) (bool, error) {
	return read(ctx, s, func(ctx context.Context) (bool, error) {
		var exists bool
		err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attempt_grants WHERE kind='secret') OR EXISTS(SELECT 1 FROM receipts WHERE (namespace='grant' AND scope='secret') OR namespace='revoke')`).Scan(&exists)
		return exists, dbError(err)
	})
}

func secretForStart(ctx context.Context, tx *sql.Tx, r admission.Record, id domain.OperationID, revision domain.SHA256Digest, deadline string, now time.Time) (*workloadsecret.Receipt, error) {
	if len(r.Request.Spec().WorkloadSecrets) == 0 {
		if id != "" {
			return nil, workloadsecret.ErrRequest
		}
		return nil, nil
	}
	if !id.Valid() {
		return nil, workloadsecret.ErrUnavailable
	}
	key := workloadsecret.Key{WorkspaceID: r.Job.WorkspaceID, JobID: r.Job.ID, AttemptID: r.Attempt.ID}
	secret, err := loadSecret(ctx, tx, key, now)
	if errors.Is(err, workloadsecret.ErrNotFound) {
		return nil, workloadsecret.ErrUnavailable
	}
	if err != nil {
		return nil, err
	}
	until, _ := workloadsecret.CanonicalTime(secret.current.ExpiresAt)
	stop, _ := workloadsecret.CanonicalTime(deadline)
	if secret.current.AuthorizationID != id || secret.current.Status != "granted" || secret.current.ReadinessRevision != revision || until.Before(stop) || !matchSecretSlots(r.Request.Spec().WorkloadSecrets, secret.current.Metadata) {
		return nil, workloadsecret.ErrUnavailable
	}
	return &secret.current, nil
}
