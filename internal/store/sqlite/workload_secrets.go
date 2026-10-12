package sqlite

import (
	"context"
	"crypto/hmac"
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
	err := tx.QueryRowContext(ctx, `SELECT original_receipt,metadata,expires_at_ms,consumed_at,revoked_at,revocation_receipt FROM workload_secret_authorizations WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(key.WorkspaceID), string(key.JobID), string(key.AttemptID)).Scan(&raw, &metadata, &expires, &consumed, &revoked, &revocation)
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
		return record, scheduler.ErrClock
	}
	if consumed.Valid {
		at, err := time.Parse(time.RFC3339Nano, consumed.String)
		if err != nil {
			return record, ErrCorrupt
		}
		if now.Before(at) {
			return record, scheduler.ErrClock
		}
		r.Status, r.ConsumedAt = "consumed", &at
	} else if revoked.Valid {
		at, err := time.Parse(time.RFC3339Nano, revoked.String)
		if err != nil {
			return record, ErrCorrupt
		}
		if now.Before(at) {
			return record, scheduler.ErrClock
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
	err = tx.QueryRowContext(ctx, `SELECT storage_released OR EXISTS(SELECT 1 FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind='cancel' AND status IN ('accepted','running')) FROM preparation_authorizations WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(key.WorkspaceID), string(key.JobID), string(key.AttemptID), string(key.WorkspaceID), string(key.JobID), string(key.AttemptID)).Scan(&blocked)
	return err == nil && !blocked, dbError(err)
}

func (s *Store) SupplyWorkloadSecrets(ctx context.Context, c workloadsecret.Command, metadata workloadsecret.Metadata, fingerprint string) (workloadsecret.Receipt, bool, error) {
	if c.Validate() != nil || metadata.Validate() != nil || !domain.SHA256Digest(fingerprint).Valid() {
		return workloadsecret.Receipt{}, false, workloadsecret.ErrRequest
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return workloadsecret.Receipt{}, false, err
	}
	defer done()
	var receipt workloadsecret.Receipt
	usable := false
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, c.WorkspaceID, c.TokenID, auth.Execute); err != nil {
			return err
		}
		var priorHash, priorJob, priorAttempt string
		err := tx.QueryRowContext(ctx, `SELECT request_hmac,job_id,attempt_id FROM workload_secret_authorizations WHERE workspace_id=? AND key_sha256=?`, string(c.WorkspaceID), c.KeyDigest).Scan(&priorHash, &priorJob, &priorAttempt)
		if err == nil {
			if !hmac.Equal([]byte(priorHash), []byte(fingerprint)) || priorJob != string(c.JobID) || priorAttempt != string(metadata.AttemptID) {
				return workloadsecret.ErrConflict
			}
			key := workloadsecret.Key{WorkspaceID: c.WorkspaceID, JobID: c.JobID, AttemptID: metadata.AttemptID}
			prior, err := loadSecret(ctx, tx, key, c.Now)
			if err != nil {
				return err
			}
			receipt = prior.original
			receipt.Replay = true
			usable, err = secretUsable(ctx, tx, key, receipt.AuthorizationID, c.Now)
			return err
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		gateCommand := executionauth.GateCommand{WorkspaceID: c.WorkspaceID, TokenID: c.TokenID, JobID: c.JobID, KeyDigest: c.KeyDigest, Now: c.Now}
		r, err := gateRecord(ctx, tx, gateCommand, metadata.AttemptID)
		if err != nil {
			return err
		}
		if !matchSecretSlots(r.Request.Spec().WorkloadSecrets, metadata) {
			return workloadsecret.ErrRequest
		}
		g, err := loadExecutionGate(ctx, tx, r, c.Now)
		if err != nil {
			return err
		}
		if g == nil || g.ReadyAt == nil || g.ReadyRevision != metadata.ReadinessRevision || g.Expired(c.Now) {
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
		if !c.Now.Before(until) || until.After(c.Now.Add(workloadsecret.MaxLifetime)) {
			return workloadsecret.ErrRequest
		}
		var exists bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workload_secret_authorizations WHERE workspace_id=? AND job_id=? AND attempt_id=?)`, string(c.WorkspaceID), string(c.JobID), string(metadata.AttemptID)).Scan(&exists); err != nil {
			return dbError(err)
		}
		if exists {
			return workloadsecret.ErrConflict
		}
		id, err := randomID("secret_", 16)
		if err != nil {
			return err
		}
		receipt = workloadsecret.Receipt{AuthorizationID: domain.OperationID(id), WorkspaceID: c.WorkspaceID, JobID: c.JobID, Metadata: metadata, Status: "granted", CreatedAt: c.Now.UTC()}
		encoded, _ := json.Marshal(receipt)
		meta, _ := json.Marshal(metadata)
		_, err = tx.ExecContext(ctx, `INSERT INTO workload_secret_authorizations(workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_hmac,metadata,original_receipt,expires_at_ms) VALUES(?,?,?,?,?,?,?,?,?)`, string(c.WorkspaceID), id, string(c.JobID), string(metadata.AttemptID), c.KeyDigest, fingerprint, string(meta), string(encoded), until.UnixMilli())
		usable = err == nil
		return dbError(err)
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
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return workloadsecret.Receipt{}, err
	}
	defer done()
	var receipt workloadsecret.Receipt
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, c.WorkspaceID, c.TokenID, auth.Execute); err != nil {
			return err
		}
		record, err := loadSecret(ctx, tx, workloadsecret.Key{WorkspaceID: c.WorkspaceID, JobID: c.JobID, AttemptID: attempt}, c.Now)
		if err != nil {
			return err
		}
		if record.current.AuthorizationID != id {
			return workloadsecret.ErrNotFound
		}
		receipt = record.current
		if receipt.Status == "revoked" {
			var key string
			if err = tx.QueryRowContext(ctx, `SELECT revocation_key_sha256 FROM workload_secret_authorizations WHERE workspace_id=? AND authorization_id=?`, string(c.WorkspaceID), string(id)).Scan(&key); err != nil {
				return dbError(err)
			}
			if key != c.KeyDigest {
				return workloadsecret.ErrConflict
			}
			receipt.Replay = true
			return nil
		}
		if receipt.Status == "consumed" {
			return workloadsecret.ErrState
		}
		receipt.Status, receipt.RevokedAt = "revoked", &c.Now
		raw, _ := json.Marshal(receipt)
		result, err := tx.ExecContext(ctx, `UPDATE workload_secret_authorizations SET revoked_at=?,revocation_key_sha256=?,revocation_receipt=? WHERE workspace_id=? AND authorization_id=? AND consumed_at IS NULL AND revoked_at IS NULL`, c.Now.UTC().Format(time.RFC3339Nano), c.KeyDigest, string(raw), string(c.WorkspaceID), string(id))
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
		return nil
	})
	if err != nil {
		return workloadsecret.Receipt{}, err
	}
	return receipt, nil
}

func (s *Store) ReadWorkloadSecrets(ctx context.Context, workspace domain.WorkspaceID, token string, job domain.JobID, id domain.OperationID, now time.Time) (workloadsecret.Receipt, error) {
	if !workspace.Valid() || !job.Valid() || !id.Valid() || !scheduler.ValidTime(now) {
		return workloadsecret.Receipt{}, workloadsecret.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return workloadsecret.Receipt{}, err
	}
	defer done()
	var receipt workloadsecret.Receipt
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, workspace, token, auth.Execute); err != nil {
			return err
		}
		var attempt string
		err := tx.QueryRowContext(ctx, `SELECT attempt_id FROM workload_secret_authorizations WHERE workspace_id=? AND job_id=? AND authorization_id=?`, string(workspace), string(job), string(id)).Scan(&attempt)
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

func (s *Store) WorkloadSecretsUsable(ctx context.Context, key workloadsecret.Key, id domain.OperationID, now time.Time) (bool, error) {
	if !key.Valid() || !id.Valid() || !scheduler.ValidTime(now) {
		return false, workloadsecret.ErrRequest
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return false, err
	}
	defer done()
	var usable bool
	err = withTx(ctx, s.db, func(tx *sql.Tx) error { var err error; usable, err = secretUsable(ctx, tx, key, id, now); return err })
	return usable, err
}

func (s *Store) HasWorkloadSecretAuthorizations(ctx context.Context) (bool, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return false, err
	}
	defer done()
	var exists bool
	err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workload_secret_authorizations)`).Scan(&exists)
	return exists, dbError(err)
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
