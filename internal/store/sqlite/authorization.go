package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

var _ executionauth.Repository = (*Store)(nil)

// authorizationInputs binds the immutable request and each pinned local object.
// Unresolved HTTPS sources cannot be authorized as if their bytes were frozen.
func authorizationInputs(record admission.Record) (string, error) {
	if record.PendingHTTPS != 0 {
		return "", executionauth.ErrInputs
	}
	raw, err := json.Marshal(struct {
		Request domain.SHA256Digest
		Objects []admission.FrozenObject
	}{record.Request.Hash(), record.Objects})
	if err != nil {
		return "", executionauth.ErrInputs
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func (s *Store) AuthorizeExecution(ctx context.Context, c executionauth.Command) (executionauth.Receipt, error) {
	if c.Validate() != nil {
		return executionauth.Receipt{}, executionauth.ErrRequest
	}
	hash, _ := c.Request.Digest(c.JobID)
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return executionauth.Receipt{}, err
	}
	defer done()
	var receipt executionauth.Receipt
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, c.WorkspaceID, c.TokenID, auth.Execute); err != nil {
			return err
		}
		var priorHash, raw string
		err := tx.QueryRowContext(ctx, "SELECT request_sha256,original_receipt FROM execution_authorizations WHERE workspace_id=? AND key_sha256=?", string(c.WorkspaceID), c.KeyDigest).Scan(&priorHash, &raw)
		if err == nil {
			if priorHash != hash {
				return executionauth.ErrConflict
			}
			if len(raw) > 4096 || json.Unmarshal([]byte(raw), &receipt) != nil || receipt.Validate() != nil || receipt.Status != executionauth.Granted || receipt.Replay || receipt.WorkspaceID != c.WorkspaceID || receipt.JobID != c.JobID || receipt.AttemptID != c.Request.AttemptID {
				return ErrCorrupt
			}
			receipt.Replay = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		record, err := readJobRecord(ctx, tx, c.WorkspaceID, c.JobID)
		if errors.Is(err, admission.ErrNotFound) {
			return executionauth.ErrNotFound
		}
		if err != nil {
			return err
		}
		if record.Attempt.ID != c.Request.AttemptID {
			return executionauth.ErrConflict
		}
		if !executionauth.Managed(record.Profile) || !scheduler.LocalOnly(record.Attempt.State) || c.Now.Before(record.Attempt.UpdatedAt) {
			return executionauth.ErrState
		}
		if err = managedProfileOwned(ctx, tx, c.WorkspaceID, record.Profile); err != nil {
			return err
		}
		if record.Request.Spec().Timeouts.RemoteWallSeconds != c.Request.MaxRemoteWallSeconds {
			return executionauth.ErrRequest
		}
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM execution_authorizations WHERE workspace_id=? AND job_id=? AND attempt_id=?", string(c.WorkspaceID), string(c.JobID), string(c.Request.AttemptID)).Scan(&count); err != nil {
			return dbError(err)
		}
		if count != 0 {
			return executionauth.ErrConflict
		}
		var barrier bool
		var phase string
		var version int64
		if err = tx.QueryRowContext(ctx, `SELECT q.dispatch_barrier,COALESCE(d.phase,'local'),COALESCE(d.version,0)
 FROM scheduler_queue q LEFT JOIN dispatch_journals d ON d.queue_seq=q.queue_seq
 WHERE q.workspace_id=? AND q.job_id=? AND q.attempt_id=?`, string(c.WorkspaceID), string(c.JobID), string(c.Request.AttemptID)).Scan(&barrier, &phase, &version); err != nil {
			return dbError(err)
		}
		if barrier || phase != "local" || version != 0 {
			return executionauth.ErrState
		}
		inputs, err := authorizationInputs(record)
		if err != nil {
			return err
		}
		if err = authorizationQuota(ctx, tx, record, c.Now); err != nil {
			return err
		}
		id, err := randomID("grant_", 16)
		if err != nil {
			return err
		}
		receipt = executionauth.Receipt{AuthorizationID: domain.OperationID(id), WorkspaceID: c.WorkspaceID, JobID: c.JobID, AttemptID: c.Request.AttemptID, Status: executionauth.Granted, MaxRemoteWallSeconds: c.Request.MaxRemoteWallSeconds, CreatedAt: c.Now.UTC()}
		encoded, _ := json.Marshal(receipt)
		binding, _ := json.Marshal(record.Profile)
		_, err = tx.ExecContext(ctx, `INSERT INTO execution_authorizations(workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_sha256,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,original_receipt,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'granted')`, string(c.WorkspaceID), id, string(c.JobID), string(c.Request.AttemptID), c.KeyDigest, hash, string(binding), record.Profile.AccountScope, inputs, record.AttemptNonce, c.Request.MaxRemoteWallSeconds, c.TokenID, c.Now.UTC().Format(time.RFC3339Nano), string(encoded))
		return dbError(err)
	})
	if err != nil {
		return executionauth.Receipt{}, err
	}
	return receipt, nil
}

// A grant is finite consent, not a claim of capacity. Managed GPU requests also
// need conservative current quota after outstanding same-account reservations.
func authorizationQuota(ctx context.Context, tx *sql.Tx, record admission.Record, now time.Time) error {
	if record.Request.Spec().Resources.Accelerator != "gpu" {
		return nil
	}
	capacity, err := managedCapacity(ctx, tx, record.Profile.AccountScope, now)
	if err != nil {
		return err
	}
	if capacity.Status != "known" || capacity.RemainingSeconds == nil || *capacity.RemainingSeconds < record.Request.Spec().Timeouts.RemoteWallSeconds {
		return executionauth.ErrQuota
	}
	return nil
}

func readAuthorization(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, id domain.OperationID) (executionauth.Receipt, error) {
	var r executionauth.Receipt
	var raw, status string
	var consumed sql.NullString
	err := tx.QueryRowContext(ctx, "SELECT original_receipt,status,consumed_at FROM execution_authorizations WHERE workspace_id=? AND job_id=? AND authorization_id=?", string(w), string(job), string(id)).Scan(&raw, &status, &consumed)
	if errors.Is(err, sql.ErrNoRows) {
		return r, executionauth.ErrNotFound
	}
	if err != nil {
		return r, dbError(err)
	}
	if len(raw) > 4096 || json.Unmarshal([]byte(raw), &r) != nil || r.Validate() != nil || r.Status != executionauth.Granted || r.Replay || r.WorkspaceID != w || r.JobID != job || r.AuthorizationID != id {
		return executionauth.Receipt{}, ErrCorrupt
	}
	r.Status = executionauth.Status(status)
	if consumed.Valid {
		at, err := time.Parse(time.RFC3339Nano, consumed.String)
		if err != nil {
			return executionauth.Receipt{}, ErrCorrupt
		}
		r.ConsumedAt = &at
	}
	if r.Validate() != nil {
		return executionauth.Receipt{}, ErrCorrupt
	}
	return r, nil
}
func (s *Store) ReadExecutionAuthorization(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, id domain.OperationID) (executionauth.Receipt, error) {
	if !w.Valid() || !job.Valid() || !id.Valid() {
		return executionauth.Receipt{}, executionauth.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return executionauth.Receipt{}, err
	}
	defer done()
	var r executionauth.Receipt
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Execute); err != nil {
			return err
		}
		var err error
		r, err = readAuthorization(ctx, tx, w, job, id)
		return err
	})
	if err != nil {
		return executionauth.Receipt{}, err
	}
	return r, nil
}
