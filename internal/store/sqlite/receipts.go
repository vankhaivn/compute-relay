package sqlite

import (
	"context"
	"crypto/hmac"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// receiptKey preserves each public operation's original idempotency namespace.
// Scope is the control/grant kind, or the authorization ID for secret revocation.
type receiptKey struct {
	workspace                            domain.WorkspaceID
	namespace, scope, key, hash, version string
	conflict                             error
}

type savedReceipt struct {
	subject string
	job     domain.JobID
	attempt domain.AttemptID
	raw     string
}

// replayOrCreate never reconstructs a saved receipt from current domain state.
// A nil creation is a lookup. Callers check state and create domain rows only
// after a miss, then save the concrete receipt in the same transaction.
func replayOrCreate(ctx context.Context, tx *sql.Tx, key receiptKey, creation *savedReceipt) (savedReceipt, bool, error) {
	var saved savedReceipt
	var hash, version string
	var job, attempt sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt FROM receipts WHERE workspace_id=? AND namespace=? AND scope=? AND key_sha256=?`, key.workspace, key.namespace, key.scope, key.key).Scan(&hash, &version, &saved.subject, &job, &attempt, &saved.raw)
	if err == nil {
		if version != key.version {
			return savedReceipt{}, false, ErrSchema
		}
		if !hmac.Equal([]byte(hash), []byte(key.hash)) {
			return savedReceipt{}, false, key.conflict
		}
		saved.job, saved.attempt = domain.JobID(job.String), domain.AttemptID(attempt.String)
		return saved, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return savedReceipt{}, false, dbError(err)
	}
	if creation == nil {
		return savedReceipt{}, false, nil
	}
	saved = *creation
	if len(saved.raw) == 0 || len(saved.raw) > 4096 {
		return savedReceipt{}, false, ErrInvalid
	}
	var jobValue, attemptValue any
	if saved.job != "" {
		jobValue = string(saved.job)
	}
	if saved.attempt != "" {
		attemptValue = string(saved.attempt)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt) VALUES(?,?,?,?,?,?,?,?,?,?)`, key.workspace, key.namespace, key.scope, key.key, key.hash, key.version, saved.subject, jobValue, attemptValue, saved.raw)
	return saved, false, dbError(err)
}

// consumeGrant is the only durable consume boundary. The caller has already
// checked frozen authority and keeps this update in its fenced intent transaction.
func consumeGrant(ctx context.Context, tx *sql.Tx, workspace domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, kind string, now time.Time, unavailable error) error {
	result, err := tx.ExecContext(ctx, `UPDATE attempt_grants SET status='consumed',consumed_at=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind=? AND status='granted' AND consumed_at IS NULL AND revoked_at IS NULL AND (expires_at_ms IS NULL OR expires_at_ms>?)`, now.UTC().Format(time.RFC3339Nano), workspace, job, attempt, kind, now.UnixMilli())
	if err != nil {
		return dbError(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if n != 1 {
		return unavailable
	}
	return nil
}
