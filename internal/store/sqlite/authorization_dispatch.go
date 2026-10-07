package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func dispatchAuthorization(ctx context.Context, tx *sql.Tx, work dispatch.Work, now time.Time) (executionauth.Status, error) {
	if !executionauth.Managed(work.Job.Profile) {
		return "", nil
	}
	var binding, account, inputs, nonce, status, created string
	var consumed sql.NullString
	var wall int64
	err := tx.QueryRowContext(ctx, "SELECT binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,status,created_at,consumed_at FROM execution_authorizations WHERE workspace_id=? AND job_id=? AND attempt_id=?", string(work.Job.Job.WorkspaceID), string(work.Job.Job.ID), string(work.Job.Attempt.ID)).Scan(&binding, &account, &inputs, &nonce, &wall, &status, &created, &consumed)
	if errors.Is(err, sql.ErrNoRows) {
		return "", dispatch.ErrPolicy
	}
	if err != nil {
		return "", dbError(err)
	}
	expected, _ := json.Marshal(work.Job.Profile)
	digest, err := authorizationInputs(work.Job)
	if err != nil || binding != string(expected) || account != work.Job.Profile.AccountScope || inputs != digest || nonce != work.Job.AttemptNonce || wall != work.Job.Request.Spec().Timeouts.RemoteWallSeconds {
		return "", ErrCorrupt
	}
	if status != string(executionauth.Granted) && status != string(executionauth.Consumed) {
		return "", dispatch.ErrPolicy
	}
	at, err := time.Parse(time.RFC3339Nano, created)
	if err != nil || !scheduler.ValidTime(at) || (status == string(executionauth.Consumed)) != consumed.Valid {
		return "", ErrCorrupt
	}
	if now.Before(at) {
		return "", scheduler.ErrClock
	}
	if consumed.Valid {
		used, err := time.Parse(time.RFC3339Nano, consumed.String)
		if err != nil || used.Before(at) {
			return "", ErrCorrupt
		}
		if now.Before(used) {
			return "", scheduler.ErrClock
		}
	}
	return executionauth.Status(status), nil
}

// ConsumeAuthorization verifies the current fenced claim and frozen job in one
// transaction. Its durable result survives a crash before the preparation intent;
// a later owner may continue only this same attempt's one-shot journal.
func (s *Store) ConsumeAuthorization(ctx context.Context, h dispatch.Handle, now time.Time) (dispatch.Work, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return dispatch.Work{}, err
	}
	defer done()
	var result dispatch.Work
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		work, err := loadDispatch(ctx, tx, h.Claim, now)
		if err != nil {
			return err
		}
		if err = checkHandle(work, h); err != nil {
			return err
		}
		if work.Journal.Phase != dispatch.Local || work.Journal.Plan != nil {
			return dispatch.ErrConflict
		}
		if err = newMutationPolicy(ctx, tx, work, now); err != nil {
			return err
		}
		status, err := dispatchAuthorization(ctx, tx, work, now)
		if err != nil {
			return err
		}
		if status == executionauth.Granted {
			updated, err := tx.ExecContext(ctx, "UPDATE execution_authorizations SET status='consumed',consumed_at=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND status='granted'", now.UTC().Format(time.RFC3339Nano), string(work.Job.Job.WorkspaceID), string(work.Job.Job.ID), string(work.Job.Attempt.ID))
			if err != nil {
				return dbError(err)
			}
			count, err := updated.RowsAffected()
			if err != nil {
				return dbError(err)
			}
			if count != 1 {
				return dispatch.ErrConflict
			}
		}
		if err = advanceSchedulerClock(ctx, tx, now); err != nil {
			return err
		}
		result = work
		return nil
	})
	if err != nil {
		return dispatch.Work{}, err
	}
	return result, nil
}
