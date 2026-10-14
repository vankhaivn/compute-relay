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
	if work.Job.Request.Spec().SeparateStart() {
		if work.Gate == nil {
			return "", dispatch.ErrPolicy
		}
		return work.Gate.Preparation.Status, nil
	}
	var binding, account, inputs, nonce, status, created string
	var consumed sql.NullString
	var wall int64
	err := tx.QueryRowContext(ctx, "SELECT binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,status,created_at,consumed_at FROM attempt_grants WHERE kind='execution' AND workspace_id=? AND job_id=? AND attempt_id=?", string(work.Job.Job.WorkspaceID), string(work.Job.Job.ID), string(work.Job.Attempt.ID)).Scan(&binding, &account, &inputs, &nonce, &wall, &status, &created, &consumed)
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
		return "", ErrCorrupt
	}
	if consumed.Valid {
		used, err := time.Parse(time.RFC3339Nano, consumed.String)
		if err != nil || used.Before(at) {
			return "", ErrCorrupt
		}
		if now.Before(used) {
			return "", ErrCorrupt
		}
	}
	return executionauth.Status(status), nil
}

// ConsumeAuthorization verifies the current fenced claim and frozen job in one
// transaction. Its durable result survives a crash before the preparation intent;
// a later owner may continue only this same attempt's one-shot journal.
func (s *Store) ConsumeAuthorization(ctx context.Context, h dispatch.Handle) (dispatch.Work, error) {
	var result dispatch.Work
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
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
		if status == executionauth.Granted && work.Gate != nil {
			if err = consumeGatePreparation(ctx, tx, &work, now); err != nil {
				return err
			}
		} else if status == executionauth.Granted {
			if err = consumeGrant(ctx, tx, work.Job.Job.WorkspaceID, work.Job.Job.ID, work.Job.Attempt.ID, "execution", now, dispatch.ErrConflict); err != nil {
				return err
			}
		}
		result = work
		return nil
	})
	if err != nil {
		return dispatch.Work{}, err
	}
	return result, nil
}
