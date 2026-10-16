package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

func consumeGatePreparation(ctx context.Context, tx *sql.Tx, work *dispatch.Work, now time.Time) error {
	if err := consumeGrant(ctx, tx, work.Job.Job.WorkspaceID, work.Job.Job.ID, work.Job.Attempt.ID, "prepare", now, dispatch.ErrConflict); err != nil {
		return err
	}
	work.Gate.Preparation.Status, work.Gate.Preparation.ConsumedAt = executionauth.Consumed, &now
	return nil
}

// The authority claim and dispatch intent share one transaction. A crash cannot
// leave a reusable start permit behind a possibly sent Docker command.
func commitGateStep(ctx context.Context, tx *sql.Tx, work *dispatch.Work, action *dispatch.Action, now time.Time) error {
	if !work.Job.Request.Spec().SeparateStart() {
		if action.StartAuthorization != nil {
			return dispatch.ErrInvalid
		}
		return nil
	}
	g := work.Gate
	if g == nil {
		return dispatch.ErrPolicy
	}
	switch action.Kind {
	case dispatch.PreparationSeen:
		if action.Prepared == nil || !action.Prepared.Ready || !action.Prepared.Private {
			return nil
		}
		revision := action.Prepared.ReadinessRevision()
		if !revision.Valid() {
			return dispatch.ErrInvalid
		}
		if g.ReadyRevision != "" {
			if g.ReadyRevision != revision {
				return dispatch.ErrConflict
			}
			return nil
		}
		result, err := tx.ExecContext(ctx, `UPDATE attempt_grants SET ready_revision=?,ready_at=? WHERE kind='prepare' AND workspace_id=? AND job_id=? AND attempt_id=? AND ready_revision IS NULL AND consumed_at IS NOT NULL`, string(revision), now.UTC().Format(time.RFC3339Nano), string(work.Job.Job.WorkspaceID), string(work.Job.Job.ID), string(work.Job.Attempt.ID))
		if err != nil {
			return dbError(err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		if count != 1 {
			return dispatch.ErrConflict
		}
		g.ReadyRevision, g.ReadyAt = revision, &now
	case dispatch.BeginSubmission:
		if g.Start == nil || g.StartReceipt == nil || g.StartReceipt.Status != executionauth.Granted || work.Journal.Prepared == nil || g.Start.Validate(*work.Journal.Prepared) != nil {
			return dispatch.ErrPolicy
		}
		secretID := domain.OperationID("")
		if g.Start.Secrets != nil {
			secretID = g.Start.Secrets.AuthorizationID
		}
		secret, err := secretForStart(ctx, tx, work.Job, secretID, g.ReadyRevision, now)
		if err != nil {
			return err
		}
		if secret != nil {
			if !action.SecretsAvailable {
				return workloadsecret.ErrUnavailable
			}
			if err := consumeGrant(ctx, tx, work.Job.Job.WorkspaceID, work.Job.Job.ID, work.Job.Attempt.ID, "secret", now, workloadsecret.ErrUnavailable); err != nil {
				return err
			}
		}
		if err := consumeGrant(ctx, tx, work.Job.Job.WorkspaceID, work.Job.Job.ID, work.Job.Attempt.ID, "start", now, dispatch.ErrConflict); err != nil {
			return err
		}
		copy := g.Start.Clone()
		action.StartAuthorization = &copy
		g.StartReceipt.Status, g.StartReceipt.ConsumedAt = executionauth.Consumed, &now
	case dispatch.ExpireDispatch:
		if work.Journal.Plan == nil {
			return releaseGateStorage(ctx, tx, string(work.Job.Job.WorkspaceID), string(work.Job.Job.ID), string(work.Job.Attempt.ID))
		}
	}
	return nil
}
func releaseGateStorage(ctx context.Context, tx *sql.Tx, workspace, job, attempt string) error {
	_, err := tx.ExecContext(ctx, `UPDATE attempt_grants SET storage_released=1 WHERE kind='prepare' AND workspace_id=? AND job_id=? AND attempt_id=? AND storage_released=0`, workspace, job, attempt)
	return dbError(err)
}
