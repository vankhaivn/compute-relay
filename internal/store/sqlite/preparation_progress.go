package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

// RecordPreparationProgress checks the same lease ownership as a journal commit but never
// advances the journal, attempt revision or events. Samples only grow within one lease.
func (s *Store) RecordPreparationProgress(ctx context.Context, h dispatch.Handle, p domain.PreparationProgress, now time.Time) error {
	if !p.Valid() || p.Generation != h.Claim.Generation || !p.ObservedAt.Equal(now) {
		return dispatch.ErrInvalid
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return dispatch.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		work, err := loadDispatch(ctx, tx, h.Claim, now)
		if err != nil {
			return err
		}
		if err = checkHandle(work, h); err != nil {
			return err
		}
		if work.Journal.Phase != dispatch.Staging {
			return dispatch.ErrConflict
		}
		c := h.Claim
		var previous string
		err = tx.QueryRowContext(ctx, `SELECT progress FROM preparation_progress WHERE workspace_id=? AND job_id=? AND attempt_id=? AND generation=?`,
			string(c.WorkspaceID), string(c.JobID), string(c.AttemptID), c.Generation).Scan(&previous)
		if err == nil {
			var old domain.PreparationProgress
			if json.Unmarshal([]byte(previous), &old) != nil || !old.Valid() {
				return ErrCorrupt
			}
			if p.BytesTotal != old.BytesTotal || p.BytesCompleted < old.BytesCompleted {
				return dispatch.ErrInvalid
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO preparation_progress VALUES(?,?,?,?,?)
 ON CONFLICT(workspace_id,job_id,attempt_id) DO UPDATE SET generation=excluded.generation,progress=excluded.progress`,
			string(c.WorkspaceID), string(c.JobID), string(c.AttemptID), c.Generation, string(raw))
		return dbError(err)
	})
}

// readPreparationStatus reports the last sample only while the active attempt is preparing.
func readPreparationStatus(ctx context.Context, tx *sql.Tx, r admission.Record) (*domain.PreparationStatus, error) {
	if r.Attempt.State.Orchestration != domain.OrchestrationPreparing {
		return nil, nil
	}
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT progress FROM preparation_progress WHERE workspace_id=? AND job_id=? AND attempt_id=?`,
		string(r.Job.WorkspaceID), string(r.Job.ID), string(r.Attempt.ID)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	var p domain.PreparationProgress
	if json.Unmarshal([]byte(raw), &p) != nil || !p.Valid() {
		return nil, ErrCorrupt
	}
	return &domain.PreparationStatus{Progress: &p}, nil
}
