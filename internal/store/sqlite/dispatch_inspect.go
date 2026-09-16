package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

// InspectDispatch is an internal operator read, not a public status serializer.
// The journal contains private recovery data. It requires current operate scope,
// supports only the active attempt and returns no claim/fence or mutation permit.
// Inspection does not advance clocks, claim work, reset gates or call a provider.
func (s *Store) InspectDispatch(ctx context.Context, workspace domain.WorkspaceID, tokenID string, job domain.JobID, attempt domain.AttemptID) (dispatch.Journal, error) {
	if !workspace.Valid() || !job.Valid() || !attempt.Valid() {
		return dispatch.Journal{}, admission.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return dispatch.Journal{}, err
	}
	defer done()
	var result dispatch.Journal
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, workspace, tokenID, auth.Operate); err != nil {
			return err
		}
		record, err := readJobRecord(ctx, tx, workspace, job)
		if err != nil {
			return err
		}
		if record.Attempt.ID != attempt {
			return admission.ErrNotFound
		}
		work := dispatch.Work{Job: record}
		var barrier bool
		err = tx.QueryRowContext(ctx, "SELECT queue_seq,dispatch_barrier FROM scheduler_queue WHERE workspace_id=? AND job_id=? AND attempt_id=?", string(workspace), string(job), string(attempt)).Scan(&work.Handle.Claim.Sequence, &barrier)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrCorrupt // Admission always creates this queue row.
		}
		if err != nil {
			return dbError(err)
		}
		if err := tx.QueryRowContext(ctx, "SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&work.InstallationID); err != nil {
			return dbError(err)
		}
		if !work.InstallationID.Valid() {
			return ErrCorrupt
		}
		work.Journal, err = loadJournal(ctx, tx, work.Handle.Claim.Sequence)
		if err != nil {
			return err
		}
		j := work.Journal
		if j.Plan != nil {
			if !barrier || matchesFrozenPlan(work, *j.Plan, j.PreparationID) != nil || checkDispatchLedger(ctx, tx, work) != nil {
				return ErrCorrupt
			}
		} else if barrier && j.Phase != dispatch.Failed && j.Phase != dispatch.Prevented {
			return ErrCorrupt // A missing journal cannot rearm a committed intent.
		}
		result = j
		return nil
	})
	if err != nil {
		return dispatch.Journal{}, err
	}
	return result, nil
}
