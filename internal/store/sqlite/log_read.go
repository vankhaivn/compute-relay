package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (s *Store) ReadLogAttempt(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, attempt domain.AttemptID) (joblogs.Attempt, error) {
	if !w.Valid() || !job.Valid() || !attempt.Valid() {
		return joblogs.Attempt{}, joblogs.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return joblogs.Attempt{}, err
	}
	defer done()
	var result joblogs.Attempt
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Read); err != nil {
			return err
		}
		record, err := readJobRecord(ctx, tx, w, job)
		if errors.Is(err, admission.ErrNotFound) {
			return joblogs.ErrNotFound
		}
		if err != nil {
			return err
		}
		original, nonce, err := loadAttempt(ctx, tx, w, job, attempt)
		if errors.Is(err, admission.ErrNotFound) {
			return joblogs.ErrNotFound
		}
		if err != nil {
			return err
		}
		record.Attempt, record.AttemptNonce, record.Job.ActiveAttemptID = original, nonce, attempt
		result.Binding = provider.BindingSnapshot{Binding: record.Profile.Binding, AccountScope: record.Profile.AccountScope, CredentialRef: record.Profile.CredentialRef}
		var sequence int64
		err = tx.QueryRowContext(ctx, "SELECT queue_seq FROM scheduler_queue WHERE workspace_id=? AND job_id=? AND attempt_id=?", string(w), string(job), string(attempt)).Scan(&sequence)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		journal, err := loadJournal(ctx, tx, sequence)
		if err != nil {
			return err
		}
		if journal.Remote == nil {
			return nil
		}
		if journal.Plan == nil || journal.Prepared == nil {
			return ErrCorrupt
		}
		work := dispatch.Work{Job: record, Journal: journal}
		work.Handle.Claim.Sequence = sequence
		if err := tx.QueryRowContext(ctx, "SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&work.InstallationID); err != nil {
			return dbError(err)
		}
		if !work.InstallationID.Valid() || matchesFrozenPlan(work, *journal.Plan, journal.PreparationID) != nil || checkDispatchLedger(ctx, tx, work) != nil {
			return ErrCorrupt
		}
		remote := *journal.Remote
		if remote.Validate() != nil || remote.Identity != journal.Plan.Job.Identity {
			return ErrCorrupt
		}
		result.Remote = &remote
		return nil
	})
	if err != nil {
		return joblogs.Attempt{}, err
	}
	return result, nil
}
