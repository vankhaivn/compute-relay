package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// LoadProviderAttempt is a trusted runtime read used only to reconstruct the
// original immutable provider components after the durable orchestration journal
// already exists. It grants no application authority and creates no mutation permit.
func (s *Store) LoadProviderAttempt(ctx context.Context, id provider.Identity) (provider.Plan, provider.Prepared, error) {
	var noPlan provider.Plan
	var noPrepared provider.Prepared
	if id.Validate() != nil {
		return noPlan, noPrepared, ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return noPlan, noPrepared, err
	}
	defer done()

	var plan provider.Plan
	var prepared provider.Prepared
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		record, err := readJobRecord(ctx, tx, id.WorkspaceID, id.JobID)
		if err != nil {
			return err
		}
		if record.Attempt.ID != id.AttemptID || record.Profile.Binding.ProviderInstanceID != id.InstanceID {
			return ErrCorrupt
		}
		var sequence int64
		var barrier bool
		err = tx.QueryRowContext(ctx,
			"SELECT queue_seq,dispatch_barrier FROM scheduler_queue WHERE workspace_id=? AND job_id=? AND attempt_id=?",
			string(id.WorkspaceID), string(id.JobID), string(id.AttemptID),
		).Scan(&sequence, &barrier)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrCorrupt
		}
		if err != nil {
			return dbError(err)
		}
		journal, err := loadJournal(ctx, tx, sequence)
		if err != nil {
			return err
		}
		if !barrier || journal.Plan == nil || journal.Prepared == nil || journal.Plan.Job.Identity != id {
			return ErrCorrupt
		}
		var installation dispatch.Work
		installation.Job = record
		installation.Journal = journal
		installation.Handle.Claim.Sequence = sequence
		if err := tx.QueryRowContext(ctx, "SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&installation.InstallationID); err != nil {
			return dbError(err)
		}
		if !installation.InstallationID.Valid() ||
			matchesFrozenPlan(installation, *journal.Plan, journal.PreparationID) != nil ||
			checkDispatchLedger(ctx, tx, installation) != nil {
			return ErrCorrupt
		}
		plan = journal.Plan.Clone()
		prepared = *journal.Prepared
		return nil
	})
	if err != nil {
		return noPlan, noPrepared, err
	}
	return plan, prepared, nil
}
