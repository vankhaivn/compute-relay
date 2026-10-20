package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// RecordCleanupPreparation adds identity evidence after a failed preparation. It
// never changes the terminal attempt, grant, phase or compute eligibility.
func (s *Store) RecordCleanupPreparation(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, attempt domain.AttemptID, expected cleanup.PreviewPlan, prepared provider.Prepared) error {
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		target, err := controlTarget(ctx, tx, w, job, attempt)
		if err != nil {
			return err
		}
		work, err := controlWork(ctx, tx, target)
		if err != nil {
			return err
		}
		j := work.Journal
		if expected.WorkspaceID != w || expected.JobID != job || expected.AttemptID != attempt || target.Attempt.Revision != expected.Revision || expected.UnconfirmedPlan == nil || j.Plan == nil || j.Prepared != nil || j.SubmitStarted || !target.Attempt.State.Orchestration.Terminal() || j.Plan.Digest() != expected.UnconfirmedPlan.Digest() || j.PreparationID != expected.PreparationID || prepared.Validate(*j.Plan, j.PreparationID) != nil || !prepared.Private {
			return operations.ErrChanged
		}
		// The sole allowed refusal is the missing preparation itself. Active leases,
		// controls and unsettled execution still prohibit changing cleanup evidence.
		if _, err := eligibleCleanupAttempt(ctx, tx, target, false, now, ""); !errors.Is(err, operations.ErrState) {
			return operations.ErrChanged
		}
		resources, err := cleanupResources(ctx, tx, target)
		if err != nil {
			return err
		}
		current := expected
		current.Resources = resources
		if current.Fingerprint() != expected.Fingerprint() {
			return operations.ErrChanged
		}
		j.Prepared = &prepared
		j.Version++
		if !j.Valid() {
			return ErrCorrupt
		}
		raw, err := json.Marshal(j)
		if err != nil || len(raw) > dispatch.MaxJournalBytes {
			return ErrCorrupt
		}
		if err := writeDispatchLedger(ctx, tx, work, j, dispatch.PreparationSeen, now); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, "UPDATE dispatch_journals SET version=?,journal=? WHERE queue_seq=? AND version=?", j.Version, string(raw), work.Handle.Claim.Sequence, work.Journal.Version)
		if err != nil {
			return dbError(err)
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			return operations.ErrChanged
		}
		return nil
	})
}
