package sqlite

import (
	"context"
	"database/sql"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

// Historical explicit attempts remain readable after their composition retires.
// Only replay of their original receipts is allowed to cross this boundary.
func actionableAttempt(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID) error {
	var retired bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM retired_explicit_attempts WHERE workspace_id=? AND job_id=? AND attempt_id=?)`, w, job, attempt).Scan(&retired); err != nil {
		return dbError(err)
	}
	if retired {
		return operations.ErrState
	}
	return nil
}
