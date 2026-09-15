package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// Called only after workspace/token authorization in ReadJob. This is a cached local
// read, not reconciliation, provider polling, or permission to retry compute.
func readDispatchProblem(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, j domain.JobID, a domain.AttemptID) (*domain.Problem, error) {
	var sequence int64
	err := tx.QueryRowContext(ctx, "SELECT queue_seq FROM scheduler_queue WHERE workspace_id=? AND job_id=? AND attempt_id=?", string(w), string(j), string(a)).Scan(&sequence)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCorrupt
	}
	if err != nil {
		return nil, dbError(err)
	}
	journal, err := loadJournal(ctx, tx, sequence)
	if err != nil {
		return nil, err
	}
	return journal.Problem, nil
}
