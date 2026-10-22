package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// BeginConnectionInstall consumes the only mutation permit before any remote build.
// After an uncertain commit, the durable installing stage permits read-only recovery.
func (s *Store) BeginConnectionInstall(ctx context.Context, id domain.OperationID) (bool, error) {
	if !id.Valid() {
		return false, ErrInvalid
	}
	var consumed bool
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var stage, account string
		err := tx.QueryRowContext(ctx, `SELECT o.stage,c.account_scope FROM managed_connection_operations o
JOIN managed_connections c ON c.connection_id=o.connection_id WHERE o.operation_id=? AND o.action='install_image'`, id).Scan(&stage, &account)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		if stage != "ready" {
			return nil // An earlier intent only permits exact observation.
		}
		if err = checkRunnerImageCapacity(ctx, tx, account, true); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE managed_connection_operations SET stage='installing',updated_at=? WHERE operation_id=? AND action='install_image' AND stage='ready'`, now.Format(time.RFC3339Nano), id)
		if err != nil {
			return dbError(err)
		}
		count, err := result.RowsAffected()
		consumed = count == 1
		return dbError(err)
	})
	return consumed, err
}
