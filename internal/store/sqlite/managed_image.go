package sqlite

import (
	"context"
	"database/sql"
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
