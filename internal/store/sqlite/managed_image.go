package sqlite

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// BeginConnectionInstall consumes the only mutation permit before any remote build.
// After an uncertain commit, the durable installing stage permits read-only recovery.
func (s *Store) BeginConnectionInstall(ctx context.Context, id domain.OperationID, now time.Time) (bool, error) {
	if !id.Valid() || now.IsZero() {
		return false, ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return false, err
	}
	defer done()
	result, err := s.db.ExecContext(ctx, `UPDATE managed_connection_operations SET stage='installing',updated_at=? WHERE operation_id=? AND action='install_image' AND stage='ready'`, now.UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return false, dbError(err)
	}
	count, err := result.RowsAffected()
	return count == 1, dbError(err)
}
