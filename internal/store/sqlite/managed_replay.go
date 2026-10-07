package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

// ReplayConnection authenticates the current actor before exposing a saved receipt.
// It does not apply present-day adapter policy to an already accepted request.
func (s *Store) ReplayConnection(ctx context.Context, in connections.Accept) (connections.Record, bool, error) {
	var result connections.Record
	if !in.Workspace.Valid() || !validID(in.TokenID) || !domain.SHA256Digest(in.KeyHash).Valid() || !domain.SHA256Digest(in.Fingerprint).Valid() {
		return result, false, connections.ErrRequest
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return result, false, err
	}
	defer done()
	found := false
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, in.Workspace, in.TokenID, auth.Manage); err != nil {
			return err
		}
		var id, fingerprint, receipt string
		err := tx.QueryRowContext(ctx, `SELECT operation_id,fingerprint,receipt FROM managed_connection_operations WHERE workspace_id=? AND key_sha256=?`, in.Workspace, in.KeyHash).Scan(&id, &fingerprint, &receipt)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		if fingerprint != in.Fingerprint {
			return connections.ErrConflict
		}
		result, err = loadManagedOperation(ctx, tx, domain.OperationID(id))
		if err != nil {
			return err
		}
		if json.Unmarshal([]byte(receipt), &result.Operation) != nil {
			return ErrCorrupt
		}
		result.Operation.Replay = true
		found = true
		return nil
	})
	return result, found, err
}
