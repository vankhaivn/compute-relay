package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

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
	found := false
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, in.Workspace, in.TokenID, auth.Manage); err != nil {
			return err
		}
		prior, exists, err := replayOrCreate(ctx, tx, receiptKey{in.Workspace, "connection", "", in.KeyHash, in.Fingerprint, "", connections.ErrConflict}, nil)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		result, err = loadManagedOperation(ctx, tx, domain.OperationID(prior.subject))
		if err != nil {
			return err
		}
		if json.Unmarshal([]byte(prior.raw), &result.Operation) != nil {
			return ErrCorrupt
		}
		result.Operation.Replay = true
		found = true
		return nil
	})
	return result, found, err
}
