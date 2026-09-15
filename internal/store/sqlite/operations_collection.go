package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// PendingCollections is a bounded internal queue view, NOT a lease or a provider
// mutation permit. The M3-06 collector must acquire its own fenced byte-publication
// ownership. A read alone never changes result or operation state.
func (s *Store) PendingCollections(ctx context.Context, limit int) ([]operations.Record, error) {
	if limit < 1 || limit > 100 {
		return nil, ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return nil, err
	}
	defer done()
	result := []operations.Record{}
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT workspace_id,operation_id FROM operations WHERE kind='collect' AND status='accepted' ORDER BY operation_seq LIMIT ?`, limit)
		if err != nil {
			return dbError(err)
		}
		type key struct {
			w  domain.WorkspaceID
			id domain.OperationID
		}
		keys := []key{}
		for rows.Next() {
			var k key
			if err = rows.Scan(&k.w, &k.id); err != nil {
				rows.Close()
				return dbError(err)
			}
			keys = append(keys, k)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return dbError(err)
		}
		for _, k := range keys {
			r, err := loadControl(ctx, tx, k.w, k.id)
			if err != nil {
				return err
			}
			result = append(result, r.Record)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CompleteCollectionOperation acknowledges CURRENT persisted verification evidence,
// not caller assertions or provider downloads. It cannot change attempt/result state.
// Until M3-06 persists available/incomplete/invalid results this returns ErrState.
func (s *Store) CompleteCollectionOperation(ctx context.Context, w domain.WorkspaceID, id domain.OperationID, revision uint64, now time.Time) (operations.Record, error) {
	if !w.Valid() || !id.Valid() || revision == 0 || !scheduler.ValidTime(now) {
		return operations.Record{}, ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return operations.Record{}, err
	}
	defer done()
	var result operations.Record
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, _, err := schedulerControl(ctx, tx, now); err != nil {
			return err
		}
		r, err := loadControl(ctx, tx, w, id)
		if err != nil {
			return err
		}
		if r.Operation.Kind != domain.OperationCollect || r.Operation.Revision != revision || r.Operation.Status.Terminal() {
			return operations.ErrChanged
		}
		a, _, err := loadAttempt(ctx, tx, w, r.Operation.JobID, r.Operation.AttemptID)
		if err != nil {
			return err
		}
		if now.Before(a.UpdatedAt) || now.Before(r.Operation.UpdatedAt) {
			return scheduler.ErrClock
		}
		status, effect := domain.OperationSucceeded, operations.ResultsAvailable
		var p *domain.Problem
		switch a.State.Result {
		case domain.ResultAvailable:
		case domain.ResultIncomplete, domain.ResultInvalid:
			status, effect = domain.OperationFailed, operations.Failed
			p = controlProblem(domain.CodeArtifactCollectionFailed, true)
		default:
			return operations.ErrState
		}
		next, err := finishControl(ctx, tx, r, status, effect, p, now)
		if err != nil {
			return err
		}
		result = next.Record
		return advanceSchedulerClock(ctx, tx, now)
	})
	if err != nil {
		return operations.Record{}, err
	}
	return result, nil
}
