package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

var _ retention.Repository = (*Store)(nil)

func (s *Store) BindRetentionStores(ctx context.Context, inputID, resultID string) error {
	if inputID == resultID || !domain.ObjectID(inputID).Valid() || !domain.ObjectID(resultID).Valid() {
		return retention.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		for kind, id := range map[string]string{"input": inputID, "result": resultID} {
			var existing string
			err := tx.QueryRowContext(ctx, `SELECT blob_identity FROM retention_roots WHERE kind=?`, kind).Scan(&existing)
			if err == nil {
				if existing != id {
					return retention.ErrChanged
				}
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return dbError(err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO retention_roots VALUES(?,?)`, kind, id); err != nil {
				return dbError(err)
			}
		}
		return nil
	})
}

// PendingRetention reads only committed irreversible tombstones. A keyset cursor
// makes permanent per-file failures visible without starving subsequent targets.
func (s *Store) PendingRetention(ctx context.Context, after int64, limit int) (retention.DeletionPage, error) {
	var page retention.DeletionPage
	if after < 0 || limit < 1 || limit > 100 {
		return page, retention.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return page, err
	}
	defer done()
	rows, err := s.db.QueryContext(ctx, `SELECT inventory_seq,kind,workspace_id,object_id,bytes,sha256 FROM retention_inventory WHERE inventory_seq>? AND expired_at IS NOT NULL AND deleted_at IS NULL ORDER BY inventory_seq LIMIT ?`, after, limit)
	if err != nil {
		return page, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var d retention.Deletion
		if err := rows.Scan(&page.NextAfter, &d.Kind, &d.Object.WorkspaceID, &d.Object.ID, &d.Object.Bytes, &d.Object.SHA256); err != nil {
			return retention.DeletionPage{}, dbError(err)
		}
		if !d.Valid() || page.NextAfter <= after {
			return retention.DeletionPage{}, ErrCorrupt
		}
		page.Items = append(page.Items, d)
	}
	if err := rows.Err(); err != nil {
		return retention.DeletionPage{}, dbError(err)
	}
	return page, nil
}

func (s *Store) CompleteRetention(ctx context.Context, d retention.Deletion, now time.Time) error {
	if !d.Valid() {
		return retention.ErrInvalid
	}
	return s.collectionTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		item, err := readRetentionItem(ctx, tx, d.Kind, d.Object.WorkspaceID, d.Object.ID)
		if err != nil {
			return err
		}
		if item.deletion != d || !item.expired.Valid {
			return retention.ErrChanged
		}
		at, err := time.Parse(time.RFC3339Nano, item.expired.String)
		if err != nil || now.Before(at) {
			return retention.ErrInvalid
		}
		if item.deleted.Valid {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE retention_inventory SET deleted_at=? WHERE inventory_seq=? AND deleted_at IS NULL`, now.UTC().Format(time.RFC3339Nano), item.sequence); err != nil {
			return dbError(err)
		}
		return retentionAudit(ctx, tx, d.Object.WorkspaceID, string(d.Kind), string(d.Object.ID), "delete", now)
	})
}
