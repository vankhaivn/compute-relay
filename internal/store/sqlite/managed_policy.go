package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// Aliases of one verified account share the most restrictive retained connection
// limit. Adding an alias never multiplies capacity, including across workspaces.
func refreshManagedAccountPolicy(ctx context.Context, tx *sql.Tx, scope string) error {
	if scope == "" {
		return nil
	}
	var capacity int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(min(CASE max_active_attempts WHEN 0 THEN 1 ELSE max_active_attempts END),1) FROM managed_connections WHERE account_scope=? AND new_work!='removed'`, scope).Scan(&capacity); err != nil {
		return dbError(err)
	}
	policy, _ := json.Marshal(scheduler.AccountPolicy{MaxActive: capacity, StrictQuota: true})
	_, err := tx.ExecContext(ctx, `INSERT INTO scheduler_accounts VALUES(?,?) ON CONFLICT(account_scope) DO UPDATE SET policy=json_set(scheduler_accounts.policy,'$.max_active',?)`, scope, string(policy), capacity)
	return dbError(err)
}
