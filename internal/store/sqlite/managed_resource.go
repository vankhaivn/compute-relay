package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// A nonempty kind with an empty ID is unresolved migration evidence, not another
// capacity pool. Verified associations are retained even after removing an alias.
func managedResource(ctx context.Context, tx *sql.Tx, account string) (connections.ResourceIdentity, error) {
	var r connections.ResourceIdentity
	err := tx.QueryRowContext(ctx, `SELECT resource_kind,resource_scope FROM managed_resource_accounts WHERE account_scope=?`, account).Scan(&r.Kind, &r.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return r, nil
	}
	if err != nil {
		return r, dbError(err)
	}
	if !domain.ObjectID(r.Kind).Valid() || r.ID != "" && r.Validate() != nil {
		return r, ErrCorrupt
	}
	return r, nil
}

func bindManagedResource(ctx context.Context, tx *sql.Tx, account string, resource *connections.ResourceIdentity) error {
	old, err := managedResource(ctx, tx, account)
	if err != nil {
		return err
	}
	if resource == nil {
		if old.Kind != "" {
			return connections.ErrConflict
		}
		return nil
	}
	if resource.Validate() != nil || old.Kind != "" && old.Kind != resource.Kind || old.ID != "" && old.ID != resource.ID {
		return connections.ErrConflict
	}
	var differentKind bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM managed_resource_accounts WHERE resource_scope=? AND resource_kind!=?)`, resource.ID, resource.Kind).Scan(&differentKind); err != nil {
		return dbError(err)
	}
	if differentKind {
		return connections.ErrConflict
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO managed_resource_accounts VALUES(?,?,?)
 ON CONFLICT(account_scope) DO UPDATE SET resource_scope=excluded.resource_scope`, account, resource.Kind, resource.ID)
	return dbError(err)
}

func managedResourceLimit(ctx context.Context, tx *sql.Tx, resource string) (int, error) {
	var limit int
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(min(CASE c.max_active_attempts WHEN 0 THEN 1 ELSE c.max_active_attempts END),1)
 FROM managed_connections c JOIN managed_resource_accounts r ON c.account_scope=r.account_scope
 WHERE r.resource_scope=? AND c.new_work!='removed'`, resource).Scan(&limit)
	if err != nil {
		return 0, dbError(err)
	}
	if limit < 1 || limit > 64 {
		return 0, ErrCorrupt
	}
	return limit, nil
}

func schedulerResources(ctx context.Context, tx *sql.Tx, rows []scheduler.Candidate, frozen map[int64]string, receipts map[int64]connections.ResourceIdentity, now time.Time) error {
	accounts := map[string]connections.ResourceIdentity{}
	limits := map[string]int{}
	for i := range rows {
		r := &rows[i]
		resource, found := accounts[r.AccountScope]
		if !found {
			var err error
			resource, err = managedResource(ctx, tx, r.AccountScope)
			if err != nil {
				return err
			}
			accounts[r.AccountScope] = resource
		}
		if frozen[r.Sequence] != "" && frozen[r.Sequence] != resource.ID {
			return ErrCorrupt
		}
		if resource.Kind == "" {
			continue
		}
		r.ResourceKind, r.ResourceScope = resource.Kind, resource.ID
		r.ResourceMaxActive = 1
		held := receipts[r.Sequence]
		hasReceipt := held.Kind != "" || held.ID != ""
		if hasReceipt && (held.Validate() != nil || held != resource) {
			return ErrCorrupt
		}
		if frozen[r.Sequence] == "" && scheduler.HoldsCapacity(*r, now) && !hasReceipt {
			// Native identity observed now cannot establish the historical
			// location of an already-active attempt accepted before migration.
			r.ResourceScope = ""
			continue
		}
		if resource.ID != "" {
			limit, found := limits[resource.ID]
			if !found {
				var err error
				limit, err = managedResourceLimit(ctx, tx, resource.ID)
				if err != nil {
					return err
				}
				limits[resource.ID] = limit
			}
			r.ResourceMaxActive = min(limit, r.Policy.MaxActive)
		}
	}
	return nil
}
