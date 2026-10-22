package sqlite

import (
	"context"
	"database/sql"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

// Each accepted install can leave a distinct tagged image, even after a failed
// reply. Retained operation receipts are the reservation ledger. This policy is
// local to this installation and is not a Docker quota or deletion authority.
const runnerImageInstallLimit = provider.RunnerImageCountLimit

func checkRunnerImageCapacity(ctx context.Context, tx *sql.Tx, account string, existing bool) error {
	resource, err := managedResource(ctx, tx, account)
	if err != nil {
		return err
	}
	if resource.Kind != "machine" || resource.Validate() != nil {
		return provider.WithReason(provider.ErrResourceCountLimit, "runner_image_scope_unknown")
	}
	var count int
	// Associations retain their original machine identity after aliases are
	// removed. Unresolved legacy associations count conservatively on every host.
	err = tx.QueryRowContext(ctx, `SELECT count(*) FROM managed_connection_operations o
JOIN managed_connections c ON c.connection_id=o.connection_id
LEFT JOIN managed_resource_accounts r ON r.account_scope=c.account_scope
WHERE o.action='install_image' AND (r.resource_scope=? OR r.resource_scope='' OR r.resource_scope IS NULL)`, resource.ID).Scan(&count)
	if err != nil {
		return dbError(err)
	}
	limit := runnerImageInstallLimit
	if existing {
		// The current accepted operation already occupies one reservation.
		limit++
	}
	if count >= limit {
		return provider.WithReason(provider.ErrResourceCountLimit, "runner_image_count_limit")
	}
	return nil
}
