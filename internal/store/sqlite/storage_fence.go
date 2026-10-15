package sqlite

import (
	"context"
	"database/sql"

	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

func storageContentFencedInTx(ctx context.Context, tx *sql.Tx, scope hoststorage.Scope, class string, sha string, bytes int64) (bool, error) {
	var fenced bool
	var err error
	switch class {
	case "cache":
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_cache_generations g
		 JOIN host_storage_cleanup_operations o ON o.installation_id=g.installation_id AND o.machine_id=g.machine_id
		 AND o.engine_id=g.engine_id AND o.class='volume' AND o.object_id=g.volume_name
		 WHERE g.installation_id=? AND g.machine_id=? AND g.engine_id=? AND g.sha256=? AND g.bytes=?
		 AND (o.status IN ('accepted','running') OR o.result='unknown'))`,
			scope.InstallationID, scope.MachineID, scope.EngineID, sha, bytes).Scan(&fenced)
	case "image":
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM host_application_images g
		 JOIN host_storage_cleanup_operations o ON o.installation_id=g.installation_id AND o.machine_id=g.machine_id
		 AND o.engine_id=g.engine_id AND o.class='image' AND o.object_id=g.expected_image_id
		 WHERE g.installation_id=? AND g.machine_id=? AND g.engine_id=? AND g.archive_sha256=? AND g.archive_bytes=?
		 AND (o.status IN ('accepted','running') OR o.result='unknown'))`,
			scope.InstallationID, scope.MachineID, scope.EngineID, sha, bytes).Scan(&fenced)
	default:
		return false, ErrInvalid
	}
	return fenced, dbError(err)
}
