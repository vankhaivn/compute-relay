package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

// StorageProfile is one enabled alias for a verified machine. A periodic scan
// uses the frozen profile to retrieve only its explicitly configured credential.
type StorageProfile struct {
	WorkspaceID domain.WorkspaceID
	Profile     admission.Profile
	Scope       hoststorage.Scope
}

func (s *Store) ActiveStorageProfiles(ctx context.Context) ([]StorageProfile, error) {
	profiles := []StorageProfile{}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		rows, err := tx.QueryContext(ctx, `SELECT c.workspace_id,p.snapshot FROM managed_connections c
		 JOIN profiles x ON x.profile=c.current_profile
		 JOIN profile_revisions p ON p.profile=x.profile AND p.revision=x.revision
		 JOIN managed_profile_runtime m ON m.profile=x.profile AND m.revision=x.revision
		 WHERE c.provider_type='ssh' AND c.authentication='verified' AND c.new_work='enabled'
		 AND c.active_credential_key!='' AND c.pending_operation='' AND x.enabled=1
		 AND json_extract(m.config,'$.schema_version')=3
		 ORDER BY c.connection_id LIMIT 257`)
		if err != nil {
			return dbError(err)
		}
		type row struct {
			workspace domain.WorkspaceID
			profile   string
		}
		candidates := []row{}
		for rows.Next() {
			var item row
			if err := rows.Scan(&item.workspace, &item.profile); err != nil {
				rows.Close()
				return dbError(err)
			}
			candidates = append(candidates, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return dbError(err)
		}
		if len(candidates) > 256 {
			return hoststorage.ErrUnknown
		}
		seen := map[hoststorage.Scope]bool{}
		for _, item := range candidates {
			var profile admission.Profile
			if json.Unmarshal([]byte(item.profile), &profile) != nil || profile.Validate() != nil || !item.workspace.Valid() {
				return ErrCorrupt
			}
			target, err := cacheTargetInTx(ctx, tx, item.workspace, profile)
			if err != nil {
				return err
			}
			if target == nil {
				continue
			}
			scope := storageScope(target)
			if !seen[scope] {
				seen[scope] = true
				profiles = append(profiles, StorageProfile{item.workspace, profile, scope})
			}
		}
		return nil
	})
	return profiles, err
}
