package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
)

// configureManaged changes future admission policy in the accepting transaction.
// It copies the exact current profile/runtime; it never asks a provider or rebuilds
// policy from process defaults. Historical profiles and authorizations stay intact.
func configureManaged(ctx context.Context, tx *sql.Tx, prior managedRecord, op connections.Operation, configuration connections.Configuration) error {
	profileName := ""
	if prior.profile != "" {
		if prior.view.Authentication != "verified" || prior.view.NewWork != "enabled" {
			return ErrCorrupt
		}
		var raw string
		if err := tx.QueryRowContext(ctx, `SELECT r.snapshot FROM profiles p JOIN profile_revisions r ON r.profile=p.profile AND r.revision=p.revision WHERE p.profile=?`, prior.profile).Scan(&raw); err != nil {
			return dbError(err)
		}
		var profile admission.Profile
		if len(raw) > 8192 || json.Unmarshal([]byte(raw), &profile) != nil || profile.Validate() != nil ||
			profile.Binding != connections.ProfileBinding(prior.view.ID, prior.view.Revision) ||
			profile.AccountScope != prior.accountScope || profile.CredentialRef != "vault:"+prior.view.ID {
			return ErrCorrupt
		}
		oldBinding := profile.Binding
		profile.Binding = connections.ProfileBinding(op.ConnectionID, op.ConnectionRevision)
		profile.MaxRemoteWallSeconds = configuration.MaxRemoteWallSeconds
		if profile.Validate() != nil {
			return connections.ErrRequest
		}
		encoded, err := json.Marshal(profile)
		if err != nil || len(encoded) > 8192 {
			return ErrInvalid
		}
		profileName = profile.Binding.Profile
		if _, err = tx.ExecContext(ctx, `INSERT INTO profile_revisions VALUES(?,?,?)`, profileName, profile.Binding.ConfigurationRevision, string(encoded)); err != nil {
			return dbError(err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO profiles VALUES(?,?,1)`, profileName, profile.Binding.ConfigurationRevision); err != nil {
			return dbError(err)
		}
		copied, err := tx.ExecContext(ctx, `INSERT INTO managed_profile_runtime(profile,revision,config) SELECT ?,?,config FROM managed_profile_runtime WHERE profile=? AND revision=?`, profileName, profile.Binding.ConfigurationRevision, oldBinding.Profile, oldBinding.ConfigurationRevision)
		if err != nil {
			return dbError(err)
		}
		if count, err := copied.RowsAffected(); err != nil || count != 1 {
			return ErrCorrupt
		}
	}
	_, err := tx.ExecContext(ctx, `UPDATE managed_connections SET max_remote_wall_seconds=?,current_profile=?,pending_operation='' WHERE connection_id=?`, configuration.MaxRemoteWallSeconds, profileName, op.ConnectionID)
	return dbError(err)
}
