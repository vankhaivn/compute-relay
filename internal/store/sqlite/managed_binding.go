package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// ReadManagedBinding reloads the exact immutable revision held by a job. Current
// admission eligibility is deliberately irrelevant: disabled/older aliases retain
// recovery and collection access through the same verified credential slot.
func (s *Store) ReadManagedBinding(ctx context.Context, expected provider.BindingSnapshot) (connections.RuntimeBinding, error) {
	var result connections.RuntimeBinding
	id, ok := credentials.VaultReference(ports.CredentialRef(expected.CredentialRef))
	if !expected.Valid() || !ok || expected.Binding.ProviderInstanceID != domain.ProviderInstanceID(id) {
		return result, ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return result, err
	}
	defer done()
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		var raw string
		var runtimeConfig []byte
		err := tx.QueryRowContext(ctx, `SELECT p.snapshot,c.config FROM profile_revisions p
 LEFT JOIN managed_profile_runtime c ON c.profile=p.profile AND c.revision=p.revision
 WHERE p.profile=? AND p.revision=?`, expected.Binding.Profile, expected.Binding.ConfigurationRevision).Scan(&raw, &runtimeConfig)
		if errors.Is(err, sql.ErrNoRows) {
			return connections.ErrNotFound
		}
		if err != nil {
			return dbError(err)
		}
		var profile admission.Profile
		if len(raw) > 8192 || len(runtimeConfig) > 8192 || json.Unmarshal([]byte(raw), &profile) != nil || profile.Validate() != nil {
			return ErrCorrupt
		}
		canonical, err := json.Marshal(profile)
		if err != nil || !bytes.Equal(canonical, []byte(raw)) {
			return ErrCorrupt
		}
		actual := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
		if actual != expected {
			return connections.ErrNotFound
		}
		var workspace domain.WorkspaceID
		var providerType, account string
		err = tx.QueryRowContext(ctx, `SELECT workspace_id,provider_type,canonical_account FROM managed_connections WHERE connection_id=?`, id).Scan(&workspace, &providerType, &account)
		if errors.Is(err, sql.ErrNoRows) {
			return connections.ErrNotFound
		}
		if err != nil {
			return dbError(err)
		}
		if !workspace.Valid() || !domain.ObjectID(providerType).Valid() || !domain.ObjectID(account).Valid() {
			return ErrCorrupt
		}
		if profile.AccountScope != connections.AccountScope(providerType, account) {
			return ErrCorrupt
		}
		if err = managedProfileOwned(ctx, tx, workspace, profile); err != nil {
			return err
		}
		result = connections.RuntimeBinding{ProviderType: providerType, CanonicalAccount: account, Profile: profile, RuntimeConfig: append([]byte(nil), runtimeConfig...)}
		return nil
	})
	if err != nil {
		return connections.RuntimeBinding{}, err
	}
	return result, nil
}
