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
)

// Admission retains the requested immutable profile. A newer alias only supplies
// current eligibility and limits; it never changes request or replay identities.
func admissionSelection(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, frozen admission.Profile, enabled bool, spec admission.Spec) (admission.Profile, error) {
	id, ok := credentials.VaultReference(ports.CredentialRef(frozen.CredentialRef))
	if !ok {
		return frozen, admission.ErrSelection
	}
	var currentName, pending, providerType, generation, authentication, newWork string
	err := tx.QueryRowContext(ctx, `SELECT c.current_profile,c.pending_operation,c.provider_type,c.active_credential_key,c.authentication,c.new_work
 FROM managed_connections c
 WHERE c.workspace_id=? AND c.connection_id=?`, w, id).Scan(&currentName, &pending, &providerType, &generation, &authentication, &newWork)
	if errors.Is(err, sql.ErrNoRows) {
		return frozen, admission.ErrSelection
	}
	if err != nil {
		return frozen, dbError(err)
	}
	if newWork == "enabled" && pending != "" {
		return frozen, admission.ErrSelectionPending
	}
	if newWork == "enabled" && authentication == "unavailable" {
		return frozen, admission.ErrSelectionUnavailable
	}
	if pending != "" || newWork != "enabled" || authentication != "verified" || currentName == "" {
		return frozen, admission.ErrSelection
	}
	if currentName == frozen.Binding.Profile && enabled {
		return frozen, nil
	}
	var raw string
	var currentEnabled bool
	err = tx.QueryRowContext(ctx, `SELECT r.snapshot,p.enabled FROM profiles p JOIN profile_revisions r ON r.profile=p.profile AND r.revision=p.revision WHERE p.profile=?`, currentName).Scan(&raw, &currentEnabled)
	if err != nil {
		return frozen, dbError(err)
	}
	var current admission.Profile
	if len(raw) > 8192 || json.Unmarshal([]byte(raw), &current) != nil || current.Validate() != nil || current.Binding.Profile != currentName {
		return frozen, ErrCorrupt
	}
	if !currentEnabled || current.Binding.ProviderInstanceID != frozen.Binding.ProviderInstanceID || current.AccountScope != frozen.AccountScope || current.CredentialRef != frozen.CredentialRef || current.ResourceScope != frozen.ResourceScope || current.CostClass != frozen.CostClass || current.Accelerator != frozen.Accelerator {
		return frozen, admission.ErrSelection
	}
	var revision int64
	var oldGeneration string
	err = tx.QueryRowContext(ctx, `SELECT connection_revision,credential_key FROM managed_connection_operations WHERE workspace_id=? AND connection_id=? AND 'r'||connection_revision=? AND status='succeeded'`, w, frozen.Binding.ProviderInstanceID, frozen.Binding.ConfigurationRevision).Scan(&revision, &oldGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return frozen, admission.ErrSelection
	}
	if err != nil {
		return frozen, dbError(err)
	}
	if frozen.Binding != connections.ProfileBinding(string(frozen.Binding.ProviderInstanceID), revision) || oldGeneration == "" || oldGeneration != generation {
		return frozen, admission.ErrSelection
	}
	var oldRuntime, newRuntime []byte
	for _, item := range []struct {
		profile admission.Profile
		target  *[]byte
	}{{frozen, &oldRuntime}, {current, &newRuntime}} {
		if err = tx.QueryRowContext(ctx, `SELECT config FROM managed_profile_runtime WHERE profile=? AND revision=?`, item.profile.Binding.Profile, item.profile.Binding.ConfigurationRevision).Scan(item.target); err != nil {
			return frozen, dbError(err)
		}
	}
	compatible, err := sameAdmissionRuntime(providerType, oldRuntime, newRuntime)
	if err != nil {
		return frozen, err
	}
	if !compatible {
		return frozen, admission.ErrSelection
	}
	checked := current
	checked.Binding.Profile = spec.Profile
	if checked.Check(spec) != nil {
		return frozen, admission.ErrSelection
	}
	return current, nil
}

func sameAdmissionRuntime(providerType string, old, current []byte) (bool, error) {
	if bytes.Equal(old, current) {
		return true, nil
	}
	if providerType != "ssh" {
		return false, nil
	}
	// These are documented mutable admission policies, not host/image identity.
	normalize := func(raw []byte) ([]byte, error) {
		var fields map[string]json.RawMessage
		if len(raw) > 8192 || json.Unmarshal(raw, &fields) != nil || fields == nil {
			return nil, ErrCorrupt
		}
		delete(fields, "host_health")
		delete(fields, "storage_budget")
		return json.Marshal(fields)
	}
	a, err := normalize(old)
	if err != nil {
		return false, err
	}
	b, err := normalize(current)
	if err != nil {
		return false, err
	}
	return bytes.Equal(a, b), nil
}
