package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

var _ connections.Repository = (*Store)(nil)

type managedRecord struct {
	view                                                connections.Connection
	account, accountScope, credential, profile, pending string
}

func loadManaged(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id string) (managedRecord, error) {
	var r managedRecord
	var updated string
	err := tx.QueryRowContext(ctx, `SELECT connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,current_profile,pending_operation,updated_at FROM managed_connections WHERE workspace_id=? AND connection_id=?`, w, id).Scan(&r.view.ID, &r.view.Workspace, &r.view.ProviderType, &r.view.Label, &r.view.Revision, &r.view.Authentication, &r.view.NewWork, &r.account, &r.accountScope, &r.credential, &r.profile, &r.pending, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return r, connections.ErrNotFound
	}
	if err != nil {
		return r, dbError(err)
	}
	r.view.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	if err != nil {
		return r, ErrCorrupt
	}
	r.view.CredentialPresent = r.credential != ""
	r.view.Quotas = []connections.Quota{}
	if r.accountScope != "" {
		id := r.accountScope
		r.view.AccountID = &id
	}
	return r, nil
}
func managedProfileOwned(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, profile admission.Profile) error {
	id, ok := credentials.VaultReference(ports.CredentialRef(profile.CredentialRef))
	if !ok {
		return auth.ErrForbidden
	}
	var match bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM managed_connections WHERE workspace_id=? AND connection_id=? AND account_scope=? AND active_credential_key!='' AND new_work!='removed' AND NOT EXISTS(SELECT 1 FROM managed_connection_operations o WHERE o.operation_id=managed_connections.pending_operation AND o.action='remove'))`, w, id, profile.AccountScope).Scan(&match)
	if err != nil {
		return dbError(err)
	}
	if !match {
		return auth.ErrForbidden
	}
	return nil
}
func projectManaged(ctx context.Context, tx *sql.Tx, r managedRecord, now time.Time) (connections.Connection, error) {
	v := r.view
	if r.profile != "" && v.NewWork == "enabled" && v.Authentication == "verified" && r.pending == "" {
		var raw string
		var enabled bool
		if err := tx.QueryRowContext(ctx, `SELECT r.snapshot,p.enabled FROM profiles p JOIN profile_revisions r ON r.profile=p.profile AND r.revision=p.revision WHERE p.profile=?`, r.profile).Scan(&raw, &enabled); err != nil {
			return v, dbError(err)
		}
		var p admission.Profile
		if len(raw) > 8192 || json.Unmarshal([]byte(raw), &p) != nil || p.Validate() != nil || p.AccountScope != r.accountScope {
			return v, ErrCorrupt
		}
		if enabled {
			v.Selection = &connections.Selection{Profile: p.Binding.Profile, ConfigurationRevision: p.Binding.ConfigurationRevision, MaxRemoteWallSeconds: p.MaxRemoteWallSeconds, AllowRemoteInternet: p.AllowRemoteInternet}
		}
	}
	if r.accountScope != "" {
		capacity, err := managedCapacity(ctx, tx, r.accountScope, now)
		if err != nil {
			return v, err
		}
		precision := "unknown"
		if capacity.Status == "known" {
			precision = "lower_bound"
		}
		v.Quotas = []connections.Quota{{Status: capacity.Status, Resource: "gpu", Unit: "seconds", Remaining: capacity.RemainingSeconds, ObservedAt: capacity.ObservedAt, Precision: precision}}
		v.ActiveAttempts = capacity.ReservedAttempts
	}
	return v, nil
}
func managedReadActor(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, token string) error {
	_, err := actorInTx(ctx, tx, w, token, auth.Read)
	if errors.Is(err, auth.ErrForbidden) {
		_, err = actorInTx(ctx, tx, w, token, auth.Manage)
	}
	return err
}
func (s *Store) ReadConnection(ctx context.Context, w domain.WorkspaceID, token, id string, now time.Time) (connections.Connection, error) {
	var v connections.Connection
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return v, err
	}
	defer done()
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if err := managedReadActor(ctx, tx, w, token); err != nil {
			return err
		}
		r, err := loadManaged(ctx, tx, w, id)
		if err != nil {
			return err
		}
		v, err = projectManaged(ctx, tx, r, now)
		return err
	})
	return v, err
}
func (s *Store) ListConnections(ctx context.Context, w domain.WorkspaceID, token string, now time.Time) ([]connections.Connection, error) {
	result := []connections.Connection{}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return nil, err
	}
	defer done()
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Read); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT connection_id FROM managed_connections WHERE workspace_id=? AND new_work!='removed' ORDER BY connection_id LIMIT ?`, w, connections.MaxConnections+1)
		if err != nil {
			return dbError(err)
		}
		ids := []string{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return dbError(err)
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return dbError(err)
		}
		if len(ids) > connections.MaxConnections {
			return ErrCorrupt
		}
		for _, id := range ids {
			r, err := loadManaged(ctx, tx, w, id)
			if err != nil {
				return err
			}
			v, err := projectManaged(ctx, tx, r, now)
			if err != nil {
				return err
			}
			result = append(result, v)
		}
		return nil
	})
	return result, err
}
func loadManagedOperation(ctx context.Context, tx *sql.Tx, id domain.OperationID) (connections.Record, error) {
	var r connections.Record
	var receipt, status, updated string
	var problem sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT o.receipt,o.status,o.problem,o.updated_at,o.stage,o.credential_key,o.token_id,c.provider_type,c.canonical_account,c.active_credential_key FROM managed_connection_operations o JOIN managed_connections c ON c.connection_id=o.connection_id WHERE o.operation_id=?`, id).Scan(&receipt, &status, &problem, &updated, &r.Stage, &r.CredentialKey, &r.ActorTokenID, &r.ProviderType, &r.CanonicalAccount, &r.ActiveCredentialKey)
	if errors.Is(err, sql.ErrNoRows) {
		return r, connections.ErrNotFound
	}
	if err != nil {
		return r, dbError(err)
	}
	if len(receipt) > 4096 || json.Unmarshal([]byte(receipt), &r.Operation) != nil || r.Operation.ID != id {
		return r, ErrCorrupt
	}
	r.Operation.Status = status
	r.Operation.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	if err != nil {
		return r, ErrCorrupt
	}
	if problem.Valid {
		r.Operation.Problem = &problem.String
	}
	return r, nil
}
func (s *Store) ReadConnectionOperation(ctx context.Context, w domain.WorkspaceID, token string, id domain.OperationID) (connections.Operation, error) {
	var op connections.Operation
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return op, err
	}
	defer done()
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Manage); err != nil {
			return err
		}
		r, err := loadManagedOperation(ctx, tx, id)
		if err != nil {
			return err
		}
		if r.Operation.Workspace != w {
			return connections.ErrNotFound
		}
		op = r.Operation
		return nil
	})
	return op, err
}
func (s *Store) ResolveConnectionCredential(ctx context.Context, id string) (string, error) {
	if _, ok := credentials.VaultReference(ports.CredentialRef("vault:" + id)); !ok {
		return "", credentials.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return "", err
	}
	defer done()
	var key string
	err = s.db.QueryRowContext(ctx, `SELECT active_credential_key FROM managed_connections WHERE connection_id=? AND new_work!='removed' AND active_credential_key!=''`, id).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", credentials.ErrUnconfigured
	}
	return key, dbError(err)
}
func (s *Store) FingerprintInitialized(ctx context.Context) (bool, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return false, err
	}
	defer done()
	var initialized bool
	err = s.db.QueryRowContext(ctx, `SELECT fingerprint_initialized FROM managed_config WHERE singleton=1`).Scan(&initialized)
	return initialized, dbError(err)
}
func (s *Store) MarkFingerprintInitialized(ctx context.Context) error {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	_, err = s.db.ExecContext(ctx, `UPDATE managed_config SET fingerprint_initialized=1 WHERE singleton=1`)
	return dbError(err)
}
