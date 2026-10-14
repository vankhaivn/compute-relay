package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (s *Store) NextConnectionOperation(ctx context.Context) (connections.Record, bool, error) {
	var record connections.Record
	found := false
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return record, false, err
	}
	defer done()
	err = s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var id string
		cutoff := now.Add(-30 * time.Second).UTC().Format(time.RFC3339Nano)
		err := tx.QueryRowContext(ctx, `SELECT operation_id FROM managed_connection_operations WHERE (stage IN ('ready','installing') AND (status!='failed' OR updated_at<?)) OR (stage='waiting_secret' AND updated_at<?) ORDER BY CASE stage WHEN 'ready' THEN 0 ELSE 1 END,created_at,operation_id LIMIT 1`, cutoff, cutoff).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		record, err = loadManagedOperation(ctx, tx, domain.OperationID(id))
		if err != nil {
			return err
		}
		if record.Stage == "ready" {
			_, err = tx.ExecContext(ctx, `UPDATE managed_connection_operations SET status='running',problem=NULL,updated_at=? WHERE operation_id=?`, now.UTC().Format(time.RFC3339Nano), id)
			if err != nil {
				return dbError(err)
			}
			record.Operation.Status = "running"
			record.Operation.Problem = nil
			record.Operation.UpdatedAt = now.UTC()
		}
		found = true
		return nil
	})
	return record, found, err
}
func (s *Store) FailConnection(ctx context.Context, id domain.OperationID, problem string) error {
	switch problem {
	case "credential_rejected", "credential_store_unavailable", "provider_unavailable", "account_changed", "resource_changed", "active_work", "stale_revision", "unsupported_provider", "image_installation_unknown":
	default:
		return ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		record, err := loadManagedOperation(ctx, tx, id)
		if err != nil {
			return err
		}
		if record.Stage == "done" {
			return nil
		}
		if record.Operation.Action == "remove" {
			// Preserve the deletion fence and exact intent after partial vault deletion. Only
			// this already-authorized, idempotent local removal may resume in the worker.
			_, err = tx.ExecContext(ctx, `UPDATE managed_connection_operations SET status='failed',problem=?,updated_at=? WHERE operation_id=?`, problem, now.UTC().Format(time.RFC3339Nano), id)
			return dbError(err)
		}
		return finishManaged(ctx, tx, record, connections.Completion{Problem: problem}, now)
	})
}
func (s *Store) FinishConnection(ctx context.Context, record connections.Record, completion connections.Completion) error {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		return finishManaged(ctx, tx, record, completion, now)
	})
}
func finishManaged(ctx context.Context, tx *sql.Tx, record connections.Record, result connections.Completion, now time.Time) error {
	current, err := loadManagedOperation(ctx, tx, record.Operation.ID)
	if err != nil {
		return err
	}
	if current.Stage == "done" {
		return nil
	}
	op := current.Operation
	if record.Operation.ConnectionRevision != op.ConnectionRevision || record.Operation.ConnectionID != op.ConnectionID {
		return connections.ErrConflict
	}
	target, err := loadManaged(ctx, tx, op.Workspace, op.ConnectionID)
	if err != nil {
		return err
	}
	if target.pending != string(op.ID) || target.view.Revision != op.ConnectionRevision {
		return connections.ErrConflict
	}
	stamp := now.UTC().Format(time.RFC3339Nano)
	authState, newWork, credential, profile, account, scope := target.view.Authentication, target.view.NewWork, target.credential, "", target.account, target.accountScope
	var problem any
	status := "succeeded"
	if result.Problem != "" {
		problem = result.Problem
		status = "failed"
		authState = "unavailable"
		if result.Problem == "credential_rejected" {
			authState = "rejected"
		}
		if (op.Action == "replace_credential" || op.Action == "install_image") && credential != "" && account != "" {
			authState = "verified"
		}
		if current.CredentialKey != "" && current.CredentialKey != credential {
			if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO managed_secret_deletions VALUES(?)`, current.CredentialKey); err != nil {
				return dbError(err)
			}
		}
	} else {
		switch op.Action {
		case "disable":
			newWork = "disabled"
		case "remove":
			if err = managedRemovalAllowed(ctx, tx, op.ConnectionID); err != nil {
				return err
			}
			newWork = "removed"
			credential = ""
			authState = "unavailable"
			if _, err = tx.ExecContext(ctx, `DELETE FROM managed_secret_deletions WHERE credential_key IN (SELECT credential_key FROM managed_credential_generations WHERE connection_id=?)`, op.ConnectionID); err != nil {
				return dbError(err)
			}
			if _, err = tx.ExecContext(ctx, `DELETE FROM managed_credential_generations WHERE connection_id=?`, op.ConnectionID); err != nil {
				return dbError(err)
			}
		default:
			if (current.Stage != "ready" && !(op.Action == "install_image" && current.Stage == "installing")) || result.Verification.CanonicalAccount == "" || result.Profile.Validate() != nil || result.Profile.Binding != connections.ProfileBinding(op.ConnectionID, op.ConnectionRevision) || result.Profile.CredentialRef != "vault:"+op.ConnectionID {
				return ErrInvalid
			}
			expectedScope := connections.AccountScope(current.ProviderType, result.Verification.CanonicalAccount)
			if result.Profile.AccountScope != expectedScope || account != "" && account != result.Verification.CanonicalAccount {
				return connections.ErrConflict
			}
			resource := result.Verification.Resource
			if resource == nil && result.Profile.ResourceScope != "" || resource != nil && result.Profile.ResourceScope != resource.ID {
				return ErrInvalid
			}
			if err = bindManagedResource(ctx, tx, expectedScope, resource); err != nil {
				return err
			}
			if host := result.Verification.Host; host != nil {
				if host.Validate() != nil {
					return ErrInvalid
				}
				raw, err := json.Marshal(host)
				if err != nil {
					return ErrInvalid
				}
				if _, err = tx.ExecContext(ctx, `UPDATE managed_connections SET host_evidence=? WHERE connection_id=?`, raw, op.ConnectionID); err != nil {
					return dbError(err)
				}
			}
			account = result.Verification.CanonicalAccount
			scope = expectedScope
			authState = "verified"
			if credential != "" && credential != current.CredentialKey {
				if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO managed_secret_deletions VALUES(?)`, credential); err != nil {
					return dbError(err)
				}
			}
			credential = current.CredentialKey
			if credential == "" {
				return ErrInvalid
			}
			if op.Action == "enable" {
				newWork = "enabled"
			}
			if newWork == "enabled" && !result.Verification.SelectionUnavailable {
				encoded, err := json.Marshal(result.Profile)
				if err != nil || len(encoded) > 8192 {
					return ErrInvalid
				}
				profile = result.Profile.Binding.Profile
				if _, err = tx.ExecContext(ctx, `INSERT INTO profile_revisions VALUES(?,?,?)`, profile, result.Profile.Binding.ConfigurationRevision, string(encoded)); err != nil {
					return dbError(err)
				}
				if _, err = tx.ExecContext(ctx, `INSERT INTO profiles VALUES(?,?,1)`, profile, result.Profile.Binding.ConfigurationRevision); err != nil {
					return dbError(err)
				}
				if len(result.RuntimeConfig) > 8192 || len(result.RuntimeConfig) > 0 && !json.Valid(result.RuntimeConfig) {
					return ErrInvalid
				}
				config := result.RuntimeConfig
				if config == nil {
					config = []byte{}
				}
				if _, err = tx.ExecContext(ctx, `INSERT INTO managed_profile_runtime VALUES(?,?,?)`, profile, result.Profile.Binding.ConfigurationRevision, config); err != nil {
					return dbError(err)
				}
			}
			quota := result.Verification.Quota
			// Unsupported quota has no observation timestamp. Preserve that absence
			// and any previous account evidence instead of manufacturing a sample.
			if quota.Status != provider.QuotaUnknown || !quota.ObservedAt.IsZero() {
				if err = recordQuota(ctx, tx, scope, quota.Resource, quota, now); err != nil && !errors.Is(err, ErrConflict) {
					return err
				}
			}
			// Another credential for the same account may have newer evidence. Keep it.
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE managed_connections SET authentication=?,new_work=?,canonical_account=?,account_scope=?,active_credential_key=?,current_profile=?,pending_operation='',updated_at=? WHERE connection_id=?`, authState, newWork, account, scope, credential, profile, stamp, op.ConnectionID)
	if err != nil {
		return dbError(err)
	}
	if err = refreshManagedAccountPolicy(ctx, tx, scope); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE managed_connection_operations SET stage='done',status=?,problem=?,updated_at=? WHERE operation_id=?`, status, problem, stamp, op.ID)
	return dbError(err)
}
func (s *Store) ConnectionSecrets(ctx context.Context, id domain.OperationID) ([]string, error) {
	var result []string
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return nil, err
	}
	defer done()
	err = s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		r, err := loadManagedOperation(ctx, tx, id)
		if err != nil {
			return err
		}
		if r.Stage != "ready" || r.Operation.Action != "remove" {
			return connections.ErrConflict
		}
		target, err := loadManaged(ctx, tx, r.Operation.Workspace, r.Operation.ConnectionID)
		if err != nil {
			return err
		}
		if target.pending != string(id) {
			return connections.ErrConflict
		}
		if err = managedRemovalAllowed(ctx, tx, r.Operation.ConnectionID); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT credential_key FROM managed_credential_generations WHERE connection_id=? ORDER BY credential_key`, r.Operation.ConnectionID)
		if err != nil {
			return dbError(err)
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			if err = rows.Scan(&key); err != nil {
				return dbError(err)
			}
			result = append(result, key)
			if len(result) > 10000 {
				return connections.ErrLimit
			}
		}
		return dbError(rows.Err())
	})
	return result, err
}
func (s *Store) NextSecretDeletion(ctx context.Context) (string, bool, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return "", false, err
	}
	defer done()
	var key string
	err = s.db.QueryRowContext(ctx, `SELECT d.credential_key FROM managed_secret_deletions d WHERE NOT EXISTS(SELECT 1 FROM managed_connections c WHERE c.active_credential_key=d.credential_key) ORDER BY d.credential_key LIMIT 1`).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return key, err == nil, dbError(err)
}
func (s *Store) CompleteSecretDeletion(ctx context.Context, key string) error {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM managed_connections WHERE active_credential_key=?)`, key).Scan(&active); err != nil {
			return dbError(err)
		}
		if active {
			return connections.ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM managed_secret_deletions WHERE credential_key=?`, key); err != nil {
			return dbError(err)
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM managed_credential_generations WHERE credential_key=?`, key)
		return dbError(err)
	})
}

// DeferConnectionSecret retains the unacknowledged reserved write across missing or
// temporarily inaccessible vault bytes. Exact replay can finish the original intent.
func (s *Store) DeferConnectionSecret(ctx context.Context, id domain.OperationID) error {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		_, err := tx.ExecContext(ctx, `UPDATE managed_connection_operations SET updated_at=? WHERE operation_id=? AND stage='waiting_secret'`, now.Format(time.RFC3339Nano), id)
		return dbError(err)
	})
}
