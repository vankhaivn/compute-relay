package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func (s *Store) AcceptConnection(ctx context.Context, in connections.Accept) (connections.Record, error) {
	var result connections.Record
	if !in.Workspace.Valid() || !validID(in.TokenID) || !domain.SHA256Digest(in.KeyHash).Valid() || !domain.SHA256Digest(in.Fingerprint).Valid() || in.Now.IsZero() {
		return result, connections.ErrRequest
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return result, err
	}
	defer done()
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, in.Workspace, in.TokenID, auth.Manage); err != nil {
			return err
		}
		var priorID, hash, receipt string
		err := tx.QueryRowContext(ctx, `SELECT operation_id,fingerprint,receipt FROM managed_connection_operations WHERE workspace_id=? AND key_sha256=?`, in.Workspace, in.KeyHash).Scan(&priorID, &hash, &receipt)
		if err == nil {
			if hash != in.Fingerprint {
				return connections.ErrConflict
			}
			result, err = loadManagedOperation(ctx, tx, domain.OperationID(priorID))
			if err != nil {
				return err
			}
			if json.Unmarshal([]byte(receipt), &result.Operation) != nil {
				return ErrCorrupt
			}
			result.Operation.Replay = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		id := in.ConnectionID
		revision := int64(1)
		var prior managedRecord
		switch in.Action {
		case "create":
			if id != "" || in.ExpectedRevision != 0 || in.ProviderType == "" || in.Label == "" || !in.Secret {
				return connections.ErrRequest
			}
			var count int
			if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM managed_connections WHERE workspace_id=? AND new_work!='removed'`, in.Workspace).Scan(&count); err != nil {
				return dbError(err)
			}
			if count >= connections.MaxConnections {
				return connections.ErrLimit
			}
			id, err = randomID("con_", 16)
			if err != nil {
				return err
			}
		case "check", "replace_credential", "disable", "enable", "remove":
			prior, err = loadManaged(ctx, tx, in.Workspace, id)
			if err != nil {
				return err
			}
			if prior.view.Revision != in.ExpectedRevision || prior.pending != "" || prior.view.NewWork == "removed" || prior.view.Revision >= 2147483647 {
				return connections.ErrConflict
			}
			if (in.Action == "replace_credential") != in.Secret {
				return connections.ErrRequest
			}
			if in.Action == "remove" {
				if err = managedRemovalAllowed(ctx, tx, id); err != nil {
					return err
				}
			}
			revision = prior.view.Revision + 1
		default:
			return connections.ErrRequest
		}
		operationID, err := randomID("op_", 16)
		if err != nil {
			return err
		}
		stamp := in.Now.UTC().Format(time.RFC3339Nano)
		if in.Action == "create" {
			_, err = tx.ExecContext(ctx, `INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,pending_operation,updated_at) VALUES(?,?,?,?,?,'pending','enabled',?,?)`, id, in.Workspace, in.ProviderType, in.Label, revision, operationID, stamp)
		} else {
			newWork := prior.view.NewWork
			authentication := prior.view.Authentication
			if in.Action == "disable" || in.Action == "remove" {
				newWork = "disabled"
			}
			if in.Action == "check" || in.Action == "replace_credential" {
				authentication = "pending"
			}
			if prior.profile != "" {
				if _, err = tx.ExecContext(ctx, `UPDATE profiles SET enabled=0 WHERE profile=?`, prior.profile); err != nil {
					return dbError(err)
				}
			}
			_, err = tx.ExecContext(ctx, `UPDATE managed_connections SET revision=?,authentication=?,new_work=?,current_profile='',pending_operation=?,updated_at=? WHERE connection_id=?`, revision, authentication, newWork, operationID, stamp, id)
		}
		if err != nil {
			return dbError(err)
		}
		op := connections.Operation{ID: domain.OperationID(operationID), Workspace: in.Workspace, ConnectionID: id, Action: in.Action, Status: "accepted", ConnectionRevision: revision, CreatedAt: in.Now.UTC(), UpdatedAt: in.Now.UTC()}
		receiptBytes, err := json.Marshal(op)
		if err != nil {
			return ErrInvalid
		}
		stage := "ready"
		credentialKey := prior.credential
		if in.Secret {
			stage = "waiting_secret"
			credentialKey = operationID
			if _, err = tx.ExecContext(ctx, `INSERT INTO managed_credential_generations VALUES(?,?,?)`, credentialKey, id, stamp); err != nil {
				return dbError(err)
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO managed_connection_operations(operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,fingerprint,stage,status,credential_key,receipt,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,'accepted',?,?,?,?)`, operationID, in.Workspace, id, in.Action, revision, in.TokenID, in.KeyHash, in.Fingerprint, stage, credentialKey, string(receiptBytes), stamp, stamp)
		if err != nil {
			return dbError(err)
		}
		result, err = loadManagedOperation(ctx, tx, op.ID)
		return err
	})
	return result, err
}

// Removal is conservative about unresolved hardware and outstanding collection. A
// retained successful local publication or a conclusively prevented job needs no token.
func managedRemovalAllowed(ctx context.Context, tx *sql.Tx, id string) error {
	var dependent bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attempts a JOIN jobs j ON j.workspace_id=a.workspace_id AND j.job_id=a.job_id JOIN profile_revisions p ON p.profile=j.profile AND p.revision=j.profile_revision WHERE json_extract(p.snapshot,'$.credential_ref')=? AND (a.orchestration NOT IN ('succeeded','failed','cancelled','timed_out') OR json_extract(a.state,'$.RemoteActivity') NOT IN ('not_started','inactive') OR (json_extract(a.state,'$.Execution')='succeeded' AND json_extract(a.state,'$.Result') NOT IN ('available','expired'))))`, "vault:"+id).Scan(&dependent)
	if err != nil {
		return dbError(err)
	}
	if dependent {
		return connections.ErrActiveWork
	}
	return nil
}
func (s *Store) ReadyConnection(ctx context.Context, w domain.WorkspaceID, token string, id domain.OperationID) error {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
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
		if r.Stage == "ready" {
			return nil
		}
		if r.Stage != "waiting_secret" {
			return connections.ErrConflict
		}
		_, err = tx.ExecContext(ctx, `UPDATE managed_connection_operations SET stage='ready' WHERE operation_id=? AND stage='waiting_secret'`, id)
		return dbError(err)
	})
}
