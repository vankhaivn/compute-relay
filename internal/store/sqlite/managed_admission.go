package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

func (s *Store) AcceptConnection(ctx context.Context, in connections.Accept) (connections.Record, error) {
	var result connections.Record
	if !in.Workspace.Valid() || !validID(in.TokenID) || !domain.SHA256Digest(in.KeyHash).Valid() || !domain.SHA256Digest(in.Fingerprint).Valid() {
		return result, connections.ErrRequest
	}
	if in.Configuration != nil && (in.Configuration.Validate() != nil || in.Action != "create" && in.Action != "configure") || in.Action == "configure" && in.Configuration == nil {
		return result, connections.ErrRequest
	}
	if (in.Action == "install_image") != (in.Installation != nil) || in.Installation != nil && in.Installation.Validate() != nil {
		return result, connections.ErrRequest
	}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, in.Workspace, in.TokenID, auth.Manage); err != nil {
			return err
		}
		key := receiptKey{in.Workspace, "connection", "", in.KeyHash, in.Fingerprint, "", connections.ErrConflict}
		priorReceipt, found, err := replayOrCreate(ctx, tx, key, nil)
		if err != nil {
			return err
		}
		if found {
			result, err = loadManagedOperation(ctx, tx, domain.OperationID(priorReceipt.subject))
			if err != nil {
				return err
			}
			if json.Unmarshal([]byte(priorReceipt.raw), &result.Operation) != nil {
				return ErrCorrupt
			}
			result.Operation.Replay = true
			return nil
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
		case "check", "replace_credential", "disable", "enable", "remove", "configure", "install_image":
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
			if in.Action == "install_image" && (prior.account == "" || prior.credential == "" || prior.view.Host == nil || prior.view.Host.Architecture != in.Installation.Architecture) {
				return connections.ErrRequest
			}
			if in.Action == "remove" || in.Action == "replace_credential" {
				if err = watchdogConnectionRetained(ctx, tx, prior); err != nil {
					return err
				}
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
		providerType := in.ProviderType
		if in.Action != "create" {
			providerType = prior.view.ProviderType
		}
		if in.Configuration != nil && (in.Configuration.HostHealth != nil || in.Configuration.StorageBudget != nil) && providerType != "ssh" {
			return connections.ErrRequest
		}
		operationID, err := randomID("op_", 16)
		if err != nil {
			return err
		}
		stamp := now.UTC().Format(time.RFC3339Nano)
		if in.Action == "create" {
			var wall any
			var maxActive, maxProcesses int
			var storageSoft, storageHard any
			if in.Configuration != nil {
				wall = in.Configuration.MaxRemoteWallSeconds
				maxActive, maxProcesses = in.Configuration.MaxActiveAttempts, in.Configuration.MaxProcesses
				if in.Configuration.StorageBudget != nil {
					storageSoft, storageHard = in.Configuration.StorageBudget.SoftBytes, in.Configuration.StorageBudget.HardBytes
				}
			}
			var health any
			if in.ProviderType == "ssh" {
				var patch *hosthealth.PolicyPatch
				if in.Configuration != nil {
					patch = in.Configuration.HostHealth
				}
				policy, err := hosthealth.DefaultPolicy().Apply(patch)
				if err != nil {
					return connections.ErrRequest
				}
				health, err = json.Marshal(policy)
				if err != nil {
					return ErrInvalid
				}
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,pending_operation,updated_at,max_remote_wall_seconds,max_active_attempts,max_processes,host_health,storage_soft_bytes,storage_hard_bytes) VALUES(?,?,?,?,?,'pending','enabled',?,?,?,?,?,?,?,?)`, id, in.Workspace, in.ProviderType, in.Label, revision, operationID, stamp, wall, maxActive, maxProcesses, health, storageSoft, storageHard)
		} else {
			newWork := prior.view.NewWork
			authentication := prior.view.Authentication
			if in.Action == "disable" || in.Action == "remove" {
				newWork = "disabled"
			}
			if in.Action == "replace_credential" {
				authentication = "pending"
			}
			profile := ""
			if in.Action == "check" {
				profile = prior.profile
			}
			if prior.profile != "" && in.Action != "check" {
				if _, err = tx.ExecContext(ctx, `UPDATE profiles SET enabled=0 WHERE profile=?`, prior.profile); err != nil {
					return dbError(err)
				}
			}
			_, err = tx.ExecContext(ctx, `UPDATE managed_connections SET revision=?,authentication=?,new_work=?,current_profile=?,pending_operation=?,updated_at=? WHERE connection_id=?`, revision, authentication, newWork, profile, operationID, stamp, id)
		}
		if err != nil {
			return dbError(err)
		}
		op := connections.Operation{ID: domain.OperationID(operationID), Workspace: in.Workspace, ConnectionID: id, Action: in.Action, Status: "accepted", ConnectionRevision: revision, CreatedAt: now.UTC(), UpdatedAt: now.UTC()}
		if in.Action == "configure" {
			// Configuration has no remote or credential mutation. Commit its new alias,
			// saved override and terminal receipt atomically with the revision change.
			if err = configureManaged(ctx, tx, prior, op, *in.Configuration); err != nil {
				return err
			}
			op.Status = "succeeded"
		}
		installation := []byte{}
		if in.Installation != nil {
			installation, err = json.Marshal(in.Installation)
			if err != nil {
				return ErrInvalid
			}
		}
		receiptBytes, err := json.Marshal(op)
		if err != nil {
			return ErrInvalid
		}
		stage := "ready"
		if in.Action == "configure" {
			stage = "done"
		}
		credentialKey := prior.credential
		if in.Secret {
			stage = "waiting_secret"
			credentialKey = operationID
			if _, err = tx.ExecContext(ctx, `INSERT INTO managed_credential_generations VALUES(?,?,?)`, credentialKey, id, stamp); err != nil {
				return dbError(err)
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO managed_connection_operations(operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,stage,status,credential_key,created_at,updated_at,installation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, operationID, in.Workspace, id, in.Action, revision, in.TokenID, in.KeyHash, stage, op.Status, credentialKey, stamp, stamp, installation)
		if err != nil {
			return dbError(err)
		}
		_, _, err = replayOrCreate(ctx, tx, key, &savedReceipt{operationID, "", "", string(receiptBytes)})
		if err != nil {
			return err
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
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
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
