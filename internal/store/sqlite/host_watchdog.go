package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostwatchdog"
	"time"
)

func (s *Store) ResolveWatchdogTarget(ctx context.Context, w domain.WorkspaceID, token, id string) (hostwatchdog.Target, error) {
	var out hostwatchdog.Target
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		if err := managedReadActor(ctx, tx, w, token); err != nil {
			return err
		}
		r, err := loadManaged(ctx, tx, w, id)
		if err != nil {
			return err
		}
		target, err := hostHealthTarget(ctx, tx, w, id)
		if err != nil {
			return err
		}
		var frozen struct {
			Engine string `json:"engine_id"`
		}
		if json.Unmarshal(target.RuntimeConfig, &frozen) != nil || frozen.Engine == "" {
			var saved []byte
			err := tx.QueryRowContext(ctx, `SELECT target FROM host_watchdog_operations WHERE resource_id=? ORDER BY rowid DESC LIMIT 1`, target.ResourceID).Scan(&saved)
			if err != nil {
				return connections.ErrRequest
			}
			var old hostwatchdog.Target
			if json.Unmarshal(saved, &old) != nil {
				return ErrCorrupt
			}
			frozen.Engine = old.Engine
		}
		out = hostwatchdog.Target{Health: target, Revision: r.view.Revision, Engine: frozen.Engine}
		return tx.QueryRowContext(ctx, `SELECT installation_id FROM runtime_installation WHERE singleton=1`).Scan(&out.Installation)
	})
	return out, err
}
func watchdogReplay(ctx context.Context, tx *sql.Tx, in hostwatchdog.Accept) (hostwatchdog.Operation, bool, error) {
	var op hostwatchdog.Operation
	var hash string
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT request_sha256,receipt FROM host_watchdog_operations WHERE workspace_id=? AND key_sha256=?`, in.Workspace, in.Key).Scan(&hash, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return op, false, nil
	}
	if err != nil {
		return op, false, dbError(err)
	}
	if hash != in.Fingerprint {
		return op, false, connections.ErrConflict
	}
	if json.Unmarshal(raw, &op) != nil {
		return op, false, ErrCorrupt
	}
	op.Replay = true
	return op, true, nil
}
func (s *Store) WatchdogReplay(ctx context.Context, in hostwatchdog.Accept) (hostwatchdog.Operation, bool, error) {
	var op hostwatchdog.Operation
	var found bool
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		if _, err := actorInTx(ctx, tx, in.Workspace, in.Token, auth.Manage); err != nil {
			return err
		}
		var err error
		op, found, err = watchdogReplay(ctx, tx, in)
		return err
	})
	return op, found, err
}
func (s *Store) AcceptWatchdog(ctx context.Context, in hostwatchdog.Accept) (hostwatchdog.Operation, bool, error) {
	var op hostwatchdog.Operation
	var found bool
	if !domain.SHA256Digest(in.Key).Valid() || !domain.SHA256Digest(in.Fingerprint).Valid() || !in.Target.Health.Valid() || in.Plan.Validate() != nil {
		return op, false, connections.ErrRequest
	}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, in.Workspace, in.Token, auth.Manage); err != nil {
			return err
		}
		var err error
		op, found, err = watchdogReplay(ctx, tx, in)
		if err != nil || found {
			return err
		}
		r, err := loadManaged(ctx, tx, in.Workspace, in.Connection)
		if err != nil {
			return err
		}
		target, err := hostHealthTarget(ctx, tx, in.Workspace, in.Connection)
		if err != nil {
			return err
		}
		if r.view.Revision != in.Revision || r.pending != "" || target.CanonicalAccount != in.Target.Health.CanonicalAccount || target.ResourceID != in.Target.Health.ResourceID || in.Plan.Validate() != nil || in.Plan.Config.InstallationID != in.Target.Installation {
			return connections.ErrConflict
		}
		var lastAction, lastStatus string
		var lastPlan []byte
		var lastID string
		var lease int64
		err = tx.QueryRowContext(ctx, `SELECT action,status,plan,operation_id,lease_until FROM host_watchdog_operations WHERE resource_id=? ORDER BY rowid DESC LIMIT 1`, target.ResourceID).Scan(&lastAction, &lastStatus, &lastPlan, &lastID, &lease)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		if err == nil {
			if lastStatus == "queued" || lastStatus == "unknown" {
				if in.Action != "remove_host_watchdog" || lastStatus != "unknown" || lease > now.UnixMilli() {
					return connections.ErrConflict
				}
				if _, err := tx.ExecContext(ctx, `UPDATE host_watchdog_operations SET stage='done' WHERE operation_id=?`, lastID); err != nil {
					return dbError(err)
				}
			}
			if in.Action == "install_host_watchdog" && lastAction == "install_host_watchdog" {
				current, _ := json.Marshal(in.Plan)
				if string(current) != string(lastPlan) {
					return connections.ErrConflict
				}
			}
		}
		id, err := randomID("op_", 16)
		if err != nil {
			return err
		}
		op = hostwatchdog.Operation{ID: domain.OperationID(id), ConnectionID: in.Connection, Action: in.Action, Status: "queued", Revision: in.Revision, CreatedAt: now.UTC()}
		receipt, err := json.Marshal(op)
		if err != nil {
			return err
		}
		plan, err := json.Marshal(in.Plan)
		if err != nil || len(plan) > 8192 {
			return connections.ErrRequest
		}
		targetRaw, err := json.Marshal(in.Target)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO host_watchdog_operations(operation_id,workspace_id,key_sha256,request_sha256,token_id,connection_id,resource_id,action,status,stage,target,plan,receipt,created_at) VALUES(?,?,?,?,?,?,?,?,'queued','queued',?,?,?,?)`, id, in.Workspace, in.Key, in.Fingerprint, in.Token, in.Connection, target.ResourceID, in.Action, targetRaw, plan, receipt, now.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
		return nil
	})
	return op, found, err
}
func (s *Store) FinishWatchdog(ctx context.Context, id domain.OperationID, status string) error {
	if status != "succeeded" && status != "unknown" {
		return connections.ErrRequest
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		_, err := tx.ExecContext(ctx, `UPDATE host_watchdog_operations SET status=?,stage='done' WHERE operation_id=? AND status='unknown'`, status, id)
		return dbError(err)
	})
}
func (s *Store) ReadWatchdogOperation(ctx context.Context, w domain.WorkspaceID, token string, id domain.OperationID) (hostwatchdog.Operation, error) {
	var op hostwatchdog.Operation
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Manage); err != nil {
			return err
		}
		var raw []byte
		var status string
		var problem sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT receipt,status,problem FROM host_watchdog_operations WHERE workspace_id=? AND operation_id=?`, w, id).Scan(&raw, &status, &problem)
		if errors.Is(err, sql.ErrNoRows) {
			return connections.ErrNotFound
		}
		if err != nil {
			return dbError(err)
		}
		if json.Unmarshal(raw, &op) != nil {
			return ErrCorrupt
		}
		op.Status = status
		if problem.Valid {
			op.Problem = &problem.String
		}
		return nil
	})
	return op, err
}
func (s *Store) WatchdogPlan(ctx context.Context, resource string) (hostwatchdog.Plan, error) {
	var p hostwatchdog.Plan
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		var raw []byte
		err := tx.QueryRowContext(ctx, `SELECT plan FROM host_watchdog_operations WHERE resource_id=? AND action='install_host_watchdog' AND status!='failed' ORDER BY rowid DESC LIMIT 1`, resource).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			return connections.ErrNotFound
		}
		if err != nil {
			return dbError(err)
		}
		if len(raw) > 8192 || json.Unmarshal(raw, &p) != nil || p.Validate() != nil {
			return ErrCorrupt
		}
		return nil
	})
	return p, err
}

func (s *Store) PendingWatchdogs(ctx context.Context) ([]hostwatchdog.Work, error) {
	items := []hostwatchdog.Work{}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		rows, err := tx.QueryContext(ctx, `SELECT receipt,target,plan,stage FROM host_watchdog_operations WHERE stage='queued' OR (stage='applying' AND lease_until<=?) ORDER BY CASE stage WHEN 'queued' THEN 0 ELSE 1 END,observed_ms,rowid LIMIT 16`, now.UnixMilli())
		if err != nil {
			return dbError(err)
		}
		defer rows.Close()
		for rows.Next() {
			var item hostwatchdog.Work
			var receipt, target, plan []byte
			if err := rows.Scan(&receipt, &target, &plan, &item.Stage); err != nil {
				return dbError(err)
			}
			if len(target) > 16384 || len(plan) > 8192 || json.Unmarshal(receipt, &item.Operation) != nil || json.Unmarshal(target, &item.Target) != nil || json.Unmarshal(plan, &item.Plan) != nil || item.Plan.Validate() != nil {
				return ErrCorrupt
			}
			items = append(items, item)
		}
		return dbError(rows.Err())
	})
	return items, err
}
func (s *Store) ClaimWatchdog(ctx context.Context, id domain.OperationID) (bool, error) {
	claimed := false
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var workspace domain.WorkspaceID
		var token, connection, stage string
		var raw []byte
		err := tx.QueryRowContext(ctx, `SELECT workspace_id,token_id,connection_id,stage,target FROM host_watchdog_operations WHERE operation_id=?`, id).Scan(&workspace, &token, &connection, &stage, &raw)
		if err != nil {
			return dbError(err)
		}
		if stage != "queued" {
			return nil
		}
		if _, err := actorInTx(ctx, tx, workspace, token, auth.Manage); err != nil {
			if errors.Is(err, auth.ErrForbidden) || errors.Is(err, auth.ErrUnauthenticated) {
				_, err = tx.ExecContext(ctx, `UPDATE host_watchdog_operations SET stage='done',status='failed',problem='authority_revoked' WHERE operation_id=?`, id)
				return dbError(err)
			}
			return err
		}
		var frozen hostwatchdog.Target
		if json.Unmarshal(raw, &frozen) != nil {
			return ErrCorrupt
		}
		current, err := loadManaged(ctx, tx, workspace, connection)
		if err != nil {
			return err
		}
		if current.view.Revision != frozen.Revision || current.account != frozen.Health.CanonicalAccount || current.pending != "" {
			_, err = tx.ExecContext(ctx, `UPDATE host_watchdog_operations SET stage='done',status='failed',problem='stale_revision' WHERE operation_id=?`, id)
			return dbError(err)
		}
		r, err := tx.ExecContext(ctx, `UPDATE host_watchdog_operations SET stage='applying',status='unknown',lease_until=? WHERE operation_id=? AND stage='queued'`, now.Add(hostwatchdog.Timeout+5*time.Second).UnixMilli(), id)
		if err != nil {
			return dbError(err)
		}
		n, err := r.RowsAffected()
		claimed = n == 1
		return dbError(err)
	})
	return claimed, err
}

func (s *Store) ReleaseWatchdog(ctx context.Context, id domain.OperationID) error {
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		_, err := tx.ExecContext(ctx, `UPDATE host_watchdog_operations SET lease_until=0,observed_ms=? WHERE operation_id=?`, now.UnixMilli(), id)
		return dbError(err)
	})
}

// Connection removal or credential replacement must retain the only authenticated
// recovery path until explicit watchdog removal is positively confirmed.
func watchdogConnectionRetained(ctx context.Context, tx *sql.Tx, r managedRecord) error {
	if r.accountScope == "" {
		return nil
	}
	resource, err := managedResource(ctx, tx, r.accountScope)
	if err != nil {
		return err
	}
	if resource.Kind != "machine" {
		return nil
	}
	var action, status string
	err = tx.QueryRowContext(ctx, `SELECT action,status FROM host_watchdog_operations WHERE resource_id=? AND status!='failed' ORDER BY rowid DESC LIMIT 1`, resource.ID).Scan(&action, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return dbError(err)
	}
	if action == "remove_host_watchdog" && status == "succeeded" {
		return nil
	}
	return connections.ErrActiveWork
}
