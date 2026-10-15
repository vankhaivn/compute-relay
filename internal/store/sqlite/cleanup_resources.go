package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

func snapshotCleanupResources(ctx context.Context, tx *sql.Tx, op domain.Operation, resources []cleanup.Resource) error {
	if len(resources) == 0 || len(resources) > operations.MaxCleanupResources {
		return ErrInvalid
	}
	for ordinal, resource := range resources {
		if !resource.Request.Valid() || resource.Reason != "" {
			return ErrInvalid
		}
		raw, err := json.Marshal(resource)
		if err != nil || len(raw) > 16384 {
			return ErrInvalid
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO cleanup_resources VALUES(?,?,?,?,?,?,'pending','awaiting_worker')`,
			string(op.WorkspaceID), string(op.ID), string(resource.Request.LedgerID), ordinal, string(raw), string(resource.Fingerprint())); err != nil {
			return dbError(err)
		}
	}
	return nil
}

// A confirmed deletion or absence is final for the original immutable target.
// In particular, Kaggle can return 403 for a deleted private resource, so a
// later cleanup must carry its earlier receipt instead of inspecting it again.
func inheritConfirmedCleanupResources(ctx context.Context, tx *sql.Tx, op domain.Operation) error {
	current, err := loadCleanupResources(ctx, tx, op)
	if err != nil {
		return err
	}
	for _, item := range current {
		var fingerprint, state string
		err := tx.QueryRowContext(ctx, `SELECT r.target_sha256,r.state FROM cleanup_resources r
 JOIN operations c ON c.workspace_id=r.workspace_id AND c.operation_id=r.operation_id
 WHERE c.kind='cleanup' AND c.workspace_id=? AND c.job_id=? AND c.attempt_id=?
 AND c.operation_id!=? AND r.resource_id=? AND r.state IN ('deleted','absent')
 ORDER BY CASE r.state WHEN 'deleted' THEN 0 ELSE 1 END,c.operation_seq LIMIT 1`,
			string(op.WorkspaceID), string(op.JobID), string(op.AttemptID), string(op.ID), string(item.Result.ResourceID)).Scan(&fingerprint, &state)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return dbError(err)
		}
		if fingerprint != string(item.Result.Fingerprint) {
			return ErrCorrupt
		}
		if _, err = tx.ExecContext(ctx, `UPDATE cleanup_resources SET state='running',reason='prior_delete_intent' WHERE workspace_id=? AND operation_id=? AND resource_id=? AND state='pending'`,
			string(op.WorkspaceID), string(op.ID), string(item.Result.ResourceID)); err != nil {
			return dbError(err)
		}
		reason := "prior_absence_confirmed"
		if state == "deleted" {
			reason = "prior_deletion_confirmed"
		}
		if _, err = tx.ExecContext(ctx, `UPDATE cleanup_resources SET state=?,reason=? WHERE workspace_id=? AND operation_id=? AND resource_id=? AND state='running'`,
			state, reason, string(op.WorkspaceID), string(op.ID), string(item.Result.ResourceID)); err != nil {
			return dbError(err)
		}
	}
	return nil
}

func loadCleanupResources(ctx context.Context, tx *sql.Tx, op domain.Operation) ([]cleanup.ResourceWork, error) {
	rows, err := tx.QueryContext(ctx, `SELECT resource_id,target,target_sha256,state,reason FROM cleanup_resources WHERE workspace_id=? AND operation_id=? ORDER BY ordinal LIMIT ?`, string(op.WorkspaceID), string(op.ID), operations.MaxCleanupResources+1)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	var result []cleanup.ResourceWork
	for rows.Next() {
		var item cleanup.ResourceWork
		var raw string
		if err := rows.Scan(&item.Result.ResourceID, &raw, &item.Result.Fingerprint, &item.Result.State, &item.Result.Reason); err != nil {
			return nil, dbError(err)
		}
		d := json.NewDecoder(bytes.NewBufferString(raw))
		d.DisallowUnknownFields()
		if len(raw) > 16384 || d.Decode(&item.Resource) != nil || d.Decode(new(any)) != io.EOF || !item.Resource.Request.Valid() || item.Resource.Reason != "" {
			return nil, ErrCorrupt
		}
		q := item.Resource.Request
		item.Result.Purpose = q.Purpose
		id := q.Remote.Identity
		if !item.Result.Valid() || item.Resource.Fingerprint() != item.Result.Fingerprint || q.LedgerID != item.Result.ResourceID ||
			id.WorkspaceID != op.WorkspaceID || id.JobID != op.JobID || id.AttemptID != op.AttemptID {
			return nil, ErrCorrupt
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError(err)
	}
	if len(result) > operations.MaxCleanupResources {
		return nil, ErrCorrupt
	}
	return result, nil
}

func verifyCleanupLease(ctx context.Context, tx *sql.Tx, w cleanup.Work, now time.Time) (operationRow, error) {
	l := w.Lease
	if _, err := checkWorkLease(ctx, tx, cleanupLease(l), now); err != nil {
		return operationRow{}, err
	}
	row, err := loadOperation(ctx, tx, l.WorkspaceID, l.OperationID)
	if err != nil {
		return operationRow{}, err
	}
	if row.Record.Operation.Status != domain.OperationRunning || row.Record.Operation.Revision != w.Record.Operation.Revision {
		return operationRow{}, cleanup.ErrLeaseLost
	}
	return row, nil
}

func (s *Store) BeginCleanupResource(ctx context.Context, w cleanup.Work, resource cleanup.ResourceWork) error {
	if !w.Valid() || !resource.Resource.Request.Valid() || !resource.Result.Valid() {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		row, err := verifyCleanupLease(ctx, tx, w, now)
		if err != nil {
			return err
		}
		op := row.Record.Operation
		target, err := controlTarget(ctx, tx, op.WorkspaceID, op.JobID, op.AttemptID)
		if err != nil {
			return err
		}
		current, err := eligibleCleanup(ctx, tx, target, row.Request.DiscardUncollected, now, op.ID)
		if err != nil {
			return err
		}
		if current.binding != w.Binding {
			return operations.ErrChanged
		}
		found := false
		for _, r := range current.resources {
			if r.Request.LedgerID == resource.Result.ResourceID && r.Fingerprint() == resource.Result.Fingerprint {
				found = true
			}
		}
		if !found {
			return operations.ErrChanged
		}
		// Execution/output deletion must be confirmed before staging/input deletion.
		if resource.Result.Purpose == "staging" {
			var unresolved bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM cleanup_resources WHERE workspace_id=? AND operation_id=? AND ordinal<? AND state NOT IN ('deleted','absent'))`,
				string(op.WorkspaceID), string(op.ID), 1).Scan(&unresolved); err != nil {
				return dbError(err)
			}
			if unresolved && len(current.resources) > 1 {
				return operations.ErrUnresolved
			}
		}
		res, err := tx.ExecContext(ctx, `UPDATE cleanup_resources SET state='running',reason='delete_intent_recorded'
 WHERE workspace_id=? AND operation_id=? AND resource_id=? AND target_sha256=? AND state IN ('pending','running')`, string(op.WorkspaceID), string(op.ID), string(resource.Result.ResourceID), string(resource.Result.Fingerprint))
		if err != nil {
			return dbError(err)
		}
		if count, err := res.RowsAffected(); err != nil || count != 1 {
			return operations.ErrChanged
		}
		return nil
	})
}

func (s *Store) CompleteCleanupResource(ctx context.Context, w cleanup.Work, result operations.ResourceResult) error {
	if !w.Valid() || !result.Valid() || result.State == operations.ResourcePending || result.State == operations.ResourceRunning {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := verifyCleanupLease(ctx, tx, w, now); err != nil {
			return err
		}
		allowed := `state IN ('pending','running')`
		if result.State.Confirmed() {
			allowed = `state='running'`
		}
		res, err := tx.ExecContext(ctx, `UPDATE cleanup_resources SET state=?,reason=? WHERE workspace_id=? AND operation_id=? AND resource_id=? AND target_sha256=? AND `+allowed,
			string(result.State), result.Reason, string(w.Lease.WorkspaceID), string(w.Lease.OperationID), string(result.ResourceID), string(result.Fingerprint))
		if err != nil {
			return dbError(err)
		}
		if count, err := res.RowsAffected(); err != nil || count != 1 {
			return operations.ErrChanged
		}
		return nil
	})
}
