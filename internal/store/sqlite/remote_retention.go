package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

const (
	maxRemoteRetentionSeconds = 30 * 24 * 60 * 60
	retentionScanLimit        = 32
	retentionBusyDelay        = 30 * time.Second
)

type remoteDeadline struct {
	workspace domain.WorkspaceID
	job       domain.JobID
	attempt   domain.AttemptID
	kind      string
	anchorMS  int64
	dueMS     int64
}

// ceilRetentionMS never rounds an immutable anchor earlier than its local
// transaction time. The same ceiling is used when comparing a publication.
func ceilRetentionMS(at time.Time) int64 {
	ms := at.UnixMilli()
	if at.Nanosecond()%int(time.Millisecond) != 0 {
		ms++
	}
	return ms
}

func retentionSeconds(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, kind string) (sql.NullInt64, error) {
	var delay sql.NullInt64
	path := "$.retention.after_collection_seconds"
	if kind == "uncollected" {
		path = "$.retention.uncollected_max_seconds"
	} else if kind != "collected" {
		return delay, ErrInvalid
	}
	var valueType sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT json_type(request,?),json_extract(request,?) FROM jobs WHERE workspace_id=? AND job_id=?`, path, path, string(w), string(job)).Scan(&valueType, &delay)
	if err != nil {
		return delay, dbError(err)
	}
	if !valueType.Valid {
		return delay, nil // Legacy job: no declared rule of this kind.
	}
	if valueType.String != "integer" || !delay.Valid || delay.Int64 < 0 || delay.Int64 > maxRemoteRetentionSeconds || kind == "uncollected" && delay.Int64 == 0 {
		return sql.NullInt64{}, ErrCorrupt
	}
	return delay, nil
}

func insertRemoteDeadline(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, kind string, anchor time.Time) error {
	delay, err := retentionSeconds(ctx, tx, w, job, kind)
	if err != nil || !delay.Valid {
		return err
	}
	anchorMS := ceilRetentionMS(anchor)
	if anchorMS <= 0 {
		return ErrCorrupt
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO remote_retention_deadlines(workspace_id,job_id,attempt_id,kind,anchor_ms,due_ms)
 VALUES(?,?,?,?,?,?) ON CONFLICT(workspace_id,job_id,attempt_id,kind) DO NOTHING`,
		string(w), string(job), string(attempt), kind, anchorMS, anchorMS+delay.Int64*1000)
	return dbError(err)
}

// The first terminal execution (or a terminal never-started attempt) starts the
// uncollected lifetime once remote activity is confirmed inactive/not-started.
// Later observations and restarts cannot move the committed anchor.
func noteAttemptRemoteRetention(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, before, after domain.Attempt, now time.Time) error {
	if before.ID != after.ID || before.JobID != after.JobID || !retentionSettled(after.State) || retentionSettled(before.State) {
		return nil
	}
	return insertRemoteDeadline(ctx, tx, w, job, after.ID, "uncollected", now)
}

func retentionSettled(state domain.AttemptState) bool {
	return (state.Execution.Terminal() || state.Orchestration.Terminal()) &&
		(state.RemoteActivity == domain.RemoteActivityInactive || state.RemoteActivity == domain.RemoteActivityNotStarted)
}

// Called in the publication transaction, after the complete verified artifact
// set is committed. The deadline cannot precede publication acknowledgement.
func notePublishedRemoteRetention(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, now time.Time) error {
	return insertRemoteDeadline(ctx, tx, w, job, attempt, "collected", now)
}

// ScheduleRetention moves a bounded set of due rules into the ordinary cleanup
// operation ledger. Eligibility, collection-versus-expiry and prior cleanup are
// rechecked under the same write transaction that accepts the one-shot operation.
// Busy attempts are deferred so they cannot hold up unrelated due deletions.
func (s *Store) ScheduleRetention(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > retentionScanLimit {
		return 0, ErrInvalid
	}
	created := 0
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		rows, err := tx.QueryContext(ctx, `SELECT d.workspace_id,d.job_id,d.attempt_id,d.kind,d.anchor_ms,d.due_ms
 FROM remote_retention_deadlines d
 WHERE d.due_ms<=? AND d.retry_after_ms<=? AND d.operation_id IS NULL
 AND ((d.kind='collected' AND EXISTS(SELECT 1 FROM collection_publications p WHERE p.workspace_id=d.workspace_id AND p.job_id=d.job_id AND p.attempt_id=d.attempt_id))
 OR (d.kind='uncollected' AND NOT EXISTS(SELECT 1 FROM collection_publications p WHERE p.workspace_id=d.workspace_id AND p.job_id=d.job_id AND p.attempt_id=d.attempt_id)))
 AND NOT EXISTS(SELECT 1 FROM operations c WHERE c.kind='cleanup' AND c.workspace_id=d.workspace_id AND c.job_id=d.job_id AND c.attempt_id=d.attempt_id AND c.status IN ('accepted','running','succeeded'))
 AND NOT EXISTS(SELECT 1 FROM retired_explicit_attempts r WHERE r.workspace_id=d.workspace_id AND r.job_id=d.job_id AND r.attempt_id=d.attempt_id)
 ORDER BY d.due_ms,d.workspace_id,d.job_id,d.attempt_id LIMIT ?`, now.UnixMilli(), now.UnixMilli(), limit)
		if err != nil {
			return dbError(err)
		}
		var candidates []remoteDeadline
		for rows.Next() {
			var d remoteDeadline
			if err = rows.Scan(&d.workspace, &d.job, &d.attempt, &d.kind, &d.anchorMS, &d.dueMS); err != nil {
				_ = rows.Close()
				return dbError(err)
			}
			candidates = append(candidates, d)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return dbError(err)
		}
		for _, d := range candidates {
			if d.anchorMS <= 0 || d.dueMS < d.anchorMS || d.dueMS > now.UnixMilli() {
				return ErrCorrupt
			}
			delay, err := retentionSeconds(ctx, tx, d.workspace, d.job, d.kind)
			if err != nil {
				return err
			}
			if !delay.Valid || d.dueMS-d.anchorMS != delay.Int64*1000 {
				return ErrCorrupt
			}
			if d.kind == "collected" {
				var published string
				if err = tx.QueryRowContext(ctx, `SELECT verified_at FROM collection_publications WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(d.workspace), string(d.job), string(d.attempt)).Scan(&published); err != nil {
					return dbError(err)
				}
				at, parseErr := time.Parse(time.RFC3339Nano, published)
				// Pre-migration publications are rounded to the end of their
				// second so an upgrade cannot shorten the declared lifetime.
				minimum := ceilRetentionMS(at)
				if parseErr != nil || d.anchorMS < minimum || d.anchorMS > minimum+999 {
					return ErrCorrupt
				}
			}
			target, err := controlTarget(ctx, tx, d.workspace, d.job, d.attempt)
			if err != nil {
				return err
			}
			if d.kind == "uncollected" && !retentionSettled(target.Attempt.State) {
				return ErrCorrupt
			}
			unsafe, err := ambiguousPriorCleanup(ctx, tx, d)
			if err != nil {
				return err
			}
			if unsafe {
				if err = deferRemoteDeadline(ctx, tx, d, now); err != nil {
					return err
				}
				continue
			}
			request := operations.Request{AttemptID: d.attempt, DiscardUncollected: d.kind == "uncollected", Reason: "declared retention"}
			hash, err := request.Digest(d.job, domain.OperationCleanup)
			if err != nil {
				return ErrCorrupt
			}
			key := retentionOperationKey(d)
			command := operations.Command{WorkspaceID: d.workspace, TokenID: "retention_policy", JobID: d.job, Kind: domain.OperationCleanup,
				Request: request, KeyDigest: key, RequestHash: hash, Limits: admission.DefaultLimits()}
			receipt, err := admitCleanupOperation(ctx, tx, command, target, now)
			if err != nil {
				if !errors.Is(err, operations.ErrBusy) && !errors.Is(err, operations.ErrState) && !errors.Is(err, operations.ErrUnresolved) && !errors.Is(err, operations.ErrUncollected) {
					return err
				}
				if err = deferRemoteDeadline(ctx, tx, d, now); err != nil {
					return err
				}
				continue
			}
			res, err := tx.ExecContext(ctx, `UPDATE remote_retention_deadlines SET operation_id=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind=? AND operation_id IS NULL`,
				string(receipt.Operation.ID), string(d.workspace), string(d.job), string(d.attempt), d.kind)
			if err != nil {
				return dbError(err)
			}
			if changed, err := res.RowsAffected(); err != nil || changed != 1 {
				return ErrCorrupt
			}
			created++
			break // ClaimCleanup processes the accepted operation immediately.
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return created, nil
}

func deferRemoteDeadline(ctx context.Context, tx *sql.Tx, d remoteDeadline, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE remote_retention_deadlines SET retry_after_ms=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind=? AND operation_id IS NULL`,
		now.Add(retentionBusyDelay).UnixMilli(), string(d.workspace), string(d.job), string(d.attempt), d.kind)
	return dbError(err)
}

// A failed operation with a confirmed refusal can be followed by the declared
// due deletion. Unknown or still-running resource outcomes have no such proof;
// they retain their due row for inspection instead of being automatically retried.
func ambiguousPriorCleanup(ctx context.Context, tx *sql.Tx, d remoteDeadline) (bool, error) {
	var unsafe bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
 SELECT 1 FROM operations c WHERE c.kind='cleanup' AND c.workspace_id=? AND c.job_id=? AND c.attempt_id=? AND c.status='failed'
 AND (EXISTS(SELECT 1 FROM cleanup_resources r WHERE r.workspace_id=c.workspace_id AND r.operation_id=c.operation_id AND r.state IN ('running','unknown'))
 OR (NOT EXISTS(SELECT 1 FROM cleanup_resources r WHERE r.workspace_id=c.workspace_id AND r.operation_id=c.operation_id)
 AND json_extract(c.record,'$.Operation.Failure.Code')=?)))`,
		string(d.workspace), string(d.job), string(d.attempt), string(domain.CodeProviderStateUnknown)).Scan(&unsafe)
	return unsafe, dbError(err)
}

func retentionOperationKey(d remoteDeadline) string {
	raw, _ := json.Marshal(struct {
		Workspace domain.WorkspaceID `json:"workspace_id"`
		Job       domain.JobID       `json:"job_id"`
		Attempt   domain.AttemptID   `json:"attempt_id"`
		Kind      string             `json:"kind"`
	}{d.workspace, d.job, d.attempt, d.kind})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
