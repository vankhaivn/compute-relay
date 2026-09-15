package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

type retentionItem struct {
	sequence         int64
	deletion         retention.Deletion
	recorded         time.Time
	expired, deleted sql.NullString
}

func readRetentionItem(ctx context.Context, tx *sql.Tx, kind retention.Kind, w domain.WorkspaceID, id domain.ObjectID) (retentionItem, error) {
	var r retentionItem
	var at string
	r.deletion.Kind = kind
	err := tx.QueryRowContext(ctx, `SELECT inventory_seq,workspace_id,object_id,bytes,sha256,recorded_at,expired_at,deleted_at
 FROM retention_inventory WHERE kind=? AND workspace_id=? AND object_id=?`, string(kind), string(w), string(id)).Scan(&r.sequence, &r.deletion.Object.WorkspaceID, &r.deletion.Object.ID, &r.deletion.Object.Bytes, &r.deletion.Object.SHA256, &at, &r.expired, &r.deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return r, admission.ErrNotFound
	}
	if err != nil {
		return r, dbError(err)
	}
	r.recorded, err = time.Parse(time.RFC3339Nano, at)
	if err != nil || !r.deletion.Valid() || r.sequence < 1 {
		return r, ErrCorrupt
	}
	if (r.expired.Valid && !validTime(r.expired.String)) || (r.deleted.Valid && (!r.expired.Valid || !validTime(r.deleted.String))) {
		return r, ErrCorrupt
	}
	return r, nil
}

func retentionHeld(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, kind, id string) (bool, error) {
	var held bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM retention_holds WHERE workspace_id=? AND target_kind=? AND target_id=? AND active=1)`, string(w), kind, id).Scan(&held)
	return held, dbError(err)
}

// SetRetentionHold is local operator configuration, not an application route. Holds
// cannot resurrect expired data. Releasing one named hold cannot release other holds.
func (s *Store) SetRetentionHold(ctx context.Context, w domain.WorkspaceID, kind, id, hold string, active bool, now time.Time) error {
	if !w.Valid() || !domain.ObjectID(id).Valid() || !domain.ObjectID(hold).Valid() {
		return retention.ErrInvalid
	}
	return s.collectionTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		var query string
		switch kind {
		case "input":
			query = `SELECT EXISTS(SELECT 1 FROM objects WHERE workspace_id=? AND object_id=?)`
		case "job":
			query = `SELECT EXISTS(SELECT 1 FROM jobs WHERE workspace_id=? AND job_id=?)`
		case "attempt":
			query = `SELECT EXISTS(SELECT 1 FROM attempts WHERE workspace_id=? AND attempt_id=?)`
		default:
			return retention.ErrInvalid
		}
		var found bool
		if err := tx.QueryRowContext(ctx, query, string(w), id).Scan(&found); err != nil {
			return dbError(err)
		}
		if !found {
			return admission.ErrNotFound
		}
		if active {
			switch kind {
			case "input":
				item, err := readRetentionItem(ctx, tx, retention.Input, w, domain.ObjectID(id))
				if err != nil {
					return err
				}
				if item.expired.Valid {
					return retention.ErrExpired
				}
			case "job", "attempt":
				clause := "job_id"
				if kind == "attempt" {
					clause = "attempt_id"
				}
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM retention_expirations WHERE workspace_id=? AND `+clause+`=?)`, string(w), id).Scan(&found); err != nil {
					return dbError(err)
				}
				if found {
					return retention.ErrExpired
				}
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM job_objects j JOIN retention_inventory r ON r.kind='input' AND r.workspace_id=j.workspace_id AND r.object_id=j.object_id JOIN attempts a ON a.workspace_id=j.workspace_id AND a.job_id=j.job_id WHERE a.workspace_id=? AND a.`+clause+`=? AND r.expired_at IS NOT NULL)`, string(w), id).Scan(&found); err != nil {
					return dbError(err)
				}
				if found {
					return retention.ErrExpired
				}
			}
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM retention_holds WHERE workspace_id=? AND target_kind=? AND target_id=?`, string(w), kind, id).Scan(&count); err != nil {
			return dbError(err)
		}
		var previous bool
		err := tx.QueryRowContext(ctx, `SELECT active FROM retention_holds WHERE workspace_id=? AND target_kind=? AND target_id=? AND hold_id=?`, string(w), kind, id, hold).Scan(&previous)
		if err == nil && previous == active {
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		if errors.Is(err, sql.ErrNoRows) && count >= 100 {
			return retention.ErrPinned
		}
		if errors.Is(err, sql.ErrNoRows) && !active {
			return nil
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO retention_holds VALUES(?,?,?,?,?,?) ON CONFLICT(workspace_id,target_kind,target_id,hold_id) DO UPDATE SET active=excluded.active,updated_at=excluded.updated_at`, string(w), kind, id, hold, active, now.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
		action := "release"
		if active {
			action = "hold"
		}
		return retentionAudit(ctx, tx, w, kind, id, action, now)
	})
}

func retentionAudit(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, kind, id, action string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO retention_audit(workspace_id,target_kind,target_id,action,occurred_at) VALUES(?,?,?,?,?)`, string(w), kind, id, action, now.UTC().Format(time.RFC3339Nano))
	return dbError(err)
}

func retentionReference(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID) (retention.Reference, error) {
	var ref retention.Reference
	target, err := controlTarget(ctx, tx, w, job, attempt)
	if err != nil {
		return ref, err
	}
	ref.State, ref.UpdatedAt = target.Attempt.State, target.Attempt.UpdatedAt
	for kind, id := range map[string]string{"job": string(job), "attempt": string(attempt)} {
		held, err := retentionHeld(ctx, tx, w, kind, id)
		if err != nil {
			return ref, err
		}
		ref.Held = ref.Held || held
	}
	var busy bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM scheduler_queue q JOIN scheduler_leases l ON l.queue_seq=q.queue_seq WHERE q.workspace_id=? AND q.job_id=? AND q.attempt_id=? AND l.held=1) OR EXISTS(SELECT 1 FROM collection_leases WHERE workspace_id=? AND job_id=? AND attempt_id=? AND held=1) OR EXISTS(SELECT 1 FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND status IN ('accepted','running'))`, string(w), string(job), string(attempt), string(w), string(job), string(attempt), string(w), string(job), string(attempt)).Scan(&busy); err != nil {
		return ref, dbError(err)
	}
	ref.Busy = busy
	work, err := controlWork(ctx, tx, target)
	if errors.Is(err, operations.ErrUnresolved) {
		ref.RecoveryRequired = true
		return ref, nil
	}
	if err != nil {
		return ref, err
	}
	j := work.Journal
	if ref.State.Execution.Terminal() {
		if j.Observation == nil || j.Remote == nil || j.Observation.Execution != ref.State.Execution || j.Phase != dispatch.Collectible {
			ref.RecoveryRequired = true
			return ref, nil
		}
		var verified string
		err := tx.QueryRowContext(ctx, `SELECT verified_at FROM collection_publications WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(w), string(job), string(attempt)).Scan(&verified)
		if errors.Is(err, sql.ErrNoRows) {
			ref.RecoveryRequired = true
			return ref, nil
		}
		if err != nil {
			return ref, dbError(err)
		}
		ref.UpdatedAt, err = time.Parse(time.RFC3339Nano, verified)
		if err != nil {
			return ref, ErrCorrupt
		}
	} else if j.SubmitStarted && j.Phase != dispatch.Rejected {
		ref.RecoveryRequired = true
	} else if j.Plan != nil && !j.SubmitStarted && (j.Prepared == nil || !j.Prepared.Ready || !j.Prepared.Private) {
		ref.RecoveryRequired = true
	}
	return ref, nil
}

func retentionInputReferences(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id domain.ObjectID) ([]retention.Reference, error) {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT a.job_id,a.attempt_id FROM job_objects j JOIN attempts a ON a.workspace_id=j.workspace_id AND a.job_id=j.job_id WHERE j.workspace_id=? AND j.object_id=? ORDER BY a.job_id,a.attempt_id LIMIT ?`, string(w), string(id), retention.MaxReferences+1)
	if err != nil {
		return nil, dbError(err)
	}
	type key struct {
		job     domain.JobID
		attempt domain.AttemptID
	}
	keys := []key{}
	for rows.Next() {
		var k key
		if err = rows.Scan(&k.job, &k.attempt); err != nil {
			rows.Close()
			return nil, dbError(err)
		}
		keys = append(keys, k)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, dbError(err)
	}
	if len(keys) > retention.MaxReferences {
		return nil, retention.ErrPinned
	}
	refs := make([]retention.Reference, 0, len(keys))
	for _, k := range keys {
		r, err := retentionReference(ctx, tx, w, k.job, k.attempt)
		if err != nil {
			return nil, err
		}
		refs = append(refs, r)
	}
	return refs, nil
}

// ExpireRetention scans a bounded rotating page and commits only metadata tombstones.
// The cursor and decisions share the transaction; a failed commit authorizes no I/O.
func (s *Store) ExpireRetention(ctx context.Context, now time.Time, p retention.Policy, limit int) (retention.SweepReport, error) {
	var report retention.SweepReport
	if !p.Valid() || limit < 1 || limit > 100 {
		return report, retention.ErrInvalid
	}
	err := s.collectionTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		var cursor int64
		if err := tx.QueryRowContext(ctx, `SELECT inventory_seq FROM retention_cursor WHERE singleton=1`).Scan(&cursor); err != nil {
			return dbError(err)
		}
		rows, err := tx.QueryContext(ctx, `SELECT kind,workspace_id,object_id FROM retention_inventory WHERE inventory_seq>? AND expired_at IS NULL ORDER BY inventory_seq LIMIT ?`, cursor, limit)
		if err != nil {
			return dbError(err)
		}
		keys := []retention.Deletion{}
		for rows.Next() {
			var k retention.Deletion
			if err = rows.Scan(&k.Kind, &k.Object.WorkspaceID, &k.Object.ID); err != nil {
				rows.Close()
				return dbError(err)
			}
			keys = append(keys, k)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return dbError(err)
		}
		if len(keys) == 0 {
			_, err := tx.ExecContext(ctx, `UPDATE retention_cursor SET inventory_seq=0 WHERE singleton=1`)
			return dbError(err)
		}
		for _, k := range keys {
			item, err := readRetentionItem(ctx, tx, k.Kind, k.Object.WorkspaceID, k.Object.ID)
			if err != nil {
				return err
			}
			cursor = item.sequence
			report.Examined++
			if item.expired.Valid {
				continue
			}
			var n int
			if k.Kind == retention.Input {
				n, err = expireInput(ctx, tx, item, now, p)
			} else {
				n, err = expireResult(ctx, tx, item, now, p)
			}
			if err != nil {
				return err
			}
			report.Expired += n
		}
		_, err = tx.ExecContext(ctx, `UPDATE retention_cursor SET inventory_seq=? WHERE singleton=1`, cursor)
		return dbError(err)
	})
	if err != nil {
		return retention.SweepReport{}, err
	}
	return report, nil
}

func expireInput(ctx context.Context, tx *sql.Tx, item retentionItem, now time.Time, p retention.Policy) (int, error) {
	m := item.deletion.Object
	refs, err := retentionInputReferences(ctx, tx, m.WorkspaceID, m.ID)
	if err != nil {
		return 0, err
	}
	held, err := retentionHeld(ctx, tx, m.WorkspaceID, "input", string(m.ID))
	if err != nil {
		return 0, err
	}
	decision, err := p.Assess(now, item.recorded, refs, held)
	if err != nil {
		return 0, err
	}
	if !decision.Eligible {
		return 0, nil
	}
	var original domain.ObjectMetadata
	if err := tx.QueryRowContext(ctx, `SELECT workspace_id,object_id,bytes,sha256 FROM objects WHERE workspace_id=? AND object_id=?`, string(m.WorkspaceID), string(m.ID)).Scan(&original.WorkspaceID, &original.ID, &original.Bytes, &original.SHA256); err != nil {
		return 0, dbError(err)
	}
	if original != m {
		return 0, ErrCorrupt
	}
	if _, err := tx.ExecContext(ctx, `UPDATE retention_inventory SET expired_at=? WHERE inventory_seq=? AND expired_at IS NULL`, now.UTC().Format(time.RFC3339Nano), item.sequence); err != nil {
		return 0, dbError(err)
	}
	if err := retentionAudit(ctx, tx, m.WorkspaceID, "input", string(m.ID), "expire", now); err != nil {
		return 0, err
	}
	return 1, nil
}

func expireResult(ctx context.Context, tx *sql.Tx, item retentionItem, now time.Time, p retention.Policy) (int, error) {
	m := item.deletion.Object
	var job domain.JobID
	var attempt domain.AttemptID
	if err := tx.QueryRowContext(ctx, `SELECT job_id,attempt_id FROM artifacts WHERE workspace_id=? AND object_id=?`, string(m.WorkspaceID), string(m.ID)).Scan(&job, &attempt); err != nil {
		return 0, dbError(err)
	}
	ref, err := retentionReference(ctx, tx, m.WorkspaceID, job, attempt)
	if err != nil {
		return 0, err
	}
	var count, tracked, expired int
	var at string
	if err := tx.QueryRowContext(ctx, `SELECT p.file_count,count(r.inventory_seq),COALESCE(sum(r.expired_at IS NOT NULL),0),COALESCE(max(r.recorded_at),'') FROM collection_publications p JOIN artifacts a ON a.workspace_id=p.workspace_id AND a.job_id=p.job_id AND a.attempt_id=p.attempt_id LEFT JOIN retention_inventory r ON r.kind='result' AND r.workspace_id=a.workspace_id AND r.object_id=a.object_id AND r.bytes=a.bytes AND r.sha256=a.sha256 WHERE p.workspace_id=? AND p.job_id=? AND p.attempt_id=? GROUP BY p.file_count`, string(m.WorkspaceID), string(job), string(attempt)).Scan(&count, &tracked, &expired, &at); err != nil {
		return 0, dbError(err)
	}
	if count < 1 || count > collection.MaxFiles || tracked != count || expired != 0 {
		return 0, ErrCorrupt
	}
	recorded, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return 0, ErrCorrupt
	}
	decision, err := p.Assess(now, recorded, []retention.Reference{ref}, false)
	if err != nil {
		return 0, err
	}
	if !decision.Eligible {
		return 0, nil
	}
	a, _, err := loadAttempt(ctx, tx, m.WorkspaceID, job, attempt)
	if err != nil {
		return 0, err
	}
	if a.State.Result != domain.ResultAvailable {
		return 0, ErrCorrupt
	}
	state := a.State
	state.Result = domain.ResultExpired
	next, err := a.Transition(state, now.UTC())
	if err != nil || next.Revision > math.MaxInt64 {
		return 0, ErrInvalid
	}
	raw, err := json.Marshal(next.State)
	if err != nil {
		return 0, ErrInvalid
	}
	res, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?,orchestration=?,revision=?,updated_at=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND revision=?`, string(raw), string(next.State.Orchestration), next.Revision, next.UpdatedAt.Format(time.RFC3339Nano), string(m.WorkspaceID), string(job), string(attempt), a.Revision)
	if err != nil {
		return 0, dbError(err)
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return 0, retention.ErrChanged
	}
	stamp := now.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO retention_expirations VALUES(?,?,?,?)`, string(m.WorkspaceID), string(job), string(attempt), stamp); err != nil {
		return 0, dbError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE retention_inventory SET expired_at=? WHERE kind='result' AND workspace_id=? AND object_id IN(SELECT object_id FROM artifacts WHERE workspace_id=? AND job_id=? AND attempt_id=?) AND expired_at IS NULL`, stamp, string(m.WorkspaceID), string(m.WorkspaceID), string(job), string(attempt)); err != nil {
		return 0, dbError(err)
	}
	if err := retentionAudit(ctx, tx, m.WorkspaceID, "attempt", string(attempt), "expire", now); err != nil {
		return 0, err
	}
	return count, nil
}
