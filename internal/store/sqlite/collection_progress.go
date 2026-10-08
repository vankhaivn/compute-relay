package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func outputBytes(snapshot *collection.Snapshot) int64 {
	var total int64
	if snapshot != nil {
		for _, file := range snapshot.Files {
			if file.Role == "output" {
				total += file.Object.Bytes
			}
		}
	}
	return total
}

func initialProgress(w collection.Work, now time.Time) domain.CollectionProgress {
	p := domain.CollectionProgress{Scope: "selected_output_bytes", Generation: w.Lease.Generation, ObservedAt: now}
	if w.Snapshot != nil {
		total := outputBytes(w.Snapshot)
		p.BytesTotal = &total
	}
	return p
}

func saveCollectionProgress(ctx context.Context, tx *sql.Tx, l collection.Lease, stage string, p domain.CollectionProgress) error {
	if !p.Valid() || p.Generation != l.Generation {
		return collection.ErrInvalid
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return collection.ErrInvalid
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO collection_progress VALUES(?,?,?,?,?,?,?,?)
 ON CONFLICT(workspace_id,job_id,attempt_id) DO UPDATE SET operation_id=excluded.operation_id,
 generation=excluded.generation,fence=excluded.fence,stage=excluded.stage,progress=excluded.progress`,
		l.WorkspaceID, l.JobID, l.AttemptID, l.OperationID, l.Generation, l.Fence, stage, string(raw))
	return dbError(err)
}

// RecordCollectionProgress checks the same ownership as publication, but never
// advances attempt revision or emits chunk events. The engine bounds checkpoints.
func (s *Store) RecordCollectionProgress(ctx context.Context, w collection.Work, stage string, p domain.CollectionProgress, now time.Time) error {
	if stage != "discovering" && stage != "transferring" && stage != "verifying" || !p.ObservedAt.Equal(now) {
		return collection.ErrInvalid
	}
	return s.collectionTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		current, _, _, err := checkedCollection(ctx, tx, w, now)
		if err != nil {
			return err
		}
		if current.Snapshot == nil {
			if stage != "discovering" || p.BytesTotal != nil {
				return collection.ErrInvalid
			}
		} else if p.BytesTotal == nil || *p.BytesTotal != outputBytes(current.Snapshot) {
			return collection.ErrInvalid
		}
		return saveCollectionProgress(ctx, tx, w.Lease, stage, p)
	})
}

func finishCollectionProgress(ctx context.Context, tx *sql.Tx, w collection.Work, stage string, now time.Time) error {
	p := initialProgress(w, now)
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT progress FROM collection_progress WHERE workspace_id=? AND job_id=? AND attempt_id=? AND operation_id=? AND generation=? AND fence=?`,
		w.Lease.WorkspaceID, w.Lease.JobID, w.Lease.AttemptID, w.Lease.OperationID, w.Lease.Generation, w.Lease.Fence).Scan(&raw)
	if err == nil {
		if json.Unmarshal([]byte(raw), &p) != nil || !p.Valid() {
			return ErrCorrupt
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return dbError(err)
	}
	p.ObservedAt = now
	if stage == "available" {
		total := outputBytes(w.Snapshot)
		p.BytesTotal, p.BytesCompleted = &total, total
	}
	return saveCollectionProgress(ctx, tx, w.Lease, stage, p)
}

func readCollectionStatus(ctx context.Context, tx *sql.Tx, r admission.Record, now time.Time) (*domain.CollectionStatus, error) {
	a := r.Attempt
	status := &domain.CollectionStatus{Mode: r.Request.Spec().CollectionMode(), State: "not_ready", AttemptID: a.ID}
	if !a.State.Execution.Terminal() {
		return status, nil
	}
	status.State = "pending"
	if status.Mode == "manual" {
		status.State = "awaiting_request"
	}
	var id domain.OperationID
	var outcome string
	err := tx.QueryRowContext(ctx, `SELECT operation_id,status FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind='collect' ORDER BY operation_seq DESC LIMIT 1`, r.Job.WorkspaceID, r.Job.ID, a.ID).Scan(&id, &outcome)
	if err == nil {
		status.OperationID = &id
		status.State = "pending"
		if outcome == "failed" {
			status.State = "failed"
		} else if outcome != "accepted" && outcome != "succeeded" {
			status.State = "unknown"
		}
		var raw, stage string
		var until int64
		var held bool
		err = tx.QueryRowContext(ctx, `SELECT p.stage,p.progress,c.until_ms,c.held FROM collection_progress p JOIN collection_leases c
 ON c.workspace_id=p.workspace_id AND c.job_id=p.job_id AND c.attempt_id=p.attempt_id
 AND c.operation_id=p.operation_id AND c.generation=p.generation AND c.fence=p.fence
 WHERE p.workspace_id=? AND p.job_id=? AND p.attempt_id=? AND p.operation_id=?`, r.Job.WorkspaceID, r.Job.ID, a.ID, id).Scan(&stage, &raw, &until, &held)
		if err == nil {
			var p domain.CollectionProgress
			if json.Unmarshal([]byte(raw), &p) != nil || !p.Valid() {
				return nil, ErrCorrupt
			}
			status.Progress = &p
			if outcome == "accepted" && held && now.UnixMilli() < until {
				status.State = stage
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, dbError(err)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, dbError(err)
	}
	if a.State.Result == domain.ResultAvailable {
		status.State = "available"
	} else if a.State.Result == domain.ResultExpired {
		status.State = "expired"
	}
	return status, nil
}
