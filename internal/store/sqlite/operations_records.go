package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

type operationRow struct {
	operations.Record
	Started  bool
	Request  operations.Request
	Resource domain.ProviderResourceID
}

func decodeOperation(raw string, limit int) (operations.Record, error) {
	var r operations.Record
	d := json.NewDecoder(bytes.NewBufferString(raw))
	d.DisallowUnknownFields()
	if len(raw) > limit || d.Decode(&r) != nil || d.Decode(new(any)) != io.EOF || r.Validate() != nil || r.Replay {
		return operations.Record{}, ErrCorrupt
	}
	return r, nil
}

func loadOperation(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id domain.OperationID) (operationRow, error) {
	var r operationRow
	var job, attempt, kind, status, created, hash, request, raw string
	var revision uint64
	var newAttempt, resource sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record,new_attempt_id,cancel_started,resource_id
 FROM operations WHERE workspace_id=? AND operation_id=?`, string(w), string(id)).Scan(&job, &attempt, &kind, &status, &revision, &created, &hash, &request, &raw, &newAttempt, &r.Started, &resource)
	if errors.Is(err, sql.ErrNoRows) {
		return r, operations.ErrNotFound
	}
	if err != nil {
		return r, dbError(err)
	}
	r.Record, err = decodeOperation(raw, 8192)
	if err != nil {
		return operationRow{}, err
	}
	op := r.Operation
	req, wantHash, err := decodeStoredOperationRequest(op.JobID, op.Kind, []byte(request))
	if err != nil {
		return operationRow{}, ErrCorrupt
	}
	if wantHash != hash || req.AttemptID != op.AttemptID || op.ID != id || op.WorkspaceID != w ||
		string(op.JobID) != job || string(op.AttemptID) != attempt || string(op.Kind) != kind || string(op.Status) != status ||
		op.Revision != revision || op.CreatedAt.UTC().Format(time.RFC3339Nano) != created || string(r.NewAttemptID) != newAttempt.String ||
		r.Started && (op.Kind != domain.OperationCancel || op.Status == domain.OperationAccepted) {
		return operationRow{}, ErrCorrupt
	}
	r.Request, r.Resource = req, domain.ProviderResourceID(resource.String)
	if (op.Kind == domain.OperationCleanup) != r.Resource.Valid() || resource.Valid && op.Kind != domain.OperationCleanup {
		return operationRow{}, ErrCorrupt
	}
	return r, nil
}

func replayOperation(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, kind domain.OperationKind, key, hash string) (*operations.Record, error) {
	prior, found, err := replayOrCreate(ctx, tx, receiptKey{w, "operation", string(kind), key, hash, operations.CanonicalVersion(kind), operations.ErrConflict}, nil)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	id, raw := prior.subject, prior.raw
	r, err := decodeOperation(raw, 4096)
	if err != nil || string(r.Operation.ID) != id || r.Operation.WorkspaceID != w || r.Operation.Kind != kind {
		return nil, ErrCorrupt
	}
	// The receipt deliberately retains its original outcome, not the latest status.
	current, err := loadOperation(ctx, tx, w, r.Operation.ID)
	if err != nil {
		if errors.Is(err, operations.ErrNotFound) {
			return nil, ErrCorrupt
		}
		return nil, err
	}
	if current.Operation.JobID != r.Operation.JobID || current.Operation.AttemptID != r.Operation.AttemptID {
		return nil, ErrCorrupt
	}
	r.Replay = true
	return &r, nil
}

func (s *Store) ReplayOperation(ctx context.Context, w domain.WorkspaceID, token string, kind domain.OperationKind, key, hash string) (*operations.Record, error) {
	if !operations.Supported(kind) || !domain.SHA256Digest(key).Valid() || !domain.SHA256Digest(hash).Valid() {
		return nil, operations.ErrRequest
	}
	var result *operations.Record
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		var err error
		result, err = replayOperation(ctx, tx, w, kind, key, hash)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) ReadOperation(ctx context.Context, w domain.WorkspaceID, token string, id domain.OperationID) (operations.Record, error) {
	if !w.Valid() || !id.Valid() {
		return operations.Record{}, operations.ErrNotFound
	}
	var result operations.Record
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Read); err != nil {
			return err
		}
		r, err := loadOperation(ctx, tx, w, id)
		if err != nil {
			return err
		}
		result = r.Record
		if r.Operation.Kind == domain.OperationCleanup {
			resources, err := loadCleanupResources(ctx, tx, r.Operation)
			if err != nil {
				return err
			}
			for _, resource := range resources {
				result.Resources = append(result.Resources, resource.Result)
			}
		}
		return nil
	})
	if err != nil {
		return operations.Record{}, err
	}
	return result, nil
}

func insertOperation(ctx context.Context, tx *sql.Tx, c operations.Command, row operationRow) error {
	r := row.Record
	if r.Validate() != nil || r.Replay || r.Operation.Revision != 1 || r.Operation.Status != domain.OperationAccepted {
		return ErrInvalid
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > 8192 {
		return ErrInvalid
	}
	request, err := c.Request.CanonicalJSON(c.Kind)
	if err != nil {
		return ErrInvalid
	}
	op := r.Operation
	var resource any
	if op.Kind == domain.OperationCleanup {
		if !row.Resource.Valid() {
			return ErrInvalid
		}
		resource = string(row.Resource)
	} else if row.Resource != "" {
		return ErrInvalid
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO operations(workspace_id,operation_id,job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record,resource_id)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, string(op.WorkspaceID), string(op.ID), string(op.JobID), string(op.AttemptID), string(op.Kind), string(op.Status), op.Revision, op.CreatedAt.UTC().Format(time.RFC3339Nano), c.RequestHash, string(request), string(raw), resource)
	return dbError(err)
}

func saveOperation(ctx context.Context, tx *sql.Tx, before operationRow, after operationRow) error {
	if after.Validate() != nil || after.Replay || after.Operation.Revision != before.Operation.Revision+1 || after.Operation.Revision > math.MaxInt64 ||
		after.Operation.ID != before.Operation.ID || after.Operation.WorkspaceID != before.Operation.WorkspaceID || after.Operation.JobID != before.Operation.JobID || after.Operation.AttemptID != before.Operation.AttemptID || after.Operation.Kind != before.Operation.Kind || !after.Operation.CreatedAt.Equal(before.Operation.CreatedAt) || before.Started && !after.Started {
		return ErrInvalid
	}
	want, err := before.Operation.Transition(after.Operation.Status, after.Operation.Failure, after.Operation.UpdatedAt)
	if err != nil || want.Revision != after.Operation.Revision {
		return ErrInvalid
	}
	raw, err := json.Marshal(after.Record)
	if err != nil || len(raw) > 8192 {
		return ErrInvalid
	}
	var newAttempt any
	if after.NewAttemptID != "" {
		newAttempt = string(after.NewAttemptID)
	}
	res, err := tx.ExecContext(ctx, `UPDATE operations SET status=?,revision=?,record=?,new_attempt_id=?,cancel_started=? WHERE workspace_id=? AND operation_id=? AND revision=?`, string(after.Operation.Status), after.Operation.Revision, string(raw), newAttempt, after.Started, string(after.Operation.WorkspaceID), string(after.Operation.ID), before.Operation.Revision)
	if err != nil {
		return dbError(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if n != 1 {
		return operations.ErrChanged
	}
	return nil
}

func finishControl(ctx context.Context, tx *sql.Tx, before operationRow, status domain.OperationStatus, effect operations.Effect, problem *domain.Problem, now time.Time) (operationRow, error) {
	if before.Operation.Status.Terminal() {
		return before, operations.ErrChanged
	}
	after := before
	var err error
	after.Operation, err = before.Operation.Transition(status, problem, now)
	if err != nil {
		return operationRow{}, err
	}
	after.Effect = effect
	after.TerminationConfirmed = effect == operations.CancellationConfirmed
	if err = saveOperation(ctx, tx, before, after); err != nil {
		return operationRow{}, err
	}
	if status.Terminal() && after.Operation.Kind != domain.OperationCleanup {
		if err = controlEvent(ctx, tx, after.Operation.WorkspaceID, after.Operation.JobID, after.Operation.AttemptID, after.Operation.ID, domain.EventOperationCompleted, now); err != nil {
			return operationRow{}, err
		}
	}
	return after, nil
}

func controlEvent(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, op domain.OperationID, kind domain.EventType, now time.Time) error {
	var sequence int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(max(sequence),0) FROM events WHERE workspace_id=? AND job_id=?", string(w), string(job)).Scan(&sequence); err != nil {
		return dbError(err)
	}
	if sequence == math.MaxInt64 {
		return ErrConflict
	}
	id, err := randomID("evt_", 16)
	if err != nil {
		return err
	}
	e := domain.Event{ID: domain.EventID(id), Sequence: uint64(sequence + 1), WorkspaceID: w, JobID: job, AttemptID: attempt, OperationID: op, Type: kind, OccurredAt: now}
	if e.Validate() != nil {
		return ErrInvalid
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO events(workspace_id,job_id,event_id,sequence,attempt_id,operation_id,type,occurred_at) VALUES(?,?,?,?,?,?,?,?)`, string(w), string(job), id, sequence+1, string(attempt), string(op), string(kind), now.UTC().Format(time.RFC3339Nano))
	return dbError(err)
}

func saveOperationReceipt(ctx context.Context, tx *sql.Tx, c operations.Command, r operations.Record) error {
	r.Replay = false
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > 4096 {
		return ErrInvalid
	}
	_, _, err = replayOrCreate(ctx, tx, receiptKey{c.WorkspaceID, "operation", string(c.Kind), c.KeyDigest, c.RequestHash, operations.CanonicalVersion(c.Kind), operations.ErrConflict}, &savedReceipt{string(r.Operation.ID), r.Operation.JobID, r.Operation.AttemptID, string(raw)})
	return err
}
