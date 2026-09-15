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

type controlRecord struct {
	operations.Record
	Started bool
}

func decodeControl(raw string, limit int) (operations.Record, error) {
	var r operations.Record
	d := json.NewDecoder(bytes.NewBufferString(raw))
	d.DisallowUnknownFields()
	if len(raw) > limit || d.Decode(&r) != nil || d.Decode(new(any)) != io.EOF || r.Validate() != nil || r.Replay {
		return operations.Record{}, ErrCorrupt
	}
	return r, nil
}

func loadControl(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id domain.OperationID) (controlRecord, error) {
	var r controlRecord
	var job, attempt, kind, status, created, hash, request, raw string
	var revision uint64
	var newAttempt sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record,new_attempt_id,cancel_started
 FROM operations WHERE workspace_id=? AND operation_id=?`, string(w), string(id)).Scan(&job, &attempt, &kind, &status, &revision, &created, &hash, &request, &raw, &newAttempt, &r.Started)
	if errors.Is(err, sql.ErrNoRows) {
		return r, operations.ErrNotFound
	}
	if err != nil {
		return r, dbError(err)
	}
	r.Record, err = decodeControl(raw, 8192)
	if err != nil {
		return controlRecord{}, err
	}
	op := r.Operation
	req, err := operations.Parse(op.Kind, []byte(request))
	if err != nil {
		return controlRecord{}, ErrCorrupt
	}
	wantHash, err := req.Digest(op.JobID, op.Kind)
	if err != nil || wantHash != hash || req.AttemptID != op.AttemptID || op.ID != id || op.WorkspaceID != w ||
		string(op.JobID) != job || string(op.AttemptID) != attempt || string(op.Kind) != kind || string(op.Status) != status ||
		op.Revision != revision || op.CreatedAt.UTC().Format(time.RFC3339Nano) != created || string(r.NewAttemptID) != newAttempt.String ||
		r.Started && (op.Kind != domain.OperationCancel || op.Status == domain.OperationAccepted) {
		return controlRecord{}, ErrCorrupt
	}
	return r, nil
}

func replayControl(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, kind domain.OperationKind, key, hash string) (*operations.Record, error) {
	var foundHash, version, id, raw string
	err := tx.QueryRowContext(ctx, `SELECT request_sha256,canonical_version,operation_id,receipt FROM operation_idempotency WHERE workspace_id=? AND kind=? AND key_sha256=?`, string(w), string(kind), key).Scan(&foundHash, &version, &id, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	if version != operations.CanonicalVersion {
		return nil, ErrSchema
	}
	if foundHash != hash {
		return nil, operations.ErrConflict
	}
	r, err := decodeControl(raw, 4096)
	if err != nil || string(r.Operation.ID) != id || r.Operation.WorkspaceID != w || r.Operation.Kind != kind {
		return nil, ErrCorrupt
	}
	// The receipt deliberately retains its original outcome, not the latest status.
	current, err := loadControl(ctx, tx, w, r.Operation.ID)
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
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return nil, err
	}
	defer done()
	var result *operations.Record
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		var err error
		result, err = replayControl(ctx, tx, w, kind, key, hash)
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
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return operations.Record{}, err
	}
	defer done()
	var result operations.Record
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Read); err != nil {
			return err
		}
		r, err := loadControl(ctx, tx, w, id)
		result = r.Record
		return err
	})
	if err != nil {
		return operations.Record{}, err
	}
	return result, nil
}

func insertControl(ctx context.Context, tx *sql.Tx, c operations.Command, r operations.Record) error {
	if r.Validate() != nil || r.Replay || r.Operation.Revision != 1 || r.Operation.Status != domain.OperationAccepted {
		return ErrInvalid
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > 8192 {
		return ErrInvalid
	}
	request, err := json.Marshal(c.Request)
	if err != nil {
		return ErrInvalid
	}
	op := r.Operation
	_, err = tx.ExecContext(ctx, `INSERT INTO operations(workspace_id,operation_id,job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record)
 VALUES(?,?,?,?,?,?,?,?,?,?,?)`, string(op.WorkspaceID), string(op.ID), string(op.JobID), string(op.AttemptID), string(op.Kind), string(op.Status), op.Revision, op.CreatedAt.UTC().Format(time.RFC3339Nano), c.RequestHash, string(request), string(raw))
	return dbError(err)
}

func saveControl(ctx context.Context, tx *sql.Tx, before controlRecord, after controlRecord) error {
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

func finishControl(ctx context.Context, tx *sql.Tx, before controlRecord, status domain.OperationStatus, effect operations.Effect, problem *domain.Problem, now time.Time) (controlRecord, error) {
	if before.Operation.Status.Terminal() {
		return before, operations.ErrChanged
	}
	after := before
	var err error
	after.Operation, err = before.Operation.Transition(status, problem, now)
	if err != nil {
		return controlRecord{}, err
	}
	after.Effect = effect
	after.TerminationConfirmed = effect == operations.CancellationConfirmed
	if err = saveControl(ctx, tx, before, after); err != nil {
		return controlRecord{}, err
	}
	if status.Terminal() {
		if err = controlEvent(ctx, tx, after.Operation.WorkspaceID, after.Operation.JobID, after.Operation.AttemptID, after.Operation.ID, domain.EventOperationCompleted, now); err != nil {
			return controlRecord{}, err
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

func saveControlReceipt(ctx context.Context, tx *sql.Tx, c operations.Command, r operations.Record) error {
	r.Replay = false
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > 4096 {
		return ErrInvalid
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO operation_idempotency VALUES(?,?,?,?,?,?,?)`, string(c.WorkspaceID), string(c.Kind), c.KeyDigest, c.RequestHash, operations.CanonicalVersion, string(r.Operation.ID), string(raw))
	return dbError(err)
}
