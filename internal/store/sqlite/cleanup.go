package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var _ cleanup.Repository = (*Store)(nil)

// preparedVersion marks a reference to a prepared, never-executed attempt resource.
const preparedVersion = "prepared"

// cleanupTarget is internal ownership evidence for one attempt. It is never serialized
// to applications and is rebuilt from the ledger whenever a worker claims the request.
type cleanupTarget struct {
	resource  domain.ProviderResourceID
	creation  domain.OperationID
	remote    provider.RemoteReference
	binding   provider.BindingSnapshot
	collected bool
	resources []cleanup.Resource
}

// eligibleCleanup applies every admission rule inside the caller's transaction. The
// per-attempt resource is created during preparation, so the target is always the exact
// staging ledger entry; an executed attempt is addressed by its recorded execution reference.
func eligibleCleanup(ctx context.Context, tx *sql.Tx, target admission.Record, discard bool, now time.Time, self domain.OperationID) (cleanupTarget, error) {
	result, err := eligibleCleanupAttempt(ctx, tx, target, discard, now, self)
	if err != nil {
		return result, err
	}
	result.resources, err = cleanupResources(ctx, tx, target)
	if err != nil {
		return result, err
	}
	if len(result.resources) == 0 {
		return result, operations.ErrState
	}
	for _, r := range result.resources {
		if r.Reason != "" {
			return result, operations.ErrUnresolved
		}
	}
	return result, nil
}

func eligibleCleanupAttempt(ctx context.Context, tx *sql.Tx, target admission.Record, discard bool, now time.Time, self domain.OperationID) (cleanupTarget, error) {
	var result cleanupTarget
	w, job, attempt := string(target.Job.WorkspaceID), string(target.Job.ID), string(target.Attempt.ID)
	at := now.UnixMilli()
	var busy bool
	err := tx.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM scheduler_queue q JOIN work_leases l ON l.kind='dispatch' AND l.key=json_array(q.queue_seq)
 WHERE q.workspace_id=? AND q.job_id=? AND q.attempt_id=? AND l.held=1 AND l.until_ms>?)
 OR EXISTS(SELECT 1 FROM work_leases WHERE kind='collection' AND key=json_array(?,?,?) AND held=1 AND until_ms>?)
 OR EXISTS(SELECT 1 FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND status IN ('accepted','running'))
 OR EXISTS(SELECT 1 FROM cleanup_operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND status IN ('accepted','running') AND operation_id!=?)`,
		w, job, attempt, at, w, job, attempt, at, w, job, attempt, w, job, attempt, string(self)).Scan(&busy)
	if err != nil {
		return result, dbError(err)
	}
	if busy {
		return result, operations.ErrBusy
	}
	work, err := controlWork(ctx, tx, target)
	if err != nil {
		return result, err
	}
	s, j := target.Attempt.State, work.Journal
	if s.RemoteActivity != domain.RemoteActivityNotStarted && s.RemoteActivity != domain.RemoteActivityInactive {
		return result, operations.ErrUnresolved
	}
	executed := s.Execution != domain.ExecutionNotSubmitted
	if executed && !s.Execution.Terminal() {
		return result, operations.ErrUnresolved
	}
	if j.SubmitStarted {
		if executed && (j.Remote == nil || j.Observation == nil || j.Observation.Execution != s.Execution) || !executed && j.Phase != dispatch.Rejected {
			return result, operations.ErrUnresolved
		}
	} else if executed {
		return result, operations.ErrUnresolved
	}
	if !s.Orchestration.Terminal() && (s.Orchestration != domain.OrchestrationNeedsAttention || !executed || s.RemoteActivity != domain.RemoteActivityInactive) {
		return result, operations.ErrState
	}
	if j.Plan == nil || j.Prepared == nil {
		return result, operations.ErrState // Nothing Relay created remotely is recorded.
	}
	var ref string
	err = tx.QueryRowContext(ctx, `SELECT resource_id,resource_ref FROM provider_resources WHERE workspace_id=? AND job_id=? AND attempt_id=? AND purpose='staging' AND operation_id=?`, w, job, attempt, string(j.PreparationID)).Scan(&result.resource, &ref)
	if errors.Is(err, sql.ErrNoRows) {
		return result, operations.ErrState
	}
	if err != nil {
		return result, dbError(err)
	}
	if ref == "" {
		return result, operations.ErrState
	}
	if ref != j.Prepared.Resource || !result.resource.Valid() {
		return result, ErrCorrupt
	}
	result.creation = j.PreparationID
	result.remote = provider.RemoteReference{Identity: j.Prepared.Identity, Resource: j.Prepared.Resource, Version: preparedVersion}
	if executed {
		result.remote = *j.Remote
	}
	if result.remote.Validate() != nil || result.remote.Identity != j.Plan.Job.Identity {
		return result, ErrCorrupt
	}
	if executed {
		var published bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM collection_publications WHERE workspace_id=? AND job_id=? AND attempt_id=?)`, w, job, attempt).Scan(&published); err != nil {
			return result, dbError(err)
		}
		result.collected = published && (s.Result == domain.ResultAvailable || s.Result == domain.ResultExpired)
		if !result.collected && !discard {
			return result, cleanup.ErrUncollected
		}
	}
	result.binding = provider.BindingSnapshot{Binding: target.Profile.Binding, AccountScope: target.Profile.AccountScope, CredentialRef: target.Profile.CredentialRef}
	if !result.binding.Valid() {
		return result, ErrCorrupt
	}
	return result, nil
}

// pendingCleanup blocks new transfer tickets once deletion of the attempt's remote data
// was requested or applied. It never changes an existing control.
func pendingCleanup(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID) error {
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM cleanup_operations c WHERE workspace_id=? AND job_id=? AND attempt_id=? AND
 (status IN ('accepted','running','succeeded')
 OR EXISTS(SELECT 1 FROM cleanup_resources r WHERE r.workspace_id=c.workspace_id AND r.operation_id=c.operation_id AND r.state IN ('running','deleted','absent','unknown'))
 OR (status='failed' AND json_extract(c.record,'$.Operation.Failure.Code')=?
 AND NOT EXISTS(SELECT 1 FROM cleanup_resources r WHERE r.workspace_id=c.workspace_id AND r.operation_id=c.operation_id))))`, string(w), string(job), string(attempt), string(domain.CodeProviderStateUnknown)).Scan(&exists); err != nil {
		return dbError(err)
	}
	if exists {
		return operations.ErrState
	}
	return nil
}

func decodeCleanup(raw string, limit int) (cleanup.Record, error) {
	var r cleanup.Record
	d := json.NewDecoder(bytes.NewBufferString(raw))
	d.DisallowUnknownFields()
	if len(raw) > limit || d.Decode(&r) != nil || d.Decode(new(any)) != io.EOF || r.Validate() != nil || r.Replay {
		return cleanup.Record{}, ErrCorrupt
	}
	return r, nil
}

type cleanupRow struct {
	record   cleanup.Record
	request  cleanup.Request
	resource domain.ProviderResourceID
}

func loadCleanup(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id domain.OperationID) (cleanupRow, error) {
	var row cleanupRow
	var job, attempt, status, created, hash, request, raw string
	var revision uint64
	err := tx.QueryRowContext(ctx, `SELECT job_id,attempt_id,resource_id,status,revision,created_at,request_sha256,request_json,record
 FROM cleanup_operations WHERE workspace_id=? AND operation_id=?`, string(w), string(id)).Scan(&job, &attempt, &row.resource, &status, &revision, &created, &hash, &request, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return row, operations.ErrNotFound
	}
	if err != nil {
		return row, dbError(err)
	}
	row.record, err = decodeCleanup(raw, 8192)
	if err != nil {
		return cleanupRow{}, err
	}
	op := row.record.Operation
	row.request, err = cleanup.Parse([]byte(request))
	if err != nil {
		return cleanupRow{}, ErrCorrupt
	}
	wantHash, err := row.request.Digest(op.JobID)
	if err != nil || wantHash != hash || row.request.AttemptID != op.AttemptID || op.ID != id || op.WorkspaceID != w ||
		string(op.JobID) != job || string(op.AttemptID) != attempt || string(op.Status) != status || op.Revision != revision ||
		op.CreatedAt.UTC().Format(time.RFC3339Nano) != created || !row.resource.Valid() {
		return cleanupRow{}, ErrCorrupt
	}
	return row, nil
}

func replayCleanup(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, key, hash string) (*cleanup.Record, error) {
	var foundHash, version, id, raw string
	err := tx.QueryRowContext(ctx, `SELECT request_sha256,canonical_version,operation_id,receipt FROM cleanup_idempotency WHERE workspace_id=? AND key_sha256=?`, string(w), key).Scan(&foundHash, &version, &id, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	if version != cleanup.CanonicalVersion {
		return nil, ErrSchema
	}
	if foundHash != hash {
		return nil, operations.ErrConflict
	}
	r, err := decodeCleanup(raw, 4096)
	if err != nil || string(r.Operation.ID) != id || r.Operation.WorkspaceID != w {
		return nil, ErrCorrupt
	}
	// The receipt keeps its original outcome, not the latest status.
	current, err := loadCleanup(ctx, tx, w, r.Operation.ID)
	if err != nil {
		if errors.Is(err, operations.ErrNotFound) {
			return nil, ErrCorrupt
		}
		return nil, err
	}
	if current.record.Operation.JobID != r.Operation.JobID || current.record.Operation.AttemptID != r.Operation.AttemptID {
		return nil, ErrCorrupt
	}
	r.Replay = true
	return &r, nil
}

func (s *Store) ReplayCleanup(ctx context.Context, w domain.WorkspaceID, token, key, hash string) (*cleanup.Record, error) {
	if !w.Valid() || !domain.SHA256Digest(key).Valid() || !domain.SHA256Digest(hash).Valid() {
		return nil, operations.ErrRequest
	}
	var result *cleanup.Record
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		var err error
		result, err = replayCleanup(ctx, tx, w, key, hash)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) ReadCleanup(ctx context.Context, w domain.WorkspaceID, token string, id domain.OperationID) (cleanup.Record, error) {
	if !w.Valid() || !id.Valid() {
		return cleanup.Record{}, operations.ErrNotFound
	}
	var result cleanup.Record
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Read); err != nil {
			return err
		}
		row, err := loadCleanup(ctx, tx, w, id)
		result = row.record
		if err != nil {
			return err
		}
		resources, err := loadCleanupResources(ctx, tx, result.Operation)
		for _, resource := range resources {
			result.Resources = append(result.Resources, resource.Result)
		}
		return err
	})
	if err != nil {
		return cleanup.Record{}, err
	}
	return result, nil
}

// AdmitCleanup records intent only. Coalescing returns the attempt's pending or already
// succeeded cleanup; a failed one never blocks a new explicit request.
func (s *Store) AdmitCleanup(ctx context.Context, c cleanup.Command) (cleanup.Record, error) {
	if c.Validate() != nil {
		return cleanup.Record{}, operations.ErrRequest
	}
	var result cleanup.Record
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, c.WorkspaceID, c.TokenID, auth.Operate); err != nil {
			return err
		}
		prior, err := replayCleanup(ctx, tx, c.WorkspaceID, c.KeyDigest, c.RequestHash)
		if err != nil {
			return err
		}
		if prior != nil {
			result = *prior
			return nil
		}
		if _, _, err = schedulerControl(ctx, tx); err != nil {
			return err
		}
		target, err := controlTarget(ctx, tx, c.WorkspaceID, c.JobID, c.Request.AttemptID)
		if err != nil {
			return err
		}
		if err = actionableAttempt(ctx, tx, c.WorkspaceID, c.JobID, c.Request.AttemptID); err != nil {
			return err
		}
		if now.Before(target.Attempt.UpdatedAt) {
			return ErrCorrupt
		}
		var existing string
		err = tx.QueryRowContext(ctx, `SELECT operation_id FROM cleanup_operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND status IN ('accepted','running','succeeded') ORDER BY cleanup_seq DESC LIMIT 1`, string(c.WorkspaceID), string(c.JobID), string(c.Request.AttemptID)).Scan(&existing)
		if err == nil {
			row, err := loadCleanup(ctx, tx, c.WorkspaceID, domain.OperationID(existing))
			if err != nil {
				return err
			}
			if err = saveCleanupReceipt(ctx, tx, c, row.record); err != nil {
				return err
			}
			result = row.record
			result.Replay = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		plan, err := eligibleCleanup(ctx, tx, target, c.Request.DiscardUncollected, now, "")
		if err != nil {
			return err
		}
		id, err := randomID("op_", 16)
		if err != nil {
			return err
		}
		op, err := domain.NewOperation(domain.OperationID(id), c.WorkspaceID, c.JobID, c.Request.AttemptID, domain.OperationCleanup, now)
		if err != nil {
			return ErrInvalid
		}
		r := cleanup.Record{Operation: op, Effect: cleanup.Pending}
		if err = insertCleanup(ctx, tx, c, r, plan.resource); err != nil {
			return err
		}
		if err = snapshotCleanupResources(ctx, tx, op, plan.resources); err != nil {
			return err
		}
		if err = saveCleanupReceipt(ctx, tx, c, r); err != nil {
			return err
		}
		result = r
		return nil
	})
	if err != nil {
		// No identifier or success escapes a failed or uncertain acknowledgement.
		return cleanup.Record{}, err
	}
	return result, nil
}

func insertCleanup(ctx context.Context, tx *sql.Tx, c cleanup.Command, r cleanup.Record, resource domain.ProviderResourceID) error {
	if r.Validate() != nil || r.Replay || r.Operation.Revision != 1 || r.Operation.Status != domain.OperationAccepted || !resource.Valid() {
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
	_, err = tx.ExecContext(ctx, `INSERT INTO cleanup_operations(workspace_id,operation_id,job_id,attempt_id,resource_id,status,revision,created_at,request_sha256,request_json,record)
 VALUES(?,?,?,?,?,?,?,?,?,?,?)`, string(op.WorkspaceID), string(op.ID), string(op.JobID), string(op.AttemptID), string(resource), string(op.Status), op.Revision, op.CreatedAt.UTC().Format(time.RFC3339Nano), c.RequestHash, string(request), string(raw))
	return dbError(err)
}

func saveCleanupReceipt(ctx context.Context, tx *sql.Tx, c cleanup.Command, r cleanup.Record) error {
	r.Replay = false
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > 4096 {
		return ErrInvalid
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO cleanup_idempotency VALUES(?,?,?,?,?,?)`, string(c.WorkspaceID), c.KeyDigest, c.RequestHash, cleanup.CanonicalVersion, string(r.Operation.ID), string(raw))
	return dbError(err)
}
