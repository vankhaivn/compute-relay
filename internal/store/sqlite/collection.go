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

	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

var _ collection.Repository = (*Store)(nil)

func (s *Store) collectionTx(ctx context.Context, now time.Time, fn func(context.Context, *sql.Tx) error) error {
	if !scheduler.ValidTime(now) {
		return ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, _, err := schedulerControl(ctx, tx, now); err != nil {
			return err
		}
		if err := fn(ctx, tx); err != nil {
			return err
		}
		return advanceSchedulerClock(ctx, tx, now)
	})
}

// ensureInitialCollection creates the first transfer ticket only when a collector
// is explicitly invoked. Migration, admission, GET and terminal dispatch do not
// start a collector. A failed ticket is never replaced automatically.
func ensureInitialCollection(ctx context.Context, tx *sql.Tx, now time.Time) error {
	var w domain.WorkspaceID
	var job domain.JobID
	var attempt domain.AttemptID
	err := tx.QueryRowContext(ctx, `SELECT q.workspace_id,q.job_id,q.attempt_id
 FROM scheduler_queue q JOIN attempts a ON a.workspace_id=q.workspace_id AND a.job_id=q.job_id AND a.attempt_id=q.attempt_id
 JOIN dispatch_journals d ON d.queue_seq=q.queue_seq
 WHERE a.orchestration='collecting' AND d.phase='collectible'
 AND NOT EXISTS (SELECT 1 FROM operations o WHERE o.workspace_id=q.workspace_id AND o.job_id=q.job_id AND o.attempt_id=q.attempt_id AND o.kind='collect')
 ORDER BY q.queue_seq LIMIT 1`).Scan(&w, &job, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return dbError(err)
	}
	target, err := controlTarget(ctx, tx, w, job, attempt)
	if err != nil {
		return err
	}
	work, err := controlWork(ctx, tx, target)
	if err != nil {
		return err
	}
	if work.Journal.Phase != dispatch.Collectible || !target.Attempt.State.Execution.Terminal() || target.Attempt.State.Result != domain.ResultNotAvailable {
		return collection.ErrInvalid
	}
	if now.Before(target.Attempt.UpdatedAt) {
		return scheduler.ErrClock
	}
	id, err := randomID("op_", 16)
	if err != nil {
		return err
	}
	op, err := domain.NewOperation(domain.OperationID(id), w, job, attempt, domain.OperationCollect, now)
	if err != nil {
		return ErrInvalid
	}
	req := operations.Request{AttemptID: attempt, Reason: "automatic collection after terminal observation"}
	hash, err := req.Digest(job, domain.OperationCollect)
	if err != nil {
		return err
	}
	c := operations.Command{WorkspaceID: w, JobID: job, Kind: domain.OperationCollect, Request: req, RequestHash: hash, Now: now}
	r := operations.Record{Operation: op, Effect: operations.CollectionRequested}
	if err = insertControl(ctx, tx, c, r); err != nil {
		return err
	}
	if err = controlEvent(ctx, tx, w, job, attempt, op.ID, domain.EventOperationAccepted, now); err != nil {
		return err
	}
	return controlEvent(ctx, tx, w, job, attempt, op.ID, domain.EventCollectionRequested, now)
}

func (s *Store) ClaimCollection(ctx context.Context, now time.Time, ttl time.Duration, workers int) (*collection.Work, error) {
	now = now.UTC()
	if ttl < time.Second || ttl > 31*time.Minute || workers < 1 || workers > 16 {
		return nil, ErrInvalid
	}
	var result *collection.Work
	err := s.collectionTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM collection_leases WHERE held=1 AND until_ms>?", now.UnixMilli()).Scan(&count); err != nil {
			return dbError(err)
		}
		if count >= workers {
			return nil
		}
		if err := ensureInitialCollection(ctx, tx, now); err != nil {
			return err
		}
		var w domain.WorkspaceID
		var id domain.OperationID
		err := tx.QueryRowContext(ctx, `SELECT o.workspace_id,o.operation_id FROM operations o
 LEFT JOIN collection_leases c ON c.workspace_id=o.workspace_id AND c.job_id=o.job_id AND c.attempt_id=o.attempt_id
 WHERE o.kind='collect' AND o.status='accepted' AND (c.held IS NULL OR c.held=0 OR c.until_ms<=?)
 ORDER BY o.operation_seq LIMIT 1`, now.UnixMilli()).Scan(&w, &id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		op, err := loadControl(ctx, tx, w, id)
		if err != nil {
			return err
		}
		target, err := controlTarget(ctx, tx, w, op.Operation.JobID, op.Operation.AttemptID)
		if err != nil {
			return err
		}
		work, err := controlWork(ctx, tx, target)
		if err != nil {
			return err
		}
		a := target.Attempt
		if now.Before(a.UpdatedAt) || now.Before(op.Operation.UpdatedAt) {
			return scheduler.ErrClock
		}
		if a.State.Orchestration.Terminal() || !a.State.Execution.Terminal() || work.Journal.Phase != dispatch.Collectible || work.Journal.Observation == nil || work.Journal.Observation.Execution != a.State.Execution {
			return collection.ErrInvalid
		}
		switch a.State.Result {
		case domain.ResultNotAvailable, domain.ResultCollecting, domain.ResultIncomplete, domain.ResultInvalid:
		default:
			return collection.ErrInvalid
		}
		state := a.State
		state.Result = domain.ResultCollecting
		state.Orchestration = domain.OrchestrationCollecting
		a, err = controlState(ctx, tx, op, a, state, domain.EventCollectionRequested, now)
		if err != nil {
			return err
		}
		var generation int64
		err = tx.QueryRowContext(ctx, `SELECT generation FROM collection_leases WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(w), string(a.JobID), string(a.ID)).Scan(&generation)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		if generation == math.MaxInt64 {
			return ErrConflict
		}
		generation++
		fence, err := randomID("", 32)
		if err != nil {
			return err
		}
		until := time.UnixMilli(now.Add(ttl).UnixMilli()).UTC()
		_, err = tx.ExecContext(ctx, `INSERT INTO collection_leases VALUES(?,?,?,?,?,?,?,?,1)
 ON CONFLICT(workspace_id,job_id,attempt_id) DO UPDATE SET operation_id=excluded.operation_id,generation=excluded.generation,
 fence=excluded.fence,until_ms=excluded.until_ms,attempt_revision=excluded.attempt_revision,held=1`,
			string(w), string(a.JobID), string(a.ID), string(id), generation, fence, until.UnixMilli(), a.Revision)
		if err != nil {
			return dbError(err)
		}
		lease := collection.Lease{WorkspaceID: w, JobID: a.JobID, AttemptID: a.ID, OperationID: id, Generation: generation, Fence: fence, Until: until, AttemptRevision: a.Revision}
		loaded, err := collectionWork(ctx, tx, lease)
		if err != nil {
			return err
		}
		result = &loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func collectionWork(ctx context.Context, tx *sql.Tx, l collection.Lease) (collection.Work, error) {
	var result collection.Work
	target, err := controlTarget(ctx, tx, l.WorkspaceID, l.JobID, l.AttemptID)
	if err != nil {
		return result, err
	}
	work, err := controlWork(ctx, tx, target)
	if err != nil {
		return result, err
	}
	j := work.Journal
	if j.Plan == nil || j.Remote == nil || j.Observation == nil || j.Phase != dispatch.Collectible || j.Observation.Execution != target.Attempt.State.Execution {
		return result, collection.ErrInvalid
	}
	result = collection.Work{Lease: l, Plan: j.Plan.Clone(), Observation: *j.Observation, Binding: provider.BindingSnapshot{
		Binding: target.Profile.Binding, AccountScope: target.Profile.AccountScope, CredentialRef: target.Profile.CredentialRef,
	}}
	if !result.Valid() {
		return collection.Work{}, collection.ErrInvalid
	}
	var raw, digest string
	err = tx.QueryRowContext(ctx, `SELECT snapshot,snapshot_sha256 FROM collection_snapshots WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(l.WorkspaceID), string(l.JobID), string(l.AttemptID)).Scan(&raw, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return result, dbError(err)
	}
	var snapshot collection.Snapshot
	d := json.NewDecoder(bytes.NewBufferString(raw))
	d.DisallowUnknownFields()
	if len(raw) > collection.MaxSnapshotBytes || d.Decode(&snapshot) != nil || d.Decode(new(any)) != io.EOF || string(snapshot.Digest()) != digest || collection.ValidateSnapshot(result, snapshot) != nil {
		return collection.Work{}, ErrCorrupt
	}
	result.Snapshot = &snapshot
	return result, nil
}

func checkedCollection(ctx context.Context, tx *sql.Tx, w collection.Work, now time.Time) (collection.Work, controlRecord, domain.Attempt, error) {
	var empty collection.Work
	var zero controlRecord
	var noAttempt domain.Attempt
	if !w.Valid() {
		return empty, zero, noAttempt, collection.ErrInvalid
	}
	l := w.Lease
	var id, fence string
	var generation, until int64
	var revision uint64
	var held bool
	err := tx.QueryRowContext(ctx, `SELECT operation_id,generation,fence,until_ms,attempt_revision,held FROM collection_leases
 WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(l.WorkspaceID), string(l.JobID), string(l.AttemptID)).Scan(&id, &generation, &fence, &until, &revision, &held)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, zero, noAttempt, collection.ErrLeaseLost
	}
	if err != nil {
		return empty, zero, noAttempt, dbError(err)
	}
	if !held || id != string(l.OperationID) || generation != l.Generation || fence != l.Fence || until != l.Until.UnixMilli() || revision != l.AttemptRevision || now.UnixMilli() >= until {
		return empty, zero, noAttempt, collection.ErrLeaseLost
	}
	op, err := loadControl(ctx, tx, l.WorkspaceID, l.OperationID)
	if err != nil {
		return empty, zero, noAttempt, err
	}
	a, _, err := loadAttempt(ctx, tx, l.WorkspaceID, l.JobID, l.AttemptID)
	if err != nil {
		return empty, zero, noAttempt, err
	}
	if op.Operation.Kind != domain.OperationCollect || op.Operation.Status != domain.OperationAccepted || op.Operation.JobID != l.JobID || op.Operation.AttemptID != l.AttemptID || a.Revision != revision || a.State.Result != domain.ResultCollecting || a.State.Orchestration != domain.OrchestrationCollecting {
		return empty, zero, noAttempt, collection.ErrLeaseLost
	}
	if now.Before(a.UpdatedAt) || now.Before(op.Operation.UpdatedAt) {
		return empty, zero, noAttempt, scheduler.ErrClock
	}
	current, err := collectionWork(ctx, tx, l)
	if err != nil {
		return empty, zero, noAttempt, err
	}
	if current.Plan.Digest() != w.Plan.Digest() || current.Binding != w.Binding || current.Observation != w.Observation {
		return empty, zero, noAttempt, collection.ErrInvalid
	}
	return current, op, a, nil
}

func (s *Store) PinCollection(ctx context.Context, w collection.Work, snapshot collection.Snapshot, now time.Time) error {
	// All decoding/hash checks are over bounded in-memory evidence; no byte/network I/O.
	if collection.ValidateSnapshot(w, snapshot) != nil {
		return collection.ErrInvalid
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > collection.MaxSnapshotBytes {
		return collection.ErrInvalid
	}
	return s.collectionTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		current, _, _, err := checkedCollection(ctx, tx, w, now)
		if err != nil {
			return err
		}
		if current.Snapshot != nil {
			if current.Snapshot.Digest() != snapshot.Digest() {
				return collection.ErrInvalid
			}
			return nil
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO collection_snapshots VALUES(?,?,?,?,?,?)`, string(w.Lease.WorkspaceID), string(w.Lease.JobID), string(w.Lease.AttemptID), string(snapshot.Digest()), string(raw), now.UTC().Format(time.RFC3339Nano))
		return dbError(err)
	})
}

func (s *Store) CompleteCollection(ctx context.Context, w collection.Work, proof collection.Verified, now time.Time) error {
	return s.collectionTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		current, op, a, err := checkedCollection(ctx, tx, w, now)
		if err != nil {
			return err
		}
		if !proof.Matches(current) {
			return collection.ErrInvalid
		}
		snapshot := proof.Snapshot()
		state, err := collection.FinalState(a.State, proof.Phase())
		if err != nil {
			return err
		}
		l := current.Lease
		_, err = tx.ExecContext(ctx, `INSERT INTO collection_publications VALUES(?,?,?,?,?,?,?,?)`, string(l.WorkspaceID), string(l.JobID), string(l.AttemptID), string(l.OperationID), string(snapshot.Digest()), proof.Phase(), len(snapshot.Files), now.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return dbError(err)
		}
		for _, f := range snapshot.Files {
			_, err = tx.ExecContext(ctx, `INSERT INTO artifacts VALUES(?,?,?,?,?,?,?,?,?,?)`, string(l.WorkspaceID), string(l.JobID), string(l.AttemptID), string(f.ID), string(f.Object.ID), f.Path, f.Role, f.Object.Bytes, string(f.Object.SHA256), f.MediaType)
			if err != nil {
				return dbError(err)
			}
		}
		if _, err = controlState(ctx, tx, op, a, state, domain.EventArtifactVerified, now); err != nil {
			return err
		}
		if state.Orchestration.Terminal() {
			if err = controlEvent(ctx, tx, l.WorkspaceID, l.JobID, l.AttemptID, l.OperationID, domain.EventJobCompleted, now); err != nil {
				return err
			}
		}
		if err = collectionCondition(ctx, tx, l, nil); err != nil {
			return err
		}
		if _, err = finishControl(ctx, tx, op, domain.OperationSucceeded, operations.ResultsAvailable, nil, now); err != nil {
			return err
		}
		return releaseCollection(ctx, tx, l)
	})
}
func (s *Store) FailCollection(ctx context.Context, w collection.Work, f collection.Failure, now time.Time) error {
	switch f.Code {
	case domain.CodeArtifactCollectionFailed, domain.CodeArtifactMissing, domain.CodeArtifactDigestMismatch, domain.CodeResultManifestMissing, domain.CodeRemoteIdentityMismatch:
	default:
		return collection.ErrInvalid
	}
	return s.collectionTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		_, op, a, err := checkedCollection(ctx, tx, w, now)
		if err != nil {
			return err
		}
		state := a.State
		state.Orchestration = domain.OrchestrationNeedsAttention
		state.Result = domain.ResultIncomplete
		if f.Invalid {
			state.Result = domain.ResultInvalid
		}
		if _, err = controlState(ctx, tx, op, a, state, domain.EventCollectionFailed, now); err != nil {
			return err
		}
		p, err := domain.NewProblem(f.Code, "Collection did not verify; recover this attempt's artifacts without rerunning compute.", domain.FailureStageResults)
		if err != nil {
			return err
		}
		p = p.WithRetrySemantics(!f.Invalid, true).WithRecommendedAction(domain.RecommendedActionCollect)
		if f.Code == domain.CodeRemoteIdentityMismatch {
			p = p.WithRecommendedAction(domain.RecommendedActionInspectProvider)
		}
		if err = collectionCondition(ctx, tx, w.Lease, &p); err != nil {
			return err
		}
		if _, err = finishControl(ctx, tx, op, domain.OperationFailed, operations.Failed, &p, now); err != nil {
			return err
		}
		return releaseCollection(ctx, tx, w.Lease)
	})
}
func releaseCollection(ctx context.Context, tx *sql.Tx, l collection.Lease) error {
	_, err := tx.ExecContext(ctx, `UPDATE collection_leases SET held=0 WHERE workspace_id=? AND job_id=? AND attempt_id=? AND generation=? AND fence=?`, string(l.WorkspaceID), string(l.JobID), string(l.AttemptID), l.Generation, l.Fence)
	return dbError(err)
}

// Cached job status uses the same journal condition as dispatch. Updating it here
// is part of the result/event transaction, not another provider observation.
func collectionCondition(ctx context.Context, tx *sql.Tx, l collection.Lease, problem *domain.Problem) error {
	target, err := controlTarget(ctx, tx, l.WorkspaceID, l.JobID, l.AttemptID)
	if err != nil {
		return err
	}
	work, err := controlWork(ctx, tx, target)
	if err != nil {
		return err
	}
	next := work.Journal
	if next.Version == math.MaxInt64 {
		return ErrConflict
	}
	next.Version++
	next.Problem = problem
	return controlJournal(ctx, tx, work, next)
}
