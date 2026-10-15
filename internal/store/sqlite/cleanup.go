package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

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
 OR EXISTS(SELECT 1 FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND status IN ('accepted','running') AND operation_id!=?
 AND (kind!='collect' OR NOT ?))`,
		w, job, attempt, at, w, job, attempt, at, w, job, attempt, string(self), discard).Scan(&busy)
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
	settledExecution := executed && s.RemoteActivity == domain.RemoteActivityInactive &&
		(s.Orchestration == domain.OrchestrationNeedsAttention || s.Orchestration == domain.OrchestrationCollecting)
	if !s.Orchestration.Terminal() && !settledExecution {
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
			return result, operations.ErrUncollected
		}
	}
	result.binding = provider.BindingSnapshot{Binding: target.Profile.Binding, AccountScope: target.Profile.AccountScope, CredentialRef: target.Profile.CredentialRef}
	if !result.binding.Valid() {
		return result, ErrCorrupt
	}
	return result, nil
}

// Admission has already rejected active worker leases. An idle transfer ticket
// cannot extend an authorized uncollected lifetime or be reclaimed after discard.
func discardPendingCollection(ctx context.Context, tx *sql.Tx, target admission.Record, now time.Time) error {
	w, job, attempt := target.Job.WorkspaceID, target.Job.ID, target.Attempt.ID
	var id domain.OperationID
	err := tx.QueryRowContext(ctx, `SELECT operation_id FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=?
 AND kind='collect' AND status IN ('accepted','running')`, string(w), string(job), string(attempt)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return dbError(err)
	}
	row, err := loadOperation(ctx, tx, w, id)
	if err != nil {
		return err
	}
	p, err := domain.NewProblem(domain.CodeArtifactCollectionFailed, "Collection stopped because discard of this attempt's uncollected remote data was authorized.", domain.FailureStageResults)
	if err != nil {
		return ErrInvalid
	}
	p = p.WithRetrySemantics(false, false).WithRecommendedAction(domain.RecommendedActionRetryRead)
	state := target.Attempt.State
	state.Orchestration = domain.OrchestrationNeedsAttention
	if state.Result == domain.ResultCollecting {
		state.Result = domain.ResultIncomplete
	}
	if _, err = controlState(ctx, tx, row, target.Attempt, state, domain.EventCollectionFailed, now); err != nil {
		return err
	}
	if err = collectionCondition(ctx, tx, collection.Lease{WorkspaceID: w, JobID: job, AttemptID: attempt}, &p); err != nil {
		return err
	}
	_, err = finishControl(ctx, tx, row, domain.OperationFailed, operations.Failed, &p, now)
	return err
}

// Both explicit and automatic transfer tickets use the same cleanup fence.
const pendingCleanupPredicate = `(c.status IN ('accepted','running','succeeded')
 OR EXISTS(SELECT 1 FROM cleanup_resources r WHERE r.workspace_id=c.workspace_id AND r.operation_id=c.operation_id AND r.state IN ('running','deleted','absent','unknown'))
 OR (c.status='failed' AND json_extract(c.record,'$.Operation.Failure.Code')=?
 AND NOT EXISTS(SELECT 1 FROM cleanup_resources r WHERE r.workspace_id=c.workspace_id AND r.operation_id=c.operation_id)))`

// pendingCleanup blocks new transfer tickets once deletion of the attempt's remote data
// was requested or applied. It never changes an existing control.
func pendingCleanup(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID) error {
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations c WHERE kind='cleanup' AND workspace_id=? AND job_id=? AND attempt_id=? AND
 `+pendingCleanupPredicate+`)`, string(w), string(job), string(attempt), string(domain.CodeProviderStateUnknown)).Scan(&exists); err != nil {
		return dbError(err)
	}
	if exists {
		return operations.ErrState
	}
	return nil
}
