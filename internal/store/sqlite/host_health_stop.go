package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// HostHealthAttempt is private exact-attempt evidence, not an API projection.
type HostHealthAttempt struct {
	Record admission.Record
	Target hosthealth.Target
}

func (HostHealthAttempt) String() string   { return "[host health attempt]" }
func (HostHealthAttempt) GoString() string { return "[host health attempt]" }

func possibleHostExecution(state domain.AttemptState) bool {
	return !state.Orchestration.Terminal() && !state.Execution.Terminal() &&
		state.RemoteActivity != domain.RemoteActivityNotStarted && state.RemoteActivity != domain.RemoteActivityInactive
}

func (s *Store) HostHealthAttempts(ctx context.Context) ([]HostHealthAttempt, error) {
	result := []HostHealthAttempt{}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		rows, err := tx.QueryContext(ctx, `SELECT j.workspace_id,j.job_id,j.active_attempt_id FROM jobs j JOIN attempts a
 ON a.workspace_id=j.workspace_id AND a.job_id=j.job_id AND a.attempt_id=j.active_attempt_id
 WHERE a.orchestration NOT IN ('succeeded','failed','cancelled','timed_out')
 AND json_extract(a.state,'$.RemoteActivity') IN ('unknown','possible','active') ORDER BY j.workspace_id,j.job_id LIMIT ?`, scheduler.ScanLimit+1)
		if err != nil {
			return dbError(err)
		}
		ids := []scheduler.Identity{}
		for rows.Next() {
			var id scheduler.Identity
			if err := rows.Scan(&id.WorkspaceID, &id.JobID, &id.AttemptID); err != nil {
				rows.Close()
				return dbError(err)
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return dbError(err)
		}
		if len(ids) > scheduler.ScanLimit {
			return scheduler.ErrScanLimit
		}
		for _, id := range ids {
			r, err := controlTarget(ctx, tx, id.WorkspaceID, id.JobID, id.AttemptID)
			if err != nil {
				return err
			}
			if !possibleHostExecution(r.Attempt.State) {
				continue
			}
			resource, err := reservedHostResource(ctx, tx, r)
			if err != nil {
				return err
			}
			target, err := frozenHostHealthTarget(ctx, tx, id.WorkspaceID, r.Profile, resource)
			if errors.Is(err, hosthealth.ErrUnverified) {
				continue
			}
			if err != nil {
				return err
			}
			if target != nil {
				result = append(result, HostHealthAttempt{r, *target})
			}
		}
		return nil
	})
	return result, err
}

// API operations use op_<random hex>. This disjoint, deterministic namespace is
// allocated only here and binds installation, workspace, job and exact attempt.
// A request reason supplied through the API cannot impersonate this origin.
func hostHealthStopID(installation domain.RuntimeInstallationID, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID) domain.OperationID {
	digest := sha256.Sum256([]byte("compute-relay/host-health-stop/v1\x00" + string(installation) + "\x00" + string(w) + "\x00" + string(job) + "\x00" + string(attempt)))
	return domain.OperationID("op_health_" + hex.EncodeToString(digest[:]))
}

// RequestHostHealthStop requests ordinary cancellation. It never calls a provider;
// the existing dispatch fence and cancel_started gate authorize that one shot.
func (s *Store) RequestHostHealthStop(ctx context.Context, expected HostHealthAttempt, evidence hosthealth.Evidence) (bool, error) {
	requested := false
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		r, err := controlTarget(ctx, tx, expected.Record.Job.WorkspaceID, expected.Record.Job.ID, expected.Record.Attempt.ID)
		if err != nil {
			return err
		}
		if r.Profile != expected.Record.Profile || r.Attempt.Revision != expected.Record.Attempt.Revision || !possibleHostExecution(r.Attempt.State) || r.Attempt.State.Cancellation != domain.CancellationNotRequested {
			return nil
		}
		resource, err := reservedHostResource(ctx, tx, r)
		if err != nil {
			return err
		}
		target, err := frozenHostHealthTarget(ctx, tx, r.Job.WorkspaceID, r.Profile, resource)
		if errors.Is(err, hosthealth.ErrUnverified) {
			return nil
		}
		if err != nil {
			return err
		}
		if target == nil || target.ResourceID != expected.Target.ResourceID {
			return nil
		}
		history, err := hostHealthRows(ctx, tx, target.ResourceID, now)
		if err != nil {
			return err
		}
		if !hosthealth.Evaluate(evidence.View(target.ResourceID, history, now), *target.Policy, now).Stop {
			return nil
		}
		work, err := controlWork(ctx, tx, r)
		if errors.Is(err, operations.ErrUnresolved) {
			return nil
		}
		if err != nil {
			return err
		}
		if work.Journal.Plan == nil || !work.Journal.SubmitStarted {
			return nil
		}
		var existing bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=?
 AND (kind='cancel' OR kind='cleanup' AND status IN ('accepted','running')))`, r.Job.WorkspaceID, r.Job.ID, r.Attempt.ID).Scan(&existing); err != nil {
			return dbError(err)
		}
		if existing {
			return nil
		}
		id := hostHealthStopID(work.InstallationID, r.Job.WorkspaceID, r.Job.ID, r.Attempt.ID)
		op, err := domain.NewOperation(id, r.Job.WorkspaceID, r.Job.ID, r.Attempt.ID, domain.OperationCancel, now)
		if err != nil {
			return ErrInvalid
		}
		request := operations.Request{AttemptID: r.Attempt.ID, Reason: "host_critical_stopped"}
		hash, err := request.Digest(r.Job.ID, domain.OperationCancel)
		if err != nil {
			return ErrInvalid
		}
		command := operations.Command{WorkspaceID: r.Job.WorkspaceID, JobID: r.Job.ID, Kind: domain.OperationCancel, Request: request, RequestHash: hash}
		row := operationRow{Record: operations.Record{Operation: op, Effect: operations.Pending}, Request: request}
		if err := insertOperation(ctx, tx, command, row); err != nil {
			return err
		}
		if err := controlEvent(ctx, tx, r.Job.WorkspaceID, r.Job.ID, r.Attempt.ID, id, domain.EventOperationAccepted, now); err != nil {
			return err
		}
		if _, err := admitCancellation(ctx, tx, r, row, now); err != nil {
			return err
		}
		requested = true
		return nil
	})
	return requested, err
}

func hostCriticalStopProblem(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, journal dispatch.Journal) (*domain.Problem, error) {
	var installation domain.RuntimeInstallationID
	if err := tx.QueryRowContext(ctx, "SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&installation); err != nil {
		return nil, dbError(err)
	}
	if !installation.Valid() {
		return nil, ErrCorrupt
	}
	id := hostHealthStopID(installation, w, job, attempt)
	r, err := loadOperation(ctx, tx, w, id)
	if errors.Is(err, operations.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if r.Operation.Kind != domain.OperationCancel || r.Operation.JobID != job || r.Operation.AttemptID != attempt || r.Request.Reason != "host_critical_stopped" {
		return nil, ErrCorrupt
	}
	confirmed := r.TerminationConfirmed
	if observation := journal.Observation; observation != nil && journal.Valid() {
		identity := observation.Remote.Identity
		if identity.InstallationID == installation && identity.WorkspaceID == w && identity.JobID == job && identity.AttemptID == attempt &&
			observation.Execution == domain.ExecutionCancelled && observation.RemoteActivity == domain.RemoteActivityInactive && !observation.ObservedAt.Before(r.Operation.CreatedAt) {
			record, err := controlTarget(ctx, tx, w, job, attempt)
			if err != nil {
				return nil, err
			}
			if matchesFrozenPlan(dispatch.Work{InstallationID: installation, Job: record}, *journal.Plan, journal.PreparationID) != nil {
				return nil, ErrCorrupt
			}
			confirmed = true
		}
	}
	if !confirmed || !r.Started {
		return nil, nil
	}
	p := provider.Problem(domain.CodeResourceExhausted, domain.FailureStageExecution, "Relay stopped this attempt because the host reached a critical health condition.").WithDetail(domain.ReasonDetail, "host_critical_stopped").WithRetrySemantics(false, true).WithRecommendedAction(domain.RecommendedActionCollect)
	return &p, nil
}
