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
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

var _ dispatch.Repository = (*Store)(nil)

func loadJournal(ctx context.Context, tx *sql.Tx, sequence int64) (dispatch.Journal, error) {
	var j dispatch.Journal
	var raw, phase string
	var version int64
	err := tx.QueryRowContext(ctx, "SELECT version,phase,journal FROM dispatch_journals WHERE queue_seq=?", sequence).Scan(&version, &phase, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return dispatch.Journal{Phase: dispatch.Local}, nil
	}
	if err != nil {
		return j, dbError(err)
	}
	d := json.NewDecoder(bytes.NewBufferString(raw))
	d.DisallowUnknownFields()
	if len(raw) > dispatch.MaxJournalBytes || d.Decode(&j) != nil || d.Decode(new(any)) != io.EOF || j.Version != version || string(j.Phase) != phase || !j.Valid() {
		return dispatch.Journal{}, ErrCorrupt
	}
	return j, nil
}

// ownedDispatchClaim checks every component of the persisted claim, including the
// latest attempt revision. Unlike the local scheduler seam it permits observation
// behind the dispatch barrier; new side effects have additional one-shot policy gates.
func ownedDispatchClaim(ctx context.Context, tx *sql.Tx, c scheduler.Claim, now time.Time) (domain.Attempt, bool, error) {
	if !c.Valid() {
		return domain.Attempt{}, false, scheduler.ErrInvalid
	}
	if _, err := checkWorkLease(ctx, tx, dispatchLease(c), now); err != nil {
		return domain.Attempt{}, false, err
	}
	var profile string
	var barrier bool
	err := tx.QueryRowContext(ctx, `SELECT q.dispatch_barrier,r.snapshot
 FROM scheduler_queue q
 JOIN jobs j ON j.workspace_id=q.workspace_id AND j.job_id=q.job_id AND j.active_attempt_id=q.attempt_id
 JOIN profile_revisions r ON r.profile=j.profile AND r.revision=j.profile_revision
 WHERE q.queue_seq=? AND q.workspace_id=? AND q.job_id=? AND q.attempt_id=?`, c.Sequence, string(c.WorkspaceID), string(c.JobID), string(c.AttemptID)).Scan(&barrier, &profile)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Attempt{}, false, scheduler.ErrLeaseLost
	}
	if err != nil {
		return domain.Attempt{}, false, dbError(err)
	}

	// readJobRecord independently validates this snapshot; check ownership binding here
	// without resolving a mutable profile alias or requiring the submitting token again.
	var p struct {
		Binding      domain.ProviderBinding `json:"binding"`
		AccountScope string                 `json:"account_scope"`
	}
	if len(profile) > 8192 || json.Unmarshal([]byte(profile), &p) != nil || p.Binding.Validate() != nil {
		return domain.Attempt{}, false, ErrCorrupt
	}
	if p.AccountScope != c.AccountScope || p.Binding.ProviderInstanceID != c.ProviderInstanceID {
		return domain.Attempt{}, false, scheduler.ErrLeaseLost
	}
	a, _, err := loadAttempt(ctx, tx, c.WorkspaceID, c.JobID, c.AttemptID)
	if err != nil {
		return domain.Attempt{}, false, err
	}
	if a.Revision != c.AttemptRevision {
		return domain.Attempt{}, false, scheduler.ErrLeaseLost
	}
	if now.Before(a.UpdatedAt) {
		return domain.Attempt{}, false, ErrCorrupt
	}
	return a, barrier, nil
}
func loadDispatch(ctx context.Context, tx *sql.Tx, c scheduler.Claim, now time.Time) (dispatch.Work, error) {
	var work dispatch.Work
	if _, _, err := schedulerControl(ctx, tx); err != nil {
		return work, err
	}
	a, barrier, err := ownedDispatchClaim(ctx, tx, c, now)
	if err != nil {
		return work, err
	}
	j, err := loadJournal(ctx, tx, c.Sequence)
	if err != nil {
		return work, err
	}
	if (j.Plan != nil && !barrier) || j.Plan == nil && barrier && j.Phase != dispatch.Failed && j.Phase != dispatch.Prevented {
		return work, ErrCorrupt
	}
	record, err := readJobRecord(ctx, tx, c.WorkspaceID, c.JobID)
	if err != nil {
		return work, err
	}
	if record.Attempt != a {
		return work, scheduler.ErrLeaseLost
	}
	if err = tx.QueryRowContext(ctx, "SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&work.InstallationID); err != nil {
		return work, dbError(err)
	}
	work.Handle = dispatch.Handle{Claim: c, Version: j.Version}
	work.Job = record
	work.Journal = j
	work.Gate, err = loadExecutionGate(ctx, tx, record, now)
	if err != nil {
		return dispatch.Work{}, err
	}
	if !work.InstallationID.Valid() {
		return dispatch.Work{}, ErrCorrupt
	}
	if j.Plan != nil {
		if record.Request.Spec().SeparateStart() {
			g := work.Gate
			if g == nil || j.Prepared != nil && j.Prepared.Ready && j.Prepared.ReadinessRevision() != g.ReadyRevision {
				return dispatch.Work{}, ErrCorrupt
			}
			if j.SubmitStarted {
				if g.Start == nil || g.StartReceipt == nil || g.StartReceipt.Status != executionauth.Consumed || j.StartAuthorization == nil || !j.StartAuthorization.Equal(*g.Start) {
					return dispatch.Work{}, ErrCorrupt
				}
			} else if g.StartReceipt != nil && g.StartReceipt.Status == executionauth.Consumed {
				return dispatch.Work{}, ErrCorrupt
			}
		}
		status, err := dispatchAuthorization(ctx, tx, work, now)
		if err != nil || status != executionauth.Consumed {
			return dispatch.Work{}, ErrCorrupt
		}
		if err = matchesFrozenPlan(work, *j.Plan, j.PreparationID); err != nil {
			return dispatch.Work{}, ErrCorrupt
		}
		if err = checkDispatchLedger(ctx, tx, work); err != nil {
			return dispatch.Work{}, err
		}
	} else if j.Phase != dispatch.Failed && j.Phase != dispatch.Prevented && !scheduler.LocalOnly(a.State) {
		return dispatch.Work{}, scheduler.ErrPhaseGate
	}
	work.Cancellation, err = pendingCancellation(ctx, tx, work)
	if err != nil {
		return dispatch.Work{}, err
	}
	return work, nil
}
func matchesFrozenPlan(work dispatch.Work, plan provider.Plan, operation domain.OperationID) error {
	refs := map[string]domain.ObjectMetadata{}
	for _, ref := range work.Job.Objects {
		refs[ref.Role] = ref.Object
	}
	snapshot, err := dispatch.Snapshot(work.Job, refs)
	if err != nil {
		return err
	}
	job, id, err := dispatch.ResolveJob(work, snapshot)
	if err != nil {
		return err
	}
	expected, _ := json.Marshal(job)
	actual, _ := json.Marshal(plan.Job)
	if operation != id || !bytes.Equal(expected, actual) {
		return dispatch.ErrConflict
	}
	// The only currently supported after-start uncertainty is a requested GPU check.
	if len(plan.VerifyAfterStart) > 1 {
		return dispatch.ErrConflict
	}
	for _, name := range plan.VerifyAfterStart {
		if name != domain.CapabilityGPU || work.Job.Request.Spec().Resources.Accelerator != "gpu" {
			return dispatch.ErrConflict
		}
	}
	return nil
}
func (s *Store) LoadDispatch(ctx context.Context, c scheduler.Claim) (dispatch.Work, error) {
	var work dispatch.Work
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var err error
		work, err = loadDispatch(ctx, tx, c, now)
		return err
	})
	if err != nil {
		return dispatch.Work{}, err
	}
	return work, nil
}
func checkHandle(work dispatch.Work, h dispatch.Handle) error {
	if h.Version != work.Journal.Version {
		return dispatch.ErrConflict
	}
	return nil
}

func newMutationPolicy(ctx context.Context, tx *sql.Tx, work dispatch.Work, now time.Time) error {
	if err := checkHostNewWork(ctx, tx, work.Job, now); err != nil {
		return err
	}
	if work.Job.Request.Spec().Expired(now) || work.Gate != nil && work.Gate.Expired(now) {
		return admission.ErrExpired
	}
	settings, last, err := schedulerControl(ctx, tx)
	if err != nil {
		return err
	}
	if settings.Paused || !scheduler.LocalOnly(work.Job.Attempt.State) {
		return dispatch.ErrPolicy
	}
	rows, err := schedulerSnapshot(ctx, tx, settings, now)
	if err != nil {
		return err
	}
	_, view, err := scheduler.Select(rows, domain.WorkspaceID(last), settings, now)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.Sequence != work.Handle.Claim.Sequence {
			continue
		}
		if !row.ActiveAttempt || !row.WorkspaceEnabled || row.Policy.Disabled || view.LiveWorkers > settings.MaxWorkers || view.AccountReservations[row.AccountScope] > min(settings.MaxActivePerAccount, row.Policy.MaxActive) {
			return dispatch.ErrPolicy
		}
		reason, _ := row.Quota.Decide(row.Resource, row.WallSeconds, row.Policy.StrictQuota, settings.QuotaFreshFor, now)
		if reason != scheduler.Eligible {
			return dispatch.ErrPolicy
		}
		return nil
	}
	return dispatch.ErrConflict
}

func (s *Store) CommitDispatch(ctx context.Context, h dispatch.Handle, action dispatch.Action) (dispatch.Work, error) {
	// HTTP controls use their authenticated operation transaction; an ordinary worker
	// cannot bypass its operation identity/event journal through the dispatch port.
	if action.Kind == dispatch.PreventDispatch || action.Kind == dispatch.RequestReconciliation {
		return dispatch.Work{}, dispatch.ErrInvalid
	}
	var result dispatch.Work
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		work, err := loadDispatch(ctx, tx, h.Claim, now)
		if err != nil {
			return err
		}
		if err = checkHandle(work, h); err != nil {
			return err
		}
		if action.Kind == dispatch.RefuseNewWork {
			if _, enabled := ctx.Value(hostEvidenceKey{}).(hostEvidence); !enabled {
				return dispatch.ErrInvalid
			}
			err := checkHostNewWork(ctx, tx, work.Job, now)
			if !errors.Is(err, admission.ErrHostUnhealthy) {
				if err != nil {
					return err
				}
				return dispatch.ErrPolicy
			}
		}
		if action.Kind == dispatch.ExpireDispatch && !work.Job.Request.Spec().Expired(now) && (work.Gate == nil || !work.Gate.Expired(now)) {
			return dispatch.ErrInvalid
		}
		if action.Kind == dispatch.ExpireDispatch {
			action.PreparationExpired = false
			if work.Gate != nil && !work.Job.Request.Spec().Expired(now) {
				until, _ := time.Parse(time.RFC3339, work.Gate.Request.ReadyUntil)
				action.PreparationExpired = !now.Before(until)
				if work.Gate.Start != nil {
					deadline, _ := time.Parse(time.RFC3339, work.Gate.Start.ExecutionDeadline)
					action.PreparationExpired = action.PreparationExpired && !deadline.Before(until)
				}
			}
		}
		if action.Kind == dispatch.BeginPreparation || action.Kind == dispatch.BeginSubmission {
			status, err := dispatchAuthorization(ctx, tx, work, now)
			if err != nil {
				return err
			}
			if status != executionauth.Consumed {
				return dispatch.ErrPolicy
			}
			if err = newMutationPolicy(ctx, tx, work, now); err != nil {
				return err
			}
		}
		if action.Kind == dispatch.BeginPreparation {
			if action.Plan == nil {
				return dispatch.ErrInvalid
			}
			if err = matchesFrozenPlan(work, *action.Plan, action.PreparationID); err != nil {
				return err
			}
		}
		if err = commitGateStep(ctx, tx, &work, &action, now); err != nil {
			return err
		}
		next, state, event, err := dispatch.Apply(work.Journal, work.Job.Attempt.State, action, now)
		if err != nil {
			return err
		}
		if state.Orchestration.Terminal() && next.Plan == nil {
			if err = releaseGateStorage(ctx, tx, string(work.Job.Job.WorkspaceID), string(work.Job.Job.ID), string(work.Job.Attempt.ID)); err != nil {
				return err
			}
			if err = releaseHostCachePinsWithoutPreparation(ctx, tx, work.Job.Job.WorkspaceID, work.Job.Job.ID, work.Job.Attempt.ID, now); err != nil {
				return err
			}
		}
		raw, err := json.Marshal(next)
		if err != nil || len(raw) > dispatch.MaxJournalBytes {
			return dispatch.ErrInvalid
		}
		if err = writeDispatchLedger(ctx, tx, work, next, action.Kind, now); err != nil {
			return err
		}
		if action.Kind == dispatch.BeginPreparation {
			// Byte verification occurred outside SQL; the plan carries exact pinned identities.
			if _, err = schedulerTransition(ctx, tx, h.Claim.Identity, work.Job.Attempt, work.Job.Attempt.State, domain.EventInputsReady, now); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "UPDATE scheduler_queue SET dispatch_barrier=1 WHERE queue_seq=?", h.Claim.Sequence); err != nil {
				return dbError(err)
			}
		}
		updated, err := schedulerTransition(ctx, tx, h.Claim.Identity, work.Job.Attempt, state, event, now)
		if err != nil {
			return err
		}
		if h.Version == 0 {
			_, err = tx.ExecContext(ctx, "INSERT INTO dispatch_journals VALUES(?,?,?,?)", h.Claim.Sequence, next.Version, string(next.Phase), string(raw))
		} else {
			var res sql.Result
			res, err = tx.ExecContext(ctx, "UPDATE dispatch_journals SET version=?,phase=?,journal=? WHERE queue_seq=? AND version=?", next.Version, string(next.Phase), string(raw), h.Claim.Sequence, h.Version)
			if err == nil {
				var n int64
				n, err = res.RowsAffected()
				if err == nil && n != 1 {
					return dispatch.ErrConflict
				}
			}
		}
		if err != nil {
			return dbError(err)
		}
		work.Job.Attempt = updated
		work.Journal = next
		work.Handle.Version = next.Version
		work.Handle.Claim.AttemptRevision = updated.Revision
		if err = finishObservedOperations(ctx, tx, &work, action.Kind, now); err != nil {
			return err
		}
		work.Cancellation, err = pendingCancellation(ctx, tx, work)
		if err != nil {
			return err
		}
		result = work
		return nil
	})
	if err != nil {
		return dispatch.Work{}, err
	}
	return result, nil
}
