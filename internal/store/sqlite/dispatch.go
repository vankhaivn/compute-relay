package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
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
	if !c.Valid() || !scheduler.ValidTime(now) {
		return domain.Attempt{}, false, scheduler.ErrInvalid
	}
	var owner, fence, profile string
	var generation, until int64
	var held, barrier bool
	err := tx.QueryRowContext(ctx, `SELECT l.owner,l.fence,l.generation,l.until_ms,l.held,q.dispatch_barrier,r.snapshot
 FROM scheduler_leases l JOIN scheduler_queue q ON q.queue_seq=l.queue_seq
 JOIN jobs j ON j.workspace_id=q.workspace_id AND j.job_id=q.job_id AND j.active_attempt_id=q.attempt_id
 JOIN profile_revisions r ON r.profile=j.profile AND r.revision=j.profile_revision
 WHERE q.queue_seq=? AND q.workspace_id=? AND q.job_id=? AND q.attempt_id=?`, c.Sequence, string(c.WorkspaceID), string(c.JobID), string(c.AttemptID)).Scan(&owner, &fence, &generation, &until, &held, &barrier, &profile)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Attempt{}, false, scheduler.ErrLeaseLost
	}
	if err != nil {
		return domain.Attempt{}, false, dbError(err)
	}
	if !held || owner != c.Owner || fence != c.Fence || generation != c.Generation || until <= now.UnixMilli() || until != c.ExpiresAt.UnixMilli() {
		return domain.Attempt{}, false, scheduler.ErrLeaseLost
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
		return domain.Attempt{}, false, scheduler.ErrClock
	}
	return a, barrier, nil
}
func loadDispatch(ctx context.Context, tx *sql.Tx, c scheduler.Claim, now time.Time) (dispatch.Work, error) {
	var work dispatch.Work
	if _, _, err := schedulerControl(ctx, tx, now); err != nil {
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
	if (j.Plan != nil && !barrier) || j.Plan == nil && barrier && j.Phase != dispatch.Failed {
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
	if !work.InstallationID.Valid() {
		return dispatch.Work{}, ErrCorrupt
	}
	if j.Plan != nil {
		if err = matchesFrozenPlan(work, *j.Plan, j.PreparationID); err != nil {
			return dispatch.Work{}, ErrCorrupt
		}
		if err = checkDispatchLedger(ctx, tx, work); err != nil {
			return dispatch.Work{}, err
		}
	} else if j.Phase != dispatch.Failed && !scheduler.LocalOnly(a.State) {
		return dispatch.Work{}, scheduler.ErrPhaseGate
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
func (s *Store) LoadDispatch(ctx context.Context, c scheduler.Claim, now time.Time) (dispatch.Work, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return dispatch.Work{}, err
	}
	defer done()
	var work dispatch.Work
	err = withTx(ctx, s.db, func(tx *sql.Tx) error { var err error; work, err = loadDispatch(ctx, tx, c, now); return err })
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

// FreezeInput commits a verified local blob and the previously unresolved role together.
// The role is immutable. An uncertain acknowledgement preserves the blob and is resolved
// from job_objects on the next load, never by fetching a newer URL over existing history.
func (s *Store) FreezeInput(ctx context.Context, h dispatch.Handle, index int, m domain.ObjectMetadata, now time.Time) error {
	if !m.Valid() || m.WorkspaceID != h.Claim.WorkspaceID || m.Bytes > 2<<30 {
		return dispatch.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		work, err := loadDispatch(ctx, tx, h.Claim, now)
		if err != nil {
			return err
		}
		if err = checkHandle(work, h); err != nil {
			return err
		}
		if work.Journal.Phase != dispatch.Local || work.Journal.Plan != nil || !scheduler.LocalOnly(work.Job.Attempt.State) {
			return dispatch.ErrConflict
		}
		inputs := work.Job.Request.Spec().Inputs
		if index < 0 || index >= len(inputs) || inputs[index].Source.Kind != "https" || inputs[index].Source.ExpectedSHA256 != "" && inputs[index].Source.ExpectedSHA256 != m.SHA256 {
			return dispatch.ErrInvalid
		}
		role := "input:" + strconv.Itoa(index)
		var total int64
		for _, ref := range work.Job.Objects {
			if ref.Role == role {
				if ref.Object == m {
					return nil
				}
				return dispatch.ErrConflict
			}
			if ref.Role != "bundle" {
				if ref.Object.Bytes > work.Job.Profile.MaxInputBytes-total {
					return ErrCorrupt
				}
				total += ref.Object.Bytes
			}
		}
		if m.Bytes > work.Job.Profile.MaxInputBytes-total {
			return dispatch.ErrInvalid
		}
		// This object identity is generated by the worker, not supplied by an HTTP request.
		// Existing metadata must match exactly; never overwrite another object's ownership.
		var existing domain.ObjectMetadata
		err = tx.QueryRowContext(ctx, "SELECT workspace_id,object_id,bytes,sha256 FROM objects WHERE workspace_id=? AND object_id=?", string(m.WorkspaceID), string(m.ID)).Scan(&existing.WorkspaceID, &existing.ID, &existing.Bytes, &existing.SHA256)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.ExecContext(ctx, "INSERT INTO objects(workspace_id,object_id,bytes,sha256) VALUES(?,?,?,?)", string(m.WorkspaceID), string(m.ID), m.Bytes, string(m.SHA256))
		} else if err == nil && existing != m {
			return dispatch.ErrConflict
		}
		if err != nil {
			return dbError(err)
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO job_objects VALUES(?,?,?,?,?,?)", string(m.WorkspaceID), string(h.Claim.JobID), role, string(m.ID), m.Bytes, string(m.SHA256))
		if err != nil {
			return dbError(err)
		}
		return advanceSchedulerClock(ctx, tx, now)
	})
}

func newMutationPolicy(ctx context.Context, tx *sql.Tx, work dispatch.Work, now time.Time) error {
	settings, last, err := schedulerControl(ctx, tx, now)
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

func (s *Store) CommitDispatch(ctx context.Context, h dispatch.Handle, action dispatch.Action, now time.Time) (dispatch.Work, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return dispatch.Work{}, err
	}
	defer done()
	var result dispatch.Work
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		work, err := loadDispatch(ctx, tx, h.Claim, now)
		if err != nil {
			return err
		}
		if err = checkHandle(work, h); err != nil {
			return err
		}
		if action.Kind == dispatch.BeginPreparation || action.Kind == dispatch.BeginSubmission {
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
		next, state, event, err := dispatch.Apply(work.Journal, work.Job.Attempt.State, action, now)
		if err != nil {
			return err
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
		if err = advanceSchedulerClock(ctx, tx, now); err != nil {
			return err
		}
		work.Job.Attempt = updated
		work.Journal = next
		work.Handle.Version = next.Version
		work.Handle.Claim.AttemptRevision = updated.Revision
		result = work
		return nil
	})
	if err != nil {
		return dispatch.Work{}, err
	}
	return result, nil
}
