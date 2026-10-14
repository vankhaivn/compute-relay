package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// cleanupResources enumerates the ledger, never remote names or filesystem paths.
// Both independent creation records and the original journal must agree.
func cleanupResources(ctx context.Context, tx *sql.Tx, target admission.Record) ([]cleanup.Resource, error) {
	work, err := controlWork(ctx, tx, target)
	if err != nil {
		return nil, err
	}
	j := work.Journal
	w, job, attempt := string(target.Job.WorkspaceID), string(target.Job.ID), string(target.Attempt.ID)
	rows, err := tx.QueryContext(ctx, `SELECT resource_id,purpose,operation_id,instance_id,account_scope,resource_key,identity,plan_sha256,resource_ref
 FROM provider_resources WHERE workspace_id=? AND job_id=? AND attempt_id=? ORDER BY CASE purpose WHEN 'execution' THEN 0 ELSE 1 END LIMIT ?`, w, job, attempt, cleanup.MaxResources+1)
	if err != nil {
		return nil, dbError(err)
	}
	type entry struct {
		resource                                    cleanup.Resource
		instance, account, key, identity, reference string
	}
	var entries []entry
	for rows.Next() {
		var e entry
		r := &e.resource.Request
		if err = rows.Scan(&r.LedgerID, &r.Purpose, &r.OwnershipOperationID, &e.instance, &e.account, &e.key, &e.identity, &r.PlanSHA256, &e.reference); err != nil {
			_ = rows.Close()
			return nil, dbError(err)
		}
		entries = append(entries, e)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return nil, dbError(err)
	}
	if len(entries) > cleanup.MaxResources {
		return nil, ErrCorrupt
	}
	var collected bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM collection_publications WHERE workspace_id=? AND job_id=? AND attempt_id=?)`, w, job, attempt).Scan(&collected); err != nil {
		return nil, dbError(err)
	}
	collected = collected && (target.Attempt.State.Result == domain.ResultAvailable || target.Attempt.State.Result == domain.ResultExpired)
	result := make([]cleanup.Resource, 0, len(entries))
	for _, e := range entries {
		r := e.resource
		q := &r.Request
		var identity provider.Identity
		if j.Plan == nil || json.Unmarshal([]byte(e.identity), &identity) != nil || identity != j.Plan.Job.Identity || identity.Validate() != nil ||
			identity.WorkspaceID != target.Job.WorkspaceID || identity.JobID != target.Job.ID || identity.AttemptID != target.Attempt.ID ||
			string(identity.InstanceID) != e.instance || identity.ResourceKey != e.key || e.account != target.Profile.AccountScope ||
			q.PlanSHA256 != j.Plan.Digest() || !q.LedgerID.Valid() {
			return nil, ErrCorrupt
		}
		q.CreationOperationID, q.ResultsCollected, q.Mode = j.PreparationID, collected, provider.CleanupApply
		q.Remote.Identity = identity
		switch q.Purpose {
		case "staging":
			if q.OwnershipOperationID != j.PreparationID || j.Prepared != nil && e.reference != j.Prepared.Resource {
				return nil, ErrCorrupt
			}
			q.Remote.Resource, q.Remote.Version = e.reference, preparedVersion
			if j.Prepared == nil || e.reference == "" {
				r.Reason = "creation_unresolved"
			}
		case "execution":
			if string(q.OwnershipOperationID) != string(identity.IntentID) || !j.SubmitStarted {
				return nil, ErrCorrupt
			}
			// A definitive, independently journaled rejection created no execution
			// resource. Keep the original staging resource eligible; never infer this
			// from a timeout, missing reference or a remote not-found response alone.
			if j.Remote == nil && e.reference == "" && j.Phase == dispatch.Rejected && j.Problem != nil && !j.Problem.ComputeMayHaveStarted &&
				target.Attempt.State.Execution == domain.ExecutionNotSubmitted && target.Attempt.State.RemoteActivity == domain.RemoteActivityInactive {
				var rejected bool
				if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM submission_intents WHERE intent_id=? AND identity=? AND status='rejected' AND remote_ref='')`,
					string(q.OwnershipOperationID), e.identity).Scan(&rejected); err != nil {
					return nil, dbError(err)
				}
				if rejected {
					continue
				}
			}
			if j.Remote == nil || e.reference == "" {
				r.Reason = "creation_unresolved"
			} else {
				if e.reference != executionRef(*j.Remote) || j.Remote.Identity != identity {
					return nil, ErrCorrupt
				}
				q.Remote = *j.Remote
			}
		default:
			return nil, ErrCorrupt
		}
		if r.Reason == "" && !q.Valid() {
			return nil, ErrCorrupt
		}
		if r.Reason == "" {
			// Compare underlying references across staging/execution roles too. A
			// resource needed by another attempt can never be removed here.
			var shared bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_resources
 WHERE account_scope=? AND NOT (workspace_id=? AND job_id=? AND attempt_id=?)
 AND CASE WHEN purpose='execution' AND json_valid(resource_ref) THEN json_extract(resource_ref,'$.resource') ELSE resource_ref END=?)`,
				e.account, w, job, attempt, q.Remote.Resource).Scan(&shared); err != nil {
				return nil, dbError(err)
			}
			if shared {
				r.Reason = "shared_resource"
			}
		}
		result = append(result, r)
	}
	return result, nil
}

func cleanupReason(err error) string {
	switch {
	case err == nil:
		return "eligible"
	case errors.Is(err, cleanup.ErrUncollected):
		return "uncollected_results"
	case errors.Is(err, operations.ErrBusy):
		return "active_lease_or_control"
	case errors.Is(err, operations.ErrUnresolved):
		return "execution_unresolved"
	default:
		return "attempt_not_settled"
	}
}

func (s *Store) PlanCleanup(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, attempt domain.AttemptID, discard bool) (cleanup.PreviewPlan, error) {
	if !w.Valid() || !job.Valid() || !attempt.Valid() {
		return cleanup.PreviewPlan{}, operations.ErrRequest
	}
	var result cleanup.PreviewPlan
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		target, err := controlTarget(ctx, tx, w, job, attempt)
		if err != nil {
			return err
		}
		resources, err := cleanupResources(ctx, tx, target)
		if err != nil {
			return err
		}
		_, eligibility := eligibleCleanupAttempt(ctx, tx, target, discard, now, "")
		if eligibility != nil && !errors.Is(eligibility, operations.ErrBusy) && !errors.Is(eligibility, operations.ErrState) && !errors.Is(eligibility, operations.ErrUnresolved) && !errors.Is(eligibility, cleanup.ErrUncollected) {
			return eligibility
		}
		result = cleanup.PreviewPlan{WorkspaceID: w, JobID: job, AttemptID: attempt, Revision: target.Attempt.Revision,
			Binding:   provider.BindingSnapshot{Binding: target.Profile.Binding, AccountScope: target.Profile.AccountScope, CredentialRef: target.Profile.CredentialRef},
			Resources: resources, Eligible: eligibility == nil, Reason: cleanupReason(eligibility)}
		for _, r := range resources {
			if r.Reason != "" {
				result.Eligible, result.Reason = false, r.Reason
			}
		}
		if len(resources) == 0 {
			result.Eligible, result.Reason = false, "no_recorded_resources"
		}
		return nil
	})
	return result, err
}
