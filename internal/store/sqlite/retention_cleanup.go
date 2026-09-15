package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

var _ retention.PreviewRepository = (*Store)(nil)

func remoteCleanupPlan(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id domain.ProviderResourceID, p retention.Policy, now time.Time) (retention.RemotePlan, error) {
	plan := retention.RemotePlan{WorkspaceID: w, ResourceID: id}
	var instance, account, resourceKey, identity, created string
	err := tx.QueryRowContext(ctx, `SELECT job_id,attempt_id,purpose,operation_id,instance_id,account_scope,resource_key,identity,plan_sha256,resource_ref,created_at FROM provider_resources WHERE workspace_id=? AND resource_id=?`, string(w), string(id)).Scan(&plan.JobID, &plan.AttemptID, &plan.Purpose, &plan.CreationOperationID, &instance, &account, &resourceKey, &identity, &plan.PlanSHA256, &plan.Reference, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return plan, auth.ErrNotFound
	}
	if err != nil {
		return plan, dbError(err)
	}
	target, err := controlTarget(ctx, tx, w, plan.JobID, plan.AttemptID)
	if err != nil {
		return plan, err
	}
	work, err := controlWork(ctx, tx, target)
	if err != nil {
		return plan, err
	}
	j := work.Journal
	if j.Plan == nil || json.Unmarshal([]byte(identity), &plan.Identity) != nil || plan.Identity != j.Plan.Job.Identity || plan.Identity.Validate() != nil || string(plan.Identity.InstanceID) != instance || plan.Identity.ResourceKey != resourceKey || account != target.Profile.AccountScope || plan.PlanSHA256 != j.Plan.Digest() {
		return plan, ErrCorrupt
	}
	plan.Binding.Binding = target.Profile.Binding
	plan.Binding.AccountScope = target.Profile.AccountScope
	plan.Binding.CredentialRef = target.Profile.CredentialRef
	plan.AttemptRevision = target.Attempt.Revision
	switch plan.Purpose {
	case "execution":
		if string(plan.CreationOperationID) != string(plan.Identity.IntentID) {
			return plan, ErrCorrupt
		}
		plan.Remote = j.Remote
	case "staging":
		if plan.CreationOperationID != j.PreparationID {
			return plan, ErrCorrupt
		}
	default:
		return plan, ErrCorrupt
	}
	at, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return plan, ErrCorrupt
	}
	ref, err := retentionReference(ctx, tx, w, plan.JobID, plan.AttemptID)
	if err != nil {
		return plan, err
	}
	plan.Decision, err = p.Assess(now, at, []retention.Reference{ref}, false)
	if err != nil {
		return plan, err
	}
	if !plan.Decision.Eligible {
		return plan, nil
	}
	// A local decision never guesses a remote identity from a prefix. Shared exact
	// references are blocked even when different journals individually validate.
	var duplicate bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_resources WHERE instance_id=? AND account_scope=? AND purpose=? AND resource_ref=? AND resource_id!=?)`, instance, account, plan.Purpose, plan.Reference, string(id)).Scan(&duplicate); err != nil {
		return plan, dbError(err)
	}
	if plan.Reference == "" || duplicate {
		plan.Decision.Eligible = false
		plan.Decision.Reason = "identity_unresolved_or_shared"
		return plan, nil
	}
	if plan.Purpose == "staging" {
		// The existing cleanup port addresses executions, not staging datasets.
		// Preserve exact staging ownership for a later supported adapter path.
		plan.Decision.Eligible = false
		plan.Decision.Reason = "staging_preview_unavailable"
		return plan, nil
	}
	if plan.Remote == nil || plan.Remote.Validate() != nil {
		return plan, ErrCorrupt
	}
	err = tx.QueryRowContext(ctx, `SELECT snapshot_sha256 FROM collection_publications WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(w), string(plan.JobID), string(plan.AttemptID)).Scan(&plan.PublicationSHA256)
	if err != nil || !plan.PublicationSHA256.Valid() {
		return plan, ErrCorrupt
	}
	return plan, nil
}

func (s *Store) PlanRemoteCleanup(ctx context.Context, w domain.WorkspaceID, token string, id domain.ProviderResourceID, p retention.Policy, now time.Time) (retention.RemotePlan, error) {
	if !w.Valid() || !id.Valid() || !p.Valid() || now.IsZero() {
		return retention.RemotePlan{}, retention.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return retention.RemotePlan{}, err
	}
	defer done()
	var plan retention.RemotePlan
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		var err error
		plan, err = remoteCleanupPlan(ctx, tx, w, id, p, now)
		return err
	})
	if err != nil {
		return retention.RemotePlan{}, err
	}
	return plan, nil
}

func (s *Store) RecordCleanupPreview(ctx context.Context, w domain.WorkspaceID, token string, original retention.RemotePlan, p retention.Policy, outcome string, now time.Time) error {
	if w != original.WorkspaceID || !original.Decision.Eligible || !p.Valid() || !original.ResourceID.Valid() || (outcome != "would_delete" && outcome != "already_absent" && outcome != "retained") {
		return retention.ErrInvalid
	}
	return s.collectionTx(ctx, now, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		current, err := remoteCleanupPlan(ctx, tx, w, original.ResourceID, p, now)
		if err != nil {
			return err
		}
		if !current.Decision.Eligible || current.Digest() != original.Digest() {
			return retention.ErrChanged
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO cleanup_previews VALUES(?,?,?,?) ON CONFLICT(resource_id) DO UPDATE SET plan_sha256=excluded.plan_sha256,outcome=excluded.outcome,observed_at=excluded.observed_at`, string(original.ResourceID), string(original.Digest()), outcome, now.UTC().Format(time.RFC3339Nano))
		return dbError(err)
	})
}
