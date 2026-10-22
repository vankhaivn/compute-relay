package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const confirmedResourceCleanup = `EXISTS(SELECT 1 FROM cleanup_resources c WHERE c.resource_id=r.resource_id AND c.state IN ('deleted','absent'))`

// SSH uses original frozen machine evidence; unresolved legacy scopes are charged
// to every machine rather than inferred from a later login verification.
// The original staging row reserves a future kernel too. A terminal unused hold
// can be released; started/unknown creation remains charged until exact evidence.
func remoteResourceOccupancy(ctx context.Context, tx *sql.Tx, installation domain.RuntimeInstallationID, scope, providerType string) (int, int, error) {
	if providerType == "ssh" {
		var containers int
		err := tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_resources r
        JOIN managed_connections c ON c.connection_id=r.instance_id AND c.provider_type='ssh'
        JOIN jobs j ON j.workspace_id=r.workspace_id AND j.job_id=r.job_id
        JOIN profile_revisions p ON p.profile=j.profile AND p.revision=j.profile_revision
        LEFT JOIN scheduler_queue q ON q.workspace_id=r.workspace_id AND q.job_id=r.job_id AND q.attempt_id=r.attempt_id
        LEFT JOIN scheduler_resource_reservations m ON m.queue_seq=q.queue_seq
        WHERE r.purpose='staging' AND json_extract(r.identity,'$.InstallationID')=?
        AND coalesce(nullif(json_extract(p.snapshot,'$.resource_scope'),''),nullif(m.resource_scope,''),'') IN (?,'')
        AND NOT (`+confirmedResourceCleanup+`)
        AND NOT (r.resource_ref!='' AND EXISTS(SELECT 1 FROM provider_resources e JOIN cleanup_resources d ON d.resource_id=e.resource_id
        WHERE e.workspace_id=r.workspace_id AND e.job_id=r.job_id AND e.attempt_id=r.attempt_id
        AND e.purpose='execution' AND json_valid(e.resource_ref) AND json_extract(e.resource_ref,'$.resource')=r.resource_ref
        AND d.state IN ('deleted','absent')))`, installation, scope).Scan(&containers)
		return containers, 0, dbError(err)
	}
	var staging, kernels int
	query := `SELECT count(*),coalesce(sum(CASE
 WHEN EXISTS(SELECT 1 FROM provider_resources e WHERE e.workspace_id=r.workspace_id AND e.job_id=r.job_id AND e.attempt_id=r.attempt_id AND e.purpose='execution') THEN
 CASE WHEN EXISTS(SELECT 1 FROM provider_resources e JOIN cleanup_resources c ON c.resource_id=e.resource_id
 WHERE e.workspace_id=r.workspace_id AND e.job_id=r.job_id AND e.attempt_id=r.attempt_id AND e.purpose='execution' AND c.state IN ('deleted','absent'))
 OR EXISTS(SELECT 1 FROM submission_intents s WHERE s.intent_id=json_extract(r.identity,'$.IntentID') AND s.status='rejected' AND s.remote_ref='' AND s.identity=r.identity AND json_extract(a.state,'$.Execution')='not_submitted' AND json_extract(a.state,'$.RemoteActivity')='inactive') THEN 0 ELSE 1 END
 WHEN a.orchestration IN ('succeeded','failed','cancelled','timed_out') AND json_extract(a.state,'$.Execution')='not_submitted' THEN 0
 ELSE 1 END),0)
 FROM provider_resources r JOIN attempts a ON a.workspace_id=r.workspace_id AND a.job_id=r.job_id AND a.attempt_id=r.attempt_id
 WHERE r.purpose='staging' AND r.account_scope=? AND json_extract(r.identity,'$.InstallationID')=?`
	// Keep staging and kernel counts independent: staging cleanup cannot release
	// a still-unconfirmed execution object, and completed kernels still count.
	err := tx.QueryRowContext(ctx, query, scope, installation).Scan(&staging, &kernels)
	if err != nil {
		return 0, 0, dbError(err)
	}
	var deleted int
	err = tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_resources r WHERE r.purpose='staging' AND r.account_scope=? AND json_extract(r.identity,'$.InstallationID')=? AND `+confirmedResourceCleanup, scope, installation).Scan(&deleted)
	if err != nil {
		return 0, 0, dbError(err)
	}
	staging -= deleted

	return staging, kernels, nil
}

func checkRemoteResourceNewWork(ctx context.Context, tx *sql.Tx, work dispatch.Work) error {
	if work.Journal.Plan != nil {
		return nil
	}
	var kind string
	err := tx.QueryRowContext(ctx, `SELECT provider_type FROM managed_connections WHERE connection_id=?`, work.Job.Profile.Binding.ProviderInstanceID).Scan(&kind)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return dbError(err)
	}
	if kind != "kaggle" && kind != "ssh" {
		return nil
	}
	scope := work.Job.Profile.AccountScope
	if kind == "ssh" {
		scope = work.Job.Profile.ResourceScope
		if scope == "" {
			return provider.WithReason(provider.ErrResourceCountLimit, "remote_resource_scope_unknown")
		}
	}
	staging, kernels, err := remoteResourceOccupancy(ctx, tx, work.InstallationID, scope, kind)
	if err != nil {
		return err
	}
	if staging >= provider.RemoteResourceCountLimit {
		reason := "remote_staging_dataset_limit"
		if kind == "ssh" {
			reason = "remote_attempt_container_limit"
		}
		return provider.WithReason(provider.ErrResourceCountLimit, reason)
	}
	if kind == "kaggle" && kernels >= provider.RemoteResourceCountLimit {
		return provider.WithReason(provider.ErrResourceCountLimit, "remote_kernel_limit")
	}
	if kind == "kaggle" {
		objects, err := sharedObjectsForRecord(work.Job)
		if err != nil {
			return err
		}
		_, err = missingSharedCapacity(ctx, tx, work.InstallationID, scope, objects)
		return err
	}
	return nil
}

func sharedObjectsForRecord(record admission.Record) ([]domain.ObjectMetadata, error) {
	refs := map[string]domain.ObjectMetadata{}
	for _, ref := range record.Objects {
		refs[ref.Role] = ref.Object
	}
	inputs := make([]provider.FrozenInput, 0, len(record.Request.Spec().Inputs))
	for i := range record.Request.Spec().Inputs {
		object, ok := refs["input:"+strconv.Itoa(i)]
		if !ok {
			return nil, ErrCorrupt
		}
		inputs = append(inputs, provider.FrozenInput{Object: object})
	}
	return provider.SelectSharedInputs(inputs, provider.DefaultSharedInputMinBytes, provider.DefaultSharedInputLimit), nil
}

func missingSharedCapacity(ctx context.Context, tx *sql.Tx, installation domain.RuntimeInstallationID, account string, objects []domain.ObjectMetadata) ([]domain.ObjectMetadata, error) {
	var missing []domain.ObjectMetadata
	for _, object := range objects {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM shared_resource_claims WHERE installation_id=? AND account_scope=? AND resource_key=?)`, installation, account, provider.SharedResourceKey(object.SHA256, object.Bytes)).Scan(&exists); err != nil {
			return nil, dbError(err)
		}
		if !exists {
			missing = append(missing, object)
		}
	}
	if len(missing) == 0 {
		return nil, nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM shared_resource_claims WHERE installation_id=? AND account_scope=?`, installation, account).Scan(&count); err != nil {
		return nil, dbError(err)
	}
	if count > provider.RemoteResourceCountLimit-len(missing) {
		return nil, provider.WithReason(provider.ErrResourceCountLimit, "remote_shared_dataset_limit")
	}
	return missing, nil
}

func reservePreparationSharedResources(ctx context.Context, tx *sql.Tx, work dispatch.Work, j dispatch.Journal, now time.Time) error {
	var kind string
	err := tx.QueryRowContext(ctx, `SELECT provider_type FROM managed_connections WHERE connection_id=?`, work.Job.Profile.Binding.ProviderInstanceID).Scan(&kind)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return dbError(err)
	}
	if kind != "kaggle" {
		return nil
	}
	objects := provider.SelectSharedInputs(j.Plan.Job.Inputs.Inputs, provider.DefaultSharedInputMinBytes, provider.DefaultSharedInputLimit)
	missing, err := missingSharedCapacity(ctx, tx, work.InstallationID, work.Job.Profile.AccountScope, objects)
	if err != nil {
		return err
	}
	for _, object := range missing {
		claim := provider.SharedResourceClaim{Identity: j.Plan.Job.Identity, PreparationID: j.PreparationID, PlanSHA256: j.Plan.Digest(), ContentSHA256: object.SHA256, Bytes: object.Bytes}
		if err = insertSharedReservation(ctx, tx, work.Job.Profile.AccountScope, claim, now); err != nil {
			return err
		}
	}
	return nil
}

func insertSharedReservation(ctx context.Context, tx *sql.Tx, account string, claim provider.SharedResourceClaim, now time.Time) error {
	identity, _ := json.Marshal(claim.Identity)
	_, err := tx.ExecContext(ctx, `INSERT INTO shared_resource_claims(installation_id,account_scope,resource_key,content_sha256,bytes,preparation_id,identity,plan_sha256,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, claim.Identity.InstallationID, account, provider.SharedResourceKey(claim.ContentSHA256, claim.Bytes), claim.ContentSHA256, claim.Bytes, claim.PreparationID, string(identity), claim.PlanSHA256, now.UTC().Format(time.RFC3339Nano))
	return dbError(err)
}

func validateSharedClaim(ctx context.Context, tx *sql.Tx, account string, claim provider.SharedResourceClaim) error {
	if !claim.Valid() || !domain.ObjectID(account).Valid() {
		return ErrInvalid
	}
	var identity, digest, storedAccount, kind, installation string
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT r.identity,r.plan_sha256,r.account_scope,c.provider_type,j.journal,x.installation_id
 FROM provider_resources r JOIN managed_connections c ON c.connection_id=r.instance_id
 JOIN scheduler_queue q ON q.workspace_id=r.workspace_id AND q.job_id=r.job_id AND q.attempt_id=r.attempt_id
 JOIN dispatch_journals j ON j.queue_seq=q.queue_seq CROSS JOIN runtime_installation x
 WHERE r.operation_id=? AND r.purpose='staging' AND x.singleton=1`, claim.PreparationID).Scan(&identity, &digest, &storedAccount, &kind, &raw, &installation)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInvalid
	}
	if err != nil {
		return dbError(err)
	}
	expected, _ := json.Marshal(claim.Identity)
	if identity != string(expected) || digest != string(claim.PlanSHA256) || storedAccount != account || kind != "kaggle" || installation != string(claim.Identity.InstallationID) {
		return ErrInvalid
	}
	var journal dispatch.Journal
	if len(raw) > dispatch.MaxJournalBytes || json.Unmarshal([]byte(raw), &journal) != nil || !journal.Valid() || journal.Plan == nil || journal.Plan.Digest() != claim.PlanSHA256 || journal.PreparationID != claim.PreparationID || journal.Plan.Job.Identity != claim.Identity || journal.Plan.Job.Inputs == nil {
		return ErrCorrupt
	}
	for _, input := range journal.Plan.Job.Inputs.Inputs {
		if input.Object.SHA256 == claim.ContentSHA256 && input.Object.Bytes == claim.Bytes {
			return nil
		}
	}
	return ErrInvalid
}

func (s *Store) TrackSharedUse(ctx context.Context, account string, claim provider.SharedResourceClaim) error {
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if err := validateSharedClaim(ctx, tx, account, claim); err != nil {
			return err
		}
		key := provider.SharedResourceKey(claim.ContentSHA256, claim.Bytes)
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM shared_resource_claims WHERE installation_id=? AND account_scope=? AND resource_key=?)`, claim.Identity.InstallationID, account, key).Scan(&exists); err != nil {
			return dbError(err)
		}
		if exists {
			return nil
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM shared_resource_claims WHERE installation_id=? AND account_scope=?`, claim.Identity.InstallationID, account).Scan(&count); err != nil {
			return dbError(err)
		}
		if count >= provider.RemoteResourceCountLimit {
			return provider.WithReason(provider.ErrResourceCountLimit, "remote_shared_dataset_limit")
		}
		return insertSharedReservation(ctx, tx, account, claim, now)
	})
}

func (s *Store) ClaimSharedCreation(ctx context.Context, account string, claim provider.SharedResourceClaim) (bool, error) {
	started := false
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		if err := validateSharedClaim(ctx, tx, account, claim); err != nil {
			return err
		}
		key := provider.SharedResourceKey(claim.ContentSHA256, claim.Bytes)
		var owner string
		var begun bool
		err := tx.QueryRowContext(ctx, `SELECT preparation_id,creation_started FROM shared_resource_claims WHERE installation_id=? AND account_scope=? AND resource_key=?`, claim.Identity.InstallationID, account, key).Scan(&owner, &begun)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrInvalid
		}
		if err != nil {
			return dbError(err)
		}
		if begun || owner != string(claim.PreparationID) {
			return nil
		}
		result, err := tx.ExecContext(ctx, `UPDATE shared_resource_claims SET creation_started=1 WHERE installation_id=? AND account_scope=? AND resource_key=? AND preparation_id=? AND creation_started=0`, claim.Identity.InstallationID, account, key, claim.PreparationID)
		if err != nil {
			return dbError(err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		started = count == 1
		return nil
	})
	return started && err == nil, err
}
