package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func insertResource(ctx context.Context, tx *sql.Tx, work dispatch.Work, j dispatch.Journal, purpose, operation string, now time.Time) error {
	id := j.Plan.Job.Identity
	identity, _ := json.Marshal(id)
	_, err := tx.ExecContext(ctx, `INSERT INTO provider_resources(resource_id,workspace_id,job_id,attempt_id,instance_id,account_scope,purpose,operation_id,resource_key,identity,plan_sha256,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, "res_"+purpose+"_"+string(id.IntentID), string(id.WorkspaceID), string(id.JobID), string(id.AttemptID), string(id.InstanceID), work.Job.Profile.AccountScope, purpose, operation, id.ResourceKey, string(identity), string(j.Plan.Digest()), now.UTC().Format(time.RFC3339Nano))
	return dbError(err)
}
func writeDispatchLedger(ctx context.Context, tx *sql.Tx, work dispatch.Work, j dispatch.Journal, kind dispatch.Kind, now time.Time) error {
	if kind == dispatch.BeginPreparation {
		return insertResource(ctx, tx, work, j, "staging", string(j.PreparationID), now)
	}
	if j.Plan == nil {
		return nil
	}
	switch kind {
	case dispatch.PreparationSeen:
		ready := "pending"
		if j.Prepared.Ready {
			ready = "ready"
		}
		res, err := tx.ExecContext(ctx, "UPDATE provider_resources SET resource_ref=?,readiness=? WHERE operation_id=? AND purpose='staging'", j.Prepared.Resource, ready, string(j.PreparationID))
		if err != nil {
			return dbError(err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		if n != 1 {
			return ErrCorrupt
		}
	case dispatch.BeginSubmission:
		if err := insertResource(ctx, tx, work, j, "execution", string(j.Plan.Job.Identity.IntentID), now); err != nil {
			return err
		}
		identity, _ := json.Marshal(j.Plan.Job.Identity)
		prepared, _ := json.Marshal(j.Prepared)
		_, err := tx.ExecContext(ctx, `INSERT INTO submission_intents(intent_id,queue_seq,identity,prepared,status,created_at) VALUES(?,?,?,?,'started',?)`, string(j.Plan.Job.Identity.IntentID), work.Handle.Claim.Sequence, string(identity), string(prepared), now.UTC().Format(time.RFC3339Nano))
		return dbError(err)
	case dispatch.SubmissionSeen:
		status := "unknown"
		remote := ""
		if j.Phase == dispatch.Rejected {
			status = "rejected"
		}
		if j.Remote != nil {
			status = "accepted"
			encoded, _ := json.Marshal(j.Remote)
			remote = string(encoded)
		}
		res, err := tx.ExecContext(ctx, "UPDATE submission_intents SET status=?,remote_ref=? WHERE intent_id=? AND status IN ('started','unknown')", status, remote, string(j.Plan.Job.Identity.IntentID))
		if err != nil {
			return dbError(err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		if n != 1 {
			return dispatch.ErrConflict
		}
		if j.Phase == dispatch.Rejected && j.Problem != nil && j.Problem.Code == domain.CodeQuotaExhausted {
			// Rejection proves exhaustion, not an invented numerical allowance. Preserve
			// unknown values and latch the account until fresh positive quota evidence.
			resource := work.Job.Request.Spec().Resources.Accelerator
			q := provider.QuotaObservation{Status: provider.QuotaUnknown, Resource: resource, ObservedAt: now, Source: "provider_rejection", Precision: "unknown"}
			raw, _ := json.Marshal(q)
			_, err = tx.ExecContext(ctx, `INSERT INTO scheduler_quotas VALUES(?,?,?,?,1) ON CONFLICT(account_scope,resource) DO UPDATE SET observation=excluded.observation,observed_ms=excluded.observed_ms,exhausted=1`, work.Job.Profile.AccountScope, resource, string(raw), now.UnixMilli())
			if err != nil {
				return dbError(err)
			}
		}
		if j.Remote != nil {
			_, err = tx.ExecContext(ctx, "UPDATE provider_resources SET resource_ref=?,readiness='ready' WHERE operation_id=? AND purpose='execution'", executionRef(*j.Remote), string(j.Plan.Job.Identity.IntentID))
			return dbError(err)
		}
	}
	return nil
}
func executionRef(r provider.RemoteReference) string {
	b, _ := json.Marshal(struct {
		Resource string `json:"resource"`
		Version  string `json:"version"`
	}{r.Resource, r.Version})
	return string(b)
}

// Cross-check the independent one-shot/resource records before trusting a journal. A
// deleted/corrupted intent is a hard error, never permission to create the resource again.
func checkDispatchLedger(ctx context.Context, tx *sql.Tx, work dispatch.Work) error {
	j := work.Journal
	id := j.Plan.Job.Identity
	identity, _ := json.Marshal(id)
	check := func(purpose, op, wantRef, wantReady string) error {
		var w, job, a, instance, account, key, raw, hash, ref, ready string
		err := tx.QueryRowContext(ctx, `SELECT workspace_id,job_id,attempt_id,instance_id,account_scope,resource_key,identity,plan_sha256,resource_ref,readiness FROM provider_resources WHERE operation_id=? AND purpose=?`, op, purpose).Scan(&w, &job, &a, &instance, &account, &key, &raw, &hash, &ref, &ready)
		if err != nil {
			return ErrCorrupt
		}
		if w != string(id.WorkspaceID) || job != string(id.JobID) || a != string(id.AttemptID) || instance != string(id.InstanceID) || account != work.Job.Profile.AccountScope || key != id.ResourceKey || raw != string(identity) || hash != string(j.Plan.Digest()) || ref != wantRef || ready != wantReady {
			return ErrCorrupt
		}
		return nil
	}
	ref, ready := "", "unknown"
	if j.Prepared != nil {
		ref = j.Prepared.Resource
		ready = "pending"
		if j.Prepared.Ready {
			ready = "ready"
		}
	}
	if err := check("staging", string(j.PreparationID), ref, ready); err != nil {
		return err
	}
	if !j.SubmitStarted {
		return nil
	}
	var sequence int64
	var raw, prepared, status, remote string
	err := tx.QueryRowContext(ctx, "SELECT queue_seq,identity,prepared,status,remote_ref FROM submission_intents WHERE intent_id=?", string(id.IntentID)).Scan(&sequence, &raw, &prepared, &status, &remote)
	if err != nil {
		return ErrCorrupt
	}
	expected, _ := json.Marshal(j.Prepared)
	if sequence != work.Handle.Claim.Sequence || raw != string(identity) || prepared != string(expected) {
		return ErrCorrupt
	}
	if j.Remote != nil {
		b, _ := json.Marshal(j.Remote)
		if status != "accepted" || remote != string(b) {
			return ErrCorrupt
		}
		return check("execution", string(id.IntentID), executionRef(*j.Remote), "ready")
	}
	if j.Phase == dispatch.Rejected {
		if status != "rejected" {
			return ErrCorrupt
		}
	} else if status != "started" && status != "unknown" {
		return ErrCorrupt
	}
	if remote != "" {
		return ErrCorrupt
	}
	return check("execution", string(id.IntentID), "", "unknown")
}
