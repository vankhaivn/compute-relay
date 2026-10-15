package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

var _ admission.Repository = (*Store)(nil)

const createJobOperation = "job.create"

// actorInTx closes revocation/disable/scope races between HTTP auth and commit. Token IDs
// are NOT bearer secrets; only an authenticated application service calls this store seam.
func actorInTx(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, tokenID string, scope auth.Scope) ([]string, error) {
	now := transactionTime(ctx)
	if now.IsZero() {
		return nil, ErrCorrupt
	}
	if !w.Valid() || !validID(tokenID) {
		return nil, auth.ErrUnauthenticated
	}
	var encoded, profiles string
	var expiry sql.NullString
	var revoked, enabled bool
	err := tx.QueryRowContext(ctx, `SELECT t.scopes,t.expires_at,t.revoked,w.enabled,w.allowed_profiles FROM api_token_hashes t JOIN workspaces w ON w.workspace_id=t.workspace_id WHERE t.workspace_id=? AND t.token_id=?`, string(w), tokenID).Scan(&encoded, &expiry, &revoked, &enabled, &profiles)
	if errors.Is(err, sql.ErrNoRows) || revoked {
		return nil, auth.ErrUnauthenticated
	}
	if err != nil {
		return nil, dbError(err)
	}
	if !enabled {
		return nil, auth.ErrForbidden
	}
	if expiry.Valid {
		at, err := time.Parse(time.RFC3339Nano, expiry.String)
		if err != nil {
			return nil, ErrCorrupt
		}
		if !now.Before(at) {
			return nil, auth.ErrUnauthenticated
		}
	}
	var scopes []auth.Scope
	if len(encoded) > 100 || json.Unmarshal([]byte(encoded), &scopes) != nil || !auth.ValidScopes(scopes) {
		return nil, ErrCorrupt
	}
	found := false
	for _, v := range scopes {
		if v == scope {
			found = true
		}
	}
	if !found {
		return nil, auth.ErrForbidden
	}
	var workspace auth.Workspace
	workspace.ID = w
	workspace.Enabled = true
	if len(profiles) > 20000 || json.Unmarshal([]byte(profiles), &workspace.AllowedProfiles) != nil || !validWorkspace(workspace) {
		return nil, ErrCorrupt
	}
	return workspace.AllowedProfiles, nil
}

func resolveJob(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, req admission.Request) (admission.Profile, []admission.FrozenObject, error) {
	var profile admission.Profile
	spec := req.Spec()
	var raw string
	var enabled bool
	err := tx.QueryRowContext(ctx, `SELECT r.snapshot,p.enabled FROM profiles p JOIN profile_revisions r ON p.profile=r.profile AND p.revision=r.revision WHERE p.profile=?`, spec.Profile).Scan(&raw, &enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return profile, nil, auth.ErrForbidden
	}
	if err != nil {
		return profile, nil, dbError(err)
	}
	if len(raw) > 8192 || json.Unmarshal([]byte(raw), &profile) != nil || profile.Validate() != nil || profile.Binding.Profile != spec.Profile {
		return profile, nil, ErrCorrupt
	}
	if err = managedProfileOwned(ctx, tx, w, profile); err != nil {
		return profile, nil, err
	}
	var selected bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM managed_connections WHERE workspace_id=? AND current_profile=? AND new_work='enabled' AND pending_operation='')`, w, spec.Profile).Scan(&selected); err != nil {
		return profile, nil, dbError(err)
	}
	if !selected || !enabled {
		return profile, nil, admission.ErrSelection
	}
	if err = profile.Check(spec); err != nil {
		return profile, nil, err
	}
	refs := []admission.FrozenObject{}
	var total int64
	load := func(role string, id domain.ObjectID, max int64, expectedSHA domain.SHA256Digest, expectedBytes *int64) error {
		var m domain.ObjectMetadata
		err := tx.QueryRowContext(ctx, retainedInputQuery, string(w), string(id)).Scan(&m.WorkspaceID, &m.ID, &m.Bytes, &m.SHA256)
		if errors.Is(err, sql.ErrNoRows) {
			return admission.ErrInputs
		}
		if err != nil {
			return dbError(err)
		}
		if !m.Valid() || m.ID != id || m.WorkspaceID != w {
			return ErrCorrupt
		}
		if expectedBytes != nil && (m.Bytes != *expectedBytes || m.SHA256 != expectedSHA) {
			return admission.ErrInputs
		}
		if m.Bytes > max {
			return admission.ErrRequirements
		}
		if role != "bundle" {
			if m.Bytes > profile.MaxInputBytes-total {
				return admission.ErrRequirements
			}
			total += m.Bytes
		}
		refs = append(refs, admission.FrozenObject{Role: role, Object: m})
		return nil
	}
	if err = load("bundle", spec.Bundle.ObjectID, profile.MaxBundleBytes, spec.Bundle.SHA256, spec.Bundle.Bytes); err != nil {
		return profile, nil, err
	}
	for i, in := range spec.Inputs {
		if err = load("input:"+strconv.Itoa(i), in.Source.ObjectID, 2<<30, in.Source.SHA256, in.Source.Bytes); err != nil {
			return profile, nil, err
		}
	}
	return profile, refs, nil
}

func (s *Store) ValidateJob(ctx context.Context, w domain.WorkspaceID, tokenID string, req admission.Request) (admission.Validation, error) {
	if !req.Valid() {
		return admission.Validation{}, admission.ErrSpec
	}
	result := admission.Validation{Valid: true, Warnings: []string{"Offline admission checks only; provider capability and bundle validation are still required before dispatch."}, Requirements: []admission.Requirement{{Name: "provider_validation", Verification: "before_dispatch"}, {Name: "bundle_integrity_and_layout", Verification: "before_dispatch"}}}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		_, err := actorInTx(ctx, tx, w, tokenID, auth.Read)
		if err != nil {
			return err
		}
		_, _, err = resolveJob(ctx, tx, w, req)
		if err != nil {
			return err
		}
		if req.Spec().Expired(now) {
			return admission.ErrExpired
		}
		if req.Spec().Resources.Accelerator == "gpu" {
			result.Requirements = append(result.Requirements, admission.Requirement{Name: "gpu_requirements", Verification: "verify_after_start"})
		}
		return nil
	})
	if err != nil {
		return admission.Validation{}, err
	}
	return result, nil
}

func (s *Store) AdmitJob(ctx context.Context, w domain.WorkspaceID, tokenID, keyHash string, req admission.Request, limits admission.Limits) (admission.Receipt, error) {
	if !req.Valid() || !domain.SHA256Digest(keyHash).Valid() || !limits.Valid() {
		return admission.Receipt{}, admission.ErrSpec
	}
	var receipt admission.Receipt
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		_, err := actorInTx(ctx, tx, w, tokenID, auth.Write)
		if err != nil {
			return err
		}
		key := receiptKey{w, "admission", createJobOperation, keyHash, string(req.Hash()), admission.CanonicalVersion, admission.ErrConflict}
		prior, err := replayAdmission(ctx, tx, w, keyHash, req)
		if err != nil {
			return err
		}
		if prior != nil {
			receipt = *prior
			return nil
		}
		profile, refs, err := resolveJob(ctx, tx, w, req)
		if err != nil {
			return err
		}
		if req.Spec().Expired(now) {
			return admission.ErrExpired
		}
		if err = checkHostNewWork(ctx, tx, admission.Record{Job: domain.Job{WorkspaceID: w}, Profile: profile}, now); err != nil {
			return err
		}
		var total, workspace int
		err = tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(CASE WHEN j.workspace_id=? THEN 1 ELSE 0 END),0) FROM jobs j JOIN attempts a ON a.workspace_id=j.workspace_id AND a.job_id=j.job_id AND a.attempt_id=j.active_attempt_id WHERE a.orchestration NOT IN ('succeeded','failed','cancelled','timed_out')`, string(w)).Scan(&total, &workspace)
		if err != nil {
			return dbError(err)
		}
		if total >= limits.MaxOutstandingTotal || workspace >= limits.MaxOutstandingWorkspace {
			return admission.ErrLimit
		}
		job, err := randomID("job_", 16)
		if err != nil {
			return err
		}
		attempt, err := randomID("att_", 16)
		if err != nil {
			return err
		}
		event, err := randomID("evt_", 16)
		if err != nil {
			return err
		}
		nonce, err := randomID("", 32)
		if err != nil {
			return err
		}
		created := now.Format(time.RFC3339Nano)
		a, err := domain.NewAttempt(domain.AttemptID(attempt), domain.JobID(job), 1, now)
		if err != nil {
			return ErrInvalid
		}
		state, err := json.Marshal(a.State)
		if err != nil {
			return ErrInvalid
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?)`, string(w), job, string(req.Canonical()), admission.CanonicalVersion, string(req.Hash()), profile.Binding.Profile, profile.Binding.ConfigurationRevision, attempt, created)
		if err != nil {
			return dbError(err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?)`, string(w), job, attempt, 1, nonce, string(state), string(a.State.Orchestration), 1, created, created)
		if err != nil {
			return dbError(err)
		}
		for _, ref := range refs {
			_, err = tx.ExecContext(ctx, `INSERT INTO job_objects VALUES(?,?,?,?,?,?)`, string(w), job, ref.Role, string(ref.Object.ID), ref.Object.Bytes, string(ref.Object.SHA256))
			if err != nil {
				return dbError(err)
			}
		}
		if err = pinHostCacheAdmission(ctx, tx, w, domain.JobID(job), domain.AttemptID(attempt), profile, refs, now); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO events(workspace_id,job_id,event_id,sequence,attempt_id,type,occurred_at) VALUES(?,?,?,?,?,?,?)`, string(w), job, event, 1, attempt, string(domain.EventJobAccepted), created)
		if err != nil {
			return dbError(err)
		}
		receipt = admission.Receipt{JobID: domain.JobID(job), AttemptID: domain.AttemptID(attempt), Status: "queued", CreatedAt: now, Links: admission.JobLinks(w, domain.JobID(job))}
		encoded, err := json.Marshal(receipt)
		if err != nil {
			return ErrInvalid
		}
		_, _, err = replayOrCreate(ctx, tx, key, &savedReceipt{job, domain.JobID(job), domain.AttemptID(attempt), string(encoded)})
		return err
	})
	// Never return assigned IDs/success for a failed or uncertain transaction acknowledgement.
	if err != nil {
		return admission.Receipt{}, err
	}
	return receipt, nil
}
func randomID(prefix string, n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", ErrUnavailable
	}
	return prefix + hex.EncodeToString(b), nil
}
