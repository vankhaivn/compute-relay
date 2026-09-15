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

// PutProfile is a LOCAL operator operation. A revision cannot change its contents.
// Disabling/remapping a profile affects new admissions only, not historical resolution.
func (s *Store) PutProfile(ctx context.Context, p admission.Profile, enabled bool) error {
	if p.Validate() != nil {
		return ErrInvalid
	}
	b, err := json.Marshal(p)
	if err != nil || len(b) > 8192 {
		return ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		var prior string
		err := tx.QueryRowContext(ctx, "SELECT snapshot FROM profile_revisions WHERE profile=? AND revision=?", p.Binding.Profile, p.Binding.ConfigurationRevision).Scan(&prior)
		switch {
		case err == nil:
			if prior != string(b) {
				return ErrConflict
			}
		case errors.Is(err, sql.ErrNoRows):
			if _, err = tx.ExecContext(ctx, "INSERT INTO profile_revisions VALUES(?,?,?)", p.Binding.Profile, p.Binding.ConfigurationRevision, string(b)); err != nil {
				return dbError(err)
			}
		default:
			return dbError(err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO profiles VALUES(?,?,?) ON CONFLICT(profile) DO UPDATE SET revision=excluded.revision,enabled=excluded.enabled`, p.Binding.Profile, p.Binding.ConfigurationRevision, enabled)
		return dbError(err)
	})
}

// actorInTx closes revocation/disable/scope races between HTTP auth and commit. Token IDs
// are NOT bearer secrets; only an authenticated application service calls this store seam.
func actorInTx(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, tokenID string, scope auth.Scope) ([]string, error) {
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
		if !time.Now().Before(at) {
			return nil, auth.ErrUnauthenticated
		}
	}
	var scopes []auth.Scope
	if len(encoded) > 100 || json.Unmarshal([]byte(encoded), &scopes) != nil || len(scopes) == 0 || len(scopes) > 3 {
		return nil, ErrCorrupt
	}
	found := false
	seen := map[auth.Scope]bool{}
	for _, v := range scopes {
		if seen[v] || (v != auth.Read && v != auth.Write && v != auth.Operate) {
			return nil, ErrCorrupt
		}
		seen[v] = true
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

func resolveJob(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, allowed []string, req admission.Request) (admission.Profile, []admission.FrozenObject, int, error) {
	var profile admission.Profile
	spec := req.Spec()
	permitted := false
	for _, v := range allowed {
		if v == spec.Profile {
			permitted = true
		}
	}
	if !permitted {
		return profile, nil, 0, auth.ErrForbidden
	}
	var raw string
	var enabled bool
	err := tx.QueryRowContext(ctx, `SELECT r.snapshot,p.enabled FROM profiles p JOIN profile_revisions r ON p.profile=r.profile AND p.revision=r.revision WHERE p.profile=?`, spec.Profile).Scan(&raw, &enabled)
	if errors.Is(err, sql.ErrNoRows) || !enabled && err == nil {
		return profile, nil, 0, auth.ErrForbidden
	}
	if err != nil {
		return profile, nil, 0, dbError(err)
	}
	if len(raw) > 8192 || json.Unmarshal([]byte(raw), &profile) != nil || profile.Validate() != nil || profile.Binding.Profile != spec.Profile {
		return profile, nil, 0, ErrCorrupt
	}
	if err = profile.Check(spec); err != nil {
		return profile, nil, 0, err
	}
	refs := []admission.FrozenObject{}
	pending := 0
	var total int64
	load := func(role string, id domain.ObjectID, max int64) error {
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
	if err = load("bundle", spec.Bundle.ObjectID, profile.MaxBundleBytes); err != nil {
		return profile, nil, 0, err
	}
	for i, in := range spec.Inputs {
		if in.Source.Kind == "https" {
			pending++
			continue
		}
		if err = load("input:"+strconv.Itoa(i), in.Source.ObjectID, 2<<30); err != nil {
			return profile, nil, 0, err
		}
	}
	return profile, refs, pending, nil
}

func (s *Store) ValidateJob(ctx context.Context, w domain.WorkspaceID, tokenID string, req admission.Request) (admission.Validation, error) {
	if !req.Valid() {
		return admission.Validation{}, admission.ErrSpec
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return admission.Validation{}, err
	}
	defer done()
	result := admission.Validation{Valid: true, Warnings: []string{"Offline admission checks only; provider capability and bundle validation are still required before dispatch."}, Requirements: []admission.Requirement{{Name: "provider_validation", Verification: "before_dispatch"}, {Name: "bundle_integrity_and_layout", Verification: "before_dispatch"}}}
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		allowed, err := actorInTx(ctx, tx, w, tokenID, auth.Read)
		if err != nil {
			return err
		}
		_, _, pending, err := resolveJob(ctx, tx, w, allowed, req)
		if err != nil {
			return err
		}
		if pending > 0 {
			result.Requirements = append(result.Requirements, admission.Requirement{Name: "https_input_snapshots", Verification: "before_dispatch", Reason: "Source records are durable; no input download occurs during admission."})
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
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return admission.Receipt{}, err
	}
	defer done()
	var receipt admission.Receipt
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		allowed, err := actorInTx(ctx, tx, w, tokenID, auth.Write)
		if err != nil {
			return err
		}
		var hash, version, raw, jobID, attemptID string
		err = tx.QueryRowContext(ctx, `SELECT request_sha256,canonical_version,receipt,job_id,attempt_id FROM idempotency WHERE workspace_id=? AND operation=? AND key_sha256=?`, string(w), createJobOperation, keyHash).Scan(&hash, &version, &raw, &jobID, &attemptID)
		if err == nil {
			if version != admission.CanonicalVersion {
				return ErrSchema
			}
			if hash != string(req.Hash()) {
				return admission.ErrConflict
			}
			if len(raw) > 4096 || json.Unmarshal([]byte(raw), &receipt) != nil || receipt.JobID != domain.JobID(jobID) || receipt.AttemptID != domain.AttemptID(attemptID) || !receipt.JobID.Valid() || !receipt.AttemptID.Valid() || receipt.Replay || receipt.Status != "queued" || receipt.CreatedAt.IsZero() || receipt.Links != admission.JobLinks(w, receipt.JobID) {
				return ErrCorrupt
			}
			receipt.Replay = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbError(err)
		}
		profile, refs, _, err := resolveJob(ctx, tx, w, allowed, req)
		if err != nil {
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
		now := time.Now().UTC()
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
		_, err = tx.ExecContext(ctx, `INSERT INTO events(workspace_id,job_id,event_id,sequence,attempt_id,type,occurred_at) VALUES(?,?,?,?,?,?,?)`, string(w), job, event, 1, attempt, string(domain.EventJobAccepted), created)
		if err != nil {
			return dbError(err)
		}
		receipt = admission.Receipt{JobID: domain.JobID(job), AttemptID: domain.AttemptID(attempt), Status: "queued", CreatedAt: now, Links: admission.JobLinks(w, domain.JobID(job))}
		encoded, err := json.Marshal(receipt)
		if err != nil {
			return ErrInvalid
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO idempotency VALUES(?,?,?,?,?,?,?,?)`, string(w), createJobOperation, keyHash, string(req.Hash()), admission.CanonicalVersion, job, attempt, string(encoded))
		return dbError(err)
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
