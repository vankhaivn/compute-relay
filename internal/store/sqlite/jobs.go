package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func (s *Store) ReadJob(ctx context.Context, w domain.WorkspaceID, tokenID string, id domain.JobID) (admission.Record, error) {
	if !w.Valid() || !id.Valid() {
		return admission.Record{}, admission.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return admission.Record{}, err
	}
	defer done()
	var result admission.Record
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, tokenID, auth.Read); err != nil {
			return err
		}
		var err error
		result, err = readJobRecord(ctx, tx, w, id)
		if err != nil {
			return err
		}
		result.Problem, err = readDispatchProblem(ctx, tx, w, id, result.Attempt.ID)
		if err != nil {
			return err
		}
		result.Collection, err = readCollectionStatus(ctx, tx, result, time.Now().UTC())
		if err != nil {
			return err
		}
		result.Preparation, err = readPreparationStatus(ctx, tx, result)
		if err != nil {
			return err
		}
		result.Scheduling, err = readSchedulingStatus(ctx, tx, result, time.Now().UTC())
		return err
	})
	if err != nil {
		return admission.Record{}, err
	}
	return result, nil
}

// readJobRecord shares validation between authorized HTTP reads and fenced workers.
// It is not exported: callers must first verify token authority or durable ownership.
func readJobRecord(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id domain.JobID) (admission.Record, error) {
	var result admission.Record
	var raw, hash, version, profile, created string
	err := tx.QueryRowContext(ctx, `SELECT j.request,j.request_sha256,j.canonical_version,r.snapshot,j.active_attempt_id,j.created_at FROM jobs j JOIN profile_revisions r ON j.profile=r.profile AND j.profile_revision=r.revision WHERE j.workspace_id=? AND j.job_id=?`, string(w), string(id)).Scan(&raw, &hash, &version, &profile, &result.Job.ActiveAttemptID, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return result, admission.ErrNotFound
	}
	if err != nil {
		return result, dbError(err)
	}
	if version != admission.CanonicalVersion {
		return result, ErrSchema
	}
	result.Request, err = admission.Parse([]byte(raw))
	if err != nil || string(result.Request.Hash()) != hash || string(result.Request.Canonical()) != raw {
		return admission.Record{}, ErrCorrupt
	}
	if len(profile) > 8192 || json.Unmarshal([]byte(profile), &result.Profile) != nil || result.Profile.Check(result.Request.Spec()) != nil {
		return admission.Record{}, ErrCorrupt
	}
	at, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return admission.Record{}, ErrCorrupt
	}
	spec := result.Request.Spec()
	result.Job, err = domain.NewJob(domain.Job{ID: id, WorkspaceID: w, Name: spec.Name, SpecificationVersion: spec.APIVersion, SpecificationDigest: result.Request.Hash(), Binding: result.Profile.Binding, ActiveAttemptID: result.Job.ActiveAttemptID, CreatedAt: at})
	if err != nil {
		return admission.Record{}, ErrCorrupt
	}
	result.Attempt, result.AttemptNonce, err = loadAttempt(ctx, tx, w, id, result.Job.ActiveAttemptID)
	if err != nil {
		return admission.Record{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT role,object_id,bytes,sha256 FROM job_objects WHERE workspace_id=? AND job_id=? ORDER BY role`, string(w), string(id))
	if err != nil {
		return admission.Record{}, dbError(err)
	}
	refs := map[string]bool{}
	for rows.Next() {
		var ref admission.FrozenObject
		ref.Object.WorkspaceID = w
		if err = rows.Scan(&ref.Role, &ref.Object.ID, &ref.Object.Bytes, &ref.Object.SHA256); err != nil {
			rows.Close()
			return admission.Record{}, dbError(err)
		}
		if !ref.Object.Valid() || refs[ref.Role] || len(refs) >= 65 {
			rows.Close()
			return admission.Record{}, ErrCorrupt
		}
		refs[ref.Role] = true
		switch {
		case ref.Role == "bundle":
			if ref.Object.ID != spec.Bundle.ObjectID {
				rows.Close()
				return admission.Record{}, ErrCorrupt
			}
		case strings.HasPrefix(ref.Role, "input:"):
			index, err := strconv.Atoi(strings.TrimPrefix(ref.Role, "input:"))
			if err != nil || index < 0 || index >= len(spec.Inputs) || ref.Role != "input:"+strconv.Itoa(index) {
				rows.Close()
				return admission.Record{}, ErrCorrupt
			}
			source := spec.Inputs[index].Source
			if source.Kind == "object" && source.ObjectID != ref.Object.ID || source.Kind == "https" && source.ExpectedSHA256 != "" && source.ExpectedSHA256 != ref.Object.SHA256 {
				rows.Close()
				return admission.Record{}, ErrCorrupt
			}
		default:
			rows.Close()
			return admission.Record{}, ErrCorrupt
		}
		result.Objects = append(result.Objects, ref)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return admission.Record{}, dbError(err)
	}
	if !refs["bundle"] {
		return admission.Record{}, ErrCorrupt
	}
	for i, in := range spec.Inputs {
		if !refs["input:"+strconv.Itoa(i)] {
			if in.Source.Kind != "https" {
				return admission.Record{}, ErrCorrupt
			}
		}
	}
	return result, nil
}

func loadAttempt(ctx context.Context, q queryer, w domain.WorkspaceID, j domain.JobID, id domain.AttemptID) (domain.Attempt, string, error) {
	var a domain.Attempt
	var state, orchestration, created, updated, nonce string
	err := q.QueryRowContext(ctx, `SELECT attempt_id,job_id,number,state,orchestration,revision,created_at,updated_at,nonce FROM attempts WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(w), string(j), string(id)).Scan(&a.ID, &a.JobID, &a.Number, &state, &orchestration, &a.Revision, &created, &updated, &nonce)
	if errors.Is(err, sql.ErrNoRows) {
		return a, "", admission.ErrNotFound
	}
	if err != nil {
		return a, "", dbError(err)
	}
	if len(state) > 4096 || json.Unmarshal([]byte(state), &a.State) != nil || string(a.State.Orchestration) != orchestration || !domain.SHA256Digest(nonce).Valid() {
		return domain.Attempt{}, "", ErrCorrupt
	}
	a.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return domain.Attempt{}, "", ErrCorrupt
	}
	a.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	if err != nil || a.Validate() != nil {
		return domain.Attempt{}, "", ErrCorrupt
	}
	return a, nonce, nil
}
