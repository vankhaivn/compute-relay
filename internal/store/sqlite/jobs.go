package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

var _ ports.Store = (*Store)(nil)

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
		var raw, hash, version, profile, created string
		err := tx.QueryRowContext(ctx, `SELECT j.request,j.request_sha256,j.canonical_version,r.snapshot,j.active_attempt_id,j.created_at FROM jobs j JOIN profile_revisions r ON j.profile=r.profile AND j.profile_revision=r.revision WHERE j.workspace_id=? AND j.job_id=?`, string(w), string(id)).Scan(&raw, &hash, &version, &profile, &result.Job.ActiveAttemptID, &created)
		if errors.Is(err, sql.ErrNoRows) {
			return admission.ErrNotFound
		}
		if err != nil {
			return dbError(err)
		}
		if version != admission.CanonicalVersion {
			return ErrSchema
		}
		result.Request, err = admission.Parse([]byte(raw))
		if err != nil || string(result.Request.Hash()) != hash || string(result.Request.Canonical()) != raw {
			return ErrCorrupt
		}
		if len(profile) > 8192 || json.Unmarshal([]byte(profile), &result.Profile) != nil || result.Profile.Check(result.Request.Spec()) != nil {
			return ErrCorrupt
		}
		at, err := time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return ErrCorrupt
		}
		spec := result.Request.Spec()
		result.Job, err = domain.NewJob(domain.Job{ID: id, WorkspaceID: w, Name: spec.Name, SpecificationVersion: spec.APIVersion, SpecificationDigest: result.Request.Hash(), Binding: result.Profile.Binding, ActiveAttemptID: result.Job.ActiveAttemptID, CreatedAt: at})
		if err != nil {
			return ErrCorrupt
		}
		result.Attempt, result.AttemptNonce, err = loadAttempt(ctx, tx, w, id, result.Job.ActiveAttemptID)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT role,object_id,bytes,sha256 FROM job_objects WHERE workspace_id=? AND job_id=? ORDER BY role`, string(w), string(id))
		if err != nil {
			return dbError(err)
		}
		refs := map[string]bool{}
		for rows.Next() {
			var ref admission.FrozenObject
			ref.Object.WorkspaceID = w
			if err = rows.Scan(&ref.Role, &ref.Object.ID, &ref.Object.Bytes, &ref.Object.SHA256); err != nil {
				rows.Close()
				return dbError(err)
			}
			if !ref.Object.Valid() || refs[ref.Role] || len(refs) >= 65 {
				rows.Close()
				return ErrCorrupt
			}
			refs[ref.Role] = true
			switch {
			case ref.Role == "bundle":
				if ref.Object.ID != spec.Bundle.ObjectID {
					rows.Close()
					return ErrCorrupt
				}
			case strings.HasPrefix(ref.Role, "input:"):
				index, err := strconv.Atoi(strings.TrimPrefix(ref.Role, "input:"))
				if err != nil || index < 0 || index >= len(spec.Inputs) || ref.Role != "input:"+strconv.Itoa(index) {
					rows.Close()
					return ErrCorrupt
				}
				source := spec.Inputs[index].Source
				if source.Kind == "object" && source.ObjectID != ref.Object.ID || source.Kind == "https" && source.ExpectedSHA256 != "" && source.ExpectedSHA256 != ref.Object.SHA256 {
					rows.Close()
					return ErrCorrupt
				}
			default:
				rows.Close()
				return ErrCorrupt
			}
			result.Objects = append(result.Objects, ref)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return dbError(err)
		}
		if !refs["bundle"] {
			return ErrCorrupt
		}
		for i, in := range spec.Inputs {
			if !refs["input:"+strconv.Itoa(i)] {
				if in.Source.Kind == "https" {
					result.PendingHTTPS++
				} else {
					return ErrCorrupt
				}
			}
		}
		return nil
	})
	if err != nil {
		return admission.Record{}, err
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
func (s *Store) LoadAttempt(ctx context.Context, w domain.WorkspaceID, j domain.JobID, id domain.AttemptID) (domain.Attempt, error) {
	if !w.Valid() || !j.Valid() || !id.Valid() {
		return domain.Attempt{}, admission.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return domain.Attempt{}, err
	}
	defer done()
	a, _, err := loadAttempt(ctx, s.db, w, j, id)
	return a, err
}

// CommitAttempt implements the existing M2-04 CAS port. A failed revision or event
// sequence leaves BOTH state and events untouched. Operation-linked events await the
// durable operations table in M3-05. No provider call or retry is made.
func (s *Store) CommitAttempt(ctx context.Context, c ports.AttemptChange) error {
	if c.Validate() != nil || c.Event.OperationID != "" || c.After.Revision > math.MaxInt64 || c.Event.Sequence > math.MaxInt64 {
		return ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		before, _, err := loadAttempt(ctx, tx, c.WorkspaceID, c.Before.JobID, c.Before.ID)
		if err != nil {
			return err
		}
		if before != c.Before {
			return ErrConflict
		}
		var sequence uint64
		if err = tx.QueryRowContext(ctx, "SELECT COALESCE(max(sequence),0) FROM events WHERE workspace_id=? AND job_id=?", string(c.WorkspaceID), string(c.After.JobID)).Scan(&sequence); err != nil {
			return dbError(err)
		}
		if sequence == math.MaxInt64 || c.Event.Sequence != sequence+1 {
			return ErrConflict
		}
		state, err := json.Marshal(c.After.State)
		if err != nil {
			return ErrInvalid
		}
		result, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?,orchestration=?,revision=?,updated_at=? WHERE workspace_id=? AND job_id=? AND attempt_id=? AND revision=?`, string(state), string(c.After.State.Orchestration), c.After.Revision, c.After.UpdatedAt.UTC().Format(time.RFC3339Nano), string(c.WorkspaceID), string(c.After.JobID), string(c.After.ID), c.Before.Revision)
		if err != nil {
			return dbError(err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		if n != 1 {
			return ErrConflict
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO events VALUES(?,?,?,?,?,?,?,?)`, string(c.WorkspaceID), string(c.After.JobID), string(c.Event.ID), c.Event.Sequence, string(c.Event.AttemptID), string(c.Event.OperationID), string(c.Event.Type), c.Event.OccurredAt.UTC().Format(time.RFC3339Nano))
		return dbError(err)
	})
}
