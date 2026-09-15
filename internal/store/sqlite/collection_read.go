package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

var _ collection.ResultRepository = (*Store)(nil)

func (s *Store) ReadCollection(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, attempt domain.AttemptID) (collection.Result, error) {
	if !w.Valid() || !job.Valid() || !attempt.Valid() {
		return collection.Result{}, collection.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return collection.Result{}, err
	}
	defer done()
	result := collection.Result{WorkspaceID: w, JobID: job, AttemptID: attempt}
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Read); err != nil {
			return err
		}
		a, _, err := loadAttempt(ctx, tx, w, job, attempt)
		if errors.Is(err, admission.ErrNotFound) {
			return collection.ErrNotFound
		}
		if err != nil {
			return err
		}
		if a.State.Result != domain.ResultAvailable {
			return collection.ErrNotFound
		}
		var count int
		var at string
		err = tx.QueryRowContext(ctx, `SELECT phase,file_count,verified_at FROM collection_publications WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(w), string(job), string(attempt)).Scan(&result.Phase, &count, &at)
		if errors.Is(err, sql.ErrNoRows) {
			return collection.ErrNotFound
		}
		if err != nil {
			return dbError(err)
		}
		result.VerifiedAt, err = time.Parse(time.RFC3339Nano, at)
		if err != nil || count < 1 || count > collection.MaxFiles {
			return ErrCorrupt
		}
		rows, err := tx.QueryContext(ctx, `SELECT artifact_id,object_id,path,role,bytes,sha256,media_type FROM artifacts
 WHERE workspace_id=? AND job_id=? AND attempt_id=? ORDER BY path LIMIT ?`, string(w), string(job), string(attempt), collection.MaxFiles+1)
		if err != nil {
			return dbError(err)
		}
		defer rows.Close()
		var total int64
		for rows.Next() {
			var f collection.File
			f.Object.WorkspaceID = w
			if err = rows.Scan(&f.ID, &f.Object.ID, &f.Path, &f.Role, &f.Object.Bytes, &f.Object.SHA256, &f.MediaType); err != nil {
				return dbError(err)
			}
			if !f.ID.Valid() || !f.Object.Valid() || f.Object.Bytes > (4<<30)-total {
				return ErrCorrupt
			}
			total += f.Object.Bytes
			result.Files = append(result.Files, f)
		}
		if err = rows.Err(); err != nil {
			return dbError(err)
		}
		if len(result.Files) != count {
			return ErrCorrupt
		}
		return nil
	})
	if err != nil {
		return collection.Result{}, err
	}
	return result, nil
}
