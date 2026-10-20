package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

var _ retention.ReleaseRepository = (*Store)(nil)

func releaseIdentity(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, key string, request retention.ReleaseRequest) (bool, error) {
	var savedJob domain.JobID
	var saved retention.ReleaseRequest
	err := tx.QueryRowContext(ctx, `SELECT job_id,attempt_id,artifact_id,sha256 FROM result_releases WHERE workspace_id=? AND key_sha256=?`, w, key).
		Scan(&savedJob, &saved.AttemptID, &saved.ArtifactID, &saved.SHA256)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, dbError(err)
	}
	if savedJob != job || saved != request {
		return false, retention.ErrReleaseConflict
	}
	return true, nil
}

func releasedResult(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, job domain.JobID, request retention.ReleaseRequest) (retention.ReleaseReceipt, error) {
	out := retention.ReleaseReceipt{WorkspaceID: w, JobID: job, ReleaseRequest: request}
	var at string
	err := tx.QueryRowContext(ctx, `SELECT e.expired_at,count(*),sum(r.bytes),sum(r.deleted_at IS NULL),sum(CASE WHEN r.deleted_at IS NOT NULL THEN r.bytes ELSE 0 END)
 FROM retention_expirations e JOIN artifacts a ON a.workspace_id=e.workspace_id AND a.job_id=e.job_id AND a.attempt_id=e.attempt_id
 JOIN retention_inventory r ON r.kind='result' AND r.workspace_id=a.workspace_id AND r.object_id=a.object_id
 WHERE e.workspace_id=? AND e.job_id=? AND e.attempt_id=? GROUP BY e.expired_at`, w, job, request.AttemptID).
		Scan(&at, &out.Files, &out.Bytes, &out.PendingFiles, &out.ReclaimedBytes)
	if err != nil {
		return out, dbError(err)
	}
	out.ReleasedAt, err = time.Parse(time.RFC3339Nano, at)
	if err != nil || out.Files < 1 || out.Bytes < 0 || out.PendingFiles < 0 || out.PendingFiles > out.Files || out.ReclaimedBytes < 0 || out.ReclaimedBytes > out.Bytes {
		return out, ErrCorrupt
	}
	return out, nil
}

func (s *Store) AdmitResultRelease(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, key string, request retention.ReleaseRequest) (retention.ReleaseReceipt, []retention.Deletion, error) {
	var out retention.ReleaseReceipt
	var pending []retention.Deletion
	if !w.Valid() || !job.Valid() || !domain.SHA256Digest(key).Valid() || !request.Valid() {
		return out, nil, retention.ErrInvalid
	}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		replay, err := releaseIdentity(ctx, tx, w, job, key, request)
		if err != nil {
			return err
		}
		if !replay {
			var digest domain.SHA256Digest
			err := tx.QueryRowContext(ctx, `SELECT sha256 FROM artifacts WHERE workspace_id=? AND job_id=? AND attempt_id=? AND artifact_id=? AND role='output'`, w, job, request.AttemptID, request.ArtifactID).Scan(&digest)
			if errors.Is(err, sql.ErrNoRows) {
				return admission.ErrNotFound
			}
			if err != nil {
				return dbError(err)
			}
			if digest != request.SHA256 {
				return retention.ErrChanged
			}
			ref, err := retentionReference(ctx, tx, w, job, request.AttemptID)
			if err != nil {
				return err
			}
			if ref.ReleaseBlocker() != "" {
				return retention.ErrPinned
			}
			inventory, err := resultRetentionInventory(ctx, tx, w, job, request.AttemptID)
			if err != nil {
				return err
			}
			if inventory.expired == 0 {
				if _, err := tombstoneResult(ctx, tx, w, job, request.AttemptID, inventory.count, now); err != nil {
					return err
				}
			} else if ref.State.Result != domain.ResultExpired {
				return ErrCorrupt
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO result_releases VALUES(?,?,?,?,?,?)`, w, key, job, request.AttemptID, request.ArtifactID, request.SHA256); err != nil {
				return dbError(err)
			}
		}
		out, err = releasedResult(ctx, tx, w, job, request)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT r.object_id,r.bytes,r.sha256 FROM artifacts a
 JOIN retention_inventory r ON r.kind='result' AND r.workspace_id=a.workspace_id AND r.object_id=a.object_id
 WHERE a.workspace_id=? AND a.job_id=? AND a.attempt_id=? AND r.expired_at IS NOT NULL AND r.deleted_at IS NULL
 ORDER BY r.inventory_seq LIMIT ?`, w, job, request.AttemptID, retention.ReleasePageSize)
		if err != nil {
			return dbError(err)
		}
		defer rows.Close()
		for rows.Next() {
			d := retention.Deletion{Kind: retention.Result, Object: domain.ObjectMetadata{WorkspaceID: w}}
			if err := rows.Scan(&d.Object.ID, &d.Object.Bytes, &d.Object.SHA256); err != nil {
				return dbError(err)
			}
			pending = append(pending, d)
		}
		return dbError(rows.Err())
	})
	if err != nil {
		return retention.ReleaseReceipt{}, nil, err
	}
	return out, pending, nil
}

func (s *Store) ReadResultRelease(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, key string, request retention.ReleaseRequest) (retention.ReleaseReceipt, error) {
	var out retention.ReleaseReceipt
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		found, err := releaseIdentity(ctx, tx, w, job, key, request)
		if err != nil {
			return err
		}
		if !found {
			return admission.ErrNotFound
		}
		out, err = releasedResult(ctx, tx, w, job, request)
		return err
	})
	return out, err
}
