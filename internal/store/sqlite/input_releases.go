package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

var _ retention.InputReleaseRepository = (*Store)(nil)

// Preview and admission use the same transaction-local guard and exact inventory.
func inputReleasePlan(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, request retention.InputReleaseRequest, now time.Time) (retention.InputReleasePreview, []retentionItem, error) {
	out := retention.InputReleasePreview{WorkspaceID: w, Jobs: request.Jobs, MeasuredAt: now}
	selected := map[inputReleaseAttempt]bool{}
	for _, j := range request.Jobs {
		var digest domain.SHA256Digest
		err := tx.QueryRowContext(ctx, `SELECT a.sha256 FROM artifacts a JOIN collection_publications p ON p.workspace_id=a.workspace_id AND p.job_id=a.job_id AND p.attempt_id=a.attempt_id WHERE a.workspace_id=? AND a.job_id=? AND a.attempt_id=? AND a.artifact_id=? AND a.role='output'`, w, j.JobID, j.AttemptID, j.ArtifactID).Scan(&digest)
		if errors.Is(err, sql.ErrNoRows) {
			return out, nil, admission.ErrNotFound
		}
		if err != nil {
			return out, nil, dbError(err)
		}
		if digest != j.SHA256 {
			return out, nil, retention.ErrChanged
		}
		ref, err := retentionReference(ctx, tx, w, j.JobID, j.AttemptID)
		if err != nil {
			return out, nil, err
		}
		if ref.ReleaseBlocker() != "" {
			return out, nil, retention.ErrPinned
		}
		selected[inputReleaseAttempt{j.JobID, j.AttemptID}] = true
	}
	// Only objects referenced by requested jobs can enter this release. The bounded
	// scan fails closed rather than truncating a preview or its deletion permit.
	args := []any{w}
	for _, j := range request.Jobs {
		args = append(args, j.JobID)
	}
	args = append(args, retention.MaxReferences+1)
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(request.Jobs)), ",")
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT r.object_id FROM retention_inventory r JOIN job_objects o ON o.workspace_id=r.workspace_id AND o.object_id=r.object_id WHERE r.kind='input' AND r.workspace_id=? AND r.expired_at IS NULL AND o.job_id IN (`+placeholders+`) ORDER BY r.object_id LIMIT ?`, args...)
	if err != nil {
		return out, nil, dbError(err)
	}
	ids := []domain.ObjectID{}
	for rows.Next() {
		var id domain.ObjectID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return out, nil, dbError(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, nil, dbError(err)
	}
	if len(ids) > retention.MaxReferences {
		return out, nil, retention.ErrPinned
	}
	items := []retentionItem{}
	for _, id := range ids {
		item, err := readRetentionItem(ctx, tx, retention.Input, w, id)
		if err != nil {
			return out, nil, err
		}
		refs, err := retentionInputReferencesReleased(ctx, tx, w, id, selected)
		if err != nil {
			return out, nil, err
		}
		decision, err := assessInputRetention(retention.DefaultPolicy(), now, item.recorded, refs)
		if err != nil {
			return out, nil, err
		}
		if !decision.Eligible {
			continue
		}
		items = append(items, item)
		out.Files++
		out.ReleasableBytes += item.deletion.Object.Bytes
	}
	// Hash includes targets as well as objects; a stale preview cannot authorize a
	// different batch even if its objects happen to have identical sizes.
	hash := sha256.New()
	raw, _ := json.Marshal(request.Jobs)
	hash.Write(raw)
	for _, item := range items {
		raw, _ := json.Marshal(item.deletion.Object)
		hash.Write(raw)
	}
	out.PreviewSHA256 = domain.SHA256Digest(hex.EncodeToString(hash.Sum(nil)))
	return out, items, nil
}

func (s *Store) PreviewInputRelease(ctx context.Context, w domain.WorkspaceID, token string, request retention.InputReleaseRequest) (retention.InputReleasePreview, error) {
	var out retention.InputReleasePreview
	if !w.Valid() || !request.Normalize(false) {
		return out, retention.ErrInvalid
	}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Read); err != nil {
			return err
		}
		var err error
		out, _, err = inputReleasePlan(ctx, tx, w, request, now)
		return err
	})
	return out, err
}

func inputReleaseIdentity(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, key string, request retention.InputReleaseRequest) (bool, error) {
	var saved string
	err := tx.QueryRowContext(ctx, `SELECT request_json FROM input_releases WHERE workspace_id=? AND key_sha256=?`, w, key).Scan(&saved)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, dbError(err)
	}
	raw, _ := json.Marshal(request)
	if saved != string(raw) {
		return false, retention.ErrReleaseConflict
	}
	return true, nil
}
func inputReleaseReceipt(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, key string, request retention.InputReleaseRequest) (retention.InputReleaseReceipt, error) {
	out := retention.InputReleaseReceipt{WorkspaceID: w, InputReleaseRequest: request}
	var at string
	if err := tx.QueryRowContext(ctx, `SELECT released_at FROM input_releases WHERE workspace_id=? AND key_sha256=?`, w, key).Scan(&at); err != nil {
		return out, dbError(err)
	}
	var err error
	out.ReleasedAt, err = time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return out, ErrCorrupt
	}
	err = tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(r.bytes),0),COALESCE(sum(r.deleted_at IS NULL),0),COALESCE(sum(CASE WHEN r.deleted_at IS NOT NULL THEN r.bytes ELSE 0 END),0) FROM input_release_objects o JOIN retention_inventory r USING(inventory_seq) WHERE o.workspace_id=? AND o.key_sha256=?`, w, key).Scan(&out.Files, &out.Bytes, &out.PendingFiles, &out.ReclaimedBytes)
	return out, dbError(err)
}
func (s *Store) AdmitInputRelease(ctx context.Context, w domain.WorkspaceID, token, key string, request retention.InputReleaseRequest) (retention.InputReleaseReceipt, []retention.Deletion, error) {
	var out retention.InputReleaseReceipt
	var pending []retention.Deletion
	if !w.Valid() || !domain.SHA256Digest(key).Valid() || !request.Normalize(true) {
		return out, nil, retention.ErrInvalid
	}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		replay, err := inputReleaseIdentity(ctx, tx, w, key, request)
		if err != nil {
			return err
		}
		if !replay {
			preview, items, err := inputReleasePlan(ctx, tx, w, request, now)
			if err != nil {
				return err
			}
			if preview.PreviewSHA256 != request.PreviewSHA256 {
				return retention.ErrChanged
			}
			raw, _ := json.Marshal(request)
			if _, err = tx.ExecContext(ctx, `INSERT INTO input_releases VALUES(?,?,?,?)`, w, key, string(raw), now.Format(time.RFC3339Nano)); err != nil {
				return dbError(err)
			}
			for _, j := range request.Jobs {
				if _, err = tx.ExecContext(ctx, `INSERT INTO input_released_jobs VALUES(?,?,?,?,?) ON CONFLICT(workspace_id,job_id,attempt_id) DO NOTHING`, w, j.JobID, j.AttemptID, j.ArtifactID, j.SHA256); err != nil {
					return dbError(err)
				}
			}
			for _, item := range items {
				if _, err = tx.ExecContext(ctx, `UPDATE retention_inventory SET expired_at=? WHERE inventory_seq=? AND expired_at IS NULL`, now.Format(time.RFC3339Nano), item.sequence); err != nil {
					return dbError(err)
				}
				if _, err = tx.ExecContext(ctx, `INSERT INTO input_release_objects VALUES(?,?,?)`, w, key, item.sequence); err != nil {
					return dbError(err)
				}
			}
		}
		out, err = inputReleaseReceipt(ctx, tx, w, key, request)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT r.object_id,r.bytes,r.sha256 FROM input_release_objects o JOIN retention_inventory r USING(inventory_seq) WHERE o.workspace_id=? AND o.key_sha256=? AND r.deleted_at IS NULL ORDER BY r.inventory_seq LIMIT ?`, w, key, retention.ReleasePageSize)
		if err != nil {
			return dbError(err)
		}
		defer rows.Close()
		for rows.Next() {
			d := retention.Deletion{Kind: retention.Input, Object: domain.ObjectMetadata{WorkspaceID: w}}
			if err = rows.Scan(&d.Object.ID, &d.Object.Bytes, &d.Object.SHA256); err != nil {
				return dbError(err)
			}
			pending = append(pending, d)
		}
		return dbError(rows.Err())
	})
	if err != nil {
		return retention.InputReleaseReceipt{}, nil, err
	}
	return out, pending, nil
}
func (s *Store) ReadInputRelease(ctx context.Context, w domain.WorkspaceID, token, key string, request retention.InputReleaseRequest) (retention.InputReleaseReceipt, error) {
	var out retention.InputReleaseReceipt
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Operate); err != nil {
			return err
		}
		found, err := inputReleaseIdentity(ctx, tx, w, key, request)
		if err != nil {
			return err
		}
		if !found {
			return admission.ErrNotFound
		}
		out, err = inputReleaseReceipt(ctx, tx, w, key, request)
		return err
	})
	return out, err
}
