package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

var _ auth.TokenRepository = (*Store)(nil)
var _ auth.WorkspaceRepository = (*Store)(nil)
var _ objects.Repository = (*Store)(nil)

func validID(raw string) bool { _, err := domain.ParseObjectID(raw); return err == nil }
func validWorkspace(w auth.Workspace) bool {
	if !w.ID.Valid() || len(w.AllowedProfiles) > 128 {
		return false
	}
	seen := map[string]bool{}
	for _, p := range w.AllowedProfiles {
		if len(p) == 0 || len(p) > 128 || seen[p] {
			return false
		}
		for i, c := range []byte(p) {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || i > 0 && (c == '-' || c == '_' || c == '.')) {
				return false
			}
		}
		seen[p] = true
	}
	return true
}
func validToken(t auth.TokenRecord) bool {
	if !validID(t.ID) || !t.WorkspaceID.Valid() || t.Digest == ([32]byte{}) || t.CreatedAt.IsZero() || !auth.ValidScopes(t.Scopes) {
		return false
	}
	if !t.ExpiresAt.IsZero() && !t.ExpiresAt.After(t.CreatedAt) {
		return false
	}

	return true
}

// PutWorkspace is local operator configuration, not an application admin API. No
// provider/profile exists check is invented before configuration resolution is implemented.
func (s *Store) PutWorkspace(ctx context.Context, w auth.Workspace) error {
	if !validWorkspace(w) {
		return ErrInvalid
	}
	profiles := append([]string{}, w.AllowedProfiles...)
	sort.Strings(profiles)
	encoded, _ := json.Marshal(profiles)
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO workspaces(workspace_id,enabled,allowed_profiles) VALUES(?,?,?)
 ON CONFLICT(workspace_id) DO UPDATE SET enabled=excluded.enabled,allowed_profiles=excluded.allowed_profiles`, string(w.ID), w.Enabled, string(encoded))
		return dbError(err)
	})
}
func (s *Store) LookupWorkspace(ctx context.Context, id domain.WorkspaceID) (auth.Workspace, error) {
	if !id.Valid() {
		return auth.Workspace{}, auth.ErrNotFound
	}
	return read(ctx, s, func(ctx context.Context) (auth.Workspace, error) {
		var w auth.Workspace
		var encoded string
		err := s.db.QueryRowContext(ctx, "SELECT workspace_id,enabled,allowed_profiles FROM workspaces WHERE workspace_id=?", string(id)).Scan(&w.ID, &w.Enabled, &encoded)
		if errors.Is(err, sql.ErrNoRows) {
			return w, auth.ErrNotFound
		}
		if err != nil {
			return w, dbError(err)
		}
		if len(encoded) > 20000 || json.Unmarshal([]byte(encoded), &w.AllowedProfiles) != nil || !validWorkspace(w) {
			return auth.Workspace{}, ErrCorrupt
		}
		return w, nil
	})
}
func (s *Store) CreateToken(ctx context.Context, issuance auth.TokenIssuance) (auth.TokenRecord, error) {
	t := issuance.Record
	if !t.CreatedAt.IsZero() || issuance.Lifetime < 0 || issuance.Lifetime > 0 && !t.ExpiresAt.IsZero() {
		return auth.TokenRecord{}, ErrInvalid
	}
	scopes := append([]auth.Scope(nil), t.Scopes...)
	sort.Slice(scopes, func(i, j int) bool { return scopes[i] < scopes[j] })
	encoded, _ := json.Marshal(scopes)
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		t.CreatedAt = now
		if issuance.Lifetime > 0 {
			t.ExpiresAt = now.Add(issuance.Lifetime)
		}
		if !t.ExpiresAt.IsZero() && !now.Before(t.ExpiresAt) {
			return auth.ErrForbidden
		}
		if !validToken(t) {
			return ErrInvalid
		}
		var expires any
		if !t.ExpiresAt.IsZero() {
			expires = t.ExpiresAt.UTC().Format(time.RFC3339Nano)
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO api_token_hashes(token_id,workspace_id,digest,scopes,created_at,expires_at,revoked) VALUES(?,?,?,?,?,?,?)", t.ID, string(t.WorkspaceID), t.Digest[:], string(encoded), now.Format(time.RFC3339Nano), expires, t.Revoked)
		return dbError(err)
	})
	if err != nil {
		return auth.TokenRecord{}, err
	}
	t.Scopes = append([]auth.Scope(nil), t.Scopes...)
	return t, nil
}
func (s *Store) LookupToken(ctx context.Context, digest [sha256.Size]byte) (auth.TokenRecord, error) {
	var t auth.TokenRecord
	var found bool
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		var raw []byte
		var scopes, created string
		var expires sql.NullString
		err := tx.QueryRowContext(ctx, "SELECT token_id,workspace_id,digest,scopes,created_at,expires_at,revoked FROM api_token_hashes WHERE digest=?", digest[:]).Scan(&t.ID, &t.WorkspaceID, &raw, &scopes, &created, &expires, &t.Revoked)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		if len(raw) != sha256.Size || len(scopes) > 100 || json.Unmarshal([]byte(scopes), &t.Scopes) != nil {
			return ErrCorrupt
		}
		copy(t.Digest[:], raw)
		if t.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
			return ErrCorrupt
		}
		if expires.Valid {
			if t.ExpiresAt, err = time.Parse(time.RFC3339Nano, expires.String); err != nil {
				return ErrCorrupt
			}
		}
		if !validToken(t) {
			return ErrCorrupt
		}
		found = t.ExpiresAt.IsZero() || now.Before(t.ExpiresAt)
		// Commit the sampled floor even on expiry, so a wall-clock rollback cannot
		// make a token usable again after a completed authentication check.
		return nil
	})
	if err != nil {
		return auth.TokenRecord{}, err
	}
	if !found {
		return auth.TokenRecord{}, auth.ErrNotFound
	}
	return t, nil
}
func (s *Store) RevokeToken(ctx context.Context, id string) error {
	if !validID(id) {
		return auth.ErrNotFound
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		result, err := tx.ExecContext(ctx, "UPDATE api_token_hashes SET revoked=1 WHERE token_id=?", id)
		if err != nil {
			return dbError(err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return dbError(err)
		}
		if n == 0 {
			return auth.ErrNotFound
		}
		return nil
	})
}

// CommitObject acknowledges metadata only; its caller must have already verified and
// published bytes. A conflict never overwrites an immutable object. No SQL transaction
// spans BlobStore.Put, URL downloads, archive inspection or any provider operation.
func (s *Store) CommitObject(ctx context.Context, m domain.ObjectMetadata) error {
	if !m.Valid() {
		return objects.ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if err := registerRetention(ctx, tx, retention.Input, m, now); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO objects(workspace_id,object_id,bytes,sha256) VALUES(?,?,?,?)", string(m.WorkspaceID), string(m.ID), m.Bytes, string(m.SHA256))
		return dbError(err)
	})
}
func (s *Store) GetObject(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (domain.ObjectMetadata, error) {
	if !w.Valid() || !id.Valid() {
		return domain.ObjectMetadata{}, objects.ErrNotFound
	}
	return read(ctx, s, func(ctx context.Context) (domain.ObjectMetadata, error) {
		var m domain.ObjectMetadata
		err := s.db.QueryRowContext(ctx, "SELECT workspace_id,object_id,bytes,sha256 FROM objects WHERE workspace_id=? AND object_id=?", string(w), string(id)).Scan(&m.WorkspaceID, &m.ID, &m.Bytes, &m.SHA256)
		if errors.Is(err, sql.ErrNoRows) {
			return m, objects.ErrNotFound
		}
		if err != nil {
			return m, dbError(err)
		}
		if !m.Valid() {
			return domain.ObjectMetadata{}, ErrCorrupt
		}
		return m, nil
	})
}
