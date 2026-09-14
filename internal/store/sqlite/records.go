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
	if !validID(t.ID) || !t.WorkspaceID.Valid() || t.Digest == ([32]byte{}) || t.CreatedAt.IsZero() || len(t.Scopes) == 0 || len(t.Scopes) > 3 {
		return false
	}
	if !t.ExpiresAt.IsZero() && !t.ExpiresAt.After(t.CreatedAt) {
		return false
	}
	seen := map[auth.Scope]bool{}
	for _, s := range t.Scopes {
		if (s != auth.Read && s != auth.Write && s != auth.Operate) || seen[s] {
			return false
		}
		seen[s] = true
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
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	_, err = s.db.ExecContext(ctx, `INSERT INTO workspaces(workspace_id,enabled,allowed_profiles) VALUES(?,?,?)
 ON CONFLICT(workspace_id) DO UPDATE SET enabled=excluded.enabled,allowed_profiles=excluded.allowed_profiles`, string(w.ID), w.Enabled, string(encoded))
	return dbError(err)
}
func (s *Store) LookupWorkspace(ctx context.Context, id domain.WorkspaceID) (auth.Workspace, error) {
	if !id.Valid() {
		return auth.Workspace{}, auth.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return auth.Workspace{}, err
	}
	defer done()
	var w auth.Workspace
	var encoded string
	err = s.db.QueryRowContext(ctx, "SELECT workspace_id,enabled,allowed_profiles FROM workspaces WHERE workspace_id=?", string(id)).Scan(&w.ID, &w.Enabled, &encoded)
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
}
func (s *Store) CreateToken(ctx context.Context, t auth.TokenRecord) error {
	if !validToken(t) {
		return ErrInvalid
	}
	scopes := append([]auth.Scope(nil), t.Scopes...)
	sort.Slice(scopes, func(i, j int) bool { return scopes[i] < scopes[j] })
	encoded, _ := json.Marshal(scopes)
	var expires any
	if !t.ExpiresAt.IsZero() {
		expires = t.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	_, err = s.db.ExecContext(ctx, "INSERT INTO api_token_hashes(token_id,workspace_id,digest,scopes,created_at,expires_at,revoked) VALUES(?,?,?,?,?,?,?)", t.ID, string(t.WorkspaceID), t.Digest[:], string(encoded), t.CreatedAt.UTC().Format(time.RFC3339Nano), expires, t.Revoked)
	return dbError(err)
}
func (s *Store) LookupToken(ctx context.Context, digest [sha256.Size]byte) (auth.TokenRecord, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return auth.TokenRecord{}, err
	}
	defer done()
	var t auth.TokenRecord
	var raw []byte
	var scopes, created string
	var expires sql.NullString
	err = s.db.QueryRowContext(ctx, "SELECT token_id,workspace_id,digest,scopes,created_at,expires_at,revoked FROM api_token_hashes WHERE digest=?", digest[:]).Scan(&t.ID, &t.WorkspaceID, &raw, &scopes, &created, &expires, &t.Revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return t, auth.ErrNotFound
	}
	if err != nil {
		return t, dbError(err)
	}
	if len(raw) != sha256.Size || len(scopes) > 100 || json.Unmarshal([]byte(scopes), &t.Scopes) != nil {
		return auth.TokenRecord{}, ErrCorrupt
	}
	copy(t.Digest[:], raw)
	if t.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return auth.TokenRecord{}, ErrCorrupt
	}
	if expires.Valid {
		if t.ExpiresAt, err = time.Parse(time.RFC3339Nano, expires.String); err != nil {
			return auth.TokenRecord{}, ErrCorrupt
		}
	}
	if !validToken(t) {
		return auth.TokenRecord{}, ErrCorrupt
	}
	return t, nil
}
func (s *Store) RevokeToken(ctx context.Context, id string) error {
	if !validID(id) {
		return auth.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	result, err := s.db.ExecContext(ctx, "UPDATE api_token_hashes SET revoked=1 WHERE token_id=?", id)
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
}

// CommitObject acknowledges metadata only; its caller must have already verified and
// published bytes. A conflict never overwrites an immutable object. No SQL transaction
// spans BlobStore.Put, URL downloads, archive inspection or any provider operation.
func (s *Store) CommitObject(ctx context.Context, m domain.ObjectMetadata) error {
	if !m.Valid() {
		return objects.ErrInvalid
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return err
	}
	defer done()
	_, err = s.db.ExecContext(ctx, "INSERT INTO objects(workspace_id,object_id,bytes,sha256) VALUES(?,?,?,?)", string(m.WorkspaceID), string(m.ID), m.Bytes, string(m.SHA256))
	return dbError(err)
}
func (s *Store) GetObject(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (domain.ObjectMetadata, error) {
	if !w.Valid() || !id.Valid() {
		return domain.ObjectMetadata{}, objects.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	defer done()
	var m domain.ObjectMetadata
	err = s.db.QueryRowContext(ctx, "SELECT workspace_id,object_id,bytes,sha256 FROM objects WHERE workspace_id=? AND object_id=?", string(w), string(id)).Scan(&m.WorkspaceID, &m.ID, &m.Bytes, &m.SHA256)
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
}
