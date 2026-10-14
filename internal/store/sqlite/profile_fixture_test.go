package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
)

// PutProfile creates controlled immutable fixture revisions for store tests.
// It is not a runtime profile-administration path.
func (s *Store) PutProfile(ctx context.Context, p admission.Profile, enabled bool) error {
	if p.Validate() != nil {
		return ErrInvalid
	}
	b, err := json.Marshal(p)
	if err != nil || len(b) > 8192 {
		return ErrInvalid
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
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
