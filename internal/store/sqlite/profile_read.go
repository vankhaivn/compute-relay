package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

// ReadProfile is a local administrative read, not a public application serializer.
// Account scope and credential references must not be returned by application routes.
func (s *Store) ReadProfile(ctx context.Context, name string) (admission.Profile, bool, error) {
	var profile admission.Profile
	if !domain.ObjectID(name).Valid() {
		return profile, false, ErrInvalid
	}
	enabled, err := read(ctx, s, func(ctx context.Context) (bool, error) {
		var raw, revision string
		var enabled bool
		err := s.db.QueryRowContext(ctx, `SELECT r.snapshot,p.revision,p.enabled FROM profiles p JOIN profile_revisions r ON p.profile=r.profile AND p.revision=r.revision WHERE p.profile=?`, name).Scan(&raw, &revision, &enabled)
		if errors.Is(err, sql.ErrNoRows) {
			return false, auth.ErrNotFound
		}
		if err != nil {
			return false, dbError(err)
		}
		if len(raw) > 8192 || json.Unmarshal([]byte(raw), &profile) != nil || profile.Validate() != nil || profile.Binding.Profile != name || profile.Binding.ConfigurationRevision != revision {
			return false, ErrCorrupt
		}
		canonical, err := json.Marshal(profile)
		if err != nil || !bytes.Equal(canonical, []byte(raw)) {
			return false, ErrCorrupt
		}
		return enabled, nil
	})
	if err != nil {
		return admission.Profile{}, false, err
	}
	return profile, enabled, nil
}
