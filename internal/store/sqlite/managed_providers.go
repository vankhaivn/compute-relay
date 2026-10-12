package sqlite

import "context"

// HasManagedConnections checks saved provider dependencies without reading a vault
// entry or probing a provider. Disabled connections still retain recovery authority.
func (s *Store) HasManagedConnections(ctx context.Context, providerType string) (bool, error) {
	var present bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM managed_connections WHERE provider_type=? AND new_work!='removed')`, providerType).Scan(&present)
	return present, dbError(err)
}
