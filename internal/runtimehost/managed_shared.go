package runtimehost

import (
	"context"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// Account scope comes from the immutable managed binding, never a provider
// request. The store cross-checks the original preparation and frozen plan.
type managedSharedClaims struct {
	store   *sqlite.Store
	account string
}

func (c managedSharedClaims) TrackSharedUse(ctx context.Context, claim provider.SharedResourceClaim) error {
	return c.store.TrackSharedUse(ctx, c.account, claim)
}

func (c managedSharedClaims) ClaimSharedCreation(ctx context.Context, claim provider.SharedResourceClaim) (bool, error) {
	return c.store.ClaimSharedCreation(ctx, c.account, claim)
}
