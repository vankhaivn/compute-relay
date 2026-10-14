package runtimehost

import (
	"context"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// injectHostClock reopens only the metadata store before composing services or
// starting workers. The public SQLite option is the same dependency used in production.
func injectHostClock(t *testing.T, h *Host, clock func() time.Time) {
	t.Helper()
	requireNoError(t, h.store.Close())
	opts := sqlite.DefaultOptions()
	opts.Clock = clock
	root, err := h.root.Child("state")
	requireNoError(t, err)
	defer root.Close()
	h.store, err = sqlite.OpenRoot(context.Background(), root, opts, h.identity.InstallationID)
	requireNoError(t, err)
	h.access, err = auth.New(h.store, h.store)
	requireNoError(t, err)
}
