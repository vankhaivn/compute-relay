package runtimehost

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// injectHostClock reopens only the metadata store before composing services or
// starting workers. The public SQLite option is the same dependency used in production.
func injectHostClock(t *testing.T, h *Host, clock func() time.Time) {
	t.Helper()
	if err := h.store.Close(); err != nil {
		t.Fatal(err)
	}
	opts := sqlite.DefaultOptions()
	opts.Clock = clock
	var err error
	h.store, err = sqlite.Open(context.Background(), filepath.Join(h.root.Path, "state"), opts)
	if err != nil {
		t.Fatal(err)
	}
	h.access, err = auth.New(h.store, h.store)
	if err != nil {
		t.Fatal(err)
	}
}
