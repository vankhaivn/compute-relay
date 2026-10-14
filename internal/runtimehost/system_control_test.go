package runtimehost

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func systemSubmitted(t *testing.T, f *managedTestFixture, trace *systemDispatchTrace, id scheduler.Identity) {
	t.Helper()
	for range 8 {
		if trace.journal(t, id).Phase == dispatch.Submitted {
			return
		}
		if err := f.dispatchStep(t); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("execution did not become submitted")
}
func installSystemCancelProbe(t *testing.T, f *managedTestFixture, e *systemDispatchEffects, supported bool, out provider.CancellationOutcome) *systemCancellation {
	t.Helper()
	c := &systemCancellation{supported: supported, outcome: out, execution: domain.ExecutionSucceeded, f: f}
	e.cancel = c
	return c
}
func systemRemapProfile(t *testing.T, f *managedTestFixture, id scheduler.Identity) {
	t.Helper()
	old := f.dispatchRecord(t, id).Profile
	connectionID := strings.TrimPrefix(old.CredentialRef, "vault:")
	c, err := f.services.connections.Get(t.Context(), f.actor, "app", connectionID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.services.connections.Submit(t.Context(), f.actor, "app", c.ID, "remap-future", systemJSON(t, map[string]any{"action": "configure", "expected_revision": c.Revision, "configuration": connections.Configuration{MaxRemoteWallSeconds: 200}}))
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.services.connections.Get(t.Context(), f.actor, "app", c.ID)
	if err != nil || current.Selection.ConfigurationRevision == old.Binding.ConfigurationRevision {
		t.Fatal("future selection did not change", err)
	}
}

type systemBlockedInputs struct {
	operations.BlobReader
	entered, release chan struct{}
	once             sync.Once
}

func (b *systemBlockedInputs) Open(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	b.once.Do(func() {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	})
	return b.BlobReader.Open(ctx, w, id)
}
