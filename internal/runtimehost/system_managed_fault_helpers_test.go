package runtimehost

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// These decorators observe successful public repository acknowledgements. A lost
// Submit response never needs a private SQL query to rediscover the reserved target.
type systemManagedCapture struct {
	*sqlite.Store
	mu        sync.Mutex
	accepted  connections.Record
	ready     domain.OperationID
	loseReady bool
}

func (r *systemManagedCapture) AcceptConnection(ctx context.Context, in connections.Accept) (connections.Record, error) {
	record, err := r.Store.AcceptConnection(ctx, in)
	if err == nil {
		r.mu.Lock()
		r.accepted = record
		r.mu.Unlock()
	}
	return record, err
}
func (r *systemManagedCapture) ReadyConnection(ctx context.Context, w domain.WorkspaceID, token string, id domain.OperationID) error {
	if err := r.Store.ReadyConnection(ctx, w, token, id); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ready = id
	if r.loseReady {
		r.loseReady = false
		return errors.New("synthetic acknowledgement lost after ready commit")
	}
	return nil
}
func (r *systemManagedCapture) record(t *testing.T) connections.Record {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepted.Operation.ID.Valid() {
		t.Fatal("no acknowledged connection reservation")
	}
	return r.accepted
}
func (f *managedTestFixture) managedCapture(t *testing.T, loseReady bool) *systemManagedCapture {
	t.Helper()
	repo := &systemManagedCapture{Store: f.host.store, loseReady: loseReady}
	var err error
	f.services.connections, err = connections.New(f.host.access, repo, f.vault, []connections.Adapter{f.adapter})
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

type systemManagedChangedDescriptor struct {
	*managedTestAdapter
	removed bool
}

func (a systemManagedChangedDescriptor) Descriptor() connections.Descriptor {
	d := a.managedTestAdapter.Descriptor()
	if a.removed {
		d.Type = "another_fixture"
	} else {
		d.Fields[0].MaxBytes = 1
	}
	return d
}

type systemManagedPausedResolution struct {
	*sqlite.Store
	resolved, resume chan struct{}
	once             sync.Once
}

func (r *systemManagedPausedResolution) ResolveConnectionCredential(ctx context.Context, id string) (string, error) {
	key, err := r.Store.ResolveConnectionCredential(ctx, id)
	if err == nil {
		r.once.Do(func() {
			close(r.resolved)
			select {
			case <-r.resume:
			case <-ctx.Done():
			}
		})
	}
	return key, err
}
