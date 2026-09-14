// Package testsupport contains explicitly nondurable offline fixtures. Never wire this
// repository into compute-relay serve or return durable job admission from it.
package testsupport

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
)

type Memory struct {
	mu     sync.Mutex
	tokens map[[sha256.Size]byte]auth.TokenRecord
	spaces map[domain.WorkspaceID]auth.Workspace
	blobs  map[domain.WorkspaceID]map[domain.ObjectID]domain.ObjectMetadata
	owners map[auth.ResourceKind]map[string]domain.WorkspaceID
}

func NewMemory(workspaces ...auth.Workspace) *Memory {
	m := &Memory{tokens: make(map[[sha256.Size]byte]auth.TokenRecord), spaces: make(map[domain.WorkspaceID]auth.Workspace),
		blobs: make(map[domain.WorkspaceID]map[domain.ObjectID]domain.ObjectMetadata), owners: make(map[auth.ResourceKind]map[string]domain.WorkspaceID)}
	for _, w := range workspaces {
		m.SetWorkspace(w)
	}
	return m
}

func (m *Memory) SetWorkspace(w auth.Workspace) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.AllowedProfiles = append([]string(nil), w.AllowedProfiles...)
	m.spaces[w.ID] = w
}

func (m *Memory) LookupWorkspace(ctx context.Context, id domain.WorkspaceID) (auth.Workspace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return auth.Workspace{}, err
	}
	w, ok := m.spaces[id]
	if !ok {
		return auth.Workspace{}, auth.ErrNotFound
	}
	w.AllowedProfiles = append([]string(nil), w.AllowedProfiles...)
	return w, nil
}

func (m *Memory) CreateToken(ctx context.Context, record auth.TokenRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := m.tokens[record.Digest]; ok {
		return errors.New("duplicate token digest")
	}
	for _, old := range m.tokens {
		if old.ID == record.ID {
			return errors.New("duplicate token ID")
		}
	}
	record.Scopes = append([]auth.Scope(nil), record.Scopes...)
	m.tokens[record.Digest] = record
	return nil
}

func (m *Memory) LookupToken(ctx context.Context, digest [sha256.Size]byte) (auth.TokenRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return auth.TokenRecord{}, err
	}
	record, ok := m.tokens[digest]
	if !ok {
		return auth.TokenRecord{}, auth.ErrNotFound
	}
	record.Scopes = append([]auth.Scope(nil), record.Scopes...)
	return record, nil
}

func (m *Memory) RevokeToken(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	for key, record := range m.tokens {
		if record.ID == id {
			record.Revoked = true
			m.tokens[key] = record
			return nil
		}
	}
	return auth.ErrNotFound
}

func (m *Memory) CommitObject(ctx context.Context, meta domain.ObjectMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !meta.Valid() {
		return objects.ErrInvalid
	}
	if m.blobs[meta.WorkspaceID] == nil {
		m.blobs[meta.WorkspaceID] = make(map[domain.ObjectID]domain.ObjectMetadata)
	}
	if _, ok := m.blobs[meta.WorkspaceID][meta.ID]; ok {
		return errors.New("object already committed")
	}
	m.blobs[meta.WorkspaceID][meta.ID] = meta
	return nil
}

func (m *Memory) GetObject(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (domain.ObjectMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return domain.ObjectMetadata{}, err
	}
	meta, ok := m.blobs[w][id]
	if !ok {
		return domain.ObjectMetadata{}, objects.ErrNotFound
	}
	return meta, nil
}

func (m *Memory) ObjectCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, objects := range m.blobs {
		count += len(objects)
	}
	return count
}

func (m *Memory) SetOwner(kind auth.ResourceKind, id string, w domain.WorkspaceID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.owners[kind] == nil {
		m.owners[kind] = make(map[string]domain.WorkspaceID)
	}
	m.owners[kind][id] = w
}

func (m *Memory) Owner(ctx context.Context, kind auth.ResourceKind, id string) (domain.WorkspaceID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	owner, ok := m.owners[kind][id]
	if !ok {
		return "", auth.ErrNotFound
	}
	return owner, nil
}
