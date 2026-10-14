package runtimehost

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"

	"github.com/vankhaivn/compute-relay/internal/statefs"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

// The installation lock serializes first creation. This replay key is protected
// local state, separate from workload values, provider credentials and SQLite.
// Missing key plus existing receipts is inconsistent state, never key rotation.
func (h *Host) workloadSecretPool(ctx context.Context) (*workloadsecret.Pool, error) {
	if h.secretPool != nil {
		return h.secretPool, nil
	}
	path := filepath.Join(h.root.Path, "state", "workload-secret-key")
	var key []byte
	if _, err := os.Lstat(path); err == nil {
		key, err = readPrivate(path, 32)
		if err != nil || len(key) != 32 {
			clear(key)
			return nil, ErrState
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, ErrState
	} else {
		exists, err := h.store.HasWorkloadSecretAuthorizations(ctx)
		if err != nil || exists {
			return nil, ErrState
		}
		key = make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			clear(key)
			return nil, ErrState
		}
		if statefs.WriteNew(path, key) != nil || statefs.SyncDir(filepath.Dir(path)) != nil {
			clear(key)
			return nil, ErrState
		}
	}
	defer clear(key)
	pool, err := workloadsecret.NewPool(key)
	if err != nil {
		return nil, ErrState
	}
	h.secretPool = pool
	return pool, nil
}
