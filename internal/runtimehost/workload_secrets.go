package runtimehost

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
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

type managedSecretRepository struct{ workloadsecret.Repository }

func (r managedSecretRepository) SupplyWorkloadSecrets(ctx context.Context, c workloadsecret.Command, metadata workloadsecret.Metadata, fingerprint string) (workloadsecret.Receipt, bool, error) {
	type result struct {
		receipt workloadsecret.Receipt
		usable  bool
	}
	value, err := retryManagedClock(ctx, c.Now, func(at time.Time) (result, error) {
		c.Now = at
		receipt, usable, err := r.Repository.SupplyWorkloadSecrets(ctx, c, metadata, fingerprint)
		return result{receipt, usable}, err
	})
	return value.receipt, value.usable, err
}
func (r managedSecretRepository) RevokeWorkloadSecrets(ctx context.Context, c workloadsecret.Command, attempt domain.AttemptID, id domain.OperationID) (workloadsecret.Receipt, error) {
	return retryManagedClock(ctx, c.Now, func(at time.Time) (workloadsecret.Receipt, error) {
		c.Now = at
		return r.Repository.RevokeWorkloadSecrets(ctx, c, attempt, id)
	})
}
func (r managedSecretRepository) ReadWorkloadSecrets(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, id domain.OperationID, now time.Time) (workloadsecret.Receipt, error) {
	return retryManagedClock(ctx, now, func(at time.Time) (workloadsecret.Receipt, error) {
		return r.Repository.ReadWorkloadSecrets(ctx, w, token, job, id, at)
	})
}
func (r managedSecretRepository) WorkloadSecretsUsable(ctx context.Context, key workloadsecret.Key, id domain.OperationID, now time.Time) (bool, error) {
	return retryManagedClock(ctx, now, func(at time.Time) (bool, error) { return r.Repository.WorkloadSecretsUsable(ctx, key, id, at) })
}
