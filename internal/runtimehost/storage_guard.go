package runtimehost

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// managedStorageGuard refreshes a bounded machine inventory before new SSH
// work. An incomplete read is durable and closes new work until a later
// complete scan; it never turns an absence into a deletion receipt.
type managedStorageGuard struct {
	health      *managedHostHealthGuard
	store       *sqlite.Store
	credentials *connections.Service
	collect     func(context.Context, []byte, string, hoststorage.Scope) (hoststorage.Scan, error)
}

func newManagedStorageGuard(health *managedHostHealthGuard, store *sqlite.Store, credentials *connections.Service) *managedStorageGuard {
	return &managedStorageGuard{health: health, store: store, credentials: credentials, collect: sshhost.CollectStorage}
}

func (g *managedStorageGuard) scanProfile(ctx context.Context, workspace domain.WorkspaceID, profile admission.Profile) error {
	scope, err := g.store.StorageScopeForProfile(ctx, workspace, profile)
	if err != nil || !scope.Valid() {
		return err
	}
	binding := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
	frozen, err := g.store.ReadManagedBinding(ctx, binding)
	if err != nil {
		return err
	}
	if frozen.ProviderType != sshhost.ProviderType {
		return hoststorage.ErrUnverified
	}
	var scan hoststorage.Scan
	err = g.credentials.WithCredential(ctx, ports.CredentialRef(frozen.Profile.CredentialRef), func(raw []byte) error {
		var scanErr error
		scan, scanErr = g.collect(ctx, raw, frozen.CanonicalAccount, scope)
		return scanErr
	})
	if err != nil || !scan.Valid() || scan.Scope != scope {
		if recordErr := g.store.RecordIncompleteStorageScan(ctx, scope, "remote_observation_failed"); recordErr != nil {
			return recordErr
		}
		return hoststorage.ErrUnknown
	}
	if err := g.store.RecordStorageScan(ctx, scan); err != nil {
		return err
	}
	return g.store.QueueStorageEvictions(ctx, scope, workspace, profile)
}

func (g *managedStorageGuard) PreflightAdmission(ctx context.Context, w domain.WorkspaceID, token, key string, req admission.Request) (context.Context, *admission.Receipt, error) {
	checked, replay, err := g.health.PreflightAdmission(ctx, w, token, key, req)
	if err != nil || replay != nil {
		return checked, replay, err
	}
	preview, err := g.store.PreviewHostHealthAdmission(checked, w, token, key, req)
	if err != nil || preview.Replay != nil {
		return checked, preview.Replay, err
	}
	if err := g.scanProfile(checked, w, preview.Profile); err != nil {
		// An exact concurrent admission replay wins over the later observation.
		prior, replayErr := g.store.ReplayHostHealthAdmission(checked, w, token, key, req)
		if replayErr == nil && prior != nil {
			return checked, prior, nil
		}
		if replayErr != nil {
			return checked, nil, replayErr
		}
		return checked, nil, err
	}
	return checked, nil, nil
}

func (g *managedStorageGuard) BeforeNewWork(ctx context.Context, record admission.Record) (context.Context, error) {
	guarded, err := g.health.BeforeNewWork(ctx, record)
	if err != nil || record.Preparation != nil {
		return guarded, err
	}
	if err := g.scanProfile(guarded, record.Job.WorkspaceID, record.Profile); err != nil {
		return guarded, err
	}
	return guarded, nil
}

func (g *managedStorageGuard) RunOnce(ctx context.Context) error {
	profiles, err := g.store.ActiveStorageProfiles(ctx)
	if err != nil {
		return err
	}
	for _, item := range profiles {
		if err := g.scanProfile(ctx, item.WorkspaceID, item.Profile); err != nil && !errors.Is(err, hoststorage.ErrUnknown) {
			return err
		}
	}
	return nil
}

func (g *managedStorageGuard) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		if err := g.RunOnce(ctx); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
