package runtimehost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// ManagedServeConfig installs provider policy, not an account or a compute permit.
// Account verification is requested through durable connection operations. Every
// new attempt separately requires the explicit persisted authorization endpoint.
type ManagedServeConfig struct {
	PythonExecutable     string
	MachineShape         string
	MaxRemoteWallSeconds int64
	AllowRemoteInternet  bool
	MaxWorkers           int
}

func (c ManagedServeConfig) validate() error {
	if c.MachineShape != "NvidiaTeslaT4" && c.MachineShape != "NvidiaTeslaP100" ||
		c.MaxRemoteWallSeconds < 1 || c.MaxRemoteWallSeconds > 86400 || c.MaxWorkers < 1 || c.MaxWorkers > 16 {
		return ErrRequest
	}
	if _, err := kaggle.NewDiscovery(c.PythonExecutable, clock{}); err != nil {
		return ErrRequest
	}
	info, err := os.Stat(c.PythonExecutable)
	if err != nil || !info.Mode().IsRegular() || runtime.GOOS != "windows" && info.Mode().Perm()&0111 == 0 ||
		runtime.GOOS == "windows" && !strings.EqualFold(filepath.Ext(c.PythonExecutable), ".exe") {
		return ErrRequest
	}
	return nil
}

type managedServices struct {
	connections    *connections.Service
	authorizations *executionauth.Service
	dispatcher     *dispatch.Engine
	collector      *collection.Engine
	settings       scheduler.Settings
}

func (h *Host) managedWorkers(ctx context.Context, config ManagedServeConfig) (*managedServices, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	discovery, err := kaggle.NewDiscovery(config.PythonExecutable, clock{})
	if err != nil {
		return nil, ErrRequest
	}
	var vault credentials.Vault
	keychain, err := credentials.NewKeychain(h.identity.InstallationID, config.PythonExecutable)
	if err == nil {
		vault = keychain
	} else if !errors.Is(err, credentials.ErrUnsupported) {
		return nil, ErrState
	}
	return h.composeManaged(ctx, config, vault, &managedKaggleAdapter{discovery: discovery, config: config})
}

// composeManaged provides the offline fixture seam. Neither it nor its concrete
// constructors read a credential, verify an account, or create a compute permit.
func (h *Host) composeManaged(ctx context.Context, config ManagedServeConfig, vault credentials.Vault, adapter connections.Adapter) (*managedServices, error) {
	service, err := connections.New(h.access, managedConnectionRepository{h.store}, vault, []connections.Adapter{adapter}, clock{}.Now)
	if err != nil {
		return nil, ErrState
	}
	permits, err := executionauth.New(h.access, managedAuthorizationRepository{h.store}, clock{})
	if err != nil {
		return nil, ErrState
	}
	result := &managedServices{connections: service, authorizations: permits}
	if vault == nil {
		return result, nil // Unsupported protected storage never falls back or fails old jobs.
	}
	resolver := &managedSnapshotResolver{ctx: ctx, read: h.store.ReadManagedBinding}
	resolver.make = func(binding connections.RuntimeBinding) (provider.Provider, error) {
		return h.managedProvider(config, service, binding)
	}
	dispatchConfig := dispatch.DefaultConfig()
	dispatchConfig.Workers, dispatchConfig.PollDelay = config.MaxWorkers, time.Second
	result.dispatcher, err = dispatch.New(managedDispatchRepository{h.store}, resolver, h.inputs, nil, clock{}, dispatchConfig)
	if err != nil {
		return nil, ErrState
	}
	collectionConfig := collection.DefaultConfig()
	collectionConfig.Workers = config.MaxWorkers
	result.collector, err = collection.New(managedCollectionRepository{h.store}, resolver, h.results, clock{}, collectionConfig)
	if err != nil {
		return nil, ErrState
	}
	settings := scheduler.DefaultSettings()
	settings.Paused, settings.MaxWorkers, settings.MaxActivePerAccount = false, config.MaxWorkers, 1
	if err = h.store.ConfigureScheduler(ctx, settings); err != nil {
		return nil, ErrState
	}
	result.settings = settings
	return result, nil
}

func (h *Host) managedProvider(config ManagedServeConfig, resolver ports.CredentialResolver, binding connections.RuntimeBinding) (provider.Provider, error) {
	profile := binding.Profile
	if binding.ProviderType != "kaggle" || profile.Validate() != nil {
		return nil, ErrRequest
	}
	frozen, err := parseManagedKaggleRuntimeConfig(binding.RuntimeConfig)
	if err != nil {
		return nil, err
	}
	if _, ok := credentials.VaultReference(ports.CredentialRef(profile.CredentialRef)); !ok {
		return nil, ErrRequest
	}
	outer := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
	inner := outer
	inner.AccountScope = binding.CanonicalAccount
	providerConfig := kaggle.Config{InstanceID: string(profile.Binding.ProviderInstanceID), Revision: profile.Binding.ConfigurationRevision,
		AccountName: binding.CanonicalAccount, CredentialRef: ports.CredentialRef(profile.CredentialRef), PythonExecutable: config.PythonExecutable}
	policy := kaggle.DefaultExecutionPolicy()
	policy.MachineShape, policy.AllowInternet, policy.MaxWallSeconds = frozen.MachineShape, profile.AllowRemoteInternet, profile.MaxRemoteWallSeconds
	// One resolver invocation belongs to one dispatch/collection step. A fresh
	// adapter's one-attempt budget cannot admit work: the managed repository alone
	// claims durable permits and consumes them before preparation intents.
	adapter, err := kaggle.NewRuntimeAdapter(providerConfig, inner, resolver, h.inputs, clock{}, h.store.LoadProviderAttempt, policy, 1, true)
	if err != nil {
		return nil, ErrRequest
	}
	return &managedKaggleProvider{RuntimeAdapter: adapter, outer: outer, inner: inner}, nil
}

type managedSnapshotResolver struct {
	ctx  context.Context
	read func(context.Context, provider.BindingSnapshot) (connections.RuntimeBinding, error)
	make func(connections.RuntimeBinding) (provider.Provider, error)
}

func (r *managedSnapshotResolver) Resolve(expected provider.BindingSnapshot) (provider.Provider, error) {
	if !expected.Valid() {
		return nil, ErrRequest
	}
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	binding, err := r.read(ctx, expected)
	if err != nil {
		return nil, ErrState
	}
	actual := provider.BindingSnapshot{Binding: binding.Profile.Binding, AccountScope: binding.Profile.AccountScope, CredentialRef: binding.Profile.CredentialRef}
	if actual != expected {
		return nil, ErrState
	}
	return r.make(binding)
}

type connectionOperationRunner interface {
	RunOnce(context.Context) (bool, error)
}

func runConnectionOperations(ctx context.Context, worker connectionOperationRunner) error {
	for {
		delay := time.Second
		if _, err := worker.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !errors.Is(err, connections.ErrVault) {
				return ErrState
			}
			delay = 30 * time.Second // Exact deletion intents survive denied storage; other accounts stay available.
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
