package runtimehost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// ManagedServeConfig installs provider policy, not an account or a compute permit.
// Account verification is requested through durable connection operations. Every
// new attempt separately requires the explicit persisted authorization endpoint.
type ManagedServeConfig struct {
	PythonExecutable     string
	CredentialStore      string
	MachineShape         string
	MaxRemoteWallSeconds int64
	AllowRemoteInternet  bool
	MaxWorkers           int
}

func (c ManagedServeConfig) validate() error {
	if c.CredentialStore != "" && c.CredentialStore != "file" && c.CredentialStore != "keychain" {
		return ErrRequest
	}
	if c.MachineShape != "cpu" && c.MachineShape != "NvidiaTeslaT4" && c.MachineShape != "NvidiaTeslaP100" ||
		c.MaxRemoteWallSeconds < 1 || c.MaxRemoteWallSeconds > 86400 || c.MaxWorkers < 1 || c.MaxWorkers > 16 {
		return ErrRequest
	}
	if c.PythonExecutable == "" {
		if c.MachineShape != "cpu" {
			return ErrRequest
		}
		return nil
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

func (c ManagedServeConfig) accelerator() string {
	if c.MachineShape == "cpu" {
		return "cpu"
	}
	return "gpu"
}

type managedServices struct {
	connections    *connections.Service
	authorizations *executionauth.Service
	dispatcher     *dispatch.Engine
	collector      *collection.Engine
	settings       scheduler.Settings
	resolver       *managedSnapshotResolver
}

func (h *Host) managedWorkers(ctx context.Context, config ManagedServeConfig) (*managedServices, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	adapters := []connections.Adapter{newManagedSSHAdapter(config)}
	if config.PythonExecutable == "" {
		present, err := h.store.HasManagedConnections(ctx, "kaggle")
		if err != nil {
			return nil, ErrState
		}
		if present {
			return nil, ErrRequest
		}
	} else {
		discovery, discoveryErr := kaggle.NewDiscovery(config.PythonExecutable, clock{})
		if discoveryErr != nil {
			return nil, ErrRequest
		}
		adapters = append(adapters, &managedKaggleAdapter{discovery: discovery, config: config})
	}
	vault, err := h.managedVault(ctx, config)
	if err != nil {
		return nil, err
	}
	return h.composeManaged(ctx, config, vault, adapters...)
}

// composeManaged provides the offline fixture seam. Neither it nor its concrete
// constructors read a credential, verify an account, or create a compute permit.
func (h *Host) composeManaged(ctx context.Context, config ManagedServeConfig, vault credentials.Vault, adapters ...connections.Adapter) (*managedServices, error) {
	service, err := connections.New(h.access, managedConnectionRepository{h.store}, vault, adapters, clock{}.Now)
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
	result.resolver = resolver
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
	settings.Paused, settings.MaxWorkers, settings.MaxActivePerAccount = false, config.MaxWorkers, 64
	if err = h.store.ConfigureScheduler(ctx, settings); err != nil {
		return nil, ErrState
	}
	result.settings = settings
	return result, nil
}

func (h *Host) managedProvider(config ManagedServeConfig, resolver ports.CredentialResolver, binding connections.RuntimeBinding) (provider.Provider, error) {
	profile := binding.Profile
	if profile.Validate() != nil {
		return nil, ErrRequest
	}
	switch binding.ProviderType {
	case "kaggle":
		if profile.CostClass != admission.CostFreeAllowance {
			return nil, ErrRequest
		}
	case sshhost.ProviderType:
		return h.managedSSHProvider(resolver, binding)
	default:
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
	accelerator := "gpu"
	if frozen.MachineShape == "cpu" {
		policy.MachineShape = ""
		accelerator = "cpu"
	}
	if profile.Accelerator != "" && profile.Accelerator != accelerator {
		return nil, ErrRequest
	}
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
	return r.ResolveContext(r.ctx, expected)
}

func (r *managedSnapshotResolver) ResolveContext(parent context.Context, expected provider.BindingSnapshot) (provider.Provider, error) {
	if !expected.Valid() {
		return nil, ErrRequest
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
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
