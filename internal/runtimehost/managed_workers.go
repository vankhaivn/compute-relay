package runtimehost

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/hostwatchdog"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/worker"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

// ManagedServeConfig installs provider policy, not an account or a compute permit.
// Account verification is requested through durable connection operations. Every
// new attempt separately requires the explicit persisted authorization endpoint.
type ManagedServeConfig struct {
	CredentialStore      string
	MachineShape         string
	MaxRemoteWallSeconds int64
	AllowRemoteInternet  bool
	MaxWorkers           int
}

// DefaultManagedServeConfig installs finite policy without resolving an account.
func DefaultManagedServeConfig() ManagedServeConfig {
	return ManagedServeConfig{MachineShape: "cpu", MaxRemoteWallSeconds: 1800, MaxWorkers: 2}
}

func (c ManagedServeConfig) validate() error {
	if c.CredentialStore != "" && c.CredentialStore != "file" && c.CredentialStore != "keychain" {
		return ErrRequest
	}
	if c.MachineShape != "cpu" && c.MachineShape != "NvidiaTeslaT4" && c.MachineShape != "NvidiaTeslaP100" ||
		c.MaxRemoteWallSeconds < 1 || c.MaxRemoteWallSeconds > 86400 || c.MaxWorkers < 1 || c.MaxWorkers > 16 {
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
	watchdog       *hostwatchdog.Service
	health         *hosthealth.Service
	secrets        *workloadsecret.Service
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
	discovery, err := kaggle.NewDiscovery(clock{})
	if err != nil {
		return nil, ErrRequest
	}
	adapters := []connections.Adapter{newManagedSSHAdapter(config), &managedKaggleAdapter{discovery: discovery, config: config}}
	vault, err := h.managedVault(ctx, config)
	if err != nil {
		return nil, err
	}
	return h.composeManaged(ctx, config, vault, adapters...)
}

// composeManaged provides the offline fixture seam. Neither it nor its concrete
// constructors read a credential, verify an account, or create a compute permit.
func (h *Host) composeManaged(ctx context.Context, config ManagedServeConfig, vault credentials.Vault, adapters ...connections.Adapter) (*managedServices, error) {
	service, err := connections.New(h.access, h.store, vault, adapters)
	if err != nil {
		return nil, ErrState
	}
	permits, err := executionauth.New(h.access, h.store, clock{})
	if err != nil {
		return nil, ErrState
	}
	result := &managedServices{connections: service, authorizations: permits}
	result.health, err = hosthealth.NewService(h.access, h.store, managedHealthSampler{credentials: service, collect: sshhost.CollectHealth}, clock{})
	if err != nil {
		return nil, ErrState
	}
	result.watchdog, err = hostwatchdog.New(h.access, h.store, sshhost.NewWatchdogRemote(service))
	if err != nil {
		return nil, ErrState
	}
	if vault == nil {
		return result, nil // Unsupported protected storage never falls back or fails old jobs.
	}
	pool, err := h.workloadSecretPool(ctx)
	if err != nil {
		return nil, err
	}
	permits.WithSecrets(pool)
	result.secrets, err = workloadsecret.New(h.access, h.store, pool, clock{})
	if err != nil {
		return nil, ErrState
	}
	resolver := &managedSnapshotResolver{ctx: ctx, read: h.store.ReadManagedBinding}
	result.resolver = resolver
	resolver.make = func(binding connections.RuntimeBinding) (provider.Provider, error) {
		return h.managedProvider(config, service, binding)
	}
	dispatchConfig := dispatch.DefaultConfig()
	dispatchConfig.Workers, dispatchConfig.PollDelay = config.MaxWorkers, time.Second
	result.dispatcher, err = dispatch.New(h.store, resolver, h.inputs, clock{}, dispatchConfig)
	if err != nil {
		return nil, ErrState
	}
	result.dispatcher.WithSecrets(pool)
	collectionConfig := collection.DefaultConfig()
	collectionConfig.Workers = config.MaxWorkers
	result.collector, err = collection.New(h.store, resolver, h.results, clock{}, collectionConfig)
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
		AccountName: binding.CanonicalAccount, CredentialRef: ports.CredentialRef(profile.CredentialRef)}
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
	// The repository consumes a durable permit before every preparation intent.
	adapter, err := kaggle.NewRuntimeAdapter(providerConfig, inner, resolver, h.inputs, clock{}, h.store.LoadProviderAttempt, policy, true)
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
		return nil, provider.LocalStateError{Err: err}
	}
	actual := provider.BindingSnapshot{Binding: binding.Profile.Binding, AccountScope: binding.Profile.AccountScope, CredentialRef: binding.Profile.CredentialRef}
	if actual != expected {
		return nil, provider.LocalStateError{Err: ErrState}
	}
	return r.make(binding)
}

type connectionOperationRunner interface {
	RunOnce(context.Context) (bool, error)
}

func runConnectionOperations(ctx context.Context, runner connectionOperationRunner) error {
	err := worker.Run(ctx, worker.Config{Workers: 1, PollDelay: time.Second}, func(ctx context.Context, _ int) (bool, error) {
		return runner.RunOnce(ctx)
	}, func(err error) worker.Decision {
		if err == connections.ErrVault {
			// The pending vault intent remains retryable while unrelated accounts run.
			return worker.Continue
		}
		return worker.Classify(err)
	})
	if err != nil && ctx.Err() == nil {
		return ErrState
	}
	return err
}
