package runtimehost

import (
	"context"
	"os"
	"time"

	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type KaggleServeConfig struct {
	Config       kaggle.Config
	Profile      string
	MachineShape string
	MaxAttempts  int
}

type budgetedDispatchRepository struct {
	dispatch.Repository
	adapter *kaggle.RuntimeAdapter
}

func (r budgetedDispatchRepository) ClaimNext(ctx context.Context, owner string, now time.Time) (scheduler.Result, error) {
	if r.adapter == nil || r.adapter.BudgetExhausted() {
		return scheduler.Result{}, nil
	}
	return r.Repository.ClaimNext(ctx, owner, now)
}

func (h *Host) kaggleWorkers(ctx context.Context, serve KaggleServeConfig) (*dispatch.Engine, *collection.Engine, scheduler.Settings, error) {
	var noSettings scheduler.Settings
	if !profileName.MatchString(serve.Profile) ||
		(serve.MachineShape != "NvidiaTeslaT4" && serve.MachineShape != "NvidiaTeslaP100") ||
		serve.MaxAttempts < 1 || serve.MaxAttempts > 64 ||
		serve.Config.Validate() != nil {
		return nil, nil, noSettings, ErrRequest
	}
	profile, enabled, err := h.store.ReadProfile(ctx, serve.Profile)
	if err != nil || !enabled || profile.CostClass != "free_allowance" {
		return nil, nil, noSettings, ErrRequest
	}
	binding := provider.BindingSnapshot{
		Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef,
	}
	if binding.Binding.Profile != serve.Profile ||
		binding.Binding.ProviderInstanceID != domain.ProviderInstanceID(serve.Config.InstanceID) ||
		binding.Binding.ConfigurationRevision != serve.Config.Revision ||
		binding.AccountScope != serve.Config.AccountName ||
		binding.CredentialRef != string(serve.Config.CredentialRef) {
		return nil, nil, noSettings, ErrRequest
	}
	resolver, err := credentials.NewEnvironment([]ports.CredentialRef{serve.Config.CredentialRef}, os.LookupEnv)
	if err != nil {
		return nil, nil, noSettings, ErrRequest
	}
	policy := kaggle.DefaultExecutionPolicy()
	policy.MachineShape = serve.MachineShape
	policy.AllowInternet = profile.AllowRemoteInternet
	policy.MaxWallSeconds = profile.MaxRemoteWallSeconds
	adapter, err := kaggle.NewRuntimeAdapter(
		serve.Config, binding, resolver, h.inputs, clock{}, h.store.LoadProviderAttempt,
		policy, serve.MaxAttempts, true,
	)
	if err != nil {
		return nil, nil, noSettings, ErrRequest
	}
	if _, err = adapter.Check(ctx); err != nil {
		return nil, nil, noSettings, ErrState
	}
	registry := provider.NewSnapshotRegistry()
	if err = registry.Register(binding, adapter); err != nil {
		return nil, nil, noSettings, ErrState
	}

	dispatchConfig := dispatch.DefaultConfig()
	dispatchConfig.Workers = 1
	dispatchConfig.PollDelay = time.Second
	repo := budgetedDispatchRepository{Repository: h.store, adapter: adapter}
	dispatcher, err := dispatch.New(repo, registry, h.inputs, nil, clock{}, dispatchConfig)
	if err != nil {
		return nil, nil, noSettings, ErrState
	}
	collectionConfig := collection.DefaultConfig()
	collectionConfig.Workers = 1
	collector, err := collection.New(h.store, registry, h.results, clock{}, collectionConfig)
	if err != nil {
		return nil, nil, noSettings, ErrState
	}

	// Persist worker admission only after the complete composition exists. A
	// constructor failure must not leave a future process observing an unpaused
	// scheduler without an active owner.
	settings := scheduler.DefaultSettings()
	settings.Paused = false
	settings.MaxWorkers = 1
	settings.MaxActivePerAccount = 1
	if err = h.store.ConfigureScheduler(ctx, settings); err != nil {
		return nil, nil, noSettings, ErrState
	}
	return dispatcher, collector, settings, nil
}
