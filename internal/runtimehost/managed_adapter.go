package runtimehost

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

type accountDiscovery interface {
	Discover(context.Context, []byte) (kaggle.DiscoveryResult, error)
}

// The composition root owns the provider-specific credential fields and profile
// policy. The management service sees only its generic adapter contract.
type managedKaggleAdapter struct {
	discovery accountDiscovery
	config    ManagedServeConfig
}

var _ connections.Adapter = (*managedKaggleAdapter)(nil)

func (*managedKaggleAdapter) Descriptor() connections.Descriptor {
	return connections.Descriptor{
		Type: "kaggle", Label: "Kaggle",
		Fields:       []connections.Field{{Name: "api_token", Label: "API token", Required: true, WriteOnly: true, MaxBytes: credentials.MaxBytes}},
		Capabilities: connections.Capabilities{Accelerators: []string{"gpu"}, RemoteCancel: "unsupported", Quota: "supported"},
	}
}

func (*managedKaggleAdapter) Encode(fields map[string]string) ([]byte, error) {
	token, exists := fields["api_token"]
	if len(fields) != 1 || !exists || len(token) == 0 || len(token) > credentials.MaxBytes {
		return nil, connections.ErrRequest
	}
	for _, b := range token {
		if b < 33 || b > 126 {
			return nil, connections.ErrRequest
		}
	}
	return []byte(token), nil
}

func (a *managedKaggleAdapter) Verify(ctx context.Context, token []byte) (connections.Verification, error) {
	result, err := a.discovery.Discover(ctx, token)
	if errors.Is(err, kaggle.ErrCredentialRejected) {
		return connections.Verification{}, connections.ErrCredentialRejected
	}
	if err != nil || result.CanonicalAccount() == "" || result.Quota().Validate() != nil {
		return connections.Verification{}, connections.ErrUnavailable
	}
	return connections.Verification{CanonicalAccount: result.CanonicalAccount(), Quota: result.Quota()}, nil
}

func (a *managedKaggleAdapter) Profile(binding domain.ProviderBinding, accountScope string) admission.Profile {
	profile := admission.DefaultProfile(binding, accountScope)
	profile.MaxRemoteWallSeconds = a.config.MaxRemoteWallSeconds
	profile.AllowRemoteInternet = a.config.AllowRemoteInternet
	return profile
}

type managedKaggleRuntimeConfig struct {
	Version      int    `json:"schema_version"`
	MachineShape string `json:"machine_shape"`
}

func (a *managedKaggleAdapter) RuntimeConfig() []byte {
	raw, _ := json.Marshal(managedKaggleRuntimeConfig{Version: 1, MachineShape: a.config.MachineShape})
	return raw
}

func parseManagedKaggleRuntimeConfig(raw []byte) (managedKaggleRuntimeConfig, error) {
	var config managedKaggleRuntimeConfig
	fields, err := jsonwire.Object(raw, 8192)
	if err != nil || jsonwire.Fields(fields, []string{"schema_version", "machine_shape"}, nil) != nil || json.Unmarshal(raw, &config) != nil ||
		config.Version != 1 || config.MachineShape != "NvidiaTeslaT4" && config.MachineShape != "NvidiaTeslaP100" {
		return managedKaggleRuntimeConfig{}, ErrRequest
	}
	return config, nil
}

// The private Kaggle adapter verifies its canonical username. Dispatch/collection
// hold an opaque shared account scope; verify the full frozen outer binding first.
type managedKaggleProvider struct {
	*kaggle.RuntimeAdapter
	outer, inner provider.BindingSnapshot
}

func (p *managedKaggleProvider) VerifyBinding(ctx context.Context, expected provider.BindingSnapshot) error {
	if expected != p.outer {
		return kaggle.ErrRuntimeBinding
	}
	return p.RuntimeAdapter.VerifyBinding(ctx, p.inner)
}
