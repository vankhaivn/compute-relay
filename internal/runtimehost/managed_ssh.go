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
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
)

// managedSSHAdapter connects an operator-owned Docker host reached over SSH. Every
// field is write-only: the endpoint travels with the login in one vault entry.
type managedSSHAdapter struct {
	config ManagedServeConfig
	verify func(context.Context, []byte) (sshhost.HostVerification, error)
}

var _ connections.Adapter = (*managedSSHAdapter)(nil)

func newManagedSSHAdapter(config ManagedServeConfig) *managedSSHAdapter {
	return &managedSSHAdapter{config: config, verify: sshhost.VerifyHostDetails}
}

func (*managedSSHAdapter) Descriptor() connections.Descriptor {
	field := func(name, label string, required bool, max int) connections.Field {
		return connections.Field{Name: name, Label: label, Required: required, WriteOnly: true, MaxBytes: max}
	}
	return connections.Descriptor{
		Type: sshhost.ProviderType, Label: "SSH host (Docker)",
		Fields: []connections.Field{
			field(sshhost.FieldHost, "Host name or IP address", true, 255),
			field(sshhost.FieldPort, "SSH port", false, 5),
			field(sshhost.FieldUsername, "SSH user", true, 64),
			field(sshhost.FieldPassword, "Password", false, 1024),
			field(sshhost.FieldPrivateKey, "Private key (OpenSSH or PEM)", false, 16384),
			field(sshhost.FieldPassphrase, "Private key passphrase", false, 1024),
			field(sshhost.FieldHostKey, "Host key SHA256 fingerprint", false, 64),
			field(sshhost.FieldDockerPath, "Docker CLI path", false, 1024),
			field(sshhost.FieldCPUs, "CPU limit per job", false, 6),
			field(sshhost.FieldMemoryMiB, "Memory limit per job (MiB)", false, 8),
		},
		Capabilities: connections.Capabilities{Accelerators: []string{"cpu"}, RemoteCancel: "supported", Quota: "unsupported"},
	}
}

func (*managedSSHAdapter) Encode(fields map[string]string) ([]byte, error) {
	raw, err := sshhost.EncodeFields(fields)
	if err != nil {
		return nil, connections.ErrRequest
	}
	return raw, nil
}

// Verify proves the login, the host key and a Linux Docker engine. An operator-owned
// host reports no quota; CPU work then runs with an explicit unknown-quota warning.
func (a *managedSSHAdapter) Verify(ctx context.Context, raw []byte) (connections.Verification, error) {
	host, err := a.verify(ctx, raw)
	if errors.Is(err, sshhost.ErrCredentialReject) || errors.Is(err, sshhost.ErrHostKeyMismatch) {
		return connections.Verification{}, connections.ErrCredentialRejected
	}
	if err != nil || !domain.ObjectID(host.Account).Valid() {
		return connections.Verification{}, connections.ErrUnavailable
	}
	return connections.Verification{CanonicalAccount: host.Account, Host: &connections.HostEvidence{HostKeyFingerprint: host.HostKey, OS: host.OS, Architecture: host.Architecture},
		Quota: provider.QuotaObservation{Status: provider.QuotaUnknown, Resource: "cpu", Source: "operator_host"}}, nil
}

func (a *managedSSHAdapter) Profile(binding domain.ProviderBinding, accountScope string) admission.Profile {
	profile := admission.DefaultProfile(binding, accountScope)
	profile.CostClass = admission.CostOperatorManaged
	profile.MaxRemoteWallSeconds = a.config.MaxRemoteWallSeconds
	profile.AllowRemoteInternet = a.config.AllowRemoteInternet
	profile.Accelerator = "cpu"
	profile.MaxActiveAttempts, profile.MaxProcesses = 1, 4096
	return profile
}

func (*managedSSHAdapter) ValidateConfiguration(c connections.Configuration) error {
	return c.Validate()
}

type managedSSHRuntimeConfig struct {
	Version int    `json:"schema_version"`
	Image   string `json:"image"`
}

// RuntimeConfig freezes the standard image digest into each verified revision.
func (*managedSSHAdapter) RuntimeConfig() []byte {
	raw, _ := json.Marshal(managedSSHRuntimeConfig{Version: 1, Image: sshhost.StandardImage})
	return raw
}

func parseManagedSSHRuntimeConfig(raw []byte) (managedSSHRuntimeConfig, error) {
	var config managedSSHRuntimeConfig
	fields, err := jsonwire.Object(raw, 8192)
	if err != nil || jsonwire.Fields(fields, []string{"schema_version", "image"}, nil) != nil || json.Unmarshal(raw, &config) != nil ||
		config.Version != 1 || !sshhost.ValidImage(config.Image) {
		return managedSSHRuntimeConfig{}, ErrRequest
	}
	return config, nil
}

func (h *Host) managedSSHProvider(resolver ports.CredentialResolver, binding connections.RuntimeBinding) (provider.Provider, error) {
	profile := binding.Profile
	if profile.CostClass != admission.CostOperatorManaged || profile.Accelerator != "cpu" {
		return nil, ErrRequest
	}
	frozen, err := parseManagedSSHRuntimeConfig(binding.RuntimeConfig)
	if err != nil {
		return nil, err
	}
	if _, ok := credentials.VaultReference(ports.CredentialRef(profile.CredentialRef)); !ok {
		return nil, ErrRequest
	}
	snapshot := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
	adapter, err := sshhost.NewAdapter(sshhost.Config{InstanceID: profile.Binding.ProviderInstanceID, Account: binding.CanonicalAccount,
		CredentialRef: ports.CredentialRef(profile.CredentialRef), AllowInternet: profile.AllowRemoteInternet,
		MaxWallSeconds: profile.MaxRemoteWallSeconds, Image: frozen.Image, MaxProcesses: profile.MaxProcesses}, snapshot, resolver, h.inputs, clock{})
	if err != nil {
		return nil, ErrRequest
	}
	return adapter, nil
}
