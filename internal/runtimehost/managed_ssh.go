package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
)

// managedSSHAdapter connects an operator-owned Docker host reached over SSH. Every
// field is write-only: the endpoint travels with the login in one vault entry.
type managedSSHAdapter struct {
	config  ManagedServeConfig
	verify  func(context.Context, []byte, string) (sshhost.HostVerification, error)
	install func(context.Context, []byte, string, string, string, domain.OperationID, bool) (sshhost.HostVerification, error)
}

var _ connections.Adapter = (*managedSSHAdapter)(nil)

func newManagedSSHAdapter(config ManagedServeConfig) *managedSSHAdapter {
	return &managedSSHAdapter{config: config, verify: sshhost.VerifyManagedHostBound, install: sshhost.InstallImage}
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
		Capabilities: connections.ProjectCapabilities(sshhost.Capabilities(), "resource"),
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
	host, err := a.verify(ctx, raw, "")
	return sshVerification(raw, host, err)
}

func (a *managedSSHAdapter) VerifyBound(ctx context.Context, raw []byte, account string) (connections.Verification, error) {
	host, err := a.verify(ctx, raw, account)
	return sshVerification(raw, host, err)
}

func sshVerification(raw []byte, host sshhost.HostVerification, err error) (connections.Verification, error) {
	if errors.Is(err, sshhost.ErrCredentialReject) || errors.Is(err, sshhost.ErrHostKeyMismatch) {
		return connections.Verification{}, connections.ErrCredentialRejected
	}
	if errors.Is(err, sshhost.ErrImageInstallationUnknown) {
		return connections.Verification{}, connections.ErrImageInstallationUnknown
	}
	if err != nil || !domain.ObjectID(host.Account).Valid() || !domain.SHA256Digest(host.MachineID).Valid() || !domain.SHA256Digest(host.EngineID).Valid() {
		return connections.Verification{}, connections.ErrUnavailable
	}
	endpoint, err := sshhost.ParseEndpoint(raw)
	if err != nil {
		return connections.Verification{}, connections.ErrUnavailable
	}
	image := &connections.ImageEvidence{Status: "missing", RecipeSHA256: sshhost.ImageRecipeDigest()}
	var runtimeConfig []byte
	if host.ImageID != "" {
		image.Status, image.ID = "ready", host.ImageID
		runtimeConfig, _ = json.Marshal(managedSSHRuntimeConfig{Version: 3, Image: host.ImageID, Architecture: host.Architecture,
			RecipeSHA256: image.RecipeSHA256, CPUs: endpoint.CPUs, MemoryBytes: endpoint.MemoryBytes, MachineID: host.MachineID, EngineID: host.EngineID})
	}
	verified := connections.Verification{CanonicalAccount: host.Account,
		Resource:      &connections.ResourceIdentity{Kind: "machine", ID: "resource_" + host.MachineID},
		Host:          &connections.HostEvidence{HostKeyFingerprint: host.HostKey, OS: host.OS, Architecture: host.Architecture, Image: image},
		RuntimeConfig: runtimeConfig, SelectionUnavailable: host.ImageID == "",
		Quota: provider.QuotaObservation{Status: provider.QuotaUnknown, Resource: "cpu", Source: "operator_host"}}
	if verified.Host.Validate() != nil {
		return connections.Verification{}, connections.ErrUnavailable
	}
	return verified, nil
}

func (*managedSSHAdapter) PlanImageInstallation(connection connections.Connection) (connections.ImageInstallation, error) {
	if connection.Authentication != "verified" || connection.Host == nil || connection.Host.Image == nil ||
		connection.Host.Validate() != nil || connection.Host.Image.RecipeSHA256 != sshhost.ImageRecipeDigest() {
		return connections.ImageInstallation{}, connections.ErrRequest
	}
	return connections.ImageInstallation{RecipeSHA256: sshhost.ImageRecipeDigest(), Architecture: connection.Host.Architecture}, nil
}

func (a *managedSSHAdapter) InstallImage(ctx context.Context, raw []byte, record connections.Record, recoverOnly bool) (connections.Verification, error) {
	if record.Installation == nil || record.Installation.Validate() != nil || record.Installation.RecipeSHA256 != sshhost.ImageRecipeDigest() || a.install == nil ||
		record.Resource == nil && !recoverOnly || record.Resource != nil && (record.Resource.Validate() != nil || record.Resource.Kind != "machine") {
		return connections.Verification{}, connections.ErrUnsupported
	}
	machineID := ""
	if record.Resource != nil {
		machineID = strings.TrimPrefix(record.Resource.ID, "resource_")
	}
	host, err := a.install(ctx, raw, record.CanonicalAccount, machineID, record.Installation.Architecture, record.Operation.ID, recoverOnly)
	return sshVerification(raw, host, err)
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
	Version      int                `json:"schema_version"`
	Image        string             `json:"image"`
	Architecture string             `json:"architecture,omitempty"`
	RecipeSHA256 string             `json:"recipe_sha256,omitempty"`
	CPUs         string             `json:"cpus,omitempty"`
	MemoryBytes  int64              `json:"memory_bytes,omitempty"`
	MachineID    string             `json:"machine_id,omitempty"`
	EngineID     string             `json:"engine_id,omitempty"`
	HostHealth   *hosthealth.Policy `json:"host_health,omitempty"`
}

func parseManagedSSHRuntimeConfig(raw []byte) (managedSSHRuntimeConfig, error) {
	var config managedSSHRuntimeConfig
	fields, err := jsonwire.Object(raw, 8192)
	if err != nil || json.Unmarshal(raw, &config) != nil {
		return managedSSHRuntimeConfig{}, ErrRequest
	}
	switch config.Version {
	case 1:
		if jsonwire.Fields(fields, []string{"schema_version", "image"}, []string{"host_health"}) != nil || !sshhost.ValidImage(config.Image) {
			return managedSSHRuntimeConfig{}, ErrRequest
		}
	case 2, 3:
		required := []string{"schema_version", "image", "architecture", "recipe_sha256"}
		if config.Version == 3 {
			required = append(required, "machine_id", "engine_id")
			if !domain.SHA256Digest(config.MachineID).Valid() || !domain.SHA256Digest(config.EngineID).Valid() {
				return managedSSHRuntimeConfig{}, ErrRequest
			}
		}
		if jsonwire.Fields(fields, required, []string{"cpus", "memory_bytes", "host_health"}) != nil ||
			!sshhost.ValidImageID(config.Image) || !sshhost.SupportedArchitecture(config.Architecture) || !domain.SHA256Digest(config.RecipeSHA256).Valid() ||
			!sshhost.ValidResourceLimits(config.CPUs, config.MemoryBytes) {
			return managedSSHRuntimeConfig{}, ErrRequest
		}
	default:
		return managedSSHRuntimeConfig{}, ErrRequest
	}
	policy, err := hosthealth.PolicyFromRuntime(raw)
	if err != nil {
		return managedSSHRuntimeConfig{}, ErrRequest
	}
	config.HostHealth = &policy
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
	if binding.Resource != nil {
		if binding.Resource.Validate() != nil || binding.Resource.Kind != "machine" || frozen.MachineID != "" && "resource_"+frozen.MachineID != binding.Resource.ID {
			return nil, ErrRequest
		}
		frozen.MachineID = strings.TrimPrefix(binding.Resource.ID, "resource_")
	} else if frozen.Version == 3 {
		return nil, ErrRequest
	}
	if _, ok := credentials.VaultReference(ports.CredentialRef(profile.CredentialRef)); !ok {
		return nil, ErrRequest
	}
	snapshot := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
	adapter, err := sshhost.NewAdapter(sshhost.Config{InstanceID: profile.Binding.ProviderInstanceID, Account: binding.CanonicalAccount,
		CredentialRef: ports.CredentialRef(profile.CredentialRef), AllowInternet: profile.AllowRemoteInternet,
		MaxWallSeconds: profile.MaxRemoteWallSeconds, Image: frozen.Image, MaxProcesses: profile.MaxProcesses, Architecture: frozen.Architecture, ImageRecipeSHA256: frozen.RecipeSHA256, CPUs: frozen.CPUs, MemoryBytes: frozen.MemoryBytes, MachineID: frozen.MachineID, EngineID: frozen.EngineID}, snapshot, resolver, h.inputs, clock{})
	if err != nil {
		return nil, ErrRequest
	}
	return adapter.WithHostCache(h.store), nil
}
