package runtimehost

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
)

const (
	sshFixtureKey   = "SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU"
	sshFixtureOther = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)

// sshFixtureVerify stands in for a live login: the password selects the outcome and the
// presented host key.
func sshFixtureVerify(_ context.Context, raw []byte, _ string) (sshhost.HostVerification, error) {
	endpoint, err := sshhost.ParseEndpoint(raw)
	if err != nil {
		return sshhost.HostVerification{}, err
	}
	target := endpoint.Target
	switch string(endpoint.Credential.Password) {
	case "SYNTHETIC_SSH":
		target.HostKey = sshFixtureKey
	case "SYNTHETIC_SSH_REKEYED":
		target.HostKey = sshFixtureOther
	default:
		return sshhost.HostVerification{}, sshhost.ErrCredentialReject
	}
	account, err := sshhost.Account(target)
	return sshhost.HostVerification{Account: account, HostKey: target.HostKey, OS: "linux", Architecture: "amd64", ImageID: "sha256:" + strings.Repeat("1", 64), MachineID: strings.Repeat("2", 64), EngineID: strings.Repeat("3", 64)}, err
}

func (f *managedTestFixture) sshConnection(t *testing.T, key, password string) (connections.Operation, connections.Connection) {
	t.Helper()
	ctx := context.Background()
	op, err := f.services.connections.Submit(ctx, f.actor, "app", "", key, []byte(fmt.Sprintf(
		`{"provider_type":"ssh","label":"Mac Intel","credentials":{"host":"100.64.0.7","username":"kenji","password":%q}}`, password)))
	if err != nil {
		t.Fatal("accept connection", err)
	}
	if worked, err := f.services.connections.RunOnce(ctx); err != nil || !worked {
		t.Fatal("verify connection", worked, err)
	}
	if op, err = f.services.connections.Operation(ctx, f.actor, "app", op.ID); err != nil {
		t.Fatal(err)
	}
	connection, err := f.services.connections.Get(ctx, f.actor, "app", op.ConnectionID)
	requireNoError(t, err)
	if connection.Selection != nil {
		binding := f.binding(t, connection)
		profile, _, err := f.host.store.ReadProfile(ctx, binding.Binding.Profile)
		requireNoError(t, err)
		scope, err := f.host.store.StorageScopeForProfile(ctx, "app", profile)
		requireNoError(t, err)
		if scope.Valid() {
			now := time.Now().UTC()
			if f.clock != nil {
				now = f.clock.Now()
			}
			requireNoError(t, f.host.store.RecordStorageScan(ctx, hoststorage.Scan{Scope: scope, ObservedAt: now, Objects: []hoststorage.Object{}}))
		}
	}
	return op, connection
}

func TestManagedSSHConnectionPublishesAnOperatorManagedCPUProvider(t *testing.T) {
	f := newManagedTestFixture(t, true, &managedSSHAdapter{verify: sshFixtureVerify, config: ManagedServeConfig{MaxRemoteWallSeconds: 600, AllowRemoteInternet: true}})
	ctx := context.Background()
	_, connection := f.sshConnection(t, "ssh-connection-key", "SYNTHETIC_SSH")
	if connection.Authentication != "verified" || connection.Selection == nil || connection.Selection.Accelerator != "cpu" ||
		connection.Selection.MaxRemoteWallSeconds != 600 || !connection.Selection.AllowRemoteInternet || connection.ProviderType != "ssh" {
		t.Fatalf("connection = %+v", connection)
	}
	if connection.Host == nil || connection.Host.HostKeyFingerprint != sshFixtureKey || connection.Host.OS != "linux" || connection.Host.Architecture != "amd64" {
		t.Fatalf("host evidence = %+v", connection.Host)
	}
	snapshot := f.binding(t, connection)
	saved, err := f.host.store.ReadManagedBinding(ctx, snapshot)
	requireNoError(t, err)
	if saved.Profile.CostClass != admission.CostOperatorManaged || saved.Profile.Accelerator != "cpu" ||
		!strings.HasPrefix(saved.CanonicalAccount, "ssh-") || !strings.Contains(string(saved.RuntimeConfig), "sha256:"+strings.Repeat("1", 64)) {
		t.Fatalf("saved = %+v", saved)
	}
	p, err := f.host.managedProvider(f.config, f.services.connections, saved)
	requireNoError(t, err)
	if _, ok := p.(provider.ArtifactBatchFetcher); !ok {
		t.Fatal("managed SSH provider dropped bounded artifact collection")
	}
	if _, ok := p.(*sshhost.Adapter); !ok || p.Describe().Type != "ssh" || p.Describe().Support(domain.CapabilityGPU) != domain.CapabilitySupportUnsupported ||
		p.Describe().Support(domain.CapabilityRemoteCancellation) != domain.CapabilitySupportSupported {
		t.Fatalf("provider = %T %+v", p, p.Describe())
	}
	for name, mutate := range map[string]func(*connections.RuntimeBinding){
		"floating image": func(b *connections.RuntimeBinding) {
			b.RuntimeConfig = []byte(`{"schema_version":1,"image":"python:3.11"}`)
		},
		"extra config": func(b *connections.RuntimeBinding) {
			b.RuntimeConfig = []byte(`{"schema_version":1,"image":"x","shell":"sh"}`)
		},
		"free allowance":    func(b *connections.RuntimeBinding) { b.Profile.CostClass = admission.CostFreeAllowance },
		"gpu":               func(b *connections.RuntimeBinding) { b.Profile.Accelerator = "gpu" },
		"kaggle type":       func(b *connections.RuntimeBinding) { b.ProviderType = "kaggle" },
		"unknown type":      func(b *connections.RuntimeBinding) { b.ProviderType = "vm" },
		"unpinned account":  func(b *connections.RuntimeBinding) { b.CanonicalAccount = "kenji" },
		"non-vault account": func(b *connections.RuntimeBinding) { b.Profile.CredentialRef = "env:SSH_PASSWORD" },
	} {
		bad := saved
		bad.Profile = saved.Profile
		mutate(&bad)
		if _, err := f.host.managedProvider(f.config, f.services.connections, bad); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestManagedSSHConnectionRefusesRejectedLoginsAndChangedHostKeys(t *testing.T) {
	f := newManagedTestFixture(t, true, &managedSSHAdapter{verify: sshFixtureVerify, config: ManagedServeConfig{MaxRemoteWallSeconds: 600}})
	ctx := context.Background()
	op, connection := f.sshConnection(t, "ssh-rejected-key", "wrong")
	if op.Problem == nil || *op.Problem != "credential_rejected" || connection.Selection != nil {
		t.Fatalf("rejected login = %+v %+v", op, connection)
	}
	_, connection = f.sshConnection(t, "ssh-connection-key", "SYNTHETIC_SSH")
	original := f.binding(t, connection)
	replace, err := f.services.connections.Submit(ctx, f.actor, "app", connection.ID, "ssh-rekeyed-key", []byte(fmt.Sprintf(
		`{"action":"replace_credential","expected_revision":%d,"credentials":{"host":"100.64.0.7","username":"kenji","password":"SYNTHETIC_SSH_REKEYED"}}`, connection.Revision)))
	requireNoError(t, err)
	if worked, err := f.services.connections.RunOnce(ctx); err != nil || !worked {
		t.Fatal(err)
	}
	if replace, err = f.services.connections.Operation(ctx, f.actor, "app", replace.ID); err != nil || replace.Problem == nil || *replace.Problem != "account_changed" {
		t.Fatalf("changed host key = %+v, %v", replace, err)
	}
	current, err := f.services.connections.Get(ctx, f.actor, "app", connection.ID)
	if err != nil || current.Host == nil || current.Host.HostKeyFingerprint != sshFixtureKey {
		t.Fatalf("changed key overwrote public evidence: %+v, %v", current.Host, err)
	}
	saved, err := f.host.store.ReadManagedBinding(ctx, original)
	if err != nil {
		t.Fatal("changed host key broke the verified binding", err)
	}
	if key, err := sshhost.HostKeyOf(saved.CanonicalAccount); err != nil || key != sshFixtureKey {
		t.Fatalf("pinned host key = %q, %v", key, err)
	}
}

func TestManagedSSHDescriptorKeepsEveryFieldWriteOnly(t *testing.T) {
	descriptor := newManagedSSHAdapter(ManagedServeConfig{}).Descriptor()
	if descriptor.Type != "ssh" || len(descriptor.Fields) != 10 || descriptor.Capabilities.RemoteCancel != "supported" ||
		descriptor.Capabilities.Quota != "unsupported" || descriptor.Capabilities.AbsoluteDeadline != "supported" || strings.Join(descriptor.Capabilities.Accelerators, ",") != "cpu" ||
		descriptor.Capabilities.MaxRemoteWallSeconds != 604800 {
		t.Fatalf("descriptor = %+v", descriptor)
	}
	if c := descriptor.Capabilities; c.RemoteCleanup != "supported" || c.EphemeralSecrets != "supported" ||
		c.SeparateStart != "supported" || c.Concurrency == nil ||
		*c.Concurrency != (connections.ConcurrencyCapability{Support: "supported", Scope: "resource", Minimum: 1, Maximum: 64, Default: 1}) {
		t.Fatal("managed capability projection changed", c)
	}
	for _, field := range descriptor.Fields {
		if !field.WriteOnly || field.Required != (field.Name == "host" || field.Name == "username") {
			t.Errorf("field %+v", field)
		}
	}
	if _, err := (&managedSSHAdapter{}).Encode(map[string]string{"host": "100.64.0.7", "username": "kenji"}); err != connections.ErrRequest {
		t.Fatalf("login-free connection: %v", err)
	}
}

func TestManagedSSHConfigurationKeepsLimitsAcrossChecks(t *testing.T) {
	f := newManagedTestFixture(t, true, &managedSSHAdapter{verify: sshFixtureVerify, config: ManagedServeConfig{MaxRemoteWallSeconds: 600}})
	ctx := context.Background()
	_, connection := f.sshConnection(t, "ssh-limits-create", "SYNTHETIC_SSH")
	old := f.binding(t, connection)
	op, err := f.services.connections.Submit(ctx, f.actor, "app", connection.ID, "ssh-limits-configure", []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":1200,"max_active_attempts":3,"max_processes":128}}`, connection.Revision)))
	if err != nil || op.Status != "succeeded" {
		t.Fatal(op, err)
	}
	connection, err = f.services.connections.Get(ctx, f.actor, "app", connection.ID)
	requireNoError(t, err)
	_, err = f.services.connections.Submit(ctx, f.actor, "app", connection.ID, "ssh-limits-check", []byte(fmt.Sprintf(`{"action":"check","expected_revision":%d}`, connection.Revision)))
	requireNoError(t, err)
	if _, err = f.services.connections.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	connection, err = f.services.connections.Get(ctx, f.actor, "app", connection.ID)
	if err != nil || connection.Selection == nil || connection.Selection.MaxActiveAttempts != 3 || connection.Selection.MaxProcesses != 128 {
		t.Fatalf("limits after check: %+v %v", connection.Selection, err)
	}
	frozen, err := f.host.store.ReadManagedBinding(ctx, old)
	if err != nil || frozen.Profile.MaxActiveAttempts != 1 || frozen.Profile.MaxProcesses != 4096 {
		t.Fatal("old limits changed", frozen.Profile, err)
	}
	kaggle := f.connection(t, "kaggle-limit-create", "SYNTHETIC_FIRST")
	_, err = f.services.connections.Submit(ctx, f.actor, "app", kaggle.ID, "kaggle-limit-unsupported", []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":1200,"max_processes":128}}`, kaggle.Revision)))
	if err != connections.ErrRequest {
		t.Fatal("provider accepted unsupported limits", err)
	}
}

func TestManagedSSHRuntimePolicyLegacyAndFrozenHealth(t *testing.T) {
	old := []byte(fmt.Sprintf(`{"schema_version":1,"image":%q}`, sshhost.StandardImage))
	original := string(old)
	parsed, err := parseManagedSSHRuntimeConfig(old)
	if err != nil || parsed.HostHealth == nil || *parsed.HostHealth != hosthealth.LegacyPolicyV1() {
		t.Fatal("legacy interpretation changed", err)
	}
	policy := hosthealth.DefaultPolicy()
	policy.CriticalDurationSeconds = 30
	frozen, err := hosthealth.FreezeRuntimePolicy(old, policy)
	requireNoError(t, err)
	parsed, err = parseManagedSSHRuntimeConfig(frozen)
	if err != nil || parsed.HostHealth == nil || *parsed.HostHealth != policy {
		t.Fatal("frozen policy lost", err)
	}
	if string(old) != original {
		t.Fatal("old runtime rewritten")
	}
	for _, raw := range []string{fmt.Sprintf(`{"schema_version":1,"image":%q,"host_health":null}`, sshhost.StandardImage), fmt.Sprintf(`{"schema_version":1,"image":%q,"host_health":{}}`, sshhost.StandardImage)} {
		if _, err := parseManagedSSHRuntimeConfig([]byte(raw)); err == nil {
			t.Fatal("invalid health policy accepted")
		}
	}
}
