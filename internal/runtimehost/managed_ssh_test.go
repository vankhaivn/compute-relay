package runtimehost

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
)

const (
	sshFixtureKey   = "SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU"
	sshFixtureOther = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)

// sshFixtureVerify stands in for a live login: the password selects the outcome and the
// presented host key.
func sshFixtureVerify(_ context.Context, raw []byte) (sshhost.HostVerification, error) {
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
	return sshhost.HostVerification{Account: account, HostKey: target.HostKey, OS: "linux", Architecture: "amd64"}, err
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
	if err != nil {
		t.Fatal(err)
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
	if err != nil {
		t.Fatal(err)
	}
	if saved.Profile.CostClass != admission.CostOperatorManaged || saved.Profile.Accelerator != "cpu" ||
		!strings.HasPrefix(saved.CanonicalAccount, "ssh-") || !strings.Contains(string(saved.RuntimeConfig), sshhost.StandardImage) {
		t.Fatalf("saved = %+v", saved)
	}
	p, err := f.host.managedProvider(f.config, f.services.connections, saved)
	if err != nil {
		t.Fatal(err)
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
	if err != nil {
		t.Fatal(err)
	}
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
		descriptor.Capabilities.Quota != "unsupported" || strings.Join(descriptor.Capabilities.Accelerators, ",") != "cpu" {
		t.Fatalf("descriptor = %+v", descriptor)
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
