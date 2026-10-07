package runtimehost

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

type discoveryFailure struct{ err error }

func (d discoveryFailure) Discover(context.Context, []byte) (kaggle.DiscoveryResult, error) {
	return kaggle.DiscoveryResult{}, d.err
}

func TestManagedKaggleDescriptorAndCredentialEncodingAreClosed(t *testing.T) {
	adapter := &managedKaggleAdapter{}
	descriptor := adapter.Descriptor()
	if descriptor.Type != "kaggle" || len(descriptor.Fields) != 1 || descriptor.Fields[0].Name != "api_token" ||
		!descriptor.Fields[0].WriteOnly || !descriptor.Fields[0].Required || descriptor.Fields[0].MaxBytes != 8192 ||
		descriptor.Capabilities.RemoteCancel != "unsupported" || descriptor.Capabilities.Quota != "supported" {
		t.Fatal("invalid installed descriptor")
	}
	for _, fields := range []map[string]string{
		nil, {}, {"api_token": ""}, {"api_token": "bad token"}, {"api_token": "bad\n"},
		{"api_token": "\xff"}, {"api_token": strings.Repeat("x", 8193)},
		{"api_token": "SYNTHETIC_TOKEN", "username": "fixture_user"}, {"API_TOKEN": "SYNTHETIC_TOKEN"},
	} {
		if raw, err := adapter.Encode(fields); err != connections.ErrRequest || len(raw) != 0 {
			t.Fatal("undeclared/invalid credential accepted", err)
		}
	}
	raw, err := adapter.Encode(map[string]string{"api_token": "SYNTHETIC_TOKEN"})
	if err != nil || string(raw) != "SYNTHETIC_TOKEN" {
		t.Fatal("explicit token encoding failed", err)
	}
	clear(raw)
	descriptor.Fields[0].MaxBytes = 1
	if adapter.Descriptor().Fields[0].MaxBytes != 8192 {
		t.Fatal("descriptor exposed mutable shared policy")
	}
}

func TestManagedKaggleVerificationSanitizesErrorsAndProfileUsesFinitePolicy(t *testing.T) {
	for _, tc := range []struct{ input, want error }{
		{kaggle.ErrCredentialRejected, connections.ErrCredentialRejected},
		{errors.New("SYNTHETIC_TOKEN"), connections.ErrUnavailable},
		{kaggle.ErrProtocol, connections.ErrUnavailable},
		{nil, connections.ErrUnavailable},
	} {
		a := &managedKaggleAdapter{discovery: discoveryFailure{tc.input}}
		verified, err := a.Verify(context.Background(), []byte("SYNTHETIC_TOKEN"))
		if err != tc.want || verified.CanonicalAccount != "" {
			t.Fatal("invalid result or raw failure escaped", err)
		}
	}
	binding := domain.ProviderBinding{Profile: "con_fixture_r1", ProviderInstanceID: "con_fixture", ConfigurationRevision: "r1"}
	a := &managedKaggleAdapter{config: ManagedServeConfig{MaxRemoteWallSeconds: 300, AllowRemoteInternet: false}}
	p := a.Profile(binding, "account_opaque")
	if p.Validate() != nil || p.Binding != binding || p.AccountScope != "account_opaque" ||
		p.MaxRemoteWallSeconds != 300 || p.AllowRemoteInternet || p.MaxBundleBytes != 100<<20 || p.MaxInputBytes != 4<<30 ||
		p.CostClass != "free_allowance" || p.CredentialRef != "" {
		t.Fatal("profile broadened installed policy")
	}
}

func TestManagedKaggleRuntimePolicyIsClosedAndContainsNoLocalInterpreter(t *testing.T) {
	a := &managedKaggleAdapter{config: ManagedServeConfig{PythonExecutable: "/private/local/python", MachineShape: "NvidiaTeslaT4"}}
	raw := a.RuntimeConfig()
	parsed, err := parseManagedKaggleRuntimeConfig(raw)
	if err != nil || parsed.MachineShape != "NvidiaTeslaT4" || strings.Contains(string(raw), "python") || strings.Contains(string(raw), "private") {
		t.Fatal("runtime policy contains mutable local configuration", err)
	}
	for _, invalid := range []string{
		"", `{}`, `null`, `{"schema_version":2,"machine_shape":"NvidiaTeslaT4"}`,
		`{"schema_version":1,"machine_shape":"cpu"}`,
		`{"schema_version":1,"Machine_Shape":"NvidiaTeslaT4"}`,
		`{"schema_version":1,"machine_shape":"NvidiaTeslaT4","machine_shape":"NvidiaTeslaP100"}`,
		`{"schema_version":1,"machine_shape":null}`,
		`{"schema_version":1,"machine_shape":"NvidiaTeslaT4","python":"/private/python"}`,
	} {
		if _, err := parseManagedKaggleRuntimeConfig([]byte(invalid)); err != ErrRequest {
			t.Fatal("invalid frozen policy accepted")
		}
	}
}
