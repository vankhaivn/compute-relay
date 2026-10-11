package runtimehost

import (
	"context"
	"errors"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestManagedLogResolverUsesRequestCancellation(t *testing.T) {
	job := fake.ExampleJob("fixture")
	binding := provider.BindingSnapshot{Binding: job.Binding, AccountScope: "fixture_account", CredentialRef: "fixture_credential"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	readCalled, made := false, false
	resolver := managedSnapshotResolver{ctx: context.Background(), read: func(got context.Context, expected provider.BindingSnapshot) (connections.RuntimeBinding, error) {
		readCalled = true
		if !errors.Is(got.Err(), context.Canceled) || expected != binding {
			t.Fatal("log resolution lost request cancellation or original binding")
		}
		return connections.RuntimeBinding{}, got.Err()
	}, make: func(connections.RuntimeBinding) (provider.Provider, error) { made = true; return nil, nil }}
	if _, err := resolver.ResolveContext(ctx, binding); err == nil || !readCalled || made {
		t.Fatal("canceled read constructed provider", err)
	}
}
