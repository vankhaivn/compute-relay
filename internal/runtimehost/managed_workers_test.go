package runtimehost

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type managedTestVault struct {
	mu      sync.Mutex
	entries map[string][]byte
	calls   int
}

func (v *managedTestVault) Create(_ context.Context, key string, value []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	if old, exists := v.entries[key]; exists && !bytes.Equal(old, value) {
		return credentials.ErrConflict
	}
	v.entries[key] = append([]byte(nil), value...)
	return nil
}
func (v *managedTestVault) WithSecret(_ context.Context, key string, use func([]byte) error) error {
	v.mu.Lock()
	v.calls++
	value, exists := v.entries[key]
	copy := append([]byte(nil), value...)
	v.mu.Unlock()
	defer clear(copy)
	if !exists {
		return credentials.ErrUnconfigured
	}
	return use(copy)
}
func (v *managedTestVault) Delete(_ context.Context, key string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	clear(v.entries[key])
	delete(v.entries, key)
	return nil
}

type managedTestAdapter struct {
	*managedKaggleAdapter
	checks         int
	quotaRemaining *float64
}

func (a *managedTestAdapter) Verify(_ context.Context, token []byte) (connections.Verification, error) {
	a.checks++
	account := "fixture_user"
	switch string(token) {
	case "SYNTHETIC_FIRST", "SYNTHETIC_SAME_ACCOUNT", "SYNTHETIC_REPLACEMENT":
	case "SYNTHETIC_OTHER_ACCOUNT":
		account = "other_user"
	default:
		return connections.Verification{}, connections.ErrCredentialRejected
	}
	limit, used, remaining := 3600.0, 0.0, 3600.0
	if a.quotaRemaining != nil {
		remaining = *a.quotaRemaining
		used = limit - remaining
	}
	return connections.Verification{CanonicalAccount: account, Quota: provider.QuotaObservation{
		Status: provider.QuotaKnown, Resource: "gpu", Unit: "seconds", Limit: &limit, Used: &used, Remaining: &remaining,
		ObservedAt: time.Now().UTC(), Source: "offline-fixture", Precision: "lower_bound",
	}}, nil
}

type managedTestFixture struct {
	host     *Host
	services *managedServices
	adapter  *managedTestAdapter
	vault    *managedTestVault
	actor    auth.Principal
	config   ManagedServeConfig
}

func newManagedTestFixture(t *testing.T, withVault bool) *managedTestFixture {
	t.Helper()
	ctx := context.Background()
	h, err := Open(ctx, initialized(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if _, err := h.CreateWorkspace(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	secret, _, err := h.access.Issue(ctx, "app", []auth.Scope{auth.Read, auth.Write, auth.Manage, auth.Execute}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	actor, err := h.access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	python, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	config := ManagedServeConfig{PythonExecutable: python, MachineShape: "NvidiaTeslaT4", MaxRemoteWallSeconds: 300, MaxWorkers: 2}
	adapter := &managedTestAdapter{managedKaggleAdapter: &managedKaggleAdapter{config: config}}
	f := &managedTestFixture{host: h, adapter: adapter, actor: actor, config: config}
	var vault credentials.Vault
	if withVault {
		f.vault = &managedTestVault{entries: map[string][]byte{}}
		vault = f.vault
	}
	f.services, err = h.composeManaged(ctx, config, vault, adapter)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *managedTestFixture) connection(t *testing.T, key, token string) connections.Connection {
	t.Helper()
	op, err := f.services.connections.Submit(context.Background(), f.actor, "app", "", key,
		[]byte(fmt.Sprintf(`{"provider_type":"kaggle","label":"Fixture","credentials":{"api_token":%q}}`, token)))
	if err != nil {
		t.Fatal("accept connection", err)
	}
	if worked, err := f.services.connections.RunOnce(context.Background()); err != nil || !worked {
		t.Fatal("verify connection", worked, err)
	}
	connection, err := f.services.connections.Get(context.Background(), f.actor, "app", op.ConnectionID)
	if err != nil || connection.Selection == nil || connection.Authentication != "verified" {
		t.Fatal("verified connection unavailable", err)
	}
	return connection
}

func (f *managedTestFixture) binding(t *testing.T, c connections.Connection) provider.BindingSnapshot {
	t.Helper()
	profile, _, err := f.host.store.ReadProfile(context.Background(), c.Selection.Profile)
	if err != nil {
		t.Fatal(err)
	}
	return provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
}

func TestManagedCompositionStartsWithoutAccountCallsCredentialsOrGrants(t *testing.T) {
	f := newManagedTestFixture(t, true)
	if f.adapter.checks != 0 || f.vault.calls != 0 || f.services.dispatcher == nil || f.services.collector == nil {
		t.Fatal("composition performed provider/vault work")
	}
	if worked, err := f.services.dispatcher.RunOnce(context.Background(), "fixture_worker"); worked || err != nil {
		t.Fatal("empty startup attempted dispatch", err)
	}
	if worked, err := f.services.collector.RunOnce(context.Background()); worked || err != nil {
		t.Fatal("empty startup attempted collection", err)
	}
	if worked, err := f.services.connections.RunOnce(context.Background()); worked || err != nil {
		t.Fatal("empty startup invented connection work", err)
	}
	descriptors, err := f.services.connections.Descriptors(context.Background(), f.actor, "app")
	if err != nil || len(descriptors) != 1 || descriptors[0].CredentialStorage != "available" || f.adapter.checks != 0 || f.vault.calls != 0 {
		t.Fatal("descriptor read probed availability", err)
	}
	list, err := f.services.connections.List(context.Background(), f.actor, "app")
	if err != nil || len(list) != 0 || f.adapter.checks != 0 || f.vault.calls != 0 {
		t.Fatal("status read performed account work", err)
	}
}

func TestManagedWorkerPoolsStayAvailableWhileIdle(t *testing.T) {
	f := newManagedTestFixture(t, true)
	f.connection(t, "idle-account-test-key", "SYNTHETIC_FIRST")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- f.services.dispatcher.Run(ctx) }()
	go func() { results <- f.services.collector.Run(ctx) }()
	for i := 0; i < 2; i++ {
		if err := <-results; !errors.Is(err, context.DeadlineExceeded) {
			t.Error("idle worker pool exited", err)
		}
	}
}

func TestManagedClaimsRetryStaleClockWithoutWeakeningStoreGuard(t *testing.T) {
	f := newManagedTestFixture(t, true)
	ctx := context.Background()
	now := time.Now().UTC()
	stale := now.Add(-time.Second)
	if _, err := f.host.store.ClaimNextManaged(ctx, "fixture_worker", now); err != nil {
		t.Fatal(err)
	}
	dispatchRepo := managedDispatchRepository{f.host.store}
	collectionRepo := managedCollectionRepository{f.host.store}
	if result, err := dispatchRepo.ClaimNext(ctx, "fixture_worker", stale); err != nil || result.Claim != nil {
		t.Fatal("stale dispatch clock admitted work or stopped polling", err)
	}
	if result, err := dispatchRepo.ClaimRecovery(ctx, "fixture_worker", stale); err != nil || result != nil {
		t.Fatal("stale recovery clock admitted work or stopped polling", err)
	}
	if result, err := collectionRepo.ClaimCollection(ctx, stale, time.Minute, 2); err != nil || result != nil {
		t.Fatal("stale collection clock admitted work or stopped polling", err)
	}
	// The wrappers cannot move persisted time backwards or bypass its protection.
	for _, claim := range []func() error{
		func() error { _, err := f.host.store.ClaimNextManaged(ctx, "fixture_worker", stale); return err },
		func() error { _, err := f.host.store.ClaimRecoveryManaged(ctx, "fixture_worker", stale); return err },
		func() error { _, err := f.host.store.ClaimCollectionManaged(ctx, stale, time.Minute, 2); return err },
	} {
		if err := claim(); !errors.Is(err, scheduler.ErrClock) {
			t.Fatal("persisted monotonic guard changed", err)
		}
	}
	if _, err := f.host.store.ClaimNextManaged(ctx, "fixture_worker", now.Add(time.Second)); err != nil {
		t.Fatal("claims cannot resume once time catches up", err)
	}
}

func TestManagedUnsupportedVaultKeepsHTTPServicesAndDisablesWorkers(t *testing.T) {
	f := newManagedTestFixture(t, false)
	if f.services.connections == nil || f.services.authorizations == nil || f.services.dispatcher != nil || f.services.collector != nil {
		t.Fatal("unsupported vault enabled dispatch or hid management")
	}
	descriptors, err := f.services.connections.Descriptors(context.Background(), f.actor, "app")
	if err != nil || len(descriptors) != 1 || descriptors[0].CredentialStorage != "unsupported" {
		t.Fatal("unsupported storage falsely available", err)
	}
	_, err = f.services.connections.Submit(context.Background(), f.actor, "app", "", "unsupported-vault-key",
		[]byte(`{"provider_type":"kaggle","label":"Fixture","credentials":{"api_token":"SYNTHETIC_FIRST"}}`))
	if err != connections.ErrVault || f.adapter.checks != 0 {
		t.Fatal("unsupported vault fell back or contacted provider", err)
	}
}

func TestManagedResolverPreservesAccountsAliasesRotationAndFrozenMachine(t *testing.T) {
	f := newManagedTestFixture(t, true)
	one := f.connection(t, "first-connection-key", "SYNTHETIC_FIRST")
	two := f.connection(t, "same-account-key", "SYNTHETIC_SAME_ACCOUNT")
	other := f.connection(t, "other-account-key", "SYNTHETIC_OTHER_ACCOUNT")
	old := f.binding(t, one)
	if *one.AccountID != *two.AccountID || *one.AccountID == *other.AccountID {
		t.Fatal("credential count changed capacity identity")
	}
	// The next publication uses a new configured shape; the old profile must keep T4.
	f.adapter.config.MachineShape = "NvidiaTeslaP100"
	_, err := f.services.connections.Submit(context.Background(), f.actor, "app", one.ID, "replace-account-key",
		[]byte(fmt.Sprintf(`{"action":"replace_credential","expected_revision":%d,"credentials":{"api_token":"SYNTHETIC_REPLACEMENT"}}`, one.Revision)))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := f.services.connections.RunOnce(context.Background()); err != nil || !worked {
		t.Fatal("replacement failed", err)
	}
	updated, err := f.services.connections.Get(context.Background(), f.actor, "app", one.ID)
	if err != nil || updated.Selection == nil || updated.Selection.Profile == one.Selection.Profile {
		t.Fatal("replacement repointed existing alias", err)
	}
	current := f.binding(t, updated)
	resolver := &managedSnapshotResolver{ctx: context.Background(), read: f.host.store.ReadManagedBinding,
		make: func(binding connections.RuntimeBinding) (provider.Provider, error) {
			config := f.config
			config.MachineShape = "NvidiaTeslaP100"
			return f.host.managedProvider(config, f.services.connections, binding)
		}}
	checks, vaultCalls := f.adapter.checks, f.vault.calls
	for _, expected := range []provider.BindingSnapshot{old, f.binding(t, two), f.binding(t, other), current, old} {
		p, err := resolver.Resolve(expected)
		if err != nil {
			t.Fatal("frozen revision missing", err)
		}
		wrapped, ok := p.(*managedKaggleProvider)
		if !ok || wrapped.outer != expected || wrapped.inner.AccountScope == expected.AccountScope {
			t.Fatal("canonical account confused with public opaque scope")
		}
		shape := "NvidiaTeslaT4"
		if expected == current {
			shape = "NvidiaTeslaP100"
		}
		found := false
		for _, capability := range p.Describe().Capabilities {
			if capability.Name == domain.CapabilityGPU {
				found = strings.Contains(strings.Join(capability.Conditions, " "), shape)
			}
		}
		if !found {
			t.Fatal("restart reinterpreted frozen machine shape")
		}
		wrong := expected
		wrong.AccountScope = "wrong_account"
		if err := wrapped.VerifyBinding(context.Background(), wrong); err == nil {
			t.Fatal("wrong outer snapshot reached canonical account")
		}
	}
	if f.adapter.checks != checks || f.vault.calls != vaultCalls {
		t.Fatal("lazy construction performed account checks")
	}
	if err := f.services.connections.WithCredential(context.Background(), ports.CredentialRef(old.CredentialRef), func(raw []byte) error {
		if string(raw) != "SYNTHETIC_REPLACEMENT" {
			t.Fatal("old job did not retain stable same-account slot")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Disable changes admission only, not recovery of any saved binding.
	_, err = f.services.connections.Submit(context.Background(), f.actor, "app", one.ID, "disable-account-key",
		[]byte(fmt.Sprintf(`{"action":"disable","expected_revision":%d}`, updated.Revision)))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := f.services.connections.RunOnce(context.Background()); err != nil || !worked {
		t.Fatal("disable failed", err)
	}
	if _, err := resolver.Resolve(old); err != nil {
		t.Fatal("disable removed old recovery binding", err)
	}
}

func TestManagedResolverRejectsSubstitutedSnapshotsAndUnfrozenPolicy(t *testing.T) {
	f := newManagedTestFixture(t, true)
	one := f.connection(t, "binding-policy-key", "SYNTHETIC_FIRST")
	expected := f.binding(t, one)
	binding, err := f.host.store.ReadManagedBinding(context.Background(), expected)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{nil, []byte(`{}`), []byte(`{"schema_version":1,"machine_shape":"paid-gpu"}`)} {
		bad := binding
		bad.RuntimeConfig = raw
		if _, err := f.host.managedProvider(f.config, f.services.connections, bad); err != ErrRequest {
			t.Fatal("missing or unreviewed policy defaulted", err)
		}
	}
	made := false
	resolver := &managedSnapshotResolver{ctx: context.Background(), read: func(context.Context, provider.BindingSnapshot) (connections.RuntimeBinding, error) {
		binding.Profile.Binding.ConfigurationRevision = "changed"
		return binding, nil
	}, make: func(connections.RuntimeBinding) (provider.Provider, error) { made = true; return nil, nil }}
	if _, err := resolver.Resolve(expected); err != ErrState || made {
		t.Fatal("resolver substituted a current alias", err)
	}
}

type connectionRunnerFunc func(context.Context) (bool, error)

func (f connectionRunnerFunc) RunOnce(ctx context.Context) (bool, error) { return f(ctx) }

func TestManagedConnectionLoopKeepsDeniedCleanupRetryableAndJoinsCancellation(t *testing.T) {
	for _, firstErr := range []error{nil, connections.ErrVault} {
		ctx, cancel := context.WithCancel(context.Background())
		called, done := make(chan struct{}), make(chan error, 1)
		go func() {
			done <- runConnectionOperations(ctx, connectionRunnerFunc(func(context.Context) (bool, error) {
				close(called)
				return false, firstErr
			}))
		}()
		<-called
		select {
		case err := <-done:
			cancel()
			t.Fatal("operation loop stopped unrelated services", err)
		case <-time.After(20 * time.Millisecond):
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal("operation loop failed to join", err)
		}
	}
	if err := runConnectionOperations(context.Background(), connectionRunnerFunc(func(context.Context) (bool, error) {
		return false, errors.New("SYNTHETIC_PRIVATE_DIAGNOSTIC")
	})); err != ErrState {
		t.Fatal("raw operation worker error escaped", err)
	}
}
