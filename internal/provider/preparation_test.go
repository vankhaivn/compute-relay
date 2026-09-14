package provider_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

type recoveryClock struct{}

func (recoveryClock) Now() time.Time { return time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC) }
func TestInputDigestMatchesRunnerCanonicalEncoding(t *testing.T) {
	s := provider.InputSnapshot{Bundle: domain.ObjectMetadata{ID: "code", WorkspaceID: "a", Bytes: 0, SHA256: provider.Digest(nil)}, Inputs: []provider.FrozenInput{
		{Name: "right", Target: "z/data.txt", Object: domain.ObjectMetadata{ID: "b", WorkspaceID: "a", Bytes: 3, SHA256: provider.Digest([]byte("abc"))}},
		{Name: "left", Target: "a&b.txt", Object: domain.ObjectMetadata{ID: "c", WorkspaceID: "a", Bytes: 0, SHA256: provider.Digest(nil)}},
	}}
	if s.Validate("a") != nil {
		t.Fatal("fixture invalid")
	}
	// Golden from the runner input_digest encoding (sorted-key ASCII JSON).
	if got := s.InputDigest(); got != "65d38f6e926e41f2ab41dcc03ee47f5b19f67ba972be277ab570ed76524a362c" {
		t.Fatalf("runner digest mismatch: %s", got)
	}
	s.Inputs = nil
	if s.InputDigest() != provider.Digest([]byte("[]")) {
		t.Fatal("empty input manifest is not []")
	}
}
func TestFrozenRegistryRetainsRevisionAndRejectsBindingMismatch(t *testing.T) {
	backend, err := fake.NewBackend(fake.DefaultScenario())
	if err != nil {
		t.Fatal(err)
	}
	one := provider.BindingSnapshot{Binding: domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "instance", ConfigurationRevision: "one"}, AccountScope: "account", CredentialRef: "env:FIXTURE"}
	p, err := fake.NewBound(backend, recoveryClock{}, one)
	if err != nil {
		t.Fatal(err)
	}
	r := provider.NewSnapshotRegistry()
	if err = r.Register(one, p); err != nil {
		t.Fatal(err)
	}
	if err = r.Register(one, p); err == nil {
		t.Fatal("revision overwritten")
	}
	two := one
	two.Binding.ConfigurationRevision = "two"
	if _, err = r.Resolve(two); err == nil {
		t.Fatal("mutable alias fallback")
	}
	if err = p.VerifyBinding(context.Background(), two); err == nil {
		t.Fatal("account/config mismatch accepted")
	}
	other := one
	other.AccountScope = "other"
	if err = p.VerifyBinding(context.Background(), other); err == nil {
		t.Fatal("account rotation silently accepted")
	}
	got, err := r.Resolve(one)
	if err != nil || got != p {
		t.Fatal("original recovery binding lost")
	}
}
func TestPreparationLookupDoesNotCreateResources(t *testing.T) {
	b, _ := fake.NewBackend(fake.DefaultScenario())
	p, _ := fake.New(b, recoveryClock{}, "instance")
	digest := provider.Digest(nil)
	job := provider.ResolvedJob{Identity: provider.Identity{InstallationID: "runtime", WorkspaceID: "a", JobID: "job", AttemptID: "attempt", InstanceID: "instance", IntentID: "intent", ResourceKey: "resource", Nonce: strings.Repeat("a", 32), BundleSHA256: digest, InputManifestSHA256: digest}, Binding: domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "instance", ConfigurationRevision: "one"}, Specification: []byte(`{}`), SpecificationSHA256: provider.Digest([]byte(`{}`)), Required: []domain.CapabilityName{domain.CapabilityBatchExecution}, WallSeconds: 10}
	plan, err := p.Validate(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	seen, err := p.ReconcilePreparation(context.Background(), plan, "prep")
	if err != nil || seen.Status != provider.ReconciliationNotFound || b.Stats().PrepareCalls != 0 {
		t.Fatal("lookup mutated provider")
	}
	prepared, err := p.Prepare(context.Background(), plan, "prep")
	if err != nil {
		t.Fatal(err)
	}
	seen, err = p.ReconcilePreparation(context.Background(), plan, "prep")
	if err != nil || seen.Prepared == nil || *seen.Prepared != prepared || b.Stats().PrepareCalls != 1 {
		t.Fatal("staging recovery repeated creation")
	}
	if !prepared.Private {
		t.Fatal("fixture did not establish private staging")
	}
}
