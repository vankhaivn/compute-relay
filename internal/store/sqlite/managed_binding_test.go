package sqlite

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func managedBindingFixture(t *testing.T) (*dispatchFixture, provider.BindingSnapshot) {
	t.Helper()
	f := newDispatchFixture(t, fake.DefaultScenario())
	f.profile.Binding = connections.ProfileBinding("con_fixture", 1)
	f.profile.AccountScope = connections.AccountScope("fixture", "fixture_account")
	f.profile.CredentialRef = "vault:con_fixture"
	seedAuthorizationConnection(t, f, "a", "con_fixture")
	if err := f.s.PutProfile(context.Background(), f.profile, false); err != nil {
		t.Fatal(err)
	}
	return f, provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
}

func TestManagedBindingReadPreservesDisabledAndObsoleteFrozenProfiles(t *testing.T) {
	f, original := managedBindingFixture(t)
	ctx := context.Background()
	newProfile := f.profile
	newProfile.Binding = connections.ProfileBinding("con_fixture", 2)
	newProfile.MaxRemoteWallSeconds = 10
	if err := f.s.PutProfile(ctx, newProfile, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.db.Exec(`UPDATE managed_connections SET new_work='disabled',current_profile=?,authentication='pending' WHERE connection_id='con_fixture'`, newProfile.Binding.Profile); err != nil {
		t.Fatal(err)
	}
	got, err := f.s.ReadManagedBinding(ctx, original)
	if err != nil || got.ProviderType != "fixture" || got.CanonicalAccount != "fixture_account" || got.Profile != f.profile {
		t.Fatal("old recovery binding was replaced by current alias", err)
	}
	raw, err := json.Marshal(got)
	if err != nil || string(raw) != "{}" || strings.Contains(string(raw), "fixture_account") {
		t.Fatal("private runtime binding is publicly serializable")
	}
	// Rotation changes only the stable slot's active generation, not this profile.
	if _, err := f.s.db.Exec(`UPDATE managed_connections SET active_credential_key='replacement_key' WHERE connection_id='con_fixture'`); err != nil {
		t.Fatal(err)
	}
	rotated, err := f.s.ReadManagedBinding(ctx, original)
	key, keyErr := f.s.ResolveConnectionCredential(ctx, "con_fixture")
	if err != nil || keyErr != nil || !reflect.DeepEqual(rotated, got) || key != "replacement_key" {
		t.Fatal("rotation retargeted frozen work", err, keyErr)
	}
}

func TestManagedBindingReadRejectsChangedSnapshotMissingOwnerAndRemovedConnection(t *testing.T) {
	f, original := managedBindingFixture(t)
	for _, mutate := range []func(*provider.BindingSnapshot){
		func(s *provider.BindingSnapshot) { s.Binding.Profile = "other_profile" },
		func(s *provider.BindingSnapshot) { s.Binding.ConfigurationRevision = "r9" },
		func(s *provider.BindingSnapshot) { s.Binding.ProviderInstanceID = "con_other" },
		func(s *provider.BindingSnapshot) { s.AccountScope = "account_other" },
		func(s *provider.BindingSnapshot) { s.CredentialRef = "vault:con_other" },
		func(s *provider.BindingSnapshot) { s.CredentialRef = "env:AMBIENT" },
	} {
		bad := original
		mutate(&bad)
		if got, err := f.s.ReadManagedBinding(context.Background(), bad); err == nil || got.CanonicalAccount != "" {
			t.Fatal("changed snapshot resolved")
		}
	}
	if _, err := f.s.db.Exec(`UPDATE managed_connections SET canonical_account='other_account' WHERE connection_id='con_fixture'`); err == nil {
		t.Fatal("canonical account changed underneath the frozen binding")
	}
	for _, update := range []string{
		`UPDATE managed_connections SET active_credential_key='' WHERE connection_id='con_fixture'`,
		`UPDATE managed_connections SET active_credential_key='fixture_key',new_work='removed' WHERE connection_id='con_fixture'`,
		`DELETE FROM managed_connections WHERE connection_id='con_fixture'`,
	} {
		if _, err := f.s.db.Exec(update); err != nil {
			t.Fatal(err)
		}
		if got, err := f.s.ReadManagedBinding(context.Background(), original); err == nil || got.CanonicalAccount != "" {
			t.Fatal("unowned or changed canonical account resolved")
		}
	}
}
