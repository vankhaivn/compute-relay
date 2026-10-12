package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type limitedStateAdapter struct{ *stateAdapter }

func (a limitedStateAdapter) Profile(binding domain.ProviderBinding, account string) admission.Profile {
	p := a.stateAdapter.Profile(binding, account)
	p.CostClass, p.MaxActiveAttempts, p.MaxProcesses = admission.CostOperatorManaged, 1, 4096
	return p
}
func (limitedStateAdapter) ValidateConfiguration(c connections.Configuration) error {
	return c.Validate()
}

func TestManagedLimitsFreezeAndShareCapacityAcrossWorkspaces(t *testing.T) {
	f := newManagedStateFixture(t)
	ctx := context.Background()
	var err error
	f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{limitedStateAdapter{f.discovery}}, f.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	settings := scheduler.DefaultSettings()
	settings.MaxWorkers, settings.MaxActivePerAccount = 4, 64
	if err = f.s.ConfigureScheduler(ctx, settings); err != nil {
		t.Fatal(err)
	}
	first := f.create(t, "a", "limited-first", managedStateSecret)
	alias := f.create(t, "b", "limited-alias", managedStateSecret)
	if first.Selection.MaxActiveAttempts != 1 || first.Selection.MaxProcesses != 4096 || *first.AccountID != *alias.AccountID {
		t.Fatal("wrong defaults or account grouping")
	}
	frozen, _, err := f.s.ReadProfile(ctx, first.Selection.Profile)
	if err != nil {
		t.Fatal(err)
	}
	configure := func(w domain.WorkspaceID, c connections.Connection, key string, active, processes int) connections.Connection {
		t.Helper()
		body := []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":1800,"max_active_attempts":%d,"max_processes":%d}}`, c.Revision, active, processes))
		op, err := f.service.Submit(ctx, f.principals[w], w, c.ID, key, body)
		if err != nil || op.Status != "succeeded" {
			t.Fatal(op, err)
		}
		return f.get(t, w, c.ID)
	}
	first = configure("a", first, "raise-first", 2, 256)
	var policy scheduler.AccountPolicy
	readPolicy := func() int {
		t.Helper()
		var raw string
		if err := f.s.db.QueryRow(`SELECT policy FROM scheduler_accounts WHERE account_scope=?`, *first.AccountID).Scan(&raw); err != nil || json.Unmarshal([]byte(raw), &policy) != nil {
			t.Fatal("read policy", err)
		}
		return policy.MaxActive
	}
	if readPolicy() != 1 {
		t.Fatal("an alias multiplied the shared host capacity")
	}
	alias = configure("b", alias, "raise-alias", 2, 512)
	if readPolicy() != 2 {
		t.Fatal("shared capacity was not raised")
	}
	beforeCalls := f.discovery.count()
	configureConnection(t, f, first, "preserve-omitted-limits", 7200)
	first = f.get(t, "a", first.ID)
	if first.Configuration.MaxActiveAttempts != 2 || first.Selection.MaxProcesses != 256 || f.discovery.count() != beforeCalls {
		t.Fatal("wall-only update lost limits or contacted provider")
	}
	// Load the exact immutable profile directly, independent of its current alias.
	var raw string
	if err := f.s.db.QueryRow(`SELECT snapshot FROM profile_revisions WHERE profile=? AND revision=?`, frozen.Binding.Profile, frozen.Binding.ConfigurationRevision).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var retained admission.Profile
	if json.Unmarshal([]byte(raw), &retained) != nil || retained != frozen || retained.MaxProcesses != 4096 || retained.MaxActiveAttempts != 1 {
		t.Fatal("configuration rewrote the old profile")
	}
	permits, err := executionauth.New(f.access, f.s, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range []connections.Connection{first, alias, first} {
		w := c.Workspace
		receipt, _ := f.admit(t, w, c, fmt.Sprintf("limited-job-%d", i))
		if _, err := permits.Authorize(ctx, f.principals[w], w, receipt.JobID, fmt.Sprintf("limited-grant-%d", i), authorizationRequest(receipt.AttemptID, 10)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 2 {
		result, err := f.s.ClaimNextManaged(ctx, fmt.Sprintf("limited-worker-%d", i), f.clock.Now())
		if err != nil || result.Claim == nil {
			t.Fatalf("slot %d not available: %+v %v", i, result.View, err)
		}
	}
	result, err := f.s.ClaimNextManaged(ctx, "limited-third", f.clock.Now())
	if err != nil || result.Claim != nil || result.View.AccountReservations[*first.AccountID] != 2 {
		t.Fatal("shared host allowed a third attempt", result, err)
	}
}
