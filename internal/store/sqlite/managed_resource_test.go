package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type resourceStateAdapter struct {
	limitedStateAdapter
	changed bool
}

func (a *resourceStateAdapter) Verify(ctx context.Context, secret []byte) (connections.Verification, error) {
	v, err := a.stateAdapter.Verify(ctx, secret)
	if err != nil {
		return v, err
	}
	native := strings.Repeat("a", 64)
	switch string(secret) {
	case "SYNTHETIC_LAN", "SYNTHETIC_LAN_KEY":
		v.CanonicalAccount = "lan_account"
	case "SYNTHETIC_VPN":
		v.CanonicalAccount = "vpn_account"
	case "SYNTHETIC_DNS":
		v.CanonicalAccount = "dns_account"
	case "SYNTHETIC_OTHER_HOST":
		v.CanonicalAccount, native = "other_host_account", strings.Repeat("b", 64)
	default:
		return connections.Verification{}, connections.ErrCredentialRejected
	}
	if a.changed {
		native = strings.Repeat("c", 64)
	}
	v.Resource = &connections.ResourceIdentity{Kind: "machine", ID: "resource_" + native}
	return v, nil
}

func resourceFixture(t *testing.T) (*managedStateFixture, *resourceStateAdapter) {
	t.Helper()
	f := newManagedStateFixture(t)
	a := &resourceStateAdapter{limitedStateAdapter: limitedStateAdapter{f.discovery}}
	bindResourceFixture(t, f, a)
	settings := scheduler.DefaultSettings()
	settings.MaxWorkers, settings.MaxActivePerAccount = 4, 64
	requireNoError(t, f.s.ConfigureScheduler(context.Background(), settings))
	return f, a
}

func bindResourceFixture(t *testing.T, f *managedStateFixture, a *resourceStateAdapter) {
	t.Helper()
	var err error
	f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{a})
	requireNoError(t, err)
}

func configureResourceLimit(t *testing.T, f *managedStateFixture, c connections.Connection, limit int) connections.Connection {
	t.Helper()
	raw := []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":1800,"max_active_attempts":%d}}`, c.Revision, limit))
	op, err := f.service.Submit(context.Background(), f.principals[c.Workspace], c.Workspace, c.ID, fmt.Sprintf("limit-%s-%d", c.ID, c.Revision), raw)
	if err != nil || op.Status != "succeeded" {
		t.Fatal(op, err)
	}
	return f.get(t, c.Workspace, c.ID)
}

func grantResourceJob(t *testing.T, f *managedStateFixture, c connections.Connection, key string) scheduler.Identity {
	t.Helper()
	receipt, _ := f.admit(t, c.Workspace, c, key)
	permits, err := executionauth.New(f.access, f.s, f.clock)
	requireNoError(t, err)
	if _, err := permits.Authorize(context.Background(), f.principals[c.Workspace], c.Workspace, receipt.JobID, key+"-grant", authorizationRequest(receipt.AttemptID, 10)); err != nil {
		t.Fatal(err)
	}
	return scheduler.Identity{WorkspaceID: c.Workspace, JobID: receipt.JobID, AttemptID: receipt.AttemptID}
}

func TestManagedResourceSharesLANVPNAndWorkspaceCapacityAcrossRestart(t *testing.T) {
	f, adapter := resourceFixture(t)
	ctx := context.Background()
	lan := configureResourceLimit(t, f, f.create(t, "a", "lan-connection", "SYNTHETIC_LAN"), 2)
	vpn := configureResourceLimit(t, f, f.create(t, "b", "vpn-connection", "SYNTHETIC_VPN"), 2)
	if *lan.AccountID == *vpn.AccountID {
		t.Fatal("test paths did not have distinct authenticated accounts")
	}
	first := grantResourceJob(t, f, lan, "lan-job-key")
	second := grantResourceJob(t, f, vpn, "vpn-job-key")
	third := grantResourceJob(t, f, vpn, "third-job")
	for _, want := range []scheduler.Identity{first, second} {
		claim, err := f.s.ClaimNext(ctx, "worker")
		if err != nil || claim.Claim == nil || claim.Claim.Identity != want {
			t.Fatal("two configured slots not available", claim, err)
		}
		state := domain.InitialAttemptState()
		state.Orchestration, state.Execution, state.RemoteActivity = domain.OrchestrationRunning, domain.ExecutionRunning, domain.RemoteActivityActive
		schedulerSetState(t, f.s, want, state)
	}
	assertWaiting := func() {
		t.Helper()
		result, err := f.s.ClaimNext(ctx, "third")
		if err != nil || result.Claim != nil || len(result.View.Heads) != 1 || result.View.Heads[0].Reason != scheduler.ResourceLimit || result.View.ResourceReservations["resource_"+strings.Repeat("a", 64)] != 2 {
			t.Fatal("shared machine admitted a third job", result, err)
		}
	}
	assertWaiting()
	f.action(t, "a", lan, "password-to-key", "replace_credential", "SYNTHETIC_LAN_KEY")
	f.run(t)
	f.restartManaged(t)
	bindResourceFixture(t, f, adapter)
	f.clock.advance(time.Minute)
	assertWaiting()
	// A retained alias at its serial default lowers future capacity but cannot
	// cancel either accepted execution or erase their reservations.
	dns := f.create(t, "a", "dns-alias", "SYNTHETIC_DNS")
	assertWaiting()
	configureResourceLimit(t, f, dns, 2)
	state := domain.InitialAttemptState()
	state.Orchestration, state.Execution, state.RemoteActivity, state.Result = domain.OrchestrationCollecting, domain.ExecutionSucceeded, domain.RemoteActivityInactive, domain.ResultCollecting
	schedulerSetState(t, f.s, first, state)
	result, err := f.s.ClaimNext(ctx, "third-after-release")
	if err != nil || result.Claim == nil || result.Claim.Identity != third {
		t.Fatal("confirmed inactivity did not release capacity before collection", result, err)
	}
}

func TestManagedResourceKeepsOtherMachinesAndFrozenLowerLimits(t *testing.T) {
	f, _ := resourceFixture(t)
	ctx := context.Background()
	lan := f.create(t, "a", "old-lan-key", "SYNTHETIC_LAN")
	first := grantResourceJob(t, f, lan, "old-serial-job")
	if result, err := f.s.ClaimNext(ctx, "old-worker"); err != nil || result.Claim == nil {
		t.Fatal(result, err)
	}
	lan = configureResourceLimit(t, f, lan, 2)
	vpn := configureResourceLimit(t, f, f.create(t, "b", "new-vpn-key", "SYNTHETIC_VPN"), 2)
	grantResourceJob(t, f, vpn, "new-parallel-job")
	result, err := f.s.ClaimNext(ctx, "new-worker")
	if err != nil || result.Claim != nil || result.View.Heads[0].Reason != scheduler.ResourceLimit {
		t.Fatal("old active serial policy was widened", result, err)
	}
	other := configureResourceLimit(t, f, f.create(t, "a", "unrelated-host", "SYNTHETIC_OTHER_HOST"), 2)
	independent := grantResourceJob(t, f, other, "unrelated-job")
	result, err = f.s.ClaimNext(ctx, "other-worker")
	if err != nil || result.Claim == nil || result.Claim.Identity != independent {
		t.Fatal("different machine was grouped into the old host", result, err)
	}
	retained, err := f.s.ReadJob(ctx, "a", f.principals["a"].TokenID(), first.JobID)
	if err != nil || retained.Profile.MaxActiveAttempts != 1 || retained.Profile.ResourceScope != "resource_"+strings.Repeat("a", 64) {
		t.Fatal("new policy rewrote old admission", err)
	}
}

func TestManagedResourceRecheckPreservesAccountAssociation(t *testing.T) {
	f, adapter := resourceFixture(t)
	ctx := context.Background()
	lan := f.create(t, "a", "bound-lan", "SYNTHETIC_LAN")
	job := grantResourceJob(t, f, lan, "bound-job")
	before, err := f.s.ReadJob(ctx, "a", f.principals["a"].TokenID(), job.JobID)
	requireNoError(t, err)
	adapter.changed = true
	op := f.action(t, "a", lan, "changed-native-identity", "check", "")
	f.run(t)
	result, err := f.service.Operation(ctx, f.principals["a"], "a", op.ID)
	if err != nil || result.Status != "failed" || result.Problem == nil || *result.Problem != "resource_changed" {
		t.Fatal("recheck silently moved the connection", result, err)
	}
	var scope string
	if err := f.s.db.QueryRow(`SELECT resource_scope FROM managed_resource_accounts WHERE account_scope=?`, *lan.AccountID).Scan(&scope); err != nil || scope != before.Profile.ResourceScope {
		t.Fatal("identity mismatch replaced the reservation association", scope, err)
	}
}
