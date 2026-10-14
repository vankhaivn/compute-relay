package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type preResourceSSHAdapter struct{ legacySSHStateAdapter }

func (preResourceSSHAdapter) RuntimeConfig() []byte {
	return []byte(fmt.Sprintf(`{"schema_version":2,"image":%q,"architecture":"amd64","recipe_sha256":%q}`, "sha256:"+strings.Repeat("1", 64), sshhost.ImageRecipeDigest()))
}

type upgradedResourceSSHAdapter struct{ preResourceSSHAdapter }

func (a upgradedResourceSSHAdapter) Verify(ctx context.Context, raw []byte) (connections.Verification, error) {
	v, err := a.stateAdapter.Verify(ctx, raw)
	v.Resource = &connections.ResourceIdentity{Kind: "machine", ID: "resource_" + strings.Repeat("a", 64)}
	return v, err
}
func (upgradedResourceSSHAdapter) ValidateConfiguration(c connections.Configuration) error {
	return c.Validate()
}

func TestResourceMigrationPreservesOldUnknownReservationsAndQueuedBindings(t *testing.T) {
	for _, version := range []int{20, 21} {
		t.Run(fmt.Sprintf("schema_%d", version), func(t *testing.T) {
			testResourceMigrationPreservesOldReservations(t, version)
		})
	}
}

func testResourceMigrationPreservesOldReservations(t *testing.T, version int) {
	t.Helper()
	f := newManagedStateFixture(t)
	ctx := context.Background()
	legacy := preResourceSSHAdapter{legacySSHStateAdapter{f.discovery}}
	var err error
	f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{legacy})
	requireNoError(t, err)
	settings := scheduler.DefaultSettings()
	settings.MaxWorkers, settings.MaxActivePerAccount = 4, 64
	requireNoError(t, f.s.ConfigureScheduler(ctx, settings))
	create := func(w domain.WorkspaceID, key, secret string) connections.Connection {
		t.Helper()
		body := strings.Replace(string(managedCreateBody(secret)), `"fixture"`, `"ssh"`, 1)
		op, err := f.service.Submit(ctx, f.principals[w], w, "", key, []byte(body))
		requireNoError(t, err)
		f.run(t)
		return f.get(t, w, op.ConnectionID)
	}
	lan := create("a", "legacy-lan-create", managedStateSecret)
	vpn := create("b", "legacy-vpn-create", managedStateOther)
	admit := func(c connections.Connection, key string) (scheduler.Identity, admission.Record) {
		t.Helper()
		jobs, err := admission.New(f.access, f.s, admission.DefaultLimits())
		requireNoError(t, err)
		raw := strings.Replace(dispatchJSON, `"profile":"fixture"`, `"profile":"`+c.Selection.Profile+`"`, 1)
		r, err := jobs.Submit(ctx, f.principals[c.Workspace], c.Workspace, key, []byte(raw))
		requireNoError(t, err)
		permits, err := executionauth.New(f.access, f.s, f.clock)
		requireNoError(t, err)
		if _, err := permits.Authorize(ctx, f.principals[c.Workspace], c.Workspace, r.JobID, key+"-grant", authorizationRequest(r.AttemptID, 10)); err != nil {
			t.Fatal(err)
		}
		record, err := jobs.Get(ctx, f.principals[c.Workspace], c.Workspace, r.JobID)
		requireNoError(t, err)
		return scheduler.Identity{WorkspaceID: c.Workspace, JobID: r.JobID, AttemptID: r.AttemptID}, record
	}
	first, original := admit(lan, "legacy-first-job")
	second, _ := admit(vpn, "legacy-second-job")
	for _, id := range []scheduler.Identity{first, second} {
		result, err := f.s.ClaimNext(ctx, "legacy-worker")
		if err != nil || result.Claim == nil || result.Claim.Identity != id {
			t.Fatal("legacy fixture did not reserve the expected attempt", result, err)
		}
		state := domain.InitialAttemptState()
		state.Orchestration, state.Execution, state.RemoteActivity = domain.OrchestrationReconciling, domain.ExecutionUnknown, domain.RemoteActivityPossible
		schedulerSetState(t, f.s, id, state)
	}
	queued, queuedOriginal := admit(lan, "legacy-queued-job")
	oldPath := copyFixtureAtSchema(t, f.s, version)
	requireNoError(t, f.s.Close())
	f.s, err = Open(ctx, oldPath, DefaultOptions())
	if err != nil {
		t.Fatal("upgrade", err)
	}
	f.root = oldPath
	f.bind(t)
	upgraded := upgradedResourceSSHAdapter{legacy}
	f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{upgraded})
	requireNoError(t, err)
	for _, c := range []connections.Connection{lan, vpn} {
		current := f.get(t, c.Workspace, c.ID)
		if current.Selection != nil || !current.CredentialPresent || current.Concurrency == nil || current.Concurrency.EffectiveLimit != nil || current.Concurrency.WaitReason == nil || *current.Concurrency.WaitReason != string(scheduler.ResourceUnverified) {
			t.Fatal("migration invented identity or lost credentials", current)
		}
		f.action(t, c.Workspace, current, "verify-resource-"+c.ID, "check", "")
		f.run(t)
		configureResourceLimit(t, f, f.get(t, c.Workspace, c.ID), 2)
	}
	f.clock.advance(time.Minute)
	result, err := f.s.ClaimNext(ctx, "after-upgrade")
	if err != nil || result.Claim != nil || len(result.View.Heads) != 1 || result.View.Heads[0].Reason != scheduler.ResourceUnresolved {
		t.Fatal("current metadata relocated historical unknown execution", result, err)
	}
	retained, err := f.s.ReadJob(ctx, "a", f.principals["a"].TokenID(), first.JobID)
	if err != nil || retained.Profile != original.Profile || retained.Job != original.Job {
		t.Fatal("migration changed accepted execution", err)
	}
	binding := provider.BindingSnapshot{Binding: retained.Profile.Binding, AccountScope: retained.Profile.AccountScope, CredentialRef: retained.Profile.CredentialRef}
	runtime, err := f.s.ReadManagedBinding(ctx, binding)
	if err != nil || string(runtime.RuntimeConfig) != string(legacy.RuntimeConfig()) || runtime.Resource == nil {
		t.Fatal("migration lost original recovery policy", runtime, err)
	}
	for _, id := range []scheduler.Identity{first, second} {
		state := domain.InitialAttemptState()
		state.Orchestration, state.Execution, state.RemoteActivity, state.Result = domain.OrchestrationCollecting, domain.ExecutionSucceeded, domain.RemoteActivityInactive, domain.ResultCollecting
		schedulerSetState(t, f.s, id, state)
	}
	result, err = f.s.ClaimNext(ctx, "legacy-queued-after-proof")
	if err != nil || result.Claim == nil || result.Claim.Identity != queued {
		t.Fatal("confirmed release did not unblock preserved queued work", result, err)
	}
	retained, err = f.s.ReadJob(ctx, "a", f.principals["a"].TokenID(), queued.JobID)
	if err != nil || retained.Profile != queuedOriginal.Profile || retained.Job != queuedOriginal.Job {
		t.Fatal("queued admission was rewritten", err)
	}
	snapshot, err := f.s.ClaimNext(ctx, "inspect-upgraded-reservation")
	if err != nil || snapshot.Claim != nil {
		t.Fatal("claimed attempt became schedulable again", snapshot, err)
	}
	view := snapshot.View
	if err != nil || view.UnresolvedResources["machine"] || view.ResourceReservations["resource_"+strings.Repeat("a", 64)] != 1 {
		t.Fatal("post-upgrade claim lost its durable verified association", view, err)
	}
}
