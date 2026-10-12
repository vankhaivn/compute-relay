package sqlite

import (
	"context"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestSchedulingStatusUsesManagedQueueWithLegacyStandaloneHead(t *testing.T) {
	ctx := context.Background()
	f := newDispatchFixture(t, fake.DefaultScenario())
	f.seed(t, "a", 1, false)
	f.profile.Binding.ConfigurationRevision = "managed"
	f.profile.Binding.ProviderInstanceID = "con_fixture"
	f.profile.CredentialRef = "vault:con_fixture"
	seedAuthorizationConnection(t, f, "a", "con_fixture")
	if err := f.s.PutProfile(ctx, f.profile, true); err != nil {
		t.Fatal(err)
	}
	id := f.seed(t, "a", 2, false)
	grantAuthorization(t, f, id)
	_, actor := authorizationService(t, f, "a", auth.Read)
	record, err := f.s.ReadJob(ctx, "a", actor.TokenID(), id.JobID)
	if err != nil || record.Scheduling == nil || record.Scheduling.Reason != string(scheduler.Eligible) {
		t.Fatal("standalone queue head hid managed eligibility", record.Scheduling, err)
	}
	claim, err := f.s.ClaimNextManaged(ctx, "managed-worker", f.clock.Now())
	if err != nil || claim.Claim == nil || claim.Claim.Identity != id {
		t.Fatal("status did not match actual managed claim", claim, err)
	}
}
