package sqlite

import (
	"context"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestSchedulingStatusSkipsTerminalQueueHead(t *testing.T) {
	ctx := context.Background()
	f := managedFixture(t, fake.DefaultScenario())
	first := f.seed(t, "a", 1, false)
	state := f.state(t, first)
	state.Orchestration = domain.OrchestrationFailed
	schedulerSetState(t, f.s, first, state)
	id := f.seed(t, "a", 2, false)
	grantAuthorization(t, f, id)
	_, actor := authorizationService(t, f, "a", auth.Read)
	record, err := f.s.ReadJob(ctx, "a", actor.TokenID(), id.JobID)
	if err != nil || record.Scheduling == nil || record.Scheduling.Reason != string(scheduler.Eligible) {
		t.Fatal("terminal queue head hid eligibility", record.Scheduling, err)
	}
	claim, err := f.s.ClaimNext(ctx, "worker")
	if err != nil || claim.Claim == nil || claim.Claim.Identity != id {
		t.Fatal("status did not match actual claim", claim, err)
	}
}
