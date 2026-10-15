package dispatch

import (
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestHealthRefusalDoesNotCreateOrDiscardRemoteAuthority(t *testing.T) {
	w, _, _ := fixtureWork(t)
	j, state := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: RefuseNewWork})
	if j.Phase != Failed || state.Execution != domain.ExecutionNotSubmitted || state.Orchestration != domain.OrchestrationFailed || scheduler.HoldsAccount(state, false) || j.Problem == nil || j.Problem.Details[domain.ReasonDetail] != "host_unhealthy" || j.Problem.ComputeMayHaveStarted || j.Problem.SafeOperationRetry {
		t.Fatal("local refusal fabricated or retained compute authority")
	}
	j, state = readyFixture(t)
	oldPlan, oldPrepared := j.Plan.Digest(), *j.Prepared
	j, state = apply(t, j, state, Action{Kind: RefuseNewWork})
	if j.Phase != Ready || j.SubmitStarted || !scheduler.HoldsAccount(state, true) || j.Plan.Digest() != oldPlan || *j.Prepared != oldPrepared || state.Execution != domain.ExecutionNotSubmitted {
		t.Fatal("prepared refusal discarded ownership or started work")
	}
	j, state = apply(t, j, state, Action{Kind: BeginSubmission})
	if j.Problem != nil {
		t.Fatal("later authorized start retained stale refusal")
	}
	if _, _, _, err := Apply(j, state, Action{Kind: RefuseNewWork}, time.Now()); err == nil {
		t.Fatal("health refusal rewrote potentially submitted work")
	}
}
