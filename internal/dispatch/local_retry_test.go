package dispatch

import (
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"testing"
	"time"
)

func TestLocalTransientFaultWaitsForTimeBudget(t *testing.T) {
	for _, code := range []domain.ErrorCode{domain.CodeProviderUnreachable, domain.CodeProviderStateUnknown, domain.CodeInputFetchFailed} {
		t.Run(string(code), func(t *testing.T) {
			w, _, _ := fixtureWork(t)
			j, state := w.Journal, w.Job.Attempt.State
			start := time.Now().UTC()
			for n := 0; n < MaxFailures+3; n++ {
				var err error
				j, state, _, err = Apply(j, state, Action{Kind: Fault, Code: code}, start.Add(time.Duration(n)*time.Second))
				if err != nil || j.Phase != Local || state.Orchestration != domain.OrchestrationQueued || !j.Problem.SafeOperationRetry || j.Problem.RecommendedAction != domain.RecommendedActionWait {
					t.Fatal("short fault exhausted queue", j, state, err)
				}
				if delay := nextPassDelay(time.Second, j); delay < 15*time.Second || delay > 2*time.Minute {
					t.Fatal("unbounded cadence", delay)
				}
			}
			j, state, _, err := Apply(j, state, Action{Kind: Fault, Code: code}, start.Add(LocalFailureBudget))
			if err != nil || j.Phase != Failed || state.Orchestration != domain.OrchestrationFailed || j.Problem.RecommendedAction == domain.RecommendedActionReconcile {
				t.Fatal("budget expiry did not end truthfully", j, state, err)
			}
		})
	}
}

func TestAbsenceEvidenceMustMatchFrozenRemoteAndBeFresh(t *testing.T) {
	j, state := readyFixture(t)
	j, state = apply(t, j, state, Action{Kind: BeginSubmission})
	remote := provider.RemoteReference{Identity: j.Plan.Job.Identity, Resource: "exec", Version: "v1"}
	j, state = apply(t, j, state, Action{Kind: SubmissionSeen, Submission: &provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: &remote}})
	now := time.Now().UTC()
	wrong := provider.RemoteAbsence{Remote: remote, ObservedAt: now}
	wrong.Remote.Version = "different"
	if _, _, _, err := Apply(j, state, Action{Kind: RemoteAbsenceSeen, Absence: &wrong}, now); err == nil {
		t.Fatal("foreign absence released original capacity")
	}
	future := provider.RemoteAbsence{Remote: remote, ObservedAt: now.Add(time.Second)}
	if _, _, _, err := Apply(j, state, Action{Kind: RemoteAbsenceSeen, Absence: &future}, now); err == nil {
		t.Fatal("future absence accepted")
	}
}
