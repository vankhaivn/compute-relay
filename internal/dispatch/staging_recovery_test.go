package dispatch

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestReconcilePreSubmissionUsesOriginalStagingIntent(t *testing.T) {
	for _, phase := range []Phase{Staging, Ready, Attention} {
		t.Run(string(phase), func(t *testing.T) {
			w, plan, prep := fixtureWork(t)
			j, state := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: BeginPreparation, Plan: &plan, PreparationID: prep})
			if phase == Ready {
				j, state = readyFixture(t)
			} else if phase == Attention {
				for i := 0; i < MaxFailures; i++ {
					j, state = apply(t, j, state, Action{Kind: Fault, Code: domain.CodeStagingFailed})
				}
			}
			old := j
			j, state = apply(t, j, state, Action{Kind: RequestReconciliation})
			if j.Phase != Staging || j.SubmitStarted || j.Remote != nil || j.Failures != 0 || j.Problem != nil ||
				state.Orchestration != domain.OrchestrationReconciling || state.Execution != domain.ExecutionNotSubmitted ||
				j.PreparationID != old.PreparationID || !reflect.DeepEqual(j.Plan, old.Plan) || !reflect.DeepEqual(j.Prepared, old.Prepared) {
				t.Fatal("reconciliation changed the frozen intent or invented submission")
			}
			for _, kind := range []Kind{BeginPreparation, BeginSubmission} {
				if _, _, _, err := Apply(j, state, Action{Kind: kind, Plan: &plan, PreparationID: prep}, time.Now()); !errors.Is(err, ErrConflict) {
					t.Fatalf("reconciliation rearmed %s: %v", kind, err)
				}
			}
		})
	}
}

func TestReconcilePreSubmissionRejectsUnsafeStates(t *testing.T) {
	w, plan, prep := fixtureWork(t)
	base, state := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: BeginPreparation, Plan: &plan, PreparationID: prep})
	for _, code := range []domain.ErrorCode{domain.CodeRemoteIdentityMismatch, domain.CodePrivateStagingUnavailable} {
		j, s := apply(t, base, state, Action{Kind: Fault, Code: code})
		if _, _, _, err := Apply(j, s, Action{Kind: RequestReconciliation}, time.Now()); !errors.Is(err, ErrConflict) {
			t.Fatalf("permanent %s was rearmed: %v", code, err)
		}
	}
	for _, change := range []func(*domain.AttemptState){
		func(s *domain.AttemptState) { s.Cancellation = domain.CancellationRequested },
		func(s *domain.AttemptState) { s.RemoteActivity = domain.RemoteActivityUnknown },
		func(s *domain.AttemptState) { s.DeadlineExceeded = true },
		func(s *domain.AttemptState) { s.Orchestration = domain.OrchestrationQueued },
	} {
		s := state
		change(&s)
		if _, _, _, err := Apply(base, s, Action{Kind: RequestReconciliation}, time.Now()); !errors.Is(err, ErrConflict) {
			t.Fatalf("unsafe pre-submission state rearmed: %+v, %v", s, err)
		}
	}
	if _, _, _, err := Apply(w.Journal, w.Job.Attempt.State, Action{Kind: RequestReconciliation}, time.Now()); !errors.Is(err, ErrConflict) {
		t.Fatalf("local preparation was granted by reconcile: %v", err)
	}
}

func TestReconcileAmbiguousSubmissionStillCannotSubmitAgain(t *testing.T) {
	for _, knownRemote := range []bool{false, true} {
		j, state := readyFixture(t)
		j, state = apply(t, j, state, Action{Kind: BeginSubmission})
		if knownRemote {
			remote := provider.RemoteReference{Identity: j.Plan.Job.Identity, Resource: "execution", Version: "1"}
			j, state = apply(t, j, state, Action{Kind: SubmissionSeen, Submission: &provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: &remote}})
		}
		for i := 0; i < MaxFailures; i++ {
			j, state = apply(t, j, state, Action{Kind: Fault, Code: domain.CodeProviderSubmissionUnknown})
		}
		before := j
		j, state = apply(t, j, state, Action{Kind: RequestReconciliation})
		expected := Submitting
		if knownRemote {
			expected = Submitted
		}
		if j.Phase != expected || !j.SubmitStarted || !reflect.DeepEqual(j.Remote, before.Remote) || j.PreparationID != before.PreparationID || !reflect.DeepEqual(j.Plan, before.Plan) {
			t.Fatal("ambiguous submission was rearmed or changed identity")
		}
		if _, _, _, err := Apply(j, state, Action{Kind: BeginSubmission}, time.Now()); !errors.Is(err, ErrConflict) {
			t.Fatal("reconciliation permitted another submit", err)
		}
	}
}
