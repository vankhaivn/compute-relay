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
				j, state = apply(t, j, state, Action{Kind: Fault, Code: domain.CodeStagingFailed})
				// A journal written before the observation grace was introduced.
				j.Phase, state.Orchestration = Attention, domain.OrchestrationNeedsAttention
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

func TestStagingFailureReasonSurvivesDeferredReconciliations(t *testing.T) {
	w, plan, prep := fixtureWork(t)
	j, state := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: BeginPreparation, Plan: &plan, PreparationID: prep})
	j, state = apply(t, j, state, Action{Kind: Fault, Code: domain.CodeStagingFailed, Reason: "shared_create_failed_http_500"})
	for i := 0; i < MaxFailures+2; i++ {
		j, state = apply(t, j, state, Action{Kind: Fault, Code: domain.CodeStagingNotReady})
	}
	if j.Phase != Staging || j.PreparationObservationStarted == nil || j.Problem.Code != domain.CodeStagingNotReady || j.Problem.PublicDetails()[domain.ReasonDetail] != "shared_create_failed_http_500" {
		t.Fatal("deferred reconciliation lost the failed step", j.Problem)
	}
	if _, _, _, err := Apply(w.Journal, w.Job.Attempt.State, Action{Kind: Fault, Code: domain.CodeStagingFailed, Reason: "Not closed"}, time.Now()); !errors.Is(err, ErrInvalid) {
		t.Fatal("an open reason was recorded", err)
	}
}

func TestEndUnconfirmedPreparationRejectsKnownOrPossibleExecution(t *testing.T) {
	w, plan, prep := fixtureWork(t)
	base, state := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: BeginPreparation, Plan: &plan, PreparationID: prep})
	for _, name := range []string{"local", "prepared", "submitted", "cancelled", "possible_activity", "terminal"} {
		t.Run(name, func(t *testing.T) {
			j, s := base, state
			switch name {
			case "local":
				j, s = w.Journal, w.Job.Attempt.State
			case "prepared":
				j, s = readyFixture(t)
			case "submitted":
				j, s = readyFixture(t)
				j, s = apply(t, j, s, Action{Kind: BeginSubmission})
			case "cancelled":
				s.Cancellation = domain.CancellationRequested
			case "possible_activity":
				s.RemoteActivity = domain.RemoteActivityPossible
			case "terminal":
				j, s = apply(t, j, s, Action{Kind: EndUnconfirmedPreparation})
			}
			if _, _, _, err := Apply(j, s, Action{Kind: EndUnconfirmedPreparation}, time.Now()); err == nil {
				t.Fatal("unsafe preparation terminated")
			}
		})
	}
}

func TestUnconfirmedPreparationKeepsFirstAndLatestCause(t *testing.T) {
	w, plan, prep := fixtureWork(t)
	j, state := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: BeginPreparation, Plan: &plan, PreparationID: prep})
	j, state = apply(t, j, state, Action{Kind: Fault, Code: domain.CodeStagingFailed, Reason: "staging_not_visible"})
	j, state = apply(t, j, state, Action{Kind: Fault, Code: domain.CodeStagingNotReady, Cause: "staging_lookup_failed_http_429"})
	if d := j.Problem.PublicDetails(); d[domain.ReasonDetail] != "staging_not_visible" || d[domain.CauseDetail] != "staging_lookup_failed_http_429" {
		t.Fatal("observation lost its failed step or read cause", d)
	}
	for _, kind := range []Kind{Fault, EndUnconfirmedPreparation} {
		if _, _, _, err := Apply(j, state, Action{Kind: kind, Code: domain.CodeStagingNotReady, Cause: "Not closed"}, time.Now()); !errors.Is(err, ErrInvalid) {
			t.Fatal("an open cause was recorded", kind, err)
		}
	}
	ended, _ := apply(t, j, state, Action{Kind: EndUnconfirmedPreparation})
	if d := ended.Problem.PublicDetails(); d[domain.ReasonDetail] != "staging_unconfirmed" || d[domain.FirstCauseDetail] != "staging_not_visible" || d[domain.CauseDetail] != "staging_lookup_failed_http_429" {
		t.Fatal("terminal staging lost its causes", d)
	}
	ended, _ = apply(t, j, state, Action{Kind: EndUnconfirmedPreparation, Cause: "staging_absent_http_403"})
	if d := ended.Problem.PublicDetails(); d[domain.ReasonDetail] != "staging_unconfirmed" || d[domain.CauseDetail] != "staging_absent_http_403" {
		t.Fatal("terminal staging ignored the latest read cause", d)
	}
}
