package dispatch

import (
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestExpiryCannotManufactureRemoteInactivity(t *testing.T) {
	for _, phase := range []Phase{Local, Staging, Ready, Submitting, Submitted} {
		t.Run(string(phase), func(t *testing.T) {
			w, p, op := fixtureWork(t)
			j, s := w.Journal, w.Job.Attempt.State
			if phase != Local {
				j, s = apply(t, j, s, Action{Kind: BeginPreparation, Plan: &p, PreparationID: op})
			}
			if phase != Local && phase != Staging {
				prepared := provider.Prepared{Identity: p.Job.Identity, PreparationID: op, PlanSHA256: p.Digest(), Resource: "fixture-resource", Ready: true, Private: true}
				j, s = apply(t, j, s, Action{Kind: PreparationSeen, Prepared: &prepared})
			}
			if phase == Submitting || phase == Submitted {
				j, s = apply(t, j, s, Action{Kind: BeginSubmission})
			}
			if phase == Submitted {
				out := provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: &provider.RemoteReference{Identity: p.Job.Identity, Resource: "fixture", Version: "1"}}
				j, s = apply(t, j, s, Action{Kind: SubmissionSeen, Submission: &out})
			}
			next, state, _, err := Apply(j, s, Action{Kind: ExpireDispatch}, time.Now())
			if j.SubmitStarted {
				if err == nil {
					t.Fatal("local deadline claimed possible execution was inactive")
				}
				return
			}
			if err != nil || next.Phase != Prevented || state.Orchestration != domain.OrchestrationTimedOut || state.Execution != domain.ExecutionNotSubmitted || !state.DeadlineExceeded || state.Cancellation != domain.CancellationNotRequested || next.Problem.Code != domain.CodeRemoteTimeout {
				t.Fatal("expiry lost non-execution evidence", state, err)
			}
			if _, _, _, err = Apply(next, state, Action{Kind: BeginSubmission}, time.Now()); err == nil {
				t.Fatal("expired permit restarted")
			}
		})
	}
}

func TestDeadlineIsCapabilityGatedBeforePreparation(t *testing.T) {
	w, p, _ := fixtureWork(t)
	raw := strings.Replace(string(w.Job.Request.Canonical()), `"timeouts":{`, `"timeouts":{"execution_deadline":"2026-10-12T16:00:00Z",`, 1)
	var err error
	w.Job.Request, err = admission.Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := ResolveJob(w, *p.Job.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	d := provider.Descriptor{Type: "fixture", InstanceID: job.Identity.InstanceID, Version: "1"}
	for _, cap := range job.Required {
		if cap != domain.CapabilityAbsoluteDeadline {
			d.Capabilities = append(d.Capabilities, domain.CapabilityStatus{Name: cap, Support: domain.CapabilitySupportSupported, Evidence: domain.EvidenceImplementedOffline})
		}
	}
	if err = ValidatedPlan(job, provider.Plan{Job: job}, d); err == nil {
		t.Fatal("provider without absolute deadline support accepted the job")
	}
}
