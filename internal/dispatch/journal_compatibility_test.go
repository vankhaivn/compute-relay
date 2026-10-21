package dispatch

import (
	"bytes"
	"encoding/json"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"testing"
	"time"
)

// This is the strict journal field set used before transient-recovery metadata.
type legacyJournalShape struct {
	Version                       int64                        `json:"version"`
	Phase                         Phase                        `json:"phase"`
	Plan                          *provider.Plan               `json:"plan,omitempty"`
	PreparationID                 domain.OperationID           `json:"preparation_id,omitempty"`
	PreparationObservationStarted *time.Time                   `json:"preparation_observation_started,omitempty"`
	Prepared                      *provider.Prepared           `json:"prepared,omitempty"`
	SubmitStarted                 bool                         `json:"submit_started"`
	StartAuthorization            *provider.StartAuthorization `json:"start_authorization,omitempty"`
	Remote                        *provider.RemoteReference    `json:"remote,omitempty"`
	Observation                   *provider.Observation        `json:"observation,omitempty"`
	Problem                       *domain.Problem              `json:"problem,omitempty"`
	Failures                      int                          `json:"failures"`
}

func TestReliabilityJournalMetadataKeepsStrictLegacyShape(t *testing.T) {
	w, _, _ := fixtureWork(t)
	local, state := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: Fault, Code: domain.CodeProviderUnreachable})
	ready, s := readyFixture(t)
	ready, s = apply(t, ready, s, Action{Kind: BeginSubmission})
	remote := provider.RemoteReference{Identity: ready.Plan.Job.Identity, Resource: "exec", Version: "1"}
	ready, s = apply(t, ready, s, Action{Kind: SubmissionSeen, Submission: &provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: &remote}})
	now := time.Now().UTC()
	absent, _, _, err := Apply(ready, s, Action{Kind: RemoteAbsenceSeen, Absence: &provider.RemoteAbsence{Remote: remote, ObservedAt: now}}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range []Journal{local, absent} {
		raw, err := json.Marshal(j)
		if err != nil {
			t.Fatal(err)
		}
		var old legacyJournalShape
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&old); err != nil {
			t.Fatal("old strict reader rejects journal", err)
		}
		if old.Problem == nil || old.Problem.Validate() != nil || old.Observation != nil && old.Observation.Validate(*old.Remote) != nil {
			t.Fatal("old nested contracts reject evidence")
		}
		var restored Journal
		if json.Unmarshal(raw, &restored) != nil || !restored.Valid() {
			t.Fatal("new reader rejected roundtrip")
		}
		if len(restored.Problem.PublicDetails()) > 1 {
			t.Fatal("private metadata leaked publicly")
		}
	}
	raw, _ := json.Marshal(local)
	var reloaded Journal
	_ = json.Unmarshal(raw, &reloaded)
	started, _ := journalTime(journalDetail(local, localFailureDetail))
	ended, endedState, _, err := Apply(reloaded, state, Action{Kind: Fault, Code: domain.CodeProviderUnreachable}, started.Add(LocalFailureBudget))
	if err != nil || ended.Phase != Failed || endedState.Orchestration != domain.OrchestrationFailed {
		t.Fatal("restart reset budget", err)
	}
	if proof := absent.RemoteAbsence(); proof == nil || proof.Remote != remote || !proof.ObservedAt.Equal(now) {
		t.Fatal("absence evidence was not pinned durably")
	}
}
