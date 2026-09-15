package dispatch

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

const requestFixture = `{"api_version":"compute-connector/v1alpha1","name":"dispatch-fixture","profile":"fixture","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","never-executed.py"]},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":2,"finalization_grace_seconds":2}}`

func fixtureWork(t testing.TB) (Work, provider.Plan, domain.OperationID) {
	t.Helper()
	req, err := admission.Parse([]byte(requestFixture))
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	a, err := domain.NewAttempt("attempt", "job", 1, at)
	if err != nil {
		t.Fatal(err)
	}
	next := a.State
	next.Orchestration = domain.OrchestrationPreparing
	a, err = a.Transition(next, at)
	if err != nil {
		t.Fatal(err)
	}
	profile := admission.DefaultProfile(domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "instance", ConfigurationRevision: "revision"}, "account")
	job, err := domain.NewJob(domain.Job{ID: "job", WorkspaceID: "workspace", Name: "dispatch-fixture", SpecificationVersion: "compute-connector/v1alpha1", SpecificationDigest: req.Hash(), Binding: profile.Binding, ActiveAttemptID: a.ID, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	refs := map[string]domain.ObjectMetadata{
		"bundle":  {ID: "code", WorkspaceID: "workspace", Bytes: 8, SHA256: provider.Digest([]byte("bundle"))},
		"input:0": {ID: "data", WorkspaceID: "workspace", Bytes: 3, SHA256: provider.Digest([]byte("abc"))},
	}
	record := admission.Record{Job: job, Attempt: a, AttemptNonce: strings.Repeat("a", 64), Request: req, Profile: profile}
	for role, m := range refs {
		record.Objects = append(record.Objects, admission.FrozenObject{Role: role, Object: m})
	}
	w := Work{InstallationID: "installation", Job: record, Journal: Journal{Phase: Local}}
	snapshot, err := Snapshot(record, refs)
	if err != nil {
		t.Fatal(err)
	}
	resolved, op, err := ResolveJob(w, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return w, provider.Plan{Job: resolved}, op
}
func apply(t testing.TB, j Journal, s domain.AttemptState, a Action) (Journal, domain.AttemptState) {
	t.Helper()
	next, state, event, err := Apply(j, s, a, time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("%s from %s: %v", a.Kind, j.Phase, err)
	}
	if !next.Valid() || state.Validate() != nil || !event.Valid() || next.Version != j.Version+1 {
		t.Fatal("invalid transition output")
	}
	return next, state
}
func readyFixture(t testing.TB) (Journal, domain.AttemptState) {
	t.Helper()
	w, p, op := fixtureWork(t)
	j, s := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: BeginPreparation, Plan: &p, PreparationID: op})
	prepared := provider.Prepared{Identity: p.Job.Identity, PreparationID: op, PlanSHA256: p.Digest(), Resource: "fixture-resource", Ready: true, Private: true}
	return apply(t, j, s, Action{Kind: PreparationSeen, Prepared: &prepared})
}
func TestSubmissionGatesCannotBeReplayed(t *testing.T) {
	for _, status := range []provider.SubmissionStatus{provider.SubmissionAccepted, provider.SubmissionUnknown, provider.SubmissionRejected} {
		t.Run(string(status), func(t *testing.T) {
			j, s := readyFixture(t)
			j, s = apply(t, j, s, Action{Kind: BeginSubmission})
			old := j.Version
			if _, _, _, err := Apply(j, s, Action{Kind: BeginSubmission}, time.Now()); err == nil {
				t.Fatal("second submit gate opened")
			}
			if j.Version != old {
				t.Fatal("input journal mutated")
			}
			outcome := provider.SubmissionOutcome{Status: status}
			switch status {
			case provider.SubmissionAccepted:
				outcome.Remote = &provider.RemoteReference{Identity: j.Plan.Job.Identity, Resource: "exec", Version: "1"}
			case provider.SubmissionUnknown:
				outcome.Problem = problem(domain.CodeProviderSubmissionUnknown, true)
			case provider.SubmissionRejected:
				outcome.Problem = problem(domain.CodeProviderRejected, false)
			}
			j, s = apply(t, j, s, Action{Kind: SubmissionSeen, Submission: &outcome})
			if _, _, _, err := Apply(j, s, Action{Kind: BeginSubmission}, time.Now()); err == nil {
				t.Fatal("outcome permitted compute replay")
			}
			if status == provider.SubmissionRejected {
				if s.Orchestration != domain.OrchestrationFailed || s.Execution != domain.ExecutionNotSubmitted || scheduler.HoldsAccount(s, true) {
					t.Fatal("rejection was not proven non-acceptance")
				}
			} else if !scheduler.HoldsAccount(s, true) || s.Result != domain.ResultNotAvailable {
				t.Fatal("acceptance/uncertainty released capacity or fabricated results")
			}
		})
	}
}
func TestAmbiguityStopsWithoutInventingNonAcceptance(t *testing.T) {
	j, s := readyFixture(t)
	j, s = apply(t, j, s, Action{Kind: BeginSubmission})
	for n := 0; n < MaxFailures; n++ {
		j, s = apply(t, j, s, Action{Kind: Fault, Code: domain.CodeProviderSubmissionUnknown})
	}
	if j.Phase != Attention || s.Orchestration != domain.OrchestrationNeedsAttention || !scheduler.HoldsAccount(s, true) || !j.Problem.ComputeMayHaveStarted || j.Problem.SafeOperationRetry {
		t.Fatal("uncertainty was cleared")
	}
	if j.Recoverable() {
		t.Fatal("exhausted automatic recovery did not stop")
	}
}
func TestPrivateReadinessAndIdentityAreRequired(t *testing.T) {
	w, p, op := fixtureWork(t)
	j, s := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: BeginPreparation, Plan: &p, PreparationID: op})
	pending := provider.Prepared{Identity: p.Job.Identity, PreparationID: op, PlanSHA256: p.Digest(), Resource: "staging", Private: true}
	j, s = apply(t, j, s, Action{Kind: PreparationSeen, Prepared: &pending})
	if _, _, _, err := Apply(j, s, Action{Kind: BeginSubmission}, time.Now()); err == nil {
		t.Fatal("pending staging submitted")
	}
	wrong := pending
	wrong.Identity.Nonce = strings.Repeat("b", 64)
	if _, _, _, err := Apply(j, s, Action{Kind: PreparationSeen, Prepared: &wrong}, time.Now()); err == nil {
		t.Fatal("foreign preparation accepted")
	}
	pending.Ready = true
	pending.Private = false
	j, s = apply(t, j, s, Action{Kind: PreparationSeen, Prepared: &pending})
	if j.Phase != Attention || j.Prepared == nil || j.Prepared.Resource != "staging" || j.Problem.Code != domain.CodePrivateStagingUnavailable {
		t.Fatal("unsafe staging evidence was lost or accepted")
	}
	if _, _, _, err := Apply(j, s, Action{Kind: BeginSubmission}, time.Now()); err == nil {
		t.Fatal("public preparation submitted")
	}
}
func TestObservationDoesNotInventSuccessOrRegressEvidence(t *testing.T) {
	j, s := readyFixture(t)
	j, s = apply(t, j, s, Action{Kind: BeginSubmission})
	remote := provider.RemoteReference{Identity: j.Plan.Job.Identity, Resource: "exec", Version: "v1"}
	j, s = apply(t, j, s, Action{Kind: SubmissionSeen, Submission: &provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: &remote}})
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	obs := provider.Observation{Remote: remote, Execution: domain.ExecutionRunning, RemoteActivity: domain.RemoteActivityActive, ReleaseEvidence: domain.ReleaseEvidenceNotObservable, ObservedAt: now, RawState: "secret-provider-canary"}
	j, s = apply(t, j, s, Action{Kind: ObservationSeen, Observation: &obs})
	if j.Observation.RawState != "running" {
		t.Fatal("raw diagnostics persisted")
	}
	obs.Execution = domain.ExecutionUnknown
	obs.RemoteActivity = domain.RemoteActivityUnknown
	obs.ObservedAt = now.Add(time.Second)
	j, s = apply(t, j, s, Action{Kind: ObservationSeen, Observation: &obs})
	if s.Execution != domain.ExecutionRunning || s.RemoteActivity != domain.RemoteActivityActive {
		t.Fatal("uncertain poll erased positive evidence")
	}
	obs.Execution = domain.ExecutionQueued
	obs.RemoteActivity = domain.RemoteActivityPossible
	obs.ObservedAt = now.Add(2 * time.Second)
	if _, _, _, err := Apply(j, s, Action{Kind: ObservationSeen, Observation: &obs}, now.Add(time.Hour)); !errors.Is(err, ErrStaleObservation) {
		t.Fatalf("regressive poll: %v", err)
	}
	obs.Execution = domain.ExecutionSucceeded
	obs.RemoteActivity = domain.RemoteActivityInactive
	obs.ObservedAt = now.Add(3 * time.Second)
	j, s = apply(t, j, s, Action{Kind: ObservationSeen, Observation: &obs})
	if j.Phase != Collectible || s.Orchestration != domain.OrchestrationCollecting || s.Result != domain.ResultNotAvailable || scheduler.HoldsAccount(s, true) {
		t.Fatal("terminal observation was not separated from result verification")
	}
}
func TestLocalFailureStaysPreCompute(t *testing.T) {
	w, _, _ := fixtureWork(t)
	j, s := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: Fault, Code: domain.CodeInputDigestMismatch})
	if j.Phase != Failed || j.Problem.ComputeMayHaveStarted || s.Execution != domain.ExecutionNotSubmitted || s.ReleaseEvidence != domain.ReleaseEvidenceNotApplicable {
		t.Fatal("invalid local failure")
	}
}
func TestResolvedSnapshotNeverTransmitsSourceURLOrChangesOriginal(t *testing.T) {
	w, _, _ := fixtureWork(t)
	raw := strings.Replace(requestFixture, `"kind":"object","object_id":"data"`, `"kind":"https","url":"https://example.com/source?canary=private-name"`, 1)
	req, err := admission.Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	w.Job.Request = req
	refs := map[string]domain.ObjectMetadata{}
	for _, ref := range w.Job.Objects {
		refs[ref.Role] = ref.Object
	}
	snap, err := Snapshot(w.Job, refs)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := ResolveJob(w, snap)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(job)
	if strings.Contains(string(encoded), "canary") || !strings.Contains(string(req.Canonical()), "canary") {
		t.Fatal("source exposure or original request mutation")
	}
	before := job.Inputs.InputDigest()
	copy := job.Clone()
	copy.Inputs.Inputs[0].Object.Bytes++
	if job.Inputs.InputDigest() != before {
		t.Fatal("snapshot aliased through clone")
	}
}
func TestRunnerCompatibilityAndNoCapabilityDowngrade(t *testing.T) {
	w, p, _ := fixtureWork(t)
	d := provider.Descriptor{InstanceID: p.Job.Identity.InstanceID}
	for _, name := range p.Job.Required {
		d.Capabilities = append(d.Capabilities, domain.CapabilityStatus{Name: name, Support: domain.CapabilitySupportSupported, Evidence: domain.EvidenceImplementedOffline})
	}
	if err := ValidatedPlan(p.Job, p, d); err != nil {
		t.Fatal(err)
	}
	changed := p.Clone()
	changed.Job.WallSeconds++
	if err := ValidatedPlan(p.Job, changed, d); err == nil {
		t.Fatal("adapter changed wall budget")
	}
	spec := w.Job.Request.Spec()
	spec.Execution.Environment = map[string]string{"SECRET_TOKEN": "do-not-stage"}
	if runnerCompatible(spec, packaging.Manifest{}) == nil {
		t.Fatal("runner-reserved secret environment accepted")
	}
	if disjointRunnerPaths([]string{"A/x", "a/y"}) || disjointRunnerPaths([]string{"a", "a/x"}) || !disjointRunnerPaths([]string{"a/x", "a/y"}) {
		t.Fatal("implicit-directory/case collision")
	}
}
func FuzzApplyNeverRepeatsMutation(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4})
	f.Add([]byte{0, 0, 1, 1, 2, 2, 5, 5, 3})
	w, p, op := fixtureWork(f)
	f.Fuzz(func(t *testing.T, steps []byte) {
		if len(steps) > 100 {
			return
		}
		j, s := w.Journal, w.Job.Attempt.State
		prepared := provider.Prepared{Identity: p.Job.Identity, PreparationID: op, PlanSHA256: p.Digest(), Resource: "resource", Ready: true, Private: true}
		remote := provider.RemoteReference{Identity: p.Job.Identity, Resource: "run", Version: "1"}
		prepares, submits := 0, 0
		for _, v := range steps {
			a := Action{}
			switch v % 6 {
			case 0:
				a = Action{Kind: BeginPreparation, Plan: &p, PreparationID: op}
			case 1:
				a = Action{Kind: PreparationSeen, Prepared: &prepared}
			case 2:
				a = Action{Kind: BeginSubmission}
			case 3:
				a = Action{Kind: SubmissionSeen, Submission: &provider.SubmissionOutcome{Status: provider.SubmissionAccepted, Remote: &remote}}
			case 4:
				a = Action{Kind: Fault, Code: domain.CodeProviderUnreachable}
			case 5:
				a = Action{Kind: SubmissionSeen, Submission: &provider.SubmissionOutcome{Status: provider.SubmissionUnknown, Problem: problem(domain.CodeProviderSubmissionUnknown, true)}}
			}
			next, state, _, err := Apply(j, s, a, time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC))
			if err != nil {
				continue
			}
			if a.Kind == BeginPreparation {
				prepares++
			}
			if a.Kind == BeginSubmission {
				submits++
			}
			if prepares > 1 || submits > 1 || !next.Valid() || state.Validate() != nil {
				t.Fatal("mutation replay or invalid state")
			}
			j, s = next, state
		}
	})
}
