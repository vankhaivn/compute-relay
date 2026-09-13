package fake_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/provider/providertest"
)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC) }
func basic(t *testing.T, s fake.Scenario) (*fake.Basic, *fake.Backend) {
	t.Helper()
	b, err := fake.NewBackend(s)
	must(t, err)
	p, err := fake.New(b, fixedClock{}, "fixture")
	must(t, err)
	return p, b
}
func start(t *testing.T, p provider.Provider, job provider.ResolvedJob) (provider.Prepared, provider.RemoteReference) {
	t.Helper()
	plan, err := p.Validate(context.Background(), job)
	must(t, err)
	prepared, err := p.Prepare(context.Background(), plan, "prepare_fixture")
	must(t, err)
	out := p.Submit(context.Background(), prepared)
	must(t, out.Validate(prepared.Identity))
	if out.Status != provider.SubmissionAccepted {
		t.Fatalf("unexpected submission: %+v", out)
	}
	return prepared, *out.Remote
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func TestProviderContract(t *testing.T) {
	providertest.Run(t, func(t *testing.T, mode providertest.Mode) providertest.Harness {
		s := fake.DefaultScenario()
		switch mode {
		case providertest.LostResponse:
			s.Mode = fake.AcceptLoseResponse
		case providertest.Unresolved:
			s.Mode = fake.Unresolved
		case providertest.Rejected:
			s.Mode = fake.Reject
		}
		p, b := basic(t, s)
		return providertest.Harness{Provider: p, Job: fake.ExampleJob("fixture"), Advance: p.Advance, Executions: func() int { return b.Stats().Executions }, Reconnect: func() provider.Provider {
			fresh, err := fake.New(b, fixedClock{}, "fixture")
			must(t, err)
			return fresh
		}}
	})
}
func TestDelayedPreparationAndDuplicateSubmission(t *testing.T) {
	s := fake.DefaultScenario()
	s.PreparationReady = false
	p, b := basic(t, s)
	job := fake.ExampleJob("fixture")
	plan, err := p.Validate(context.Background(), job)
	must(t, err)
	prepared, err := p.Prepare(context.Background(), plan, "prepare_fixture")
	must(t, err)
	if prepared.Ready {
		t.Fatal("upload treated as ready")
	}
	out := p.Submit(context.Background(), prepared)
	must(t, out.Validate(prepared.Identity))
	if out.Status != provider.SubmissionRejected || b.Stats().Executions != 0 {
		t.Fatal("unready staging submitted")
	}
	must(t, p.MarkReady(job.Identity))
	prepared, err = p.Prepare(context.Background(), plan, "prepare_fixture")
	must(t, err)
	out = p.Submit(context.Background(), prepared)
	must(t, out.Validate(prepared.Identity))
	if out.Status != provider.SubmissionAccepted {
		t.Fatal("ready fixture rejected")
	}
	repeat := p.Submit(context.Background(), prepared)
	must(t, repeat.Validate(prepared.Identity))
	if repeat.Status != provider.SubmissionUnknown || b.Stats().Executions != 1 {
		t.Fatal("repeated submission created compute")
	}
}
func TestFrozenPlanAndChangedIdentity(t *testing.T) {
	p, _ := basic(t, fake.DefaultScenario())
	job := fake.ExampleJob("fixture")
	plan, err := p.Validate(context.Background(), job)
	must(t, err)
	original := plan.Digest()
	job.Specification[0] = '!'
	job.Required[0] = domain.CapabilityGPU
	if plan.Digest() != original {
		t.Fatal("plan aliases caller memory")
	}
	_, err = p.Prepare(context.Background(), plan, "prepare_fixture")
	must(t, err)
	plan.Job.Identity.Nonce = "different-fixture-nonce"
	if _, err = p.Prepare(context.Background(), plan, "prepare_fixture"); err == nil {
		t.Fatal("same attempt accepted changed nonce")
	}
	d := p.Describe()
	d.Capabilities[0].Support = domain.CapabilitySupportUnsupported
	if p.Describe().Capabilities[0].Support != domain.CapabilitySupportSupported {
		t.Fatal("descriptor aliases provider state")
	}
}
func TestRequiredGPUIsNeverSilentlyDowngraded(t *testing.T) {
	for _, support := range []domain.CapabilitySupport{domain.CapabilitySupportUnsupported, domain.CapabilitySupportUnknown} {
		t.Run(string(support), func(t *testing.T) {
			s := fake.DefaultScenario()
			s.GPU = support
			p, b := basic(t, s)
			job := fake.ExampleJob("fixture")
			job.Required = append(job.Required, domain.CapabilityGPU)
			plan, err := p.Validate(context.Background(), job)
			if support == domain.CapabilitySupportUnsupported {
				var problem domain.Problem
				if !errors.As(err, &problem) || problem.Code != domain.CodeUnsupportedCapability || b.Stats().Executions != 0 {
					t.Fatal("unsupported GPU did not fail before dispatch")
				}
				return
			}
			must(t, err)
			if len(plan.VerifyAfterStart) != 1 || plan.VerifyAfterStart[0] != domain.CapabilityGPU {
				t.Fatal("unknown requirement disappeared")
			}
			_, remote := start(t, p, job)
			must(t, p.Advance(remote))
			must(t, p.Advance(remote))
			obs, err := p.Observe(context.Background(), remote)
			must(t, err)
			if obs.Execution != domain.ExecutionFailed {
				t.Fatal("unverified GPU became success")
			}
			page, err := p.ListArtifacts(context.Background(), remote, provider.PageRequest{Limit: 100})
			must(t, err)
			for _, a := range page.Artifacts {
				if a.Path == "execution-result.json" {
					var dst bytes.Buffer
					_, err := p.FetchArtifact(context.Background(), remote, a, &dst, 1<<20)
					must(t, err)
					var m map[string]any
					must(t, json.Unmarshal(dst.Bytes(), &m))
					if m["phase"] != "resource_check_failed" {
						t.Fatalf("optimistic GPU manifest: %s", dst.Bytes())
					}
				}
			}
		})
	}
}
func TestReferenceCursorAndCleanupIsolation(t *testing.T) {
	p, _ := basic(t, fake.DefaultScenario())
	_, remote := start(t, p, fake.ExampleJob("fixture"))
	bad := remote
	bad.Version = "another-run"
	if _, err := p.Observe(context.Background(), bad); err == nil {
		t.Fatal("wrong version observed")
	}
	must(t, p.Advance(remote))
	must(t, p.Advance(remote))
	page, err := p.ListArtifacts(context.Background(), remote, provider.PageRequest{Limit: 1})
	must(t, err)
	if page.NextCursor == "" {
		t.Fatal("pagination not exercised")
	}
	job := fake.ExampleJob("fixture")
	job.Identity.AttemptID = "att_second"
	job.Identity.IntentID = "intent_second"
	job.Identity.Nonce = "another-fixture-nonce"
	job.Identity.ResourceKey = "resource_second"
	_, second := start(t, p, job)
	must(t, p.Advance(second))
	must(t, p.Advance(second))
	if _, err := p.ListArtifacts(context.Background(), second, provider.PageRequest{Cursor: page.NextCursor, Limit: 1}); err == nil {
		t.Fatal("cursor crossed attempts")
	}
	request := provider.CleanupRequest{Remote: remote, LedgerID: "ledger_fixture", CreationOperationID: "wrong_prepare", ResultsCollected: true, Mode: provider.CleanupApply}
	if _, err := p.Cleanup(context.Background(), request); err == nil {
		t.Fatal("forged ownership accepted")
	}
	request.CreationOperationID = "prepare_fixture"
	request.ResultsCollected = false
	if _, err := p.Cleanup(context.Background(), request); err == nil {
		t.Fatal("uncollected outputs deleted")
	}
}
func TestOptionalCancellationRequiresTerminalObservation(t *testing.T) {
	b, err := fake.NewBackend(fake.DefaultScenario())
	must(t, err)
	p, err := fake.NewComplete(b, fixedClock{}, "fixture")
	must(t, err)
	_, remote := start(t, p, fake.ExampleJob("fixture"))
	cancel, err := provider.RequestCancellation(context.Background(), p, remote, "cancel_fixture")
	must(t, err)
	if cancel.Status != domain.CancellationAccepted || cancel.TerminationConfirmed {
		t.Fatal("request was treated as termination")
	}
	before, err := p.Observe(context.Background(), remote)
	must(t, err)
	if before.Execution.Terminal() {
		t.Fatal("request terminated without evidence")
	}
	must(t, p.Advance(remote))
	after, err := p.Observe(context.Background(), remote)
	must(t, err)
	if after.Execution != domain.ExecutionCancelled || after.ReleaseEvidence != domain.ReleaseEvidenceNotObservable {
		t.Fatal("invalid cancellation evidence")
	}
	late, err := p.Cancel(context.Background(), remote, "cancel_late")
	must(t, err)
	if late.Status != domain.CancellationTooLate {
		t.Fatal("terminal history rewritten")
	}
	logs, err := provider.ReadAvailableLogs(context.Background(), p, remote, provider.PageRequest{Limit: 1})
	must(t, err)
	if logs.Availability != "after_completion" || logs.NextCursor == "" {
		t.Fatal("log availability/pagination missing")
	}
	quota, err := provider.ReadAvailableQuota(context.Background(), p)
	must(t, err)
	if quota.Status != provider.QuotaKnown || quota.Remaining == nil || quota.Source != "fake fixture" {
		t.Fatal("invalid synthetic quota")
	}
}
func TestConcurrentInstancesAndExplicitAttempts(t *testing.T) {
	p, b := basic(t, fake.DefaultScenario())
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job := fake.ExampleJob("fixture")
			job.Identity.AttemptID = domain.AttemptID(fmt.Sprintf("att_%d", i))
			job.Identity.ResourceKey = fmt.Sprintf("resource_%d", i)
			plan, err := p.Validate(context.Background(), job)
			if err != nil {
				t.Error(err)
				return
			}
			prepared, err := p.Prepare(context.Background(), plan, "prepare_fixture")
			if err != nil {
				t.Error(err)
				return
			}
			out := p.Submit(context.Background(), prepared)
			if err := out.Validate(prepared.Identity); err != nil {
				t.Error(err)
			}
			if out.Status != provider.SubmissionAccepted {
				t.Error("distinct attempt rejected")
			}
		}(i)
	}
	wg.Wait()
	if b.Stats().Executions != 20 {
		t.Fatal("lost or duplicate concurrent executions")
	}
}
func TestWorkloadCommandsAndCredentialsAreNotUsed(t *testing.T) {
	t.Setenv("KAGGLE_API_TOKEN", "never-read-canary")
	p, _ := basic(t, fake.DefaultScenario())
	job := fake.ExampleJob("fixture")
	sentinel := filepath.Join(t.TempDir(), "must-not-exist")
	var spec map[string]any
	must(t, json.Unmarshal(job.Specification, &spec))
	spec["execution"].(map[string]any)["command"] = []string{"sh", "-c", "touch " + sentinel}
	data, err := json.Marshal(spec)
	must(t, err)
	job.Specification = data
	job.SpecificationSHA256 = provider.Digest(data)
	_, remote := start(t, p, job)
	must(t, p.Advance(remote))
	must(t, p.Advance(remote))
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatal("workload executed locally")
	}
	page, err := p.ListArtifacts(context.Background(), remote, provider.PageRequest{Limit: 100})
	must(t, err)
	for _, a := range page.Artifacts {
		var dst bytes.Buffer
		_, err := p.FetchArtifact(context.Background(), remote, a, &dst, 1<<20)
		must(t, err)
		if strings.Contains(dst.String(), "never-read-canary") {
			t.Fatal("credential leaked into fixture")
		}
	}
}
func TestPartialCollectionRetryNeverResubmits(t *testing.T) {
	p, b := basic(t, fake.DefaultScenario())
	_, remote := start(t, p, fake.ExampleJob("fixture"))
	must(t, p.Advance(remote))
	must(t, p.Advance(remote))
	page, err := p.ListArtifacts(context.Background(), remote, provider.PageRequest{Limit: 100})
	must(t, err)
	a := page.Artifacts[0]
	if _, err := p.FetchArtifact(context.Background(), remote, a, brokenWriter{}, 1<<20); err == nil {
		t.Fatal("failed writer was reported successful")
	}
	var dst bytes.Buffer
	_, err = p.FetchArtifact(context.Background(), remote, a, &dst, 1<<20)
	must(t, err)
	if b.Stats().Executions != 1 || b.Stats().SubmitCalls != 1 {
		t.Fatal("collection reran compute")
	}
}

type brokenWriter struct{}

func (brokenWriter) Write(p []byte) (int, error) { return 0, io.ErrClosedPipe }

func TestObservationsDriveDomainWithoutTerminalRollback(t *testing.T) {
	p, _ := basic(t, fake.DefaultScenario())
	_, remote := start(t, p, fake.ExampleJob("fixture"))
	now := fixedClock{}.Now()
	attempt, err := domain.NewAttempt(remote.Identity.AttemptID, remote.Identity.JobID, 1, now)
	must(t, err)
	apply := func(next domain.AttemptState) {
		t.Helper()
		now = now.Add(time.Second)
		attempt, err = attempt.Transition(next, now)
		must(t, err)
	}
	s := attempt.State
	s.Orchestration = domain.OrchestrationPreparing
	apply(s)
	s.Orchestration = domain.OrchestrationDispatching
	s.Execution = domain.ExecutionUnknown
	s.RemoteActivity = domain.RemoteActivityPossible
	apply(s)
	obs, err := p.Observe(context.Background(), remote)
	must(t, err)
	s.Orchestration = domain.OrchestrationSubmitted
	s.Execution = obs.Execution
	s.RemoteActivity = obs.RemoteActivity
	apply(s)
	must(t, p.Advance(remote))
	obs, err = p.Observe(context.Background(), remote)
	must(t, err)
	s.Orchestration = domain.OrchestrationRunning
	s.Execution = obs.Execution
	s.RemoteActivity = obs.RemoteActivity
	apply(s)
	stale := s
	must(t, p.Advance(remote))
	obs, err = p.Observe(context.Background(), remote)
	must(t, err)
	s.Orchestration = domain.OrchestrationCollecting
	s.Execution = obs.Execution
	s.RemoteActivity = obs.RemoteActivity
	s.ReleaseEvidence = obs.ReleaseEvidence
	s.Result = domain.ResultCollecting
	apply(s)
	// Simulated collection evidence; real publication and event transaction belong to M3.
	s.Orchestration = domain.OrchestrationSucceeded
	s.Result = domain.ResultAvailable
	apply(s)
	if _, err := attempt.Transition(stale, now.Add(time.Second)); err == nil {
		t.Fatal("stale provider observation moved terminal attempt backward")
	}
}

func TestUnknownObservationAndDeferredFeaturesStayExplicit(t *testing.T) {
	s := fake.DefaultScenario()
	s.States = []domain.ExecutionState{domain.ExecutionUnknown}
	p, _ := basic(t, s)
	_, remote := start(t, p, fake.ExampleJob("fixture"))
	obs, err := p.Observe(context.Background(), remote)
	must(t, err)
	if obs.Execution != domain.ExecutionUnknown || obs.RemoteActivity != domain.RemoteActivityUnknown || obs.RawState == "" {
		t.Fatal("unknown provider state was normalized to success/failure")
	}
	for _, cap := range []domain.CapabilityName{domain.CapabilityCustomContainer, domain.CapabilityRetainedSessions, "new_unsupported_feature"} {
		job := fake.ExampleJob("fixture")
		job.Required = append(job.Required, cap)
		if _, err := p.Validate(context.Background(), job); err == nil {
			t.Fatalf("unsupported requirement %q accepted", cap)
		}
	}
}

func TestCancellationCompletionRace(t *testing.T) {
	b, err := fake.NewBackend(fake.DefaultScenario())
	must(t, err)
	p, err := fake.NewComplete(b, fixedClock{}, "fixture")
	must(t, err)
	_, remote := start(t, p, fake.ExampleJob("fixture"))
	must(t, p.Advance(remote))
	must(t, p.Advance(remote))
	cancel, err := p.Cancel(context.Background(), remote, "cancel_late")
	must(t, err)
	obs, err := p.Observe(context.Background(), remote)
	must(t, err)
	if cancel.Status != domain.CancellationTooLate || obs.Execution != domain.ExecutionSucceeded {
		t.Fatal("late cancellation rewrote completed execution")
	}
}
