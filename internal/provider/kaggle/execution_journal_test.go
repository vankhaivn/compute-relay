package kaggle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

// Test-only composition of the concrete per-attempt execution component and the
// existing fake capability surface. No production Provider stub or registry is added.
type executionJournalAdapter struct {
	*journalStagingAdapter
	executor *Executor
}

func (p *executionJournalAdapter) Submit(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
	return p.executor.Submit(ctx, prepared)
}
func (p *executionJournalAdapter) ReconcileSubmission(ctx context.Context, id provider.Identity) (provider.Reconciliation, error) {
	return p.executor.ReconcileSubmission(ctx, id)
}
func (p *executionJournalAdapter) Observe(ctx context.Context, ref provider.RemoteReference) (provider.Observation, error) {
	return p.executor.Observe(ctx, ref)
}

type executionJournalRepository struct {
	dispatch.Repository
	failBefore bool
	lose       dispatch.Kind
	last       dispatch.Handle
}

func (r *executionJournalRepository) CommitDispatch(ctx context.Context, h dispatch.Handle, action dispatch.Action, now time.Time) (dispatch.Work, error) {
	r.last = h
	if action.Kind == dispatch.BeginSubmission && r.failBefore {
		r.failBefore = false
		return dispatch.Work{}, errStagingCommitFixture
	}
	work, err := r.Repository.CommitDispatch(ctx, h, action, now)
	if err == nil && action.Kind == r.lose {
		r.lose = ""
		return dispatch.Work{}, errStagingCommitFixture
	}
	return work, err
}

type executionJournalFixture struct {
	f                        *stagingJournalFixture
	repo                     *executionJournalRepository
	executor                 *Executor
	saves, reads             int
	exists, loseSave, swapped bool
	raw                      string
	source                   domain.SHA256Digest
}

func newExecutionJournalFixture(t *testing.T) *executionJournalFixture {
	t.Helper()
	f := newStagingJournalFixture(t)
	f.step(t, false)
	f.ready = true
	f.step(t, false)
	x := &executionJournalFixture{f: f, raw: "RUNNING"}
	x.attach(t)
	return x
}

func (x *executionJournalFixture) attach(t *testing.T) {
	t.Helper()
	f := x.f
	j := f.journal(t)
	resolver, err := credentials.NewEnvironment([]ports.CredentialRef{f.config.CredentialRef}, func(string) (string, bool) { return "SYNTHETIC_TOKEN", true })
	if err != nil {
		t.Fatal(err)
	}
	stage, err := NewStager(f.config, DefaultStagingPolicy(), resolver, f.blobs, false)
	if err != nil {
		t.Fatal(err)
	}
	stage.local = func(context.Context, Config, Mode, []byte) (Report, error) { return baseline(Local), nil }
	stage.run = func(_ context.Context, c Config, mode string, token []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
		if mode != "observe" || c != f.config || string(token) != "SYNTHETIC_TOKEN" {
			t.Fatal("execution refreshed staging or changed account")
		}
		return stageResponse(p, "ready"), nil
	}
	x.executor, err = NewExecutor(stage, DefaultExecutionPolicy(), *j.Plan, *j.Prepared, true)
	if err != nil {
		t.Fatal(err)
	}
	x.executor.now = f.clock.Now
	if x.source == "" {
		x.source = x.executor.request.SourceSHA256
	} else if x.source != x.executor.request.SourceSHA256 {
		t.Fatal("restart or profile remapping changed original source")
	}
	x.executor.run = func(_ context.Context, c Config, mode string, token []byte, r executionRequest) (executionResponse, error) {
		if c != f.config || string(token) != "SYNTHETIC_TOKEN" || r.SourceSHA256 != x.source {
			t.Fatal("execution lost frozen binding")
		}
		// Prove durable intent/ownership through an independent read-only SQLite
		// connection BEFORE the simulated SDK effect, not an in-memory flag.
		f.inspect(t, func(db *sql.DB) {
			for _, query := range []string{"SELECT count(*) FROM submission_intents", "SELECT count(*) FROM provider_resources WHERE purpose='execution' AND cleanup_state='pinned'", "SELECT count(*) FROM events WHERE type='submission.intent_recorded'"} {
				var n int
				if err := db.QueryRow(query).Scan(&n); err != nil || n != 1 {
					t.Fatal("execution helper preceded durable intent", query, n, err)
				}
			}
		})
		if mode == "submit" {
			x.saves++
			x.exists = true
			if x.loseSave {
				x.loseSave = false
				return executionResponse{}, errors.New("synthetic lost save acknowledgement")
			}
		} else {
			x.reads++
			if r.Source != "" {
				t.Fatal("recovery retransmitted private source")
			}
		}
		if !x.exists {
			return executionResponse{Protocol: 1, Status: "not_found"}, nil
		}
		response := executionFound(r, x.raw)
		if x.swapped {
			response.KernelID = "43"
		}
		return response, nil
	}
	backend, err := fake.NewBackend(fake.DefaultScenario())
	if err != nil {
		t.Fatal(err)
	}
	binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
	base, err := fake.NewBound(backend, f.clock, binding)
	if err != nil {
		t.Fatal(err)
	}
	registry := provider.NewSnapshotRegistry()
	adapter := &executionJournalAdapter{journalStagingAdapter: &journalStagingAdapter{Bound: base, stage: stage}, executor: x.executor}
	if err := registry.Register(binding, adapter); err != nil {
		t.Fatal(err)
	}
	x.repo = &executionJournalRepository{Repository: f.store}
	f.engine, err = dispatch.New(x.repo, registry, f.blobs, nil, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
}
func (x *executionJournalFixture) restart(t *testing.T) {
	t.Helper()
	x.f.restart(t)
	x.attach(t)
}
func (x *executionJournalFixture) step(t *testing.T, wantError bool) {
	t.Helper()
	x.f.clock.advance()
	worked, err := x.f.engine.RunOnce(context.Background(), "execution_fixture_worker")
	if !worked || (err != nil) != wantError {
		t.Fatal("unexpected execution step", worked, err, wantError)
	}
}
func (x *executionJournalFixture) counts(t *testing.T, intents int) {
	t.Helper()
	x.f.inspect(t, func(db *sql.DB) {
		for _, query := range []string{"SELECT count(*) FROM submission_intents", "SELECT count(*) FROM provider_resources WHERE purpose='execution'", "SELECT count(*) FROM events WHERE type='submission.intent_recorded'"} {
			var n int
			if err := db.QueryRow(query).Scan(&n); err != nil || n != intents {
				t.Fatal("duplicate or missing execution history", query, n, err)
			}
		}
		var n int
		if err := db.QueryRow("SELECT count(*) FROM attempts").Scan(&n); err != nil || n != 1 {
			t.Fatal("unexpected compute retry", n, err)
		}
		if err := db.QueryRow("SELECT count(*) FROM collection_publications").Scan(&n); err != nil || n != 0 {
			t.Fatal("observation fabricated verified results", n, err)
		}
	})
}

func (x *executionJournalFixture) assertRunningEvidence(t *testing.T) {
	t.Helper()
	x.f.inspect(t, func(db *sql.DB) {
		var raw string
		if err := db.QueryRow("SELECT state FROM attempts WHERE workspace_id=? AND job_id=? AND attempt_id=?", "workspace", string(x.f.receipt.JobID), string(x.f.receipt.AttemptID)).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var attempt domain.Attempt
		if err := json.Unmarshal([]byte(raw), &attempt.State); err != nil {
			t.Fatal(err)
		}
		if attempt.State.Execution != domain.ExecutionRunning || attempt.State.RemoteActivity != domain.RemoteActivityActive || attempt.State.ReleaseEvidence != domain.ReleaseEvidenceNotObservable || attempt.State.Orchestration.Terminal() {
			t.Fatal("unknown poll erased confirmed attempt evidence", attempt.State)
		}
	})
}

func TestExecutionM3LostSaveAndOutcomeAcknowledgementsRecoverSameAttempt(t *testing.T) {
	for _, fault := range []string{"save", "outcome"} {
		t.Run(fault, func(t *testing.T) {
			x := newExecutionJournalFixture(t)
			x.loseSave = fault == "save"
			if fault == "outcome" {
				x.repo.lose = dispatch.SubmissionSeen
			}
			x.step(t, fault == "outcome")
			before := x.f.journal(t)
			if !before.SubmitStarted || x.saves != 1 {
				t.Fatal("submission identity not persisted")
			}
			changed := x.f.profile
			changed.Binding.ConfigurationRevision = "replacement"
			changed.AccountScope = "other_account"
			if err := x.f.store.PutProfile(context.Background(), changed, false); err != nil {
				t.Fatal(err)
			}
			x.restart(t)
			x.step(t, false)
			if fault == "save" {
				x.step(t, false) // reconciliation then version-bound running observation
			}
			running := x.f.journal(t)
			if running.Phase != dispatch.Submitted || running.Observation == nil || running.Observation.Execution != domain.ExecutionRunning || running.Plan.Digest() != before.Plan.Digest() || x.saves != 1 {
				t.Fatal("lost original running attempt")
			}
			x.assertRunningEvidence(t)
			x.raw = "UNKNOWN"
			x.step(t, false)
			unknown := x.f.journal(t)
			if unknown.Observation == nil || unknown.Observation.Execution != domain.ExecutionUnknown || unknown.Remote == nil || *unknown.Remote != *running.Remote || unknown.Problem == nil || !unknown.Problem.ComputeMayHaveStarted || unknown.Problem.SafeOperationRetry {
				t.Fatal("latest unknown observation or ambiguity was concealed")
			}
			// Raw observations stay truthful while the independent attempt state
			// retains stronger previously confirmed execution/activity evidence.
			x.assertRunningEvidence(t)
			x.raw = "COMPLETE"
			x.step(t, false)
			terminal := x.f.journal(t)
			if terminal.Phase != dispatch.Collectible || terminal.Remote == nil || *terminal.Remote != *running.Remote || terminal.Observation.ReleaseEvidence != domain.ReleaseEvidenceNotObservable || x.saves != 1 || x.f.creates != 1 {
				t.Fatal("terminal observation changed identity or invented release")
			}
			x.counts(t, 1)
		})
	}
}

func TestExecutionM3LostIntentAcknowledgementNeverRearmsMissingKernel(t *testing.T) {
	x := newExecutionJournalFixture(t)
	x.repo.lose = dispatch.BeginSubmission
	x.step(t, true)
	x.restart(t)
	for i := 0; i < dispatch.MaxFailures; i++ {
		x.step(t, true)
	}
	j := x.f.journal(t)
	if j.Phase != dispatch.Attention || !j.SubmitStarted || j.Remote != nil || x.saves != 0 || x.reads != dispatch.MaxFailures || j.Problem == nil || !j.Problem.ComputeMayHaveStarted {
		t.Fatal("missing kernel rearmed committed intent", j.Phase, x.saves, x.reads)
	}
	x.counts(t, 1)
}

func TestExecutionM3FailedIntentAllowsOnlyUncommittedWorkToRetry(t *testing.T) {
	x := newExecutionJournalFixture(t)
	x.repo.failBefore = true
	x.step(t, true)
	x.counts(t, 0)
	if x.saves != 0 || x.reads != 0 {
		t.Fatal("failed intent invoked helper")
	}
	x.restart(t)
	x.step(t, false)
	if x.saves != 1 || x.f.journal(t).Phase != dispatch.Submitted {
		t.Fatal("safe local retry changed identity")
	}
	x.counts(t, 1)
}

func TestExecutionM3ReplacementAndLatePollCannotRewriteTerminalEvidence(t *testing.T) {
	x := newExecutionJournalFixture(t)
	x.step(t, false)
	original := *x.f.journal(t).Remote
	x.swapped = true
	x.step(t, true)
	if *x.f.journal(t).Remote != original || x.saves != 1 {
		t.Fatal("replacement adopted or resubmitted")
	}
	x.swapped, x.raw = false, "COMPLETE"
	x.step(t, false)
	before := x.f.journal(t)
	x.raw = "RUNNING"
	late, err := x.executor.Observe(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.f.store.CommitDispatch(context.Background(), x.repo.last, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &late}, x.f.clock.Now()); err == nil {
		t.Fatal("stale publisher rewrote terminal evidence")
	}
	after := x.f.journal(t)
	if before.Version != after.Version || after.Phase != dispatch.Collectible || after.Observation.Execution != domain.ExecutionSucceeded || x.saves != 1 {
		t.Fatal("late poll regressed durable terminal state")
	}
	x.counts(t, 1)
}
