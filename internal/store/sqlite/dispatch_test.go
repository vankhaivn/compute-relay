package sqlite

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/httpsinput"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

const dispatchJSON = `{"api_version":"compute-connector/v1alpha1","name":"dispatch-fixture","profile":"fixture","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","never-executed.py"]},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":2,"finalization_grace_seconds":2}}`

type dispatchClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *dispatchClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *dispatchClock) advance(d time.Duration) { c.mu.Lock(); c.at = c.at.Add(d); c.mu.Unlock() }

type dispatchFixture struct {
	s        *Store
	root     string
	blobs    *blobfs.Store
	clock    *dispatchClock
	profile  admission.Profile
	registry *provider.SnapshotRegistry
	backend  *fake.Backend
	adapter  *probeProvider
	engine   *dispatch.Engine
}
type probeProvider struct {
	*fake.Bound
	store          *Store
	losePrepare    bool
	panicSubmit    bool
	validateError  bool
	public         bool
	rejectQuota    bool
	prepareEntered chan struct{}
	prepareRelease chan struct{}
}

func (p *probeProvider) Prepare(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.Prepared, error) {
	var n int
	if err := p.store.db.QueryRowContext(ctx, "SELECT count(*) FROM provider_resources WHERE operation_id=? AND purpose='staging'", string(op)).Scan(&n); err != nil || n != 1 {
		return provider.Prepared{}, errors.New("prepare called without committed ownership intent")
	}
	if p.prepareEntered != nil {
		close(p.prepareEntered)
		select {
		case <-p.prepareRelease:
		case <-ctx.Done():
			return provider.Prepared{}, ctx.Err()
		}
	}
	prepared, err := p.Bound.Prepare(ctx, plan, op)
	if p.public {
		prepared.Private = false
	}
	if p.losePrepare {
		return provider.Prepared{}, errors.New("private-response-canary")
	}
	return prepared, err
}
func (p *probeProvider) Validate(ctx context.Context, j provider.ResolvedJob) (provider.Plan, error) {
	if p.validateError {
		return provider.Plan{}, provider.Problem(domain.CodeConfigurationInvalid, domain.FailureStageLocalRuntime, "temporary configuration fixture")
	}
	return p.Bound.Validate(ctx, j)
}
func (p *probeProvider) Submit(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
	var n int
	if err := p.store.db.QueryRowContext(ctx, "SELECT count(*) FROM submission_intents WHERE intent_id=? AND status='started'", string(prepared.Identity.IntentID)).Scan(&n); err != nil || n != 1 {
		panic("submit called without committed intent")
	}
	outcome := p.Bound.Submit(ctx, prepared)
	if p.rejectQuota && outcome.Status == provider.SubmissionRejected {
		problem := provider.Problem(domain.CodeQuotaExhausted, domain.FailureStageSubmission, "fixture exhausted")
		outcome.Problem = &problem
	}
	if p.panicSubmit {
		panic("provider-secret-canary")
	}
	return outcome
}
func fixtureBundle(t testing.TB) []byte {
	t.Helper()
	payload := []byte("raise SystemExit('fixture must never run on the control host')\n")
	m := packaging.Manifest{Version: packaging.Version, Files: []packaging.File{{Path: "never-executed.py", Bytes: int64(len(payload)), SHA256: string(provider.Digest(payload))}}}
	manifest, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	gz.Header.OS = 255
	tw := tar.NewWriter(gz)
	for _, entry := range []struct {
		name string
		data []byte
	}{{packaging.ManifestPath, manifest}, {"code/never-executed.py", payload}} {
		if err = tw.WriteHeader(&tar.Header{Name: entry.name, Size: int64(len(entry.data)), Typeflag: tar.TypeReg, Mode: 0644, ModTime: time.Unix(0, 0).UTC(), Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err = tw.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err = gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func newDispatchFixture(t testing.TB, scenario fake.Scenario) *dispatchFixture {
	t.Helper()
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "state")
	s, err := Open(ctx, root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	f := &dispatchFixture{s: s, root: root, blobs: blobs, clock: &dispatchClock{at: time.Now().UTC().Truncate(time.Millisecond).Add(time.Second)}}
	t.Cleanup(func() { _ = f.s.Close(); _ = blobs.Close() })
	for _, w := range []domain.WorkspaceID{"a", "b"} {
		if err = s.PutWorkspace(ctx, auth.Workspace{ID: w, Enabled: true, AllowedProfiles: []string{"fixture"}}); err != nil {
			t.Fatal(err)
		}
		for id, data := range map[domain.ObjectID][]byte{"code": fixtureBundle(t), "data": []byte("frozen-input")} {
			m, err := blobs.Put(ctx, domain.ObjectMetadata{ID: id, WorkspaceID: w, Bytes: -1}, bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			if err = s.CommitObject(ctx, m); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.profile = admission.DefaultProfile(domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "fake_1", ConfigurationRevision: "one"}, "shared_account")
	if err = s.PutProfile(ctx, f.profile, true); err != nil {
		t.Fatal(err)
	}
	if err = s.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
		t.Fatal(err)
	}
	f.backend, err = fake.NewBackend(scenario)
	if err != nil {
		t.Fatal(err)
	}
	f.attach(t, s, nil)
	return f
}
func (f *dispatchFixture) attach(t testing.TB, s *Store, fetch dispatch.HTTPSFetcher) {
	t.Helper()
	f.s = s
	binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
	p, err := fake.NewBound(f.backend, f.clock, binding)
	if err != nil {
		t.Fatal(err)
	}
	f.adapter = &probeProvider{Bound: p, store: s}
	f.registry = provider.NewSnapshotRegistry()
	if err = f.registry.Register(binding, f.adapter); err != nil {
		t.Fatal(err)
	}
	f.engine, err = dispatch.New(s, f.registry, f.blobs, fetch, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
}

// These are controlled admitted-record fixtures, not a replacement admission path.
// The separate executable smoke goes through the actual auth/admission services.
func (f *dispatchFixture) seed(t testing.TB, w domain.WorkspaceID, number int, url bool) scheduler.Identity {
	t.Helper()
	ctx := context.Background()
	raw := dispatchJSON
	if url {
		raw = strings.Replace(raw, `"kind":"object","object_id":"data"`, `"kind":"https","url":"https://example.com/input?name=private-source-canary"`, 1)
	}
	req, err := admission.Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	id := scheduler.Identity{WorkspaceID: w, JobID: domain.JobID(fmt.Sprintf("job_%d", number)), AttemptID: domain.AttemptID(fmt.Sprintf("attempt_%d", number))}
	at := f.clock.Now().Add(-time.Millisecond).Format(time.RFC3339Nano)
	state, _ := json.Marshal(domain.InitialAttemptState())
	err = withTx(ctx, f.s.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?)", string(w), string(id.JobID), string(req.Canonical()), admission.CanonicalVersion, string(req.Hash()), f.profile.Binding.Profile, f.profile.Binding.ConfigurationRevision, string(id.AttemptID), at)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?)", string(w), string(id.JobID), string(id.AttemptID), 1, strings.Repeat("a", 64), string(state), "queued", 1, at, at)
		if err != nil {
			return err
		}
		for role, object := range map[string]string{"bundle": "code", "input:0": "data"} {
			if url && role == "input:0" {
				continue
			}
			_, err = tx.ExecContext(ctx, "INSERT INTO job_objects SELECT ?,?,?,object_id,bytes,sha256 FROM objects WHERE workspace_id=? AND object_id=?", string(w), string(id.JobID), role, string(w), object)
			if err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO events(workspace_id,job_id,event_id,sequence,attempt_id,type,occurred_at) VALUES(?,?,?,1,?,'job.accepted',?)", string(w), string(id.JobID), "accepted_"+string(id.AttemptID), string(id.AttemptID), at)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func (f *dispatchFixture) step(t testing.TB) error {
	t.Helper()
	f.clock.advance(10 * time.Minute)
	worked, err := f.engine.RunOnce(context.Background(), "worker")
	if !worked && err == nil {
		t.Fatal("expected one phase of work")
	}
	return err
}
func (f *dispatchFixture) journal(t testing.TB, id scheduler.Identity) dispatch.Journal {
	t.Helper()
	var seq int64
	var j dispatch.Journal
	if err := f.s.db.QueryRow("SELECT queue_seq FROM scheduler_queue WHERE workspace_id=? AND job_id=? AND attempt_id=?", string(id.WorkspaceID), string(id.JobID), string(id.AttemptID)).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	err := withTx(context.Background(), f.s.db, func(tx *sql.Tx) error { var err error; j, err = loadJournal(context.Background(), tx, seq); return err })
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func (f *dispatchFixture) state(t testing.TB, id scheduler.Identity) domain.AttemptState {
	t.Helper()
	a, err := f.s.LoadAttempt(context.Background(), id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	return a.State
}
func (f *dispatchFixture) restart(t testing.TB) {
	t.Helper()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), f.root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	f.attach(t, s, nil)
}
func (f *dispatchFixture) claim(t testing.TB) dispatch.Work {
	t.Helper()
	r, err := f.s.ClaimNext(context.Background(), "manual", f.clock.Now())
	if err != nil || r.Claim == nil {
		t.Fatalf("claim: %v", err)
	}
	w, err := f.s.LoadDispatch(context.Background(), *r.Claim, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func storePlan(t testing.TB, w dispatch.Work) (provider.Plan, domain.OperationID) {
	t.Helper()
	refs := map[string]domain.ObjectMetadata{}
	for _, r := range w.Job.Objects {
		refs[r.Role] = r.Object
	}
	snapshot, err := dispatch.Snapshot(w.Job, refs)
	if err != nil {
		t.Fatal(err)
	}
	job, op, err := dispatch.ResolveJob(w, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return provider.Plan{Job: job}, op
}

func TestDispatchLostSubmitResponseRestartAndTerminalCollectionGate(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.AcceptLoseResponse
	f := newDispatchFixture(t, scenario)
	id := f.seed(t, "a", 1, false)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if f.journal(t, id).Phase != dispatch.Ready || f.backend.Stats().Executions != 0 {
		t.Fatal("prepare allocated execution")
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	j := f.journal(t, id)
	if j.Phase != dispatch.Submitting || !j.SubmitStarted || !j.Problem.ComputeMayHaveStarted {
		t.Fatal("lost response not recorded")
	}
	f.restart(t)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	j = f.journal(t, id)
	if j.Phase != dispatch.Submitted || j.Remote == nil {
		t.Fatal("reference not rediscovered")
	}
	if err := f.adapter.Advance(*j.Remote); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if f.state(t, id).Orchestration != domain.OrchestrationRunning {
		t.Fatal("running evidence lost")
	}
	if err := f.adapter.Advance(*j.Remote); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	state := f.state(t, id)
	stats := f.backend.Stats()
	if state.Orchestration != domain.OrchestrationCollecting || state.Result != domain.ResultNotAvailable || state.RemoteActivity != domain.RemoteActivityInactive || stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 {
		t.Fatalf("unsafe terminal result: %+v %+v", state, stats)
	}
	if f.journal(t, id).Phase != dispatch.Collectible {
		t.Fatal("collection gate absent")
	}
}
func TestDispatchStagingLossAndDelayedReadinessNeverRepeatPrepare(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.PreparationReady = false
	f := newDispatchFixture(t, scenario)
	id := f.seed(t, "a", 1, false)
	f.adapter.losePrepare = true
	if err := f.step(t); err == nil {
		t.Fatal("lost staging response was hidden")
	}
	f.restart(t)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	j := f.journal(t, id)
	if j.Phase != dispatch.Staging || j.Prepared == nil || j.Prepared.Ready || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("unready staging submitted")
	}
	if err := f.adapter.MarkReady(j.Plan.Job.Identity); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if stats := f.backend.Stats(); stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 {
		t.Fatalf("duplicate staging/submit: %+v", stats)
	}
}
func TestDispatchUnknownNotFoundStopsAndKeepsAccountCapacity(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Unresolved
	f := newDispatchFixture(t, scenario)
	id := f.seed(t, "a", 1, false)
	f.seed(t, "b", 2, false)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < dispatch.MaxFailures; i++ {
		if err := f.step(t); err == nil {
			t.Fatal("unresolved poll did not report a problem")
		}
	}
	j := f.journal(t, id)
	if j.Phase != dispatch.Attention || !j.Problem.ComputeMayHaveStarted {
		t.Fatal("not-found was treated as non-acceptance")
	}
	f.clock.advance(time.Hour)
	worked, err := f.engine.RunOnce(context.Background(), "new-worker")
	if err != nil || worked || f.backend.Stats().SubmitCalls != 1 {
		t.Fatalf("ambiguous account slot was released: %v %v", worked, err)
	}
}
func TestDispatchPublicStagingAndProvenRejection(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(fmt.Sprint(public), func(t *testing.T) {
			scenario := fake.DefaultScenario()
			scenario.Mode = fake.Reject
			f := newDispatchFixture(t, scenario)
			id := f.seed(t, "a", 1, false)
			f.adapter.public = public
			if err := f.step(t); err != nil {
				t.Fatal(err)
			}
			if !public {
				if err := f.step(t); err != nil {
					t.Fatal(err)
				}
			}
			j := f.journal(t, id)
			if public {
				if j.Phase != dispatch.Attention || j.Prepared == nil || j.Prepared.Private || j.Problem.Code != domain.CodePrivateStagingUnavailable {
					t.Fatal("public resource not preserved/stopped")
				}
			} else if j.Phase != dispatch.Rejected || j.Problem.ComputeMayHaveStarted || f.state(t, id).Execution != domain.ExecutionNotSubmitted {
				t.Fatal("proven rejection confused with remote failure")
			}
			before := f.backend.Stats()
			f.clock.advance(time.Hour)
			_, err := f.engine.RunOnce(context.Background(), "again")
			if err != nil {
				t.Fatal(err)
			}
			if f.backend.Stats() != before {
				t.Fatal("rejected/public work retried")
			}
		})
	}
}

type lostDispatchAck struct {
	*Store
	kind dispatch.Kind
	once atomic.Bool
}

func (r *lostDispatchAck) CommitDispatch(ctx context.Context, h dispatch.Handle, a dispatch.Action, now time.Time) (dispatch.Work, error) {
	w, err := r.Store.CommitDispatch(ctx, h, a, now)
	if err == nil && a.Kind == r.kind && r.once.CompareAndSwap(false, true) {
		return dispatch.Work{}, ErrUnavailable
	}
	return w, err
}
func TestDispatchCommitAcknowledgementLossDoesNotRepeatMutation(t *testing.T) {
	for _, kind := range []dispatch.Kind{dispatch.BeginPreparation, dispatch.BeginSubmission, dispatch.SubmissionSeen} {
		t.Run(string(kind), func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			wrapped := &lostDispatchAck{Store: f.s, kind: kind}
			var err error
			f.engine, err = dispatch.New(wrapped, f.registry, f.blobs, nil, f.clock, dispatch.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			if kind != dispatch.BeginPreparation {
				if err = f.step(t); err != nil {
					t.Fatal(err)
				}
			}
			if err = f.step(t); err == nil {
				t.Fatal("lost ack returned success")
			}
			f.restart(t)
			if kind == dispatch.SubmissionSeen {
				if err = f.step(t); err != nil {
					t.Fatal(err)
				}
			} else {
				_ = f.step(t)
			}
			stats := f.backend.Stats()
			if kind == dispatch.BeginPreparation && (stats.PrepareCalls != 0 || stats.SubmitCalls != 0) {
				t.Fatal("recovery created unacknowledged preparation")
			}
			if kind == dispatch.BeginSubmission && stats.SubmitCalls != 0 {
				t.Fatal("recovery submitted unacknowledged intent")
			}
			if kind == dispatch.SubmissionSeen && (stats.SubmitCalls != 1 || stats.Executions != 1) {
				t.Fatal("accepted execution repeated")
			}
			if kind != dispatch.SubmissionSeen && f.journal(t, id).Phase == dispatch.Rejected {
				t.Fatal("missing resource became proven rejection")
			}
		})
	}
}
func TestDispatchRollbackAtLedgerJournalAndEvent(t *testing.T) {
	for _, table := range []string{"provider_resources", "dispatch_journals", "events"} {
		t.Run(table, func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			work := f.claim(t)
			plan, op := storePlan(t, work)
			// Trigger names/tables come only from this static corpus, not external input.
			_, err := f.s.db.Exec("CREATE TRIGGER inject_dispatch_failure BEFORE INSERT ON " + table + " BEGIN SELECT RAISE(ABORT,'injected'); END;")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op}, f.clock.Now()); err == nil {
				t.Fatal("injected commit succeeded")
			}
			if f.journal(t, id).Version != 0 || f.state(t, id) != work.Job.Attempt.State {
				t.Fatal("failed transaction published partial state")
			}
			var n int
			if err = f.s.db.QueryRow("SELECT count(*) FROM provider_resources").Scan(&n); err != nil || n != 0 {
				t.Fatal("orphan intent after rollback")
			}
			if _, err = f.s.db.Exec("DROP TRIGGER inject_dispatch_failure"); err != nil {
				t.Fatal(err)
			}
			if _, err = f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op}, f.clock.Now()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDispatchSubmissionRollbackAndFencedConcurrentGate(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(time.Minute)
	claim, err := f.s.ClaimRecovery(context.Background(), "gate", f.clock.Now())
	if err != nil || claim == nil {
		t.Fatal(err)
	}
	work, err := f.s.LoadDispatch(context.Background(), *claim, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.db.Exec("CREATE TRIGGER fail_submit BEFORE INSERT ON submission_intents BEGIN SELECT RAISE(ABORT,'no'); END;"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginSubmission}, f.clock.Now()); err == nil {
		t.Fatal("partial submission accepted")
	}
	if f.journal(t, id).Phase != dispatch.Ready {
		t.Fatal("submission rollback lost readiness")
	}
	var count int
	_ = f.s.db.QueryRow("SELECT count(*) FROM provider_resources WHERE purpose='execution'").Scan(&count)
	if count != 0 {
		t.Fatal("execution intent leaked from rollback")
	}
	_, _ = f.s.db.Exec("DROP TRIGGER fail_submit")
	var successes atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginSubmission}, f.clock.Now()); e == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("one-shot winners: %d", successes.Load())
	}
	if _, err = f.s.RenewDispatch(context.Background(), work.Handle, f.clock.Now()); err == nil {
		t.Fatal("old handle renewed after state/version advance")
	}
	f.clock.advance(time.Minute)
	newClaim, err := f.s.ClaimRecovery(context.Background(), "takeover", f.clock.Now())
	if err != nil || newClaim == nil {
		t.Fatal(err)
	}
	if newClaim.Generation <= claim.Generation || newClaim.AttemptID != claim.AttemptID {
		t.Fatal("takeover changed attempt instead of ownership")
	}
	if _, err = f.s.LoadDispatch(context.Background(), *claim, f.clock.Now()); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatalf("old worker still owned: %v", err)
	}
}
func TestDispatchPauseAndAccountPolicyBlockNewCallsButAllowObservation(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	settings := scheduler.DefaultSettings()
	settings.Paused = true
	if err := f.s.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); !errors.Is(err, dispatch.ErrPolicy) {
		t.Fatalf("paused submit: %v", err)
	}
	if f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("paused configuration called submit")
	}
	settings.Paused = false
	_ = f.s.ConfigureScheduler(context.Background(), settings)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	_ = f.s.ConfigureAccount(context.Background(), "shared_account", scheduler.AccountPolicy{Disabled: true, MaxActive: 1})
	if err := f.step(t); err != nil {
		t.Fatal("disabling new dispatch blocked observation", err)
	}
	if f.journal(t, id).Observation == nil {
		t.Fatal("observation recovery disappeared")
	}
}

type fixtureFetcher struct {
	calls int
	body  string
}

func (f *fixtureFetcher) Fetch(ctx context.Context, r httpsinput.Request, accept func(context.Context, int64, io.Reader) error) error {
	f.calls++
	return accept(ctx, -1, strings.NewReader(f.body))
}
func TestDispatchURLFreezeSurvivesRestartWithoutRefetch(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, true)
	fetch := &fixtureFetcher{body: "original immutable bytes"}
	f.attach(t, f.s, fetch)
	f.adapter.validateError = true
	if err := f.step(t); err == nil {
		t.Fatal("fixture validation failure ignored")
	}
	if fetch.calls != 1 {
		t.Fatal("input not frozen")
	}
	f.restart(t)
	fetch.body = "changed remote source"
	f.attach(t, f.s, fetch)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if fetch.calls != 1 {
		t.Fatal("frozen URL re-fetched")
	}
	j := f.journal(t, id)
	if j.Plan.Job.Inputs.Inputs[0].Object.SHA256 != provider.Digest([]byte("original immutable bytes")) {
		t.Fatal("snapshot changed")
	}
	raw, _ := json.Marshal(j.Plan)
	if bytes.Contains(raw, []byte("private-source-canary")) {
		t.Fatal("source URL leaked into remote plan")
	}
}
func TestDispatchMalformedBundleAndForeignFreezeNeverStage(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, true)
	w := f.claim(t)
	foreign := domain.ObjectMetadata{ID: "foreign", WorkspaceID: "b", Bytes: 0, SHA256: provider.Digest(nil)}
	if err := f.s.FreezeInput(context.Background(), w.Handle, 0, foreign, f.clock.Now()); err == nil {
		t.Fatal("foreign input accepted")
	}
	_ = f.s.YieldDispatch(context.Background(), w.Handle, f.clock.Now(), 0)
	f.attach(t, f.s, &fixtureFetcher{body: "input"})
	// Replace only disposable fixture bytes; never a source or operator directory.
	// The controlled BlobStore lies about bundle bytes to exercise orchestration revalidation.
	bad := &corruptBundle{BlobStore: f.blobs}
	var err error
	f.engine, err = dispatch.New(f.s, f.registry, bad, &fixtureFetcher{body: "input"}, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.step(t); err == nil {
		t.Fatal("corrupt bundle accepted")
	}
	if f.journal(t, id).Phase != dispatch.Failed || f.backend.Stats().PrepareCalls != 0 {
		t.Fatal("invalid bytes reached staging")
	}
}

type corruptBundle struct{ dispatch.BlobStore }

func (b *corruptBundle) Open(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	if id == "code" {
		return io.NopCloser(strings.NewReader("not-a-bundle")), nil
	}
	return b.BlobStore.Open(ctx, w, id)
}
func TestDispatchProviderPanicAfterAcceptanceIsReconciled(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	f.adapter.panicSubmit = true
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	j := f.journal(t, id)
	raw, _ := json.Marshal(j)
	if j.Phase != dispatch.Submitting || bytes.Contains(raw, []byte("secret-canary")) {
		t.Fatal("panic escaped ambiguity/redaction")
	}
	f.restart(t)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if f.backend.Stats().SubmitCalls != 1 || f.journal(t, id).Remote == nil {
		t.Fatal("panic retried compute")
	}
}
func TestDispatchFrozenProfileDoesNotFollowRemap(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	changed := f.profile
	changed.Binding.ConfigurationRevision = "two"
	changed.Binding.ProviderInstanceID = "other"
	changed.AccountScope = "other_account"
	if err := f.s.PutProfile(context.Background(), changed, true); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if f.journal(t, id).Plan.Job.Identity.InstanceID != f.profile.Binding.ProviderInstanceID {
		t.Fatal("profile silently remapped")
	}
}
func TestDispatchDoesNotExecutePayloadOnHost(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	f.seed(t, "a", 1, false)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(f.root, "answer.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unexpected local workload output")
	}
	if f.backend.Stats().Executions != 0 {
		t.Fatal("preparation triggered execution")
	}
}

func TestDispatchRejectedQuotaLatchesWithoutInventedAllowance(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Reject
	f := newDispatchFixture(t, scenario)
	f.adapter.rejectQuota = true
	id := f.seed(t, "a", 1, false)
	f.seed(t, "b", 2, false)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	j := f.journal(t, id)
	if j.Problem == nil || j.Problem.Code != domain.CodeQuotaExhausted {
		t.Fatal("quota rejection was hidden")
	}
	var raw string
	var exhausted bool
	if err := f.s.db.QueryRow("SELECT observation,exhausted FROM scheduler_quotas WHERE account_scope=? AND resource='cpu'", f.profile.AccountScope).Scan(&raw, &exhausted); err != nil {
		t.Fatal(err)
	}
	var q provider.QuotaObservation
	if json.Unmarshal([]byte(raw), &q) != nil || !exhausted || q.Limit != nil || q.Remaining != nil || q.Used != nil || q.Status != provider.QuotaUnknown {
		t.Fatal("invented provider values", raw)
	}
	f.clock.advance(time.Hour)
	worked, err := f.engine.RunOnce(context.Background(), "quota_blocked")
	if err != nil || worked {
		t.Fatal("exhausted account automatically submitted another job", worked, err)
	}
	if stats := f.backend.Stats(); stats.SubmitCalls != 1 || stats.Executions != 0 {
		t.Fatal(stats)
	}
}

func TestDispatchDiskFullRollsBackIntentAndBarrier(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	work := f.claim(t)
	plan, op := storePlan(t, work)
	if _, err := f.s.db.Exec("CREATE TABLE dispatch_fill(value BLOB) STRICT"); err != nil {
		t.Fatal(err)
	}
	var pages, maximum int64
	if err := f.s.db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if err := f.s.db.QueryRow(fmt.Sprintf("PRAGMA max_page_count=%d", pages+1)).Scan(&maximum); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.db.Exec("CREATE TRIGGER dispatch_full BEFORE INSERT ON provider_resources BEGIN INSERT INTO dispatch_fill VALUES(zeroblob(1048576)); END"); err != nil {
		t.Fatal(err)
	}
	_, err := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op}, f.clock.Now())
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("expected real SQLITE_FULL: %v", err)
	}
	var n int
	if err = f.s.db.QueryRow("SELECT count(*) FROM provider_resources").Scan(&n); err != nil || n != 0 {
		t.Fatal("partial resource", n, err)
	}
	if j := f.journal(t, id); j.Phase != dispatch.Local || j.Version != 0 {
		t.Fatal("partial journal", j.Phase)
	}
	if err = f.s.db.QueryRow("SELECT dispatch_barrier FROM scheduler_queue WHERE queue_seq=?", work.Handle.Claim.Sequence).Scan(&n); err != nil || n != 0 {
		t.Fatal("partial barrier", n, err)
	}
	if stats := f.backend.Stats(); stats.PrepareCalls != 0 || stats.SubmitCalls != 0 {
		t.Fatal(stats)
	}
}
