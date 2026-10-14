package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/testsupport"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

const dispatchJSON = `{"api_version":"compute-connector/v1alpha1","name":"dispatch-fixture","profile":"fixture","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","never-executed.py"]},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":2,"finalization_grace_seconds":2}}`

type dispatchClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *dispatchClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *dispatchClock) advance(d time.Duration) { c.mu.Lock(); c.at = c.at.Add(d); c.mu.Unlock() }

type dispatchFixture struct {
	s             *Store
	root          string
	blobs         *blobfs.Store
	clock         *dispatchClock
	profile       admission.Profile
	registry      *provider.SnapshotRegistry
	backend       *fake.Backend
	adapter       *probeProvider
	engine        *dispatch.Engine
	authorizeSeed bool
	extraProfiles map[domain.WorkspaceID]admission.Profile
}
type probeProvider struct {
	*fake.Bound
	store       *Store
	losePrepare bool
	rejectQuota bool
}

func (p *probeProvider) Prepare(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.Prepared, error) {
	var n int
	if err := p.store.db.QueryRowContext(ctx, "SELECT count(*) FROM provider_resources WHERE operation_id=? AND purpose='staging'", string(op)).Scan(&n); err != nil || n != 1 {
		return provider.Prepared{}, errors.New("prepare called without committed ownership intent")
	}
	prepared, err := p.Bound.Prepare(ctx, plan, op)
	if p.losePrepare {
		return provider.Prepared{}, errors.New("private-response-canary")
	}
	return prepared, err
}
func (p *probeProvider) Start(ctx context.Context, prepared provider.Prepared, authorization *provider.StartAuthorization, material *workloadsecret.Material) provider.SubmissionOutcome {
	var n int
	if err := p.store.db.QueryRowContext(ctx, "SELECT count(*) FROM submission_intents WHERE intent_id=? AND status='started'", string(prepared.Identity.IntentID)).Scan(&n); err != nil || n != 1 {
		panic("submit called without committed intent")
	}
	if authorization != nil {
		if material != nil {
			return p.startWithSecrets(ctx, prepared, *authorization, *material)
		}
		return p.startControlled(ctx, prepared, *authorization)
	}
	outcome := p.Bound.Start(ctx, prepared, authorization, material)
	if p.rejectQuota && outcome.Status == provider.SubmissionRejected {
		problem := provider.Problem(domain.CodeQuotaExhausted, domain.FailureStageSubmission, "fixture exhausted")
		outcome.Problem = &problem
	}
	return outcome
}
func fixtureBundle(t testing.TB) []byte {
	t.Helper()
	return testsupport.Bundle(t, map[string][]byte{
		"never-executed.py": []byte("raise SystemExit('fixture must never run on the control host')\n"),
	})
}
func newDispatchFixture(t testing.TB, scenario fake.Scenario) *dispatchFixture {
	t.Helper()
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "state")
	testClock := &dispatchClock{at: time.Now().UTC().Truncate(time.Millisecond).Add(time.Second)}
	opts := DefaultOptions()
	opts.Clock = testClock.Now
	s, err := Open(ctx, root, opts)
	requireNoError(t, err)
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	requireNoError(t, err)
	f := &dispatchFixture{authorizeSeed: true, extraProfiles: map[domain.WorkspaceID]admission.Profile{}, s: s, root: root, blobs: blobs, clock: testClock}
	t.Cleanup(func() { _ = f.s.Close(); _ = blobs.Close() })
	for _, w := range []domain.WorkspaceID{"a", "b"} {
		err = s.PutWorkspace(ctx, auth.Workspace{ID: w, Enabled: true, AllowedProfiles: []string{"fixture"}})
		requireNoError(t, err)
		for id, data := range map[domain.ObjectID][]byte{"code": fixtureBundle(t), "data": []byte("frozen-input")} {
			m, err := blobs.Put(ctx, domain.ObjectMetadata{ID: id, WorkspaceID: w, Bytes: -1}, bytes.NewReader(data))
			requireNoError(t, err)
			err = s.CommitObject(ctx, m)
			requireNoError(t, err)
		}
	}
	f.profile = admission.DefaultProfile(domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "fake_1", ConfigurationRevision: "one"}, "shared_account")
	f.profile.CredentialRef = "vault:con_fixture"
	f.profile.MaxActiveAttempts = 64
	err = s.PutProfile(ctx, f.profile, true)
	requireNoError(t, err)
	err = s.ConfigureScheduler(ctx, scheduler.DefaultSettings())
	requireNoError(t, err)
	f.backend, err = fake.NewBackend(scenario)
	requireNoError(t, err)
	f.attach(t, s)
	return f
}
func (f *dispatchFixture) attach(t testing.TB, s *Store) {
	t.Helper()
	f.s = s
	s.options.Clock = f.clock.Now
	binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
	p, err := fake.NewBound(f.backend, f.clock, binding)
	requireNoError(t, err)
	f.adapter = &probeProvider{Bound: p, store: s}
	f.registry = provider.NewSnapshotRegistry()
	err = f.registry.Register(binding, f.adapter)
	requireNoError(t, err)
	for _, profile := range f.extraProfiles {
		binding := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
		adapter, bindErr := fake.NewBound(f.backend, f.clock, binding)
		if bindErr != nil {
			t.Fatal(bindErr)
		}
		if bindErr = f.registry.Register(binding, &probeProvider{Bound: adapter, store: s}); bindErr != nil {
			t.Fatal(bindErr)
		}
	}
	f.engine, err = dispatch.New(s, f.registry, f.blobs, f.clock, dispatch.DefaultConfig())
	requireNoError(t, err)
}

// These admitted records support private transaction and ledger invariants.
// System scenarios use runtimehost's shared managed fixture and public services.
func (f *dispatchFixture) seed(t testing.TB, w domain.WorkspaceID, number int, url bool, collectionMode ...string) scheduler.Identity {
	t.Helper()
	raw := dispatchJSON
	if len(collectionMode) == 1 {
		raw = strings.Replace(raw, `"name":"dispatch-fixture"`, `"name":"dispatch-fixture","result_collection":"`+collectionMode[0]+`"`, 1)
	}
	if url {
		raw = strings.Replace(raw, `"kind":"object","object_id":"data"`, `"kind":"https","url":"https://example.com/input?name=private-source-canary"`, 1)
	}
	return f.seedRaw(t, w, number, url, raw)
}

func (f *dispatchFixture) seedRaw(t testing.TB, w domain.WorkspaceID, number int, url bool, raw string) scheduler.Identity {
	t.Helper()
	ctx := context.Background()
	profile := f.profile
	if f.authorizeSeed {
		if w != "a" {
			profile.Binding.Profile += "_" + string(w)
			profile.CredentialRef += "_" + string(w)
			raw = strings.Replace(raw, `"profile":"`+f.profile.Binding.Profile+`"`, `"profile":"`+profile.Binding.Profile+`"`, 1)
			requireNoError(t, f.s.PutProfile(ctx, profile, true))
			f.extraProfiles[w] = profile
			binding := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope, CredentialRef: profile.CredentialRef}
			adapter, err := fake.NewBound(f.backend, f.clock, binding)
			requireNoError(t, err)
			err = f.registry.Register(binding, &probeProvider{Bound: adapter, store: f.s})
			requireNoError(t, err)
		}
		connection := strings.TrimPrefix(profile.CredentialRef, "vault:")
		_, err := f.s.db.Exec(`INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,current_profile,updated_at) VALUES(?,?,'fixture','fixture',1,'verified','enabled','fixture_account',?,'fixture_key',?,?) ON CONFLICT(connection_id) DO NOTHING`, connection, w, profile.AccountScope, profile.Binding.Profile, f.clock.Now().Format(time.RFC3339Nano))
		requireNoError(t, err)
	}
	req, err := admission.Parse([]byte(raw))
	requireNoError(t, err)
	id := scheduler.Identity{WorkspaceID: w, JobID: domain.JobID(fmt.Sprintf("job_%d", number)), AttemptID: domain.AttemptID(fmt.Sprintf("attempt_%d", number))}
	at := f.clock.Now().Add(-time.Millisecond).Format(time.RFC3339Nano)
	state, _ := json.Marshal(domain.InitialAttemptState())
	err = withTx(ctx, f.s.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?)", string(w), string(id.JobID), string(req.Canonical()), admission.CanonicalVersion, string(req.Hash()), profile.Binding.Profile, profile.Binding.ConfigurationRevision, string(id.AttemptID), at)
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
	requireNoError(t, err)
	if f.authorizeSeed && !req.Spec().SeparateStart() {
		service, actor := authorizationService(t, f, w, auth.Execute)
		_, err := service.Authorize(ctx, actor, w, id.JobID, "seed-grant-"+string(id.AttemptID), authorizationRequest(id.AttemptID, req.Spec().Timeouts.RemoteWallSeconds))
		requireNoError(t, err)
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
	requireNoError(t, f.s.db.QueryRow("SELECT queue_seq FROM scheduler_queue WHERE workspace_id=? AND job_id=? AND attempt_id=?", string(id.WorkspaceID), string(id.JobID), string(id.AttemptID)).Scan(&seq))
	err := withTx(context.Background(), f.s.db, func(tx *sql.Tx) error { var err error; j, err = loadJournal(context.Background(), tx, seq); return err })
	requireNoError(t, err)
	return j
}
func (f *dispatchFixture) state(t testing.TB, id scheduler.Identity) domain.AttemptState {
	t.Helper()
	a, _, err := loadAttempt(context.Background(), f.s.db, id.WorkspaceID, id.JobID, id.AttemptID)
	requireNoError(t, err)
	return a.State
}
func (f *dispatchFixture) restart(t testing.TB) {
	t.Helper()
	requireNoError(t, f.s.Close())
	s, err := Open(context.Background(), f.root, DefaultOptions())
	requireNoError(t, err)
	f.attach(t, s)
}
func (f *dispatchFixture) claim(t testing.TB) dispatch.Work {
	t.Helper()
	r, err := f.s.ClaimNext(context.Background(), "manual")
	if err != nil || r.Claim == nil {
		t.Fatalf("claim: %v", err)
	}
	w, err := f.s.LoadDispatch(context.Background(), *r.Claim)
	requireNoError(t, err)
	w, err = f.s.ConsumeAuthorization(context.Background(), w.Handle)
	requireNoError(t, err)
	return w
}
func storePlan(t testing.TB, w dispatch.Work) (provider.Plan, domain.OperationID) {
	t.Helper()
	refs := map[string]domain.ObjectMetadata{}
	for _, r := range w.Job.Objects {
		refs[r.Role] = r.Object
	}
	snapshot, err := dispatch.Snapshot(w.Job, refs)
	requireNoError(t, err)
	job, op, err := dispatch.ResolveJob(w, snapshot)
	requireNoError(t, err)
	return provider.Plan{Job: job}, op
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
			requireNoError(t, err)
			if _, err = f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op}); err == nil {
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
			if _, err = f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDispatchSubmissionRollbackAndFencedConcurrentGate(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	requireNoError(t, f.step(t))
	f.clock.advance(time.Minute)
	claim, err := f.s.ClaimRecovery(context.Background(), "gate")
	if err != nil || claim == nil {
		t.Fatal(err)
	}
	work, err := f.s.LoadDispatch(context.Background(), *claim)
	requireNoError(t, err)
	if _, err = f.s.db.Exec("CREATE TRIGGER fail_submit BEFORE INSERT ON submission_intents BEGIN SELECT RAISE(ABORT,'no'); END;"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginSubmission}); err == nil {
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
			if _, e := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginSubmission}); e == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("one-shot winners: %d", successes.Load())
	}
	if _, err = f.s.RenewDispatch(context.Background(), work.Handle); err == nil {
		t.Fatal("old handle renewed after state/version advance")
	}
	f.clock.advance(time.Minute)
	newClaim, err := f.s.ClaimRecovery(context.Background(), "takeover")
	if err != nil || newClaim == nil {
		t.Fatal(err)
	}
	if newClaim.Generation <= claim.Generation || newClaim.AttemptID != claim.AttemptID {
		t.Fatal("takeover changed attempt instead of ownership")
	}
	if _, err = f.s.LoadDispatch(context.Background(), *claim); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatalf("old worker still owned: %v", err)
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
	requireNoError(t, f.s.db.QueryRow("PRAGMA page_count").Scan(&pages))
	requireNoError(t, f.s.db.QueryRow(fmt.Sprintf("PRAGMA max_page_count=%d", pages+1)).Scan(&maximum))
	if _, err := f.s.db.Exec("CREATE TRIGGER dispatch_full BEFORE INSERT ON provider_resources BEGIN INSERT INTO dispatch_fill VALUES(zeroblob(1048576)); END"); err != nil {
		t.Fatal(err)
	}
	_, err := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op})
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
