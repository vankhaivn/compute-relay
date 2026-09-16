package kaggle

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// Only this test composes a fake batch provider around the concrete Stager. No
// production Provider, credential path or compute-enabled registry is added.
type journalStagingAdapter struct {
	*fake.Bound
	stage *Stager
}

func (p *journalStagingAdapter) Prepare(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.Prepared, error) {
	return p.stage.Prepare(ctx, plan, op)
}
func (p *journalStagingAdapter) ReconcilePreparation(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.PreparationObservation, error) {
	return p.stage.ReconcilePreparation(ctx, plan, op)
}
func (p *journalStagingAdapter) Submit(context.Context, provider.Prepared) provider.SubmissionOutcome {
	panic("staging-only fixture must never submit compute")
}

type stagingClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *stagingClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *stagingClock) advance()       { c.mu.Lock(); c.at = c.at.Add(10 * time.Minute); c.mu.Unlock() }

type stagingJournalRepository struct {
	dispatch.Repository
	failBefore bool
	lose       dispatch.Kind
}

var errStagingCommitFixture = errors.New("synthetic staging commit acknowledgement loss")

func (r *stagingJournalRepository) CommitDispatch(ctx context.Context, h dispatch.Handle, a dispatch.Action, now time.Time) (dispatch.Work, error) {
	if a.Kind == dispatch.BeginSubmission {
		return dispatch.Work{}, errors.New("staging fixture attempted compute admission")
	}
	if a.Kind == dispatch.BeginPreparation && r.failBefore {
		r.failBefore = false
		return dispatch.Work{}, errStagingCommitFixture
	}
	w, err := r.Repository.CommitDispatch(ctx, h, a, now)
	if err == nil && r.lose == a.Kind {
		r.lose = ""
		return dispatch.Work{}, errStagingCommitFixture
	}
	return w, err
}

type stagingJournalFixture struct {
	root                                       string
	store                                      *sqlite.Store
	blobs                                      *blobfs.Store
	clock                                      *stagingClock
	profile                                    admission.Profile
	config                                     Config
	receipt                                    admission.Receipt
	engine                                     *dispatch.Engine
	repo                                       *stagingJournalRepository
	creates, observes                          int
	created, ready, public, swapped, loseCreate bool
	marker                                     []byte
}

func stagingJournalBundle(t *testing.T) []byte {
	t.Helper()
	payload := []byte("raise SystemExit('must never run on the control host')\n")
	manifest, err := json.Marshal(packaging.Manifest{Version: packaging.Version, Files: []packaging.File{{Path: "never-executed.py", Bytes: int64(len(payload)), SHA256: string(provider.Digest(payload))}}})
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	gz.Header.OS = 255
	tw := tar.NewWriter(gz)
	for _, f := range []struct {
		name string
		data []byte
	}{{packaging.ManifestPath, manifest}, {"code/never-executed.py", payload}} {
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Size: int64(len(f.data)), Mode: 0644, Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0).UTC(), Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(f.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func newStagingJournalFixture(t *testing.T) *stagingJournalFixture {
	t.Helper()
	ctx := context.Background()
	f := &stagingJournalFixture{root: filepath.Join(t.TempDir(), "state"), clock: &stagingClock{at: time.Now().UTC().Add(time.Second)}}
	var err error
	f.store, err = sqlite.Open(ctx, f.root, sqlite.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.store.Close() })
	f.blobs, err = blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.blobs.Close() })
	if err = f.store.PutWorkspace(ctx, auth.Workspace{ID: "workspace", Enabled: true, AllowedProfiles: []string{"fixture"}}); err != nil {
		t.Fatal(err)
	}
	f.config = Config{InstanceID: "fixture_instance", Revision: "one", AccountName: "fixture_account", CredentialRef: "env:STAGING_FIXTURE_TOKEN", PythonExecutable: filepath.Join(t.TempDir(), "never-invoked.exe")}
	f.profile = admission.DefaultProfile(domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: domain.ProviderInstanceID(f.config.InstanceID), ConfigurationRevision: f.config.Revision}, f.config.AccountName)
	f.profile.CredentialRef = string(f.config.CredentialRef)
	if err = f.store.PutProfile(ctx, f.profile, true); err != nil {
		t.Fatal(err)
	}
	if err = f.store.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
		t.Fatal(err)
	}
	for id, data := range map[domain.ObjectID][]byte{"code": stagingJournalBundle(t), "input": []byte("frozen-input")} {
		m, err := f.blobs.Put(ctx, domain.ObjectMetadata{ID: id, WorkspaceID: "workspace", Bytes: -1}, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		if err = f.store.CommitObject(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	access, err := auth.New(f.store, f.store, nil)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := access.Issue(ctx, "workspace", []auth.Scope{auth.Read, auth.Write, auth.Operate}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	actor, err := access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := admission.New(access, f.store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"api_version":"compute-connector/v1alpha1","name":"staging-journal-fixture","profile":"fixture","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","never-executed.py"]},"inputs":[{"name":"input","source":{"kind":"object","object_id":"input"},"target":"data.txt"}],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":2,"finalization_grace_seconds":2}}`)
	f.receipt, err = jobs.Submit(ctx, actor, "workspace", "staging-journal-key", raw)
	if err != nil {
		t.Fatal(err)
	}
	f.attach(t)
	return f
}

// An independent READ-ONLY connection asserts facts visible after commit, not
// just the return value from an in-memory repository wrapper. It is closed before
// each restart and never uses immutable=1 on the live WAL database.
func (f *stagingJournalFixture) inspect(t *testing.T, inspect func(*sql.DB)) {
	t.Helper()
	p := filepath.ToSlash(filepath.Join(f.root, "runtime.db"))
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	u := url.URL{Scheme: "file", Path: p, RawQuery: "mode=ro&cache=private"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	inspect(db)
}
func (f *stagingJournalFixture) journal(t *testing.T) dispatch.Journal {
	t.Helper()
	var journal dispatch.Journal
	f.inspect(t, func(db *sql.DB) {
		var raw string
		if err := db.QueryRow("SELECT journal FROM dispatch_journals").Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if json.Unmarshal([]byte(raw), &journal) != nil || !journal.Valid() {
			t.Fatal("invalid durable journal")
		}
	})
	return journal
}
func (f *stagingJournalFixture) noCompute(t *testing.T) {
	t.Helper()
	f.inspect(t, func(db *sql.DB) {
		for _, q := range []string{"SELECT count(*) FROM submission_intents", "SELECT count(*) FROM provider_resources WHERE purpose='execution'", "SELECT count(*) FROM events WHERE type='submission.intent_recorded'"} {
			var n int
			if err := db.QueryRow(q).Scan(&n); err != nil || n != 0 {
				t.Fatal("unexpected compute intent", q, n, err)
			}
		}
		var n int
		if err := db.QueryRow("SELECT count(*) FROM attempts").Scan(&n); err != nil || n != 1 {
			t.Fatal("new attempt created", n, err)
		}
	})
}
func (f *stagingJournalFixture) attach(t *testing.T) {
	t.Helper()
	resolver, err := credentials.NewEnvironment([]ports.CredentialRef{f.config.CredentialRef}, func(string) (string, bool) { return "SYNTHETIC_TOKEN", true })
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewStager(f.config, DefaultStagingPolicy(), resolver, f.blobs, true)
	if err != nil {
		t.Fatal(err)
	}
	s.local = func(context.Context, Config, Mode, []byte) (Report, error) { return baseline(Local), nil }
	s.run = func(ctx context.Context, c Config, mode string, token []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
		if c != f.config || string(token) != "SYNTHETIC_TOKEN" {
			t.Fatal("changed frozen credential binding")
		}
		j := f.journal(t)
		f.inspect(t, func(db *sql.DB) {
			var n int
			if err := db.QueryRow("SELECT count(*) FROM provider_resources WHERE purpose='staging' AND operation_id=? AND plan_sha256=? AND cleanup_state='pinned'", string(j.PreparationID), string(j.Plan.Digest())).Scan(&n); err != nil || n != 1 {
				t.Fatal("helper called before durable ownership commit", n, err)
			}
		})
		if f.marker == nil {
			f.marker = append([]byte(nil), p.marker...)
		} else if !bytes.Equal(f.marker, p.marker) {
			t.Fatal("restart/remapping changed frozen marker")
		}
		if mode == "create" {
			f.creates++
			f.created = true
			if f.loseCreate {
				f.loseCreate = false
				return stagingResponse{}, errors.New("synthetic lost create acknowledgement")
			}
		} else {
			f.observes++
		}
		if !f.created {
			return stageResponse(p, "not_found"), nil
		}
		status := "pending"
		if f.ready {
			status = "ready"
		}
		r := stageResponse(p, status)
		r.Private = !f.public
		if f.swapped {
			r.DatasetID = "124"
		}
		return r, nil
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
	if err = registry.Register(binding, &journalStagingAdapter{Bound: base, stage: s}); err != nil {
		t.Fatal(err)
	}
	f.repo = &stagingJournalRepository{Repository: f.store}
	f.engine, err = dispatch.New(f.repo, registry, f.blobs, nil, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
}
func (f *stagingJournalFixture) restart(t *testing.T) {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = sqlite.Open(context.Background(), f.root, sqlite.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	f.attach(t)
}
func (f *stagingJournalFixture) step(t *testing.T, wantError bool) {
	t.Helper()
	f.clock.advance()
	worked, err := f.engine.RunOnce(context.Background(), "staging_fixture_worker")
	if !worked || (err != nil) != wantError {
		t.Fatal("unexpected staging step", worked, err, wantError)
	}
	f.noCompute(t)
}

func TestStagingM3JournalSurvivesLostAcknowledgementsAndRemap(t *testing.T) {
	for _, mode := range []string{"pending", "lost-create", "lost-observation-ack"} {
		t.Run(mode, func(t *testing.T) {
			f := newStagingJournalFixture(t)
			f.loseCreate = mode == "lost-create"
			if mode == "lost-observation-ack" {
				f.repo.lose = dispatch.PreparationSeen
			}
			f.step(t, mode != "pending")
			before := f.journal(t)
			if before.Phase != dispatch.Staging || before.SubmitStarted || f.creates != 1 {
				t.Fatal("creation became dispatch/readiness", before.Phase)
			}
			if before.Prepared != nil && before.Prepared.Ready {
				t.Fatal("pending became ready")
			}
			changed := f.profile
			changed.Binding.ConfigurationRevision = "replacement"
			changed.AccountScope = "other_account"
			if err := f.store.PutProfile(context.Background(), changed, true); err != nil {
				t.Fatal(err)
			}
			f.ready = true
			f.restart(t)
			f.step(t, false)
			after := f.journal(t)
			if after.Phase != dispatch.Ready || after.Prepared == nil || !after.Prepared.Private || !after.Prepared.Ready || after.PreparationID != before.PreparationID || after.Plan.Digest() != before.Plan.Digest() || f.creates != 1 || f.observes != 1 {
				t.Fatal("recovery changed staging identity or repeated creation")
			}
			f.inspect(t, func(db *sql.DB) {
				var ref, state string
				if err := db.QueryRow("SELECT resource_ref,readiness FROM provider_resources WHERE purpose='staging'").Scan(&ref, &state); err != nil || ref != after.Prepared.Resource || state != "ready" {
					t.Fatal("publication and ledger diverged", err)
				}
			})
		})
	}
}
func TestStagingM3LostIntentAcknowledgementNeverRecreates(t *testing.T) {
	f := newStagingJournalFixture(t)
	f.repo.lose = dispatch.BeginPreparation
	f.step(t, true)
	f.restart(t)
	for i := 0; i < dispatch.MaxFailures; i++ {
		f.step(t, true)
	}
	j := f.journal(t)
	if f.creates != 0 || f.observes != dispatch.MaxFailures || j.Phase != dispatch.Attention || j.Prepared != nil {
		t.Fatal("unacknowledged intent authorized mutation", f.creates, f.observes, j.Phase)
	}
}
func TestStagingM3FailedIntentAllowsOnlyUncommittedWorkToRetry(t *testing.T) {
	f := newStagingJournalFixture(t)
	f.repo.failBefore = true
	f.step(t, true)
	if f.creates != 0 || f.observes != 0 {
		t.Fatal("failed intent reached helper")
	}
	f.inspect(t, func(db *sql.DB) {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM provider_resources").Scan(&n); err != nil || n != 0 {
			t.Fatal("uncommitted ownership escaped", err)
		}
	})
	f.restart(t)
	f.step(t, false)
	f.ready = true
	f.step(t, false)
	if f.creates != 1 || f.journal(t).Phase != dispatch.Ready {
		t.Fatal("safe local retry lost identity")
	}
}
func TestStagingM3RejectsResourceReplacementAndPublicVisibility(t *testing.T) {
	for _, mode := range []string{"replacement", "public"} {
		t.Run(mode, func(t *testing.T) {
			f := newStagingJournalFixture(t)
			f.public = mode == "public"
			f.step(t, false)
			before := f.journal(t)
			if mode == "public" {
				if before.Phase != dispatch.Attention || before.Prepared == nil || before.Prepared.Private {
					t.Fatal("public staging not quarantined")
				}
				return
			}
			original := before.Prepared.Resource
			f.ready = true
			f.swapped = true
			f.restart(t)
			f.step(t, true)
			after := f.journal(t)
			if after.Phase == dispatch.Ready || after.Prepared == nil || after.Prepared.Resource != original || f.creates != 1 {
				t.Fatal("replacement retargeted journal")
			}
		})
	}
}
