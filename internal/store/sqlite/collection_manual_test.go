package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func collectionStatus(t *testing.T, f *dispatchFixture, id scheduler.Identity) domain.CollectionStatus {
	t.Helper()
	var status *domain.CollectionStatus
	err := withTx(context.Background(), f.s.db, func(tx *sql.Tx) error {
		r, err := readJobRecord(context.Background(), tx, id.WorkspaceID, id.JobID)
		if err != nil {
			return err
		}
		status, err = readCollectionStatus(context.Background(), tx, r, f.clock.Now())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return *status
}

func TestManualCollectionWaitsAcrossRestartAndExplicitKeysCoalesce(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t, "manual")
	for i := 0; i < 2; i++ {
		if status := collectionStatus(t, f, id); status.State != "awaiting_request" || status.Mode != "manual" || status.OperationID != nil || status.Progress != nil {
			t.Fatal(status)
		}
		if worked, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(ctx); worked || err != nil {
			t.Fatal("manual job auto-collected", worked, err)
		}
		if lists, fetches := p.counts(); lists != 0 || fetches != 0 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'") != 0 {
			t.Fatal("manual waiting performed artifact work")
		}
		f.restart(t)
	}
	service, principal, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	var wg sync.WaitGroup
	results := make(chan operations.Record, 2)
	failures := make(chan error, 2)
	for _, key := range []string{"download-first-key", "download-second-key"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			r, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, key, controlRequest(id.AttemptID))
			results <- r
			failures <- err
		}(key)
	}
	wg.Wait()
	first, second := <-results, <-results
	if e1, e2 := <-failures, <-failures; e1 != nil || e2 != nil || first.Operation.ID != second.Operation.ID {
		t.Fatal(first, second, e1, e2)
	}
	if status := collectionStatus(t, f, id); status.State != "pending" || status.OperationID == nil {
		t.Fatal(status)
	}
	f.restart(t)
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	status := collectionStatus(t, f, id)
	if status.State != "available" || status.Progress == nil || status.Progress.BytesReceived != nil || *status.Progress.BytesTotal != int64(len(p.files["outputs/answer.json"])) || status.Progress.BytesCompleted != *status.Progress.BytesTotal {
		t.Fatal(status)
	}
	service, principal, _ = controlService(t, f, "a", auth.Read, auth.Operate)
	r, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "download-first-key", controlRequest(id.AttemptID))
	if err != nil || !r.Replay || r.Operation.ID != first.Operation.ID || r.Operation.Status != domain.OperationAccepted {
		t.Fatal("original receipt changed", r, err)
	}
	_, before := p.counts()
	if _, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "download-already-available", controlRequest(id.AttemptID)); err != nil {
		t.Fatal(err)
	}
	if worked, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(ctx); worked || err != nil {
		t.Fatal(worked, err)
	}
	_, after := p.counts()
	if after != before {
		t.Fatal("available collection refetched bytes")
	}
	assertNoNewCompute(t, f)
}

func TestAutomaticCollectionAndManualFailureDiagnostics(t *testing.T) {
	for _, mode := range []string{"", "automatic", "manual"} {
		t.Run(mode, func(t *testing.T) {
			var modes []string
			if mode != "" {
				modes = []string{mode}
			}
			f, id, p, blobs := collectionFixture(t, modes...)
			if mode == "manual" {
				scenario := fake.DefaultScenario()
				scenario.States = []domain.ExecutionState{domain.ExecutionQueued, domain.ExecutionRunning, domain.ExecutionFailed}
				f, id, p, blobs = collectionScenarioFixture(t, scenario, "manual")
				var manifest map[string]any
				_ = json.Unmarshal(p.files[collection.ManifestPath], &manifest)
				manifest["phase"], manifest["exit_code"], manifest["error"] = "failed", 1, map[string]any{"code": "COMMAND_FAILED", "message": "fixture failure", "stage": "execution"}
				manifest["artifacts"] = []any{}
				delete(p.files, "outputs/answer.json")
				p.files[collection.ManifestPath], _ = json.Marshal(manifest)
				if worked, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(context.Background()); worked || err != nil {
					t.Fatal(worked, err)
				}
				service, principal, _ := controlService(t, f, "a", auth.Operate)
				if _, err := service.Submit(context.Background(), principal, "a", id.JobID, domain.OperationCollect, "explicit-diagnostic-download", controlRequest(id.AttemptID)); err != nil {
					t.Fatal(err)
				}
			}
			runCollection(t, collectionEngine(t, f, p, blobs, f.s))
			if f.state(t, id).Result != domain.ResultAvailable {
				t.Fatal("result unavailable")
			}
			if mode == "manual" && f.state(t, id).Orchestration == domain.OrchestrationSucceeded {
				t.Fatal("diagnostic publication became business success")
			}
			assertNoNewCompute(t, f)
		})
	}
}

type progressCollectionProvider struct {
	*collectionProvider
	clock *dispatchClock
	hook  func(int64) error
}

func (p *progressCollectionProvider) FetchArtifactWithProgress(ctx context.Context, remote provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	return p.FetchArtifact(ctx, remote, file, &sampleWriter{dst: dst, progress: progress, clock: p.clock, hook: p.hook}, limit)
}

type sampleWriter struct {
	dst      io.Writer
	progress func(int64) error
	clock    *dispatchClock
	n        int64
	hook     func(int64) error
}

func (w *sampleWriter) Write(data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		part := data[:min(len(data), 1024)]
		n, err := w.dst.Write(part)
		written += n
		w.n += int64(n)
		if err != nil {
			return written, err
		}
		w.clock.advance(50 * time.Millisecond)
		if err = w.progress(w.n); err != nil {
			return written, err
		}
		if w.hook != nil {
			if err = w.hook(w.n); err != nil {
				return written, err
			}
		}
		data = data[n:]
	}
	return written, nil
}

func observedEngine(t *testing.T, f *dispatchFixture, p *progressCollectionProvider, blobs collection.BlobStore, repo collection.Repository) *collection.Engine {
	t.Helper()
	registry := provider.NewSnapshotRegistry()
	p.probeProvider = f.adapter
	if err := registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, p); err != nil {
		t.Fatal(err)
	}
	e, err := collection.New(repo, registry, blobs, f.clock, collection.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestCollectionObservedProgressIsThrottledAndExcludesControlsAndCache(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "stream", true: "cache"}[cached], func(t *testing.T) {
			f, id, p, blobs := collectionFixture(t)
			if cached {
				j := f.journal(t, id)
				w := collection.Work{Lease: collection.Lease{WorkspaceID: id.WorkspaceID, JobID: id.JobID, AttemptID: id.AttemptID, OperationID: "fixture", Generation: 1, Fence: strings.Repeat("a", 32), Until: f.clock.Now().Add(time.Minute), AttemptRevision: 1}, Plan: *j.Plan, Observation: *j.Observation, Binding: provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}}
				pin := collectionPin(t, w, p)
				for _, file := range pin.Files {
					if file.Role == "output" {
						if _, err := blobs.Put(context.Background(), file.Object, bytes.NewReader(p.files[file.Path])); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if _, err := f.s.db.Exec(`CREATE TABLE progress_samples(n INTEGER); CREATE TRIGGER sample_count AFTER UPDATE ON collection_progress BEGIN INSERT INTO progress_samples VALUES(1); END;`); err != nil {
				t.Fatal(err)
			}
			observed := &progressCollectionProvider{collectionProvider: p, clock: f.clock}
			var incremental bool
			observed.hook = func(n int64) error {
				status := collectionStatus(t, f, id)
				if status.Progress != nil && status.Progress.BytesReceived != nil && *status.Progress.BytesReceived > 0 && *status.Progress.BytesReceived < int64(len(p.files["outputs/answer.json"])) {
					incremental = true
					if status.State != "transferring" || status.Progress.BytesCompleted != *status.Progress.BytesReceived || controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 {
						t.Fatal("progress exposed publication or false bytes", status)
					}
				}
				return nil
			}
			runCollection(t, observedEngine(t, f, observed, blobs, f.s))
			status := collectionStatus(t, f, id)
			size := int64(len(p.files["outputs/answer.json"]))
			if status.Progress == nil || *status.Progress.BytesTotal != size || status.Progress.BytesCompleted != size || status.Progress.BytesReceived == nil {
				t.Fatal(status)
			}
			want := size
			if cached {
				want = 0
			}
			if *status.Progress.BytesReceived != want || incremental == cached {
				t.Fatal("received includes cache/controls or lacks incremental samples", status.Progress, incremental)
			}
			if n := controlCount(t, f.s, "SELECT count(*) FROM progress_samples"); n > 7 {
				t.Fatal("per-chunk persistence", n)
			}
			assertNoNewCompute(t, f)
		})
	}
}

func TestCollectionProgressLeaseExpiryReclaimAndStaleFence(t *testing.T) {
	f, id, p, blobs := collectionFixture(t)
	w, err := f.s.ClaimCollection(context.Background(), f.clock.Now(), time.Second, 1)
	if err != nil || w == nil {
		t.Fatal(err)
	}
	if s := collectionStatus(t, f, id); s.State != "discovering" || s.Progress.BytesTotal != nil {
		t.Fatal(s)
	}
	f.clock.advance(2 * time.Second)
	if s := collectionStatus(t, f, id); s.State != "pending" {
		t.Fatal("expired lease asserted activity", s)
	}
	sample := initialProgress(*w, f.clock.Now())
	if err = f.s.RecordCollectionProgress(context.Background(), *w, "discovering", sample, f.clock.Now()); !errors.Is(err, collection.ErrLeaseLost) {
		t.Fatal("expired writer retained progress", err)
	}
	f.restart(t)
	reclaimed, err := f.s.ClaimCollection(context.Background(), f.clock.Now(), time.Second, 1)
	if err != nil || reclaimed == nil || reclaimed.Lease.Generation != 2 || reclaimed.Lease.OperationID != w.Lease.OperationID {
		t.Fatal(reclaimed, err)
	}
	if err = f.s.RecordCollectionProgress(context.Background(), *w, "discovering", sample, f.clock.Now()); !errors.Is(err, collection.ErrLeaseLost) {
		t.Fatal("stale generation wrote progress", err)
	}
	f.clock.advance(2 * time.Second)
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	if s := collectionStatus(t, f, id); s.State != "available" || s.Progress.Generation != 3 {
		t.Fatal(s)
	}
}

func TestManualPolicyAdmissionHashReplayAndMigration(t *testing.T) {
	f := newAdmission(t)
	ctx := context.Background()
	raw := []byte(strings.Replace(admissionJSON, `"name":"offline-admission"`, `"name":"offline-admission","result_collection":"manual"`, 1))
	r, err := f.service.Submit(ctx, f.p, "a", "manual-admission-key", raw)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := f.service.Submit(ctx, f.p, "a", "manual-admission-key", raw)
	if err != nil || !replay.Replay || replay.JobID != r.JobID {
		t.Fatal(replay, err)
	}
	if _, err = f.service.Submit(ctx, f.p, "a", "manual-admission-key", []byte(admissionJSON)); !errors.Is(err, admission.ErrConflict) {
		t.Fatal("manual/default changed request failed to conflict", err)
	}
	old := copyFixtureAtSchema(t, f.s, 11)
	upgraded, err := Open(ctx, old, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	record, err := upgraded.ReadJob(ctx, "a", f.p.TokenID(), r.JobID)
	if err != nil || record.Request.Spec().CollectionMode() != "manual" || !bytes.Equal(record.Request.Canonical(), mustParseManual(t, raw).Canonical()) {
		t.Fatal(record, err)
	}
}
func mustParseManual(t *testing.T, raw []byte) admission.Request {
	t.Helper()
	r, err := admission.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestManagedManualCollectionRecoveryNeedsNoNewGrant(t *testing.T) {
	ctx := context.Background()
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false, "manual")
	grantAuthorization(t, f, id)
	submittedControl(t, f, id)
	for i := 0; i < 2; i++ {
		if err := f.adapter.Advance(*f.journal(t, id).Remote); err != nil {
			t.Fatal(err)
		}
		if err := f.step(t); err != nil {
			t.Fatal(err)
		}
	}
	f.restart(t)
	if w, err := f.s.ClaimCollectionManaged(ctx, f.clock.Now(), time.Second, 1); w != nil || err != nil {
		t.Fatal("managed manual job auto-collected", w, err)
	}
	service, principal, _ := controlService(t, f, "a", auth.Operate)
	r, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "managed-manual-download", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.s.ClaimCollectionManaged(ctx, f.clock.Now(), time.Second, 1)
	if w == nil || err != nil || w.Lease.OperationID != r.Operation.ID || w.Binding.AccountScope != f.profile.AccountScope {
		t.Fatal(w, err)
	}
	f.clock.advance(2 * time.Second)
	f.restart(t)
	w, err = f.s.ClaimCollectionManaged(ctx, f.clock.Now(), time.Second, 1)
	if w == nil || err != nil || w.Lease.Generation != 2 || w.Lease.OperationID != r.Operation.ID {
		t.Fatal(w, err)
	}
	if count := controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations"); count != 1 {
		t.Fatal("collection created grant", count)
	}
	assertNoNewCompute(t, f)
}

func TestManualLateTransferFailureKeepsFullBytesUnavailableUntilNewCollect(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t, "manual")
	service, principal, _ := controlService(t, f, "a", auth.Operate)
	original, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "late-failure-original", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	p.mode = "ack"
	observed := &progressCollectionProvider{collectionProvider: p, clock: f.clock}
	e := observedEngine(t, f, observed, blobs, f.s)
	if worked, err := e.RunOnce(ctx); !worked || err == nil {
		t.Fatal(worked, err)
	}
	s := collectionStatus(t, f, id)
	if s.State != "failed" || *s.Progress.BytesReceived != *s.Progress.BytesTotal || s.Progress.BytesCompleted != *s.Progress.BytesTotal || f.state(t, id).Result == domain.ResultAvailable || controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 {
		t.Fatal("last byte became publication", s)
	}
	if worked, err := e.RunOnce(ctx); worked || err != nil {
		t.Fatal("committed failure auto-retried", worked, err)
	}
	replay, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "late-failure-original", controlRequest(id.AttemptID))
	if err != nil || replay.Operation.ID != original.Operation.ID || !replay.Replay || replay.Operation.Status != domain.OperationAccepted {
		t.Fatal(replay, err)
	}
	if _, err = service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "late-failure-explicit-recovery", controlRequest(id.AttemptID)); err != nil {
		t.Fatal(err)
	}
	p.mode = ""
	runCollection(t, e)
	if s = collectionStatus(t, f, id); s.State != "available" || s.Progress.Generation != 2 || s.Progress.BytesCompleted != *s.Progress.BytesTotal {
		t.Fatal(s)
	}
	assertNoNewCompute(t, f)
}

type progressAckFault struct {
	*Store
	fired bool
}

func (s *progressAckFault) RecordCollectionProgress(ctx context.Context, w collection.Work, stage string, p domain.CollectionProgress, now time.Time) error {
	err := s.Store.RecordCollectionProgress(ctx, w, stage, p, now)
	if err == nil && !s.fired && p.BytesCompleted > 0 {
		s.fired = true
		return collection.ErrUnavailable
	}
	return err
}
func TestProgressLostAcknowledgementRetainsTicketAndPin(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t)
	fault := &progressAckFault{Store: f.s}
	observed := &progressCollectionProvider{collectionProvider: p, clock: f.clock}
	if worked, err := observedEngine(t, f, observed, blobs, fault).RunOnce(ctx); !worked || !errors.Is(err, collection.ErrUnavailable) || !fault.fired {
		t.Fatal(worked, err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect' AND status='failed'") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_snapshots") != 1 {
		t.Fatal("progress uncertainty destroyed recovery")
	}
	before := collectionStatus(t, f, id)
	f.clock.advance(11 * time.Minute)
	f.restart(t)
	runCollection(t, observedEngine(t, f, observed, blobs, f.s))
	after := collectionStatus(t, f, id)
	if after.State != "available" || after.Progress.Generation != 2 || *before.OperationID != *after.OperationID || *after.Progress.BytesReceived != *after.Progress.BytesTotal {
		t.Fatal(before, after)
	}
	assertNoNewCompute(t, f)
}
