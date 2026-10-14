package runtimehost

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// ACK faults run after the real public repository mutation. They never replay it.
type systemCollectionRepository struct {
	*sqlite.Store
	stage   string
	fired   bool
	samples int
}

func (r *systemCollectionRepository) PinCollection(ctx context.Context, w collection.Work, pin collection.Snapshot) error {
	err := r.Store.PinCollection(ctx, w, pin)
	if err == nil && r.stage == "pin-ack" && !r.fired {
		r.fired = true
		return collection.ErrUnavailable
	}
	return err
}
func (r *systemCollectionRepository) CompleteCollection(ctx context.Context, w collection.Work, proof collection.Verified) error {
	if r.stage == "before-publication" && !r.fired {
		r.fired = true
		return collection.ErrUnavailable
	}
	err := r.Store.CompleteCollection(ctx, w, proof)
	if err == nil && r.stage == "publication-ack" && !r.fired {
		r.fired = true
		return collection.ErrUnavailable
	}
	return err
}
func (r *systemCollectionRepository) RecordCollectionProgress(ctx context.Context, w collection.Work, stage string, p domain.CollectionProgress) error {
	err := r.Store.RecordCollectionProgress(ctx, w, stage, p)
	if err == nil {
		r.samples++
	}
	if err == nil && r.stage == "progress-ack" && !r.fired && p.BytesCompleted > 0 {
		r.fired = true
		return collection.ErrUnavailable
	}
	return err
}

func TestManualCollectionWaitsAcrossRestartAndExplicitKeysCoalesce(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "manual-wait", "manual")
	source := f.collectionSource(t)
	for range 2 {
		if s := f.collectionStatus(t, job); s.State != "awaiting_request" || s.Mode != "manual" || s.OperationID != nil || s.Progress != nil {
			t.Fatal(s)
		}
		if worked, err := f.collectionEngine(t, nil).RunOnce(ctx); worked || err != nil {
			t.Fatal("manual job auto-collected", worked, err)
		}
		if lists, fetches := source.counts(); lists != 0 || fetches != 0 {
			t.Fatal("manual waiting fetched artifacts")
		}
		f.reopen(t)
	}
	service := f.controls(t)
	var wg sync.WaitGroup
	results := make(chan operations.Record, 2)
	failures := make(chan error, 2)
	for _, key := range []string{"download-first-key", "download-second-key"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			r, err := service.Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, key, systemControlBody(t, job.AttemptID))
			results <- r
			failures <- err
		}(key)
	}
	wg.Wait()
	first, second := <-results, <-results
	if e1, e2 := <-failures, <-failures; e1 != nil || e2 != nil || first.Operation.ID != second.Operation.ID {
		t.Fatal(first, second, e1, e2)
	}
	if s := f.collectionStatus(t, job); s.State != "pending" || s.OperationID == nil {
		t.Fatal(s)
	}
	f.reopen(t)
	f.collectOnce(t)
	s := f.collectionStatus(t, job)
	size := int64(len(source.files["outputs/result.json"]))
	if s.State != "available" || s.Progress == nil || s.Progress.BytesReceived == nil || s.Progress.BytesTotal == nil || *s.Progress.BytesReceived != size || *s.Progress.BytesTotal != size || s.Progress.BytesCompleted != size {
		t.Fatal(s)
	}
	replay, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "download-first-key", systemControlBody(t, job.AttemptID))
	if err != nil || !replay.Replay || replay.Operation.ID != first.Operation.ID || replay.Operation.Status != domain.OperationAccepted {
		t.Fatal("receipt changed", replay, err)
	}
	_, before := source.counts()
	if _, err = f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "download-available", systemControlBody(t, job.AttemptID)); err != nil {
		t.Fatal(err)
	}
	if worked, err := f.collectionEngine(t, nil).RunOnce(ctx); worked || err != nil {
		t.Fatal(worked, err)
	}
	if _, after := source.counts(); after != before {
		t.Fatal("available results refetched")
	}
	f.assertSingleCompute(t, job)
}

func TestAutomaticCollectionAndManualFailureDiagnostics(t *testing.T) {
	for _, mode := range []string{"", "automatic", "manual"} {
		t.Run(mode, func(t *testing.T) {
			scenario := fake.DefaultScenario()
			if mode == "manual" {
				scenario.States = []domain.ExecutionState{domain.ExecutionQueued, domain.ExecutionRunning, domain.ExecutionFailed}
			}
			f := newSystemFixture(t, scenario)
			job := f.collectibleJob(t, "collection-mode", mode)
			source := f.collectionSource(t)
			if mode == "manual" {
				source.phase = "failed"
				delete(source.files, "outputs/result.json")
				if worked, err := f.collectionEngine(t, nil).RunOnce(t.Context()); worked || err != nil {
					t.Fatal(worked, err)
				}
				if _, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCollect, "diagnostic-download", systemControlBody(t, job.AttemptID)); err != nil {
					t.Fatal(err)
				}
			}
			f.collectOnce(t)
			r, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
			if err != nil || r.Attempt.State.Result != domain.ResultAvailable || mode == "manual" && r.Attempt.State.Orchestration == domain.OrchestrationSucceeded {
				t.Fatal("diagnostic publication changed business outcome", err)
			}
			f.assertSingleCompute(t, job)
		})
	}
}

func TestCollectionObservedProgressIsThrottledAndExcludesControlsAndCache(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "stream", true: "cache"}[cached], func(t *testing.T) {
			ctx := t.Context()
			f := newSystemFixture(t, fake.DefaultScenario())
			job := f.collectibleJob(t, "observed-progress", "automatic")
			source := f.collectionSource(t)
			if cached {
				w, err := f.host.store.ClaimCollection(ctx, time.Second, 1)
				if err != nil || w == nil {
					t.Fatal(err)
				}
				p, err := f.services.resolver.Resolve(w.Binding)
				if err != nil {
					t.Fatal(err)
				}
				catalog, err := p.ListArtifacts(ctx, w.Observation.Remote)
				if err != nil {
					t.Fatal(err)
				}
				pin, err := collection.BuildSnapshot(*w, source.files[collection.ManifestPath], catalog, collection.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				for _, file := range pin.Files {
					if file.Role == "output" {
						if _, err = f.host.results.Put(ctx, file.Object, bytes.NewReader(source.files[file.Path])); err != nil {
							t.Fatal(err)
						}
					}
				}
				f.advance(t, 2*time.Second)
			}
			source.advance = func(d time.Duration) { f.clock.(*fixedClock).advance(d) }
			incremental := false
			size := int64(len(source.files["outputs/result.json"]))
			source.chunk = func(int64) error {
				s := f.collectionStatus(t, job)
				if s.Progress != nil && s.Progress.BytesReceived != nil && *s.Progress.BytesReceived > 0 && *s.Progress.BytesReceived < size {
					incremental = true
					if s.State != "transferring" || s.Progress.BytesCompleted != *s.Progress.BytesReceived {
						t.Fatal("false progress", s)
					}
					if _, err := f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, collection.ErrNotFound) {
						t.Fatal("progress exposed publication", err)
					}
				}
				return nil
			}
			repo := &systemCollectionRepository{Store: f.host.store}
			if worked, err := f.collectionEngine(t, repo).RunOnce(ctx); !worked || err != nil {
				t.Fatal(worked, err)
			}
			s := f.collectionStatus(t, job)
			want := size
			if cached {
				want = 0
			}
			if s.Progress == nil || s.Progress.BytesTotal == nil || *s.Progress.BytesTotal != size || s.Progress.BytesCompleted != size || s.Progress.BytesReceived == nil || *s.Progress.BytesReceived != want || incremental == cached {
				t.Fatal("cache/controls included or samples missing", s, incremental)
			}
			if repo.samples > 9 {
				t.Fatal("per-chunk persistence", repo.samples)
			}
			f.assertSingleCompute(t, job)
		})
	}
}

func TestManagedManualCollectionRecoveryNeedsNoNewGrant(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	connection := f.connection(t, "manual-managed-connection", "SYNTHETIC_FIRST")
	spec := f.jobSpec(t, connection)
	spec.ResultCollection = "manual"
	job := f.submitJob(t, "manual-managed", spec)
	grant := f.authorizeJob(t, job)
	f.runToCollection(t, job)
	f.collectionSource(t)
	f.reopen(t)
	if w, err := f.host.store.ClaimCollection(ctx, time.Second, 1); w != nil || err != nil {
		t.Fatal("manual job auto-collected", w, err)
	}
	op, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "managed-download", systemControlBody(t, job.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.host.store.ClaimCollection(ctx, time.Second, 1)
	if err != nil || w == nil || w.Lease.OperationID != op.Operation.ID || w.Binding != f.binding(t, connection) {
		t.Fatal("collection changed original authority", w, err)
	}
	f.advance(t, 2*time.Second)
	f.reopen(t)
	reclaimed, err := f.host.store.ClaimCollection(ctx, time.Second, 1)
	if err != nil || reclaimed == nil || reclaimed.Lease.Generation != 2 || reclaimed.Lease.OperationID != op.Operation.ID || reclaimed.Binding != w.Binding {
		t.Fatal("reclaim changed authority", reclaimed, err)
	}
	f.advance(t, 2*time.Second)
	f.collectOnce(t)
	current, err := f.services.authorizations.Get(ctx, f.actor, "app", job.JobID, grant.AuthorizationID)
	if err != nil || current.Status != executionauth.Consumed || current.AuthorizationID != grant.AuthorizationID {
		t.Fatal("collection replaced consumed grant", current, err)
	}
	f.assertSingleCompute(t, job)
}

func TestManualLateTransferFailureKeepsFullBytesUnavailableUntilNewCollect(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "late-transfer", "manual")
	source := f.collectionSource(t)
	original, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "late-original", systemControlBody(t, job.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	source.mode = "ack"
	source.advance = func(d time.Duration) { f.clock.(*fixedClock).advance(d) }
	e := f.collectionEngine(t, nil)
	if worked, err := e.RunOnce(ctx); !worked || err == nil {
		t.Fatal(worked, err)
	}
	s := f.collectionStatus(t, job)
	if s.State != "failed" || s.Progress == nil || *s.Progress.BytesReceived != *s.Progress.BytesTotal || s.Progress.BytesCompleted != *s.Progress.BytesTotal {
		t.Fatal("late failure lost full progress", s)
	}
	if _, err = f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, collection.ErrNotFound) {
		t.Fatal("last byte became publication", err)
	}
	if worked, err := e.RunOnce(ctx); worked || err != nil {
		t.Fatal("failure auto-retried", worked, err)
	}
	replay, err := f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "late-original", systemControlBody(t, job.AttemptID))
	if err != nil || !replay.Replay || replay.Operation != original.Operation {
		t.Fatal(replay, err)
	}
	if _, err = f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "late-explicit-recovery", systemControlBody(t, job.AttemptID)); err != nil {
		t.Fatal(err)
	}
	source.mode = ""
	f.collectOnce(t)
	if s = f.collectionStatus(t, job); s.State != "available" || s.Progress.Generation != 2 || s.Progress.BytesCompleted != *s.Progress.BytesTotal {
		t.Fatal(s)
	}
	f.assertSingleCompute(t, job)
}

func TestProgressLostAcknowledgementRetainsTicketAndPin(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "progress-ack", "automatic")
	source := f.collectionSource(t)
	source.advance = func(d time.Duration) { f.clock.(*fixedClock).advance(d) }
	repo := &systemCollectionRepository{Store: f.host.store, stage: "progress-ack"}
	if worked, err := f.collectionEngine(t, repo).RunOnce(ctx); !worked || !errors.Is(err, collection.ErrUnavailable) || !repo.fired {
		t.Fatal(worked, err)
	}
	before := f.collectionStatus(t, job)
	if before.OperationID == nil || before.State == "failed" {
		t.Fatal("uncertain progress failed ticket", before)
	}
	f.advance(t, 11*time.Minute)
	f.reopen(t)
	f.collectOnce(t)
	after := f.collectionStatus(t, job)
	if after.State != "available" || after.Progress.Generation != 2 || *before.OperationID != *after.OperationID || *after.Progress.BytesReceived != *after.Progress.BytesTotal {
		t.Fatal(before, after)
	}
	f.assertSingleCompute(t, job)
}

func TestCollectionRecoveryAcrossCommitBoundaries(t *testing.T) {
	for _, stage := range []string{"pin-ack", "before-publication", "publication-ack"} {
		t.Run(stage, func(t *testing.T) {
			ctx := t.Context()
			f := newSystemFixture(t, fake.DefaultScenario())
			job := f.collectibleJob(t, "collection-commit", "automatic")
			source := f.collectionSource(t)
			repo := &systemCollectionRepository{Store: f.host.store, stage: stage}
			if worked, err := f.collectionEngine(t, repo).RunOnce(ctx); !worked || !errors.Is(err, collection.ErrUnavailable) || !repo.fired {
				t.Fatal("fault did not interrupt acknowledgement", worked, err)
			}
			before := f.collectionStatus(t, job)
			lists, fetches := source.counts()
			if before.State == "failed" || before.OperationID == nil {
				t.Fatal("uncertain SQL acknowledgement failed ticket", before)
			}
			f.reopen(t)
			f.advance(t, 11*time.Minute)
			if worked, err := f.collectionEngine(t, nil).RunOnce(ctx); err != nil || worked != (stage != "publication-ack") {
				t.Fatal("recovery", worked, err)
			}
			newLists, newFetches := source.counts()
			after := f.collectionStatus(t, job)
			if newLists != lists || stage != "pin-ack" && newFetches != fetches || after.State != "available" || *before.OperationID != *after.OperationID {
				t.Fatal("recovery refreshed original pin or refetched cache", before, after, newLists, newFetches)
			}
			f.assertSingleCompute(t, job)
		})
	}
}
