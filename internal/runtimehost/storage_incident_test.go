package runtimehost

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestSystemStorageScanFailureRetainsCauseAndResumesWithoutDuplicateCompute(t *testing.T) {
	g := newHostGuardFixture(t, "healthy")
	f := g.f
	f.useFake(t, fake.DefaultScenario())
	const cause = "storage_inspect_incomplete"
	complete := false
	scans := 0
	bind := func() {
		t.Helper()
		g.bind(t)
		guard := newManagedStorageGuard(g.guard, f.host.store, f.services.connections)
		guard.collect = func(_ context.Context, _ []byte, _ string, scope hoststorage.Scope) (hoststorage.Scan, error) {
			scans++
			if !complete {
				return hoststorage.Scan{}, provider.WithReason(hoststorage.ErrUnknown, cause)
			}
			return hoststorage.Scan{Scope: scope, ObservedAt: f.fixtureClock().Now(), Objects: []hoststorage.Object{}}, nil
		}
		f.services.storageGuard = guard
		g.jobs.WithPreflight(guard)
		f.services.dispatcher.WithNewWorkGuard(guard)
	}
	bind()
	spec := g.spec(t)
	raw := systemJSON(t, spec)
	const key = "storage-incident-original-admission"
	refused := func() {
		t.Helper()
		if _, err := g.jobs.Submit(t.Context(), f.actor, "app", key, raw); !errors.Is(err, hoststorage.ErrUnknown) || provider.Reason(err, "") != cause {
			t.Fatal("incomplete inventory did not safely refuse original admission", err)
		}
		view, err := f.host.store.ConnectionStorage(t.Context(), "app", f.actor.TokenID(), g.connection.ID)
		requireNoError(t, err)
		if view.ScanState != "incomplete" || view.Reason != cause || len(view.Orphans) != 0 {
			t.Fatalf("incomplete scan lost cause or invented cleanup authority: %+v", view)
		}
		if worked, err := f.services.dispatcher.RunOnce(t.Context(), "refused-storage-worker"); worked || err != nil {
			t.Fatal("refused admission left executable work", worked, err)
		}
		if stats := f.backend.Stats(); stats.PrepareCalls != 0 || stats.SubmitCalls != 0 || stats.Executions != 0 || stats.CleanupCalls != 0 {
			t.Fatal("refused admission performed compute or cleanup", stats)
		}
	}
	refused()
	f.reopen(t)
	// Inspect persistence before any new observation can replace the failed scan.
	view, err := f.host.store.ConnectionStorage(t.Context(), "app", f.actor.TokenID(), g.connection.ID)
	requireNoError(t, err)
	if view.ScanState != "incomplete" || view.Reason != cause {
		t.Fatal("reopen lost incomplete inventory cause", view)
	}
	bind()
	refused()
	complete = true
	job, err := g.jobs.Submit(t.Context(), f.actor, "app", key, raw)
	requireNoError(t, err)
	if !job.JobID.Valid() || !job.AttemptID.Valid() {
		t.Fatal("fresh complete scan did not admit original key", job)
	}
	view, err = f.host.store.ConnectionStorage(t.Context(), "app", f.actor.TokenID(), g.connection.ID)
	requireNoError(t, err)
	if view.ScanState != "complete" || view.Reason != "" {
		t.Fatal("complete inventory retained a stale refusal", view)
	}
	f.authorizeJob(t, job)
	// Keep scans fresh at the actual pre-intent guard, then drive the same fake
	// backend to terminal execution and verified collection.
	for range 2 {
		requireNoError(t, g.step(t))
	}
	f.runToCollection(t, job)
	f.collectOnce(t)
	record, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
	requireNoError(t, err)
	if record.Attempt.ID != job.AttemptID || record.Attempt.State.Result != domain.ResultAvailable {
		t.Fatal("original admitted attempt did not complete", record.Attempt)
	}
	// Exact replay wins even if a later scan would fail, without another scan,
	// admission receipt or remote mutation.
	complete = false
	before := scans
	f.advance(t, time.Second)
	replay, err := g.jobs.Submit(t.Context(), f.actor, "app", key, raw)
	requireNoError(t, err)
	if !replay.Replay {
		t.Fatal("same admission key did not replay its saved receipt")
	}
	replay.Replay = job.Replay
	if !reflect.DeepEqual(job, replay) || scans != before {
		t.Fatal("original admission replay changed identity or refreshed inventory", job, replay, scans, before)
	}
	if stats := f.backend.Stats(); stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 || stats.CleanupCalls != 0 {
		t.Fatal("storage recovery duplicated compute or guessed cleanup", stats)
	}
}
