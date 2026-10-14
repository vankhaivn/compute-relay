package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
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
	requireNoError(t, err)
	return *status
}

func TestManualCollectionPersistsCoalescedReceiptsAndProgressRows(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t, "manual")
	f.restart(t)
	if claim, err := f.s.ClaimCollection(ctx, time.Minute, 1); err != nil || claim != nil {
		t.Fatal("manual job claimed before an explicit request", claim, err)
	}
	if lists, fetches := p.counts(); lists != 0 || fetches != 0 {
		t.Fatal("manual wait fetched provider artifacts", lists, fetches)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_progress") != 0 {
		t.Fatal("manual admission invented collection work")
	}
	service, actor, _ := controlService(t, f, "a", auth.Operate)
	for _, key := range []string{"download-first-key", "download-second-key"} {
		if _, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCollect, key, controlRequest(id.AttemptID)); err != nil {
			t.Fatal(err)
		}
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'") != 1 || controlCount(t, f.s, "SELECT count(*) FROM receipts WHERE namespace='operation' AND scope='collect'") != 2 {
		t.Fatal("coalescing did not keep one operation and both keys")
	}
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	if controlCount(t, f.s, "SELECT count(*) FROM collection_progress") != 1 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect' AND status='succeeded'") != 1 || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 {
		t.Fatal("progress, ticket and publication did not commit together")
	}
}

type progressCollectionProvider struct {
	*collectionProvider
	clock *dispatchClock
	hook  func(int64) error
}

func (p *progressCollectionProvider) Fetch(ctx context.Context, remote provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	if progress == nil {
		return p.collectionProvider.Fetch(ctx, remote, file, dst, limit, nil)
	}
	return p.collectionProvider.Fetch(ctx, remote, file, &sampleWriter{dst: dst, progress: progress, clock: p.clock, hook: p.hook}, limit, nil)
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
	requireNoError(t, registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, p))
	e, err := collection.New(repo, registry, blobs, f.clock, collection.DefaultConfig())
	requireNoError(t, err)
	return e
}

func TestCollectionProgressSQLWritesAreThrottledBeforePublication(t *testing.T) {
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
			observed.hook = func(n int64) error {
				status := collectionStatus(t, f, id)
				if status.Progress != nil && status.Progress.BytesReceived != nil && *status.Progress.BytesReceived > 0 && *status.Progress.BytesReceived < int64(len(p.files["outputs/answer.json"])) {
					if status.State != "transferring" || status.Progress.BytesCompleted != *status.Progress.BytesReceived || controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 {
						t.Fatal("progress exposed publication or false bytes", status)
					}
				}
				return nil
			}
			runCollection(t, observedEngine(t, f, observed, blobs, f.s))
			if n := controlCount(t, f.s, "SELECT count(*) FROM progress_samples"); n > 7 {
				t.Fatal("per-chunk persistence", n)
			}
		})
	}
}

func TestCollectionProgressLeaseExpiryReclaimAndStaleFence(t *testing.T) {
	f, id, p, blobs := collectionFixture(t)
	w, err := f.s.ClaimCollection(context.Background(), time.Second, 1)
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
	if err = f.s.RecordCollectionProgress(context.Background(), *w, "discovering", sample); !errors.Is(err, collection.ErrLeaseLost) {
		t.Fatal("expired writer retained progress", err)
	}
	f.restart(t)
	reclaimed, err := f.s.ClaimCollection(context.Background(), time.Second, 1)
	if err != nil || reclaimed == nil || reclaimed.Lease.Generation != 2 || reclaimed.Lease.OperationID != w.Lease.OperationID {
		t.Fatal(reclaimed, err)
	}
	if err = f.s.RecordCollectionProgress(context.Background(), *w, "discovering", sample); !errors.Is(err, collection.ErrLeaseLost) {
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
	requireNoError(t, err)
	replay, err := f.service.Submit(ctx, f.p, "a", "manual-admission-key", raw)
	if err != nil || !replay.Replay || replay.JobID != r.JobID {
		t.Fatal(replay, err)
	}
	if _, err = f.service.Submit(ctx, f.p, "a", "manual-admission-key", []byte(admissionJSON)); !errors.Is(err, admission.ErrConflict) {
		t.Fatal("manual/default changed request failed to conflict", err)
	}
	old := copyFixtureAtSchema(t, f.s, 11)
	upgraded, err := Open(ctx, old, DefaultOptions())
	requireNoError(t, err)
	defer upgraded.Close()
	record, err := upgraded.ReadJob(ctx, "a", f.p.TokenID(), r.JobID)
	if err != nil || record.Request.Spec().CollectionMode() != "manual" || !bytes.Equal(record.Request.Canonical(), mustParseManual(t, raw).Canonical()) {
		t.Fatal(record, err)
	}
}
func mustParseManual(t *testing.T, raw []byte) admission.Request {
	t.Helper()
	r, err := admission.Parse(raw)
	requireNoError(t, err)
	return r
}

type progressAckFault struct {
	*Store
	fired bool
}

func (s *progressAckFault) RecordCollectionProgress(ctx context.Context, w collection.Work, stage string, p domain.CollectionProgress) error {
	err := s.Store.RecordCollectionProgress(ctx, w, stage, p)
	if err == nil && !s.fired && p.BytesCompleted > 0 {
		s.fired = true
		return collection.ErrUnavailable
	}
	return err
}
func TestProgressAcknowledgementPreservesSQLTicketAndPin(t *testing.T) {
	ctx := context.Background()
	f, _, p, blobs := collectionFixture(t)
	fault := &progressAckFault{Store: f.s}
	observed := &progressCollectionProvider{collectionProvider: p, clock: f.clock}
	if worked, err := observedEngine(t, f, observed, blobs, fault).RunOnce(ctx); !worked || !errors.Is(err, collection.ErrUnavailable) || !fault.fired {
		t.Fatal("fault did not execute", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect' AND status='failed'") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_snapshots") != 1 {
		t.Fatal("uncertain progress destroyed SQL recovery")
	}
	f.clock.advance(11 * time.Minute)
	f.restart(t)
	runCollection(t, observedEngine(t, f, observed, blobs, f.s))
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'") != 1 || controlCount(t, f.s, "SELECT count(*) FROM collection_snapshots") != 1 || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 {
		t.Fatal("recovery duplicated ticket, pin or publication")
	}
}
