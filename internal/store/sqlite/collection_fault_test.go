package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type collectionFault struct {
	*Store
	stage string
	fired bool
}

func (s *collectionFault) PinCollection(ctx context.Context, w collection.Work, pin collection.Snapshot) error {
	err := s.Store.PinCollection(ctx, w, pin)
	if err == nil && s.stage == "pin-ack" && !s.fired {
		s.fired = true
		return collection.ErrUnavailable
	}
	return err
}
func (s *collectionFault) CompleteCollection(ctx context.Context, w collection.Work, v collection.Verified) error {
	if s.stage == "before-publication" && !s.fired {
		s.fired = true
		return collection.ErrUnavailable
	}
	err := s.Store.CompleteCollection(ctx, w, v)
	if err == nil && s.stage == "publication-ack" && !s.fired {
		s.fired = true
		return collection.ErrUnavailable
	}
	return err
}
func TestCollectionAcknowledgementsPreserveAtomicSQLHistory(t *testing.T) {
	for _, stage := range []string{"pin-ack", "before-publication", "publication-ack"} {
		t.Run(stage, func(t *testing.T) {
			f, _, p, blobs := collectionFixture(t)
			fault := &collectionFault{Store: f.s, stage: stage}
			if worked, err := collectionEngine(t, f, p, blobs, fault).RunOnce(context.Background()); !worked || !errors.Is(err, collection.ErrUnavailable) || !fault.fired {
				t.Fatal("fault did not execute", err)
			}
			want := 0
			if stage == "publication-ack" {
				want = 1
			}
			events := controlCount(t, f.s, "SELECT count(*) FROM events")
			if controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != want || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect' AND status='failed'") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_snapshots") != 1 {
				t.Fatal("uncertain commit lost pin or invented partial failure")
			}
			f.restart(t)
			f.clock.advance(11 * time.Minute)
			if _, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'") != 1 {
				t.Fatal("recovery duplicated publication or ticket")
			}
			if stage == "publication-ack" && controlCount(t, f.s, "SELECT count(*) FROM events") != events {
				t.Fatal("committed publication recovery appended events")
			}
		})
	}
}

func TestCollectionPublicationRollbackAndDiskFull(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint("disk-full=", full), func(t *testing.T) {
			f, id, p, blobs := collectionFixture(t)
			if full {
				if _, err := f.s.db.Exec("CREATE TABLE collection_full_fixture (data BLOB)"); err != nil {
					t.Fatal(err)
				}
				var pages int
				requireNoError(t, f.s.db.QueryRow("PRAGMA page_count").Scan(&pages))
				if _, err := f.s.db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages+16)); err != nil {
					t.Fatal(err)
				}
				if _, err := f.s.db.Exec(`CREATE TRIGGER collection_fault BEFORE INSERT ON collection_publications BEGIN INSERT INTO collection_full_fixture VALUES(zeroblob(4194304)); END`); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := f.s.db.Exec(`CREATE TRIGGER collection_fault BEFORE INSERT ON artifacts BEGIN SELECT RAISE(ABORT,'publication fixture failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			worked, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(context.Background())
			if !worked || err == nil {
				t.Fatal("publication fault not observed", err)
			}
			if full && !errors.Is(err, ErrDiskFull) {
				t.Fatal("did not exercise actual SQLITE_FULL", err)
			}
			if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 0 || f.state(t, id).Result != domain.ResultCollecting {
				t.Fatal("transaction leaked partial success")
			}
			_, fetches := p.counts()
			if _, err = f.s.db.Exec("PRAGMA max_page_count=1073741823"); err != nil {
				t.Fatal(err)
			}
			if _, err = f.s.db.Exec("DROP TRIGGER collection_fault"); err != nil {
				t.Fatal(err)
			}
			f.clock.advance(11 * time.Minute)
			runCollection(t, collectionEngine(t, f, p, blobs, f.s))
			_, after := p.counts()
			if after != fetches {
				t.Fatal("publication retry downloaded already verified bytes")
			}
			assertNoNewCompute(t, f)
		})
	}
}
func TestCollectionLeaseContentionStaleFenceAndForgedProof(t *testing.T) {
	ctx := context.Background()
	f, id, p, _ := collectionFixture(t)
	first, err := f.s.ClaimCollection(ctx, time.Second, 2)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := f.s.ClaimCollection(ctx, time.Second, 2)
			if err != nil {
				failures <- err
			}
			if w != nil {
				failures <- errors.New("duplicate collection owner")
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	pin := collectionPin(t, *first, p)
	err = f.s.PinCollection(ctx, *first, pin)
	requireNoError(t, err)
	f.clock.advance(2 * time.Second)
	second, err := f.s.ClaimCollection(ctx, time.Minute, 2)
	if err != nil || second == nil || second.Lease.Generation != first.Lease.Generation+1 || second.Lease.Fence == first.Lease.Fence {
		t.Fatal("lease not fenced", err)
	}
	if err = f.s.PinCollection(ctx, *first, pin); !errors.Is(err, collection.ErrLeaseLost) {
		t.Fatal("stale owner accepted", err)
	}
	if err = f.s.CompleteCollection(ctx, *second, collection.Verified{}); err == nil {
		t.Fatal("caller forged byte verification")
	}
	if _, err = f.s.db.Exec("UPDATE collection_snapshots SET snapshot_sha256=?", string(provider.Digest([]byte("tamper")))); err == nil {
		t.Fatal("immutable snapshot changed")
	}
	if _, err = f.s.db.Exec("UPDATE work_leases SET generation=1 WHERE kind='collection'"); err == nil {
		t.Fatal("generation rollback allowed")
	}
	if f.state(t, id).Result == domain.ResultAvailable {
		t.Fatal("lease test fabricated publication")
	}
	assertNoNewCompute(t, f)
}
func collectionPin(t *testing.T, w collection.Work, p *collectionProvider) collection.Snapshot {
	t.Helper()
	catalog := make([]provider.Artifact, 0, len(p.files))
	for path, data := range p.files {
		catalog = append(catalog, provider.Artifact{Remote: w.Observation.Remote, Path: path, Bytes: int64(len(data)), SHA256: provider.Digest(data)})
	}
	pin, err := collection.BuildSnapshot(w, p.files[collection.ManifestPath], catalog, collection.DefaultConfig())
	requireNoError(t, err)
	return pin
}
func TestCollectionSchemaSixUpgradePreservesDispatchAndReceipts(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t)
	service, principal, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	op, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "before-upgrade-key", controlRequest(id.AttemptID))
	requireNoError(t, err)
	before := f.journal(t, id)
	// Build actual released schema 6 rather than assuming which later tables exist.
	f.root = copyFixtureAtSchema(t, f.s, 6)
	f.restart(t)
	if f.journal(t, id).Version != before.Version || *f.journal(t, id).Remote != *before.Remote {
		t.Fatal("upgrade changed dispatch identity")
	}
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	current, err := f.s.ReadOperation(ctx, "a", principal.TokenID(), op.Operation.ID)
	if err != nil || current.Effect != operations.ResultsAvailable {
		t.Fatal("upgrade lost accepted collection", err)
	}
	var receipt string
	err = f.s.db.QueryRow("SELECT receipt FROM receipts WHERE namespace='operation' AND subject_id=?", string(op.Operation.ID)).Scan(&receipt)
	requireNoError(t, err)
	var stored operations.Record
	if json.Unmarshal([]byte(receipt), &stored) != nil || stored.Operation.Status != domain.OperationAccepted {
		t.Fatal("upgrade rewrote old receipt")
	}
	assertNoNewCompute(t, f)
}
