package sqlite

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

func (s *collectionFault) PinCollection(ctx context.Context, w collection.Work, pin collection.Snapshot, now time.Time) error {
	err := s.Store.PinCollection(ctx, w, pin, now)
	if err == nil && s.stage == "pin-ack" && !s.fired {
		s.fired = true
		return collection.ErrUnavailable
	}
	return err
}
func (s *collectionFault) CompleteCollection(ctx context.Context, w collection.Work, v collection.Verified, now time.Time) error {
	if s.stage == "before-publication" && !s.fired {
		s.fired = true
		return collection.ErrUnavailable
	}
	err := s.Store.CompleteCollection(ctx, w, v, now)
	if err == nil && s.stage == "publication-ack" && !s.fired {
		s.fired = true
		return collection.ErrUnavailable
	}
	return err
}
func TestCollectionRecoveryAcrossCommitBoundaries(t *testing.T) {
	for _, stage := range []string{"pin-ack", "before-publication", "publication-ack"} {
		t.Run(stage, func(t *testing.T) {
			f, id, p, blobs := collectionFixture(t)
			fault := &collectionFault{Store: f.s, stage: stage}
			worked, err := collectionEngine(t, f, p, blobs, fault).RunOnce(context.Background())
			if !worked || !errors.Is(err, collection.ErrUnavailable) || !fault.fired {
				t.Fatal("fault did not interrupt acknowledgement", err)
			}
			lists, fetches := p.counts()
			events := controlCount(t, f.s, "SELECT count(*) FROM events")
			wantPublications := 0
			if stage == "publication-ack" {
				wantPublications = 1
			}
			if controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != wantPublications || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect' AND status='failed'") != 0 {
				t.Fatal("lost SQL acknowledgement became a failed or partial result")
			}
			f.restart(t)
			f.clock.advance(11 * time.Minute)
			engine := collectionEngine(t, f, p, blobs, f.s)
			worked, err = engine.RunOnce(context.Background())
			if err != nil || worked != (stage != "publication-ack") {
				t.Fatal("wrong recovery behavior", worked, err)
			}
			newLists, newFetches := p.counts()
			if newLists != lists || stage != "pin-ack" && newFetches != fetches {
				t.Fatal("recovery re-listed identity or re-fetched completed blobs")
			}
			if stage == "publication-ack" && controlCount(t, f.s, "SELECT count(*) FROM events") != events {
				t.Fatal("committed receipt recovery duplicated events")
			}
			if f.state(t, id).Result != domain.ResultAvailable || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'") != 1 {
				t.Fatal("recovery did not preserve one ticket/publication")
			}
			assertNoNewCompute(t, f)
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
				if err := f.s.db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
					t.Fatal(err)
				}
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
	first, err := f.s.ClaimCollection(ctx, f.clock.Now(), time.Second, 2)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := f.s.ClaimCollection(ctx, f.clock.Now(), time.Second, 2)
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
	if err = f.s.PinCollection(ctx, *first, pin, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(2 * time.Second)
	second, err := f.s.ClaimCollection(ctx, f.clock.Now(), time.Minute, 2)
	if err != nil || second == nil || second.Lease.Generation != first.Lease.Generation+1 || second.Lease.Fence == first.Lease.Fence {
		t.Fatal("lease not fenced", err)
	}
	if err = f.s.PinCollection(ctx, *first, pin, f.clock.Now()); !errors.Is(err, collection.ErrLeaseLost) {
		t.Fatal("stale owner accepted", err)
	}
	if err = f.s.CompleteCollection(ctx, *second, collection.Verified{}, f.clock.Now()); err == nil {
		t.Fatal("caller forged byte verification")
	}
	if _, err = f.s.db.Exec("UPDATE collection_snapshots SET snapshot_sha256=?", string(provider.Digest([]byte("tamper")))); err == nil {
		t.Fatal("immutable snapshot changed")
	}
	if _, err = f.s.db.Exec("UPDATE collection_leases SET generation=1"); err == nil {
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
	if err != nil {
		t.Fatal(err)
	}
	return pin
}
func TestCollectionSchemaSixUpgradePreservesDispatchAndReceipts(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t)
	service, principal, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	op, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationCollect, "before-upgrade-key", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
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
	if err = f.s.db.QueryRow("SELECT receipt FROM operation_idempotency WHERE operation_id=?", string(op.Operation.ID)).Scan(&receipt); err != nil {
		t.Fatal(err)
	}
	var stored operations.Record
	if json.Unmarshal([]byte(receipt), &stored) != nil || stored.Operation.Status != domain.OperationAccepted {
		t.Fatal("upgrade rewrote old receipt")
	}
	assertNoNewCompute(t, f)
}

func TestCollectionProcessKillAfterDurablePin(t *testing.T) {
	f, id, p, blobs := collectionFixture(t)
	j := f.journal(t, id)
	w := collection.Work{Lease: collection.Lease{WorkspaceID: id.WorkspaceID, JobID: id.JobID, AttemptID: id.AttemptID, OperationID: "fixture", Generation: 1, Fence: "01234567890123456789012345678901", Until: f.clock.Now().Add(time.Minute), AttemptRevision: 1}, Plan: *j.Plan, Observation: *j.Observation, Binding: provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}}
	pin := collectionPin(t, w, p)
	raw, err := json.Marshal(pin)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "pin.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = f.s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCollectionCrashChild$")
	cmd.Env = append(os.Environ(), "COLLECTION_CRASH_ROOT="+f.root, "COLLECTION_CRASH_PIN="+path, "COLLECTION_CRASH_TIME="+f.clock.Now().Format(time.RFC3339Nano))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "collection-pin-durable\n" {
		t.Fatal("child did not reach durable pin", line, err)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("child was not killed")
	}
	s, err := Open(context.Background(), f.root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	f.attach(t, s, nil)
	f.clock.advance(time.Minute)
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	lists, _ := p.counts()
	if lists != 0 || f.state(t, id).Result != domain.ResultAvailable || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'") != 1 {
		t.Fatal("process recovery discarded durable pin or operation")
	}
	assertNoNewCompute(t, f)
}
func TestCollectionCrashChild(t *testing.T) {
	root := os.Getenv("COLLECTION_CRASH_ROOT")
	if root == "" {
		return
	}
	ctx := context.Background()
	s, err := Open(ctx, root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at, err := time.Parse(time.RFC3339Nano, os.Getenv("COLLECTION_CRASH_TIME"))
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.ClaimCollection(ctx, at, 30*time.Second, 2)
	if err != nil || w == nil {
		t.Fatal("claim", err)
	}
	raw, err := os.ReadFile(os.Getenv("COLLECTION_CRASH_PIN"))
	if err != nil {
		t.Fatal(err)
	}
	var pin collection.Snapshot
	if err = json.Unmarshal(raw, &pin); err != nil {
		t.Fatal(err)
	}
	if err = s.PinCollection(ctx, *w, pin, at); err != nil {
		t.Fatal(err)
	}
	fmt.Println("collection-pin-durable")
	time.Sleep(time.Hour)
}
