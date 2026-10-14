package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func clockStore(t *testing.T, options Options, version int) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state")
	root, err := statefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = initializeRoot(root.Path); err != nil {
		t.Fatal(err)
	}
	if err = statefs.WriteNew(filepath.Join(root.Path, databaseName), nil); err != nil {
		t.Fatal(err)
	}
	db, err := connect(filepath.Join(root.Path, databaseName))
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{db: db, root: root, options: options}
	t.Cleanup(func() { _ = s.Close() })
	if err = migrate(context.Background(), db, migrations[:version]); err != nil {
		t.Fatal(err)
	}
	return s, path
}

func TestClockLegacySubmillisecondWriteSurvivesRollback(t *testing.T) {
	opts := DefaultOptions()
	opts.Clock = func() time.Time { return schedulerNow }
	s, _ := clockStore(t, opts, len(migrations))
	if err := s.ConfigureScheduler(testctx, scheduler.DefaultSettings()); err != nil {
		t.Fatal(err)
	}
	id := schedulerSeed(t, s, "fraction", "a", "p", "account")
	root := copyFixtureAtSchema(t, s, 24)
	legacy, err := connect(filepath.Join(root, databaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	historical := time.Now().UTC().Truncate(time.Millisecond).Add(time.Hour).Add(123 * time.Nanosecond)
	stamp := historical.Format(time.RFC3339Nano)
	if _, err := legacy.Exec(`UPDATE attempts SET updated_at=? WHERE workspace_id=? AND job_id=? AND attempt_id=?`, stamp, id.WorkspaceID, id.JobID, id.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`UPDATE scheduler_control SET clock_ms=? WHERE singleton=1`, historical.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	before := upgradeRows(t, legacy, `SELECT state,revision,created_at,updated_at FROM attempts`)
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Clock = func() time.Time { return historical.Add(-time.Second) }
	upgraded, err := Open(testctx, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	after := upgradeRows(t, upgraded.db, `SELECT state,revision,created_at,updated_at FROM attempts`)
	if len(after) != 1 || after[0][3] != stamp || !reflect.DeepEqual(before, after) {
		t.Fatal("clock migration rewrote historical attempt", before, after)
	}
	var floor int64
	if err := upgraded.db.QueryRow(`SELECT clock_ms FROM scheduler_control WHERE singleton=1`).Scan(&floor); err != nil || floor != historical.UnixMilli()+1 {
		t.Fatal("legacy floor omitted nanosecond write", floor, err)
	}
	result, err := upgraded.ClaimNext(testctx, "rollback")
	if err != nil || result.Claim == nil {
		t.Fatalf("legacy write at %s rejected after wall rollback: result=%+v err=%v", stamp, result, err)
	}
	attempt, _, err := loadAttempt(testctx, upgraded.db, id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil || attempt.UpdatedAt.Before(historical) {
		t.Fatal("transition regressed historical time", attempt, err)
	}
}

func TestClockSamplesAfterTransactionSlotIsAcquired(t *testing.T) {
	wall := &dispatchClock{at: schedulerNow}
	opts := DefaultOptions()
	opts.Clock = wall.Now
	s, _ := clockStore(t, opts, len(migrations))
	holding, err := s.db.BeginTx(testctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holding.Rollback()
	result := make(chan time.Time, 1)
	failures := make(chan error, 1)
	go func() {
		err := s.write(testctx, func(_ context.Context, _ *sql.Tx, now time.Time) error { result <- now; return nil })
		failures <- err
	}()
	until := time.Now().Add(5 * time.Second)
	for s.db.Stats().WaitCount == 0 {
		if time.Now().After(until) {
			t.Fatal("transaction did not queue behind held connection")
		}
		time.Sleep(time.Millisecond)
	}
	wall.advance(time.Minute)
	if err := holding.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-failures; err != nil {
		t.Fatal(err)
	}
	if got := <-result; !got.Equal(wall.Now()) {
		t.Fatal("clock sampled before acquiring transaction", got, wall.Now())
	}
}

func TestClockConcurrentRollbackAndRestartKeepCommittedFloor(t *testing.T) {
	var sequence atomic.Int64
	sequence.Store(1000)
	opts := DefaultOptions()
	opts.Clock = func() time.Time { return schedulerNow.Add(time.Duration(sequence.Add(-1)) * time.Nanosecond) }
	s, root := clockStore(t, opts, len(migrations))
	var mu sync.Mutex
	var samples []time.Time
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.write(testctx, func(_ context.Context, _ *sql.Tx, now time.Time) error {
				mu.Lock()
				samples = append(samples, now)
				mu.Unlock()
				return nil
			}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(samples) != 32 {
		t.Fatal("lost clock transaction", len(samples))
	}
	if !samples[0].Equal(schedulerNow.Add(999 * time.Nanosecond)) {
		t.Fatal("discarded full wall precision", samples[0])
	}
	for i := 1; i < len(samples); i++ {
		if samples[i].Before(samples[i-1]) {
			t.Fatal("concurrent transactions regressed time", samples)
		}
	}
	floor := schedulerNow.Add(time.Millisecond)
	if !samples[len(samples)-1].Equal(floor) {
		t.Fatal("persisted floor rounded below earlier write", samples)
	}
	injected := errors.New("rollback after local write")
	callbacks := 0
	err := s.write(testctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		callbacks++
		if _, err := tx.ExecContext(ctx, `INSERT INTO workspaces VALUES('rolled-back-clock',1,'[]')`); err != nil {
			return err
		}
		return injected
	})
	if !errors.Is(err, injected) || callbacks != 1 {
		t.Fatal("failed clock transaction was retried", callbacks, err)
	}
	if _, err := s.LookupWorkspace(testctx, "rolled-back-clock"); !errors.Is(err, auth.ErrNotFound) {
		t.Fatal("failed transaction persisted prefix", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Clock = func() time.Time { return schedulerNow.Add(-time.Hour) }
	reopened, err := Open(testctx, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.write(testctx, func(_ context.Context, _ *sql.Tx, now time.Time) error {
		if !now.Equal(floor) {
			t.Fatal("restart lost committed time floor", now, floor)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestClockExpiredTokenCannotReturnAfterRollbackOrRestart(t *testing.T) {
	wall := &dispatchClock{at: schedulerNow}
	opts := DefaultOptions()
	opts.Clock = wall.Now
	s, root := clockStore(t, opts, len(migrations))
	if err := s.PutWorkspace(testctx, auth.Workspace{ID: "a", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	wall.advance(123 * time.Nanosecond)
	access, _ := auth.New(s, s)
	expiry := schedulerNow.Add(time.Second)
	secret, record, err := access.Issue(testctx, "a", []auth.Scope{auth.Read}, expiry)
	if err != nil || !record.CreatedAt.Equal(wall.Now()) || !record.ExpiresAt.Equal(expiry) {
		t.Fatal("token metadata did not use repository time", record, err)
	}
	wall.advance(time.Second)
	if _, err = access.Authenticate(testctx, secret.Reveal()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("expired token accepted", err)
	}
	wall.advance(-time.Hour)
	if _, err = access.Authenticate(testctx, secret.Reveal()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("wall rollback resurrected token", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(testctx, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	access, _ = auth.New(reopened, reopened)
	if _, err = access.Authenticate(testctx, secret.Reveal()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("restart resurrected expired token", err)
	}
}

func TestClockObjectInventoryUsesSameSampleAndRollsBackWithMetadata(t *testing.T) {
	now := schedulerNow
	opts := DefaultOptions()
	opts.Clock = func() time.Time { return now }
	s, _ := clockStore(t, opts, len(migrations))
	if err := s.PutWorkspace(testctx, auth.Workspace{ID: "a", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(123 * time.Nanosecond)
	m := object("clock-object")
	m.WorkspaceID = "a"
	if err := s.CommitObject(testctx, m); err != nil {
		t.Fatal(err)
	}
	var stamp string
	if err := s.db.QueryRow(`SELECT recorded_at FROM retention_inventory WHERE kind='input' AND object_id=?`, m.ID).Scan(&stamp); err != nil || stamp != now.Format(time.RFC3339Nano) {
		t.Fatal("retention stamp did not use transaction sample", stamp, err)
	}
	if _, err := s.db.Exec(`CREATE TEMP TRIGGER fail_object BEFORE INSERT ON objects WHEN NEW.object_id='failed-clock-object' BEGIN SELECT RAISE(ABORT,'fixture failure'); END`); err != nil {
		t.Fatal(err)
	}
	m.ID = domain.ObjectID("failed-clock-object")
	if err := s.CommitObject(testctx, m); !errors.Is(err, ErrConflict) {
		t.Fatal("insertion fault not preserved", err)
	}
	if n := schedulerCount(t, s, `SELECT count(*) FROM retention_inventory WHERE object_id='failed-clock-object'`); n != 0 {
		t.Fatal("failed metadata left orphan retention row", n)
	}
	m.ID = "untracked-object"
	if _, err := s.db.Exec(`INSERT INTO objects VALUES(?,?,?,?)`, m.WorkspaceID, m.ID, m.Bytes, m.SHA256); err == nil {
		t.Fatal("SQL guard allowed untracked object metadata")
	}
}

// A successful claim must not truncate the ceiling persisted by write.
func TestClockNewClaimSurvivesWallRollbackAndRestart(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	f.seed(t, "a", 1, false)
	f.clock.advance(123 * time.Nanosecond)
	claimedAt := f.clock.Now()
	result, err := f.s.ClaimNext(testctx, "precision-worker")
	if err != nil || result.Claim == nil {
		t.Fatal(result, err)
	}
	claim := *result.Claim
	var floor int64
	if err = f.s.db.QueryRow(`SELECT clock_ms FROM scheduler_control WHERE singleton=1`).Scan(&floor); err != nil || floor != claimedAt.UnixMilli()+1 {
		t.Fatal("claim truncated durable time floor", floor, err)
	}
	f.clock.advance(-time.Second)
	before, err := f.s.LoadDispatch(testctx, claim)
	if err != nil || before.Handle.Claim != claim || !before.Job.Attempt.UpdatedAt.Equal(claimedAt) {
		t.Fatal("rollback lost current claim or regressed attempt", before.Handle, err)
	}
	if err = f.s.Close(); err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.Clock = f.clock.Now
	reopened, err := Open(testctx, f.root, opts)
	if err != nil {
		t.Fatal(err)
	}
	f.attach(t, reopened)
	after, err := f.s.LoadDispatch(testctx, claim)
	if err != nil || after.Handle != before.Handle || after.Job.Attempt != before.Job.Attempt || after.Job.Profile != before.Job.Profile || after.Job.AttemptNonce != before.Job.AttemptNonce {
		t.Fatal("restart changed frozen claim or time", after.Handle, err)
	}
}

func TestClockLegacyQuotaOnlyFloorPreservesEvidencePrecision(t *testing.T) {
	opts := DefaultOptions()
	opts.Clock = func() time.Time { return schedulerNow.Add(-time.Second) }
	s, root := clockStore(t, opts, 24)
	observation := schedulerQuota(0, schedulerNow.Add(123*time.Nanosecond))
	raw, _ := json.Marshal(observation)
	if _, err := s.db.Exec(`INSERT INTO scheduler_quotas VALUES('account','gpu',?,?,1)`, string(raw), observation.ObservedAt.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE scheduler_control SET clock_ms=? WHERE singleton=1`, schedulerNow.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := Open(testctx, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	if err = upgraded.RecordQuota(testctx, "account", "gpu", observation); err != nil {
		t.Fatal("legacy fractional observation became future evidence", err)
	}
	var stored string
	var exhausted bool
	if err = upgraded.db.QueryRow(`SELECT observation,exhausted FROM scheduler_quotas WHERE account_scope='account' AND resource='gpu'`).Scan(&stored, &exhausted); err != nil || stored != string(raw) || !exhausted {
		t.Fatal("migration changed original quota evidence or exhaustion latch", stored, exhausted, err)
	}
}

func TestClockLegacyEmptyFloorStaysZero(t *testing.T) {
	options := DefaultOptions()
	legacy, root := clockStore(t, options, 24)
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := Open(testctx, root, options)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	var floor int64
	if err = upgraded.db.QueryRow(`SELECT clock_ms FROM scheduler_control WHERE singleton=1`).Scan(&floor); err != nil || floor != 0 {
		t.Fatal("migration manufactured elapsed runtime time", floor, err)
	}
}
