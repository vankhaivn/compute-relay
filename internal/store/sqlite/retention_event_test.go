package sqlite

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func TestRetentionExpiryEventIsAtomicAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	f, id, adapter, blobs := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, adapter, blobs, f.s))
	before, err := f.s.LoadAttempt(ctx, id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	events := controlCount(t, f.s, "SELECT count(*) FROM events")
	var sequence int64
	if err := f.s.db.QueryRow("SELECT max(sequence) FROM events WHERE workspace_id=? AND job_id=?", string(id.WorkspaceID), string(id.JobID)).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(8 * 24 * time.Hour)
	// Failure to record the job event must roll back state, every tombstone in
	// this page, the audit and the scan cursor; no deletion permit may escape.
	if _, err := f.s.db.Exec(`CREATE TRIGGER fail_expiry_event BEFORE INSERT ON events WHEN NEW.type='result.expired' BEGIN SELECT RAISE(ABORT,'injected expiry event failure'); END`); err != nil {
		t.Fatal(err)
	}
	report, err := f.s.ExpireRetention(ctx, f.clock.Now(), retention.DefaultPolicy(), 100)
	if err == nil || report != (retention.SweepReport{}) {
		t.Fatal("failed expiry event returned a committed report", report, err)
	}
	after, err := f.s.LoadAttempt(ctx, id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("event rollback changed the attempt", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM events") != events ||
		controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE expired_at IS NOT NULL") != 0 ||
		controlCount(t, f.s, "SELECT count(*) FROM retention_expirations") != 0 ||
		controlCount(t, f.s, "SELECT count(*) FROM retention_audit WHERE action='expire'") != 0 ||
		controlCount(t, f.s, "SELECT inventory_seq FROM retention_cursor WHERE singleton=1") != 0 {
		t.Fatal("expiry transaction partially committed")
	}
	if _, err := f.s.db.Exec("DROP TRIGGER fail_expiry_event"); err != nil {
		t.Fatal(err)
	}
	scanRetention(t, f)
	after, err = f.s.LoadAttempt(ctx, id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	want := before.State
	want.Result = domain.ResultExpired
	if after.State != want || after.Revision != before.Revision+1 {
		t.Fatal("expiry changed business/remote evidence or revision")
	}
	var eventSequence int64
	var operation, stamp string
	if err := f.s.db.QueryRow("SELECT sequence,operation_id,occurred_at FROM events WHERE workspace_id=? AND job_id=? AND attempt_id=? AND type=?", string(id.WorkspaceID), string(id.JobID), string(id.AttemptID), string(domain.EventResultExpired)).Scan(&eventSequence, &operation, &stamp); err != nil {
		t.Fatal(err)
	}
	at, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil || eventSequence != sequence+1 || operation != "" || !at.Equal(f.clock.Now()) {
		t.Fatal("expiry event misrepresents its identity, sequence or time")
	}
	f.restart(t)
	scanRetention(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM events WHERE type='result.expired'") != 1 || controlCount(t, f.s, "SELECT count(*) FROM events") != events+1 {
		t.Fatal("restart or repeated sweep duplicated expiry events")
	}
	assertNoNewCompute(t, f)
}
