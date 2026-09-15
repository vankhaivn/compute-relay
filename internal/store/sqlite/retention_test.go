package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func scanRetention(t *testing.T, f *dispatchFixture) {
	t.Helper()
	for i := 0; i < 3; i++ {
		if _, err := f.s.ExpireRetention(context.Background(), f.clock.Now(), retention.DefaultPolicy(), 100); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRetentionActiveAndManualPinsSurviveAgeAndRestart(t *testing.T) {
	ctx := context.Background()
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, principal, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	f.clock.advance(30 * 24 * time.Hour)
	scanRetention(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 0 {
		t.Fatal("active inputs expired")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='b' AND expired_at IS NOT NULL") != 2 {
		t.Fatal("unreferenced input window not applied")
	}
	cancelControl(t, f, service, principal, id)
	for _, hold := range []string{"one", "two"} {
		if err := f.s.SetRetentionHold(ctx, "a", "job", string(id.JobID), hold, true, f.clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	f.clock.advance(8 * 24 * time.Hour)
	f.restart(t)
	scanRetention(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 0 {
		t.Fatal("restart lost manual hold")
	}
	if err := f.s.SetRetentionHold(ctx, "a", "job", string(id.JobID), "one", false, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	scanRetention(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 0 {
		t.Fatal("one release removed another hold")
	}
	if err := f.s.SetRetentionHold(ctx, "a", "job", string(id.JobID), "two", false, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	scanRetention(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 2 {
		t.Fatal("eligible inputs did not expire")
	}
	if err := f.s.SetRetentionHold(ctx, "a", "input", "data", "late", true, f.clock.Now()); !errors.Is(err, retention.ErrExpired) {
		t.Fatal("hold resurrected expired bytes", err)
	}
	if _, err := f.s.db.Exec(`INSERT INTO job_objects SELECT workspace_id,job_id,'extra',object_id,bytes,sha256 FROM job_objects WHERE workspace_id='a' AND role='bundle'`); err == nil {
		t.Fatal("SQL allowed new reference after tombstone")
	}
	service, principal, _ = controlService(t, f, "a", auth.Read, auth.Operate)
	if _, err := service.Submit(ctx, principal, "a", id.JobID, domain.OperationRetryCompute, "retention-retry-key", controlRequest(id.AttemptID)); err == nil {
		t.Fatal("retry resurrected expired input pins")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 || controlCount(t, f.s, "SELECT count(*) FROM objects") != 4 {
		t.Fatal("retention rewrote attempt history or object metadata")
	}
	if f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("retention caused compute")
	}
}
