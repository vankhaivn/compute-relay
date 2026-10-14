package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func TestRetentionExpiryPreservesSQLVerificationHistory(t *testing.T) {
	f, _, p, blobs := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	artifacts := controlCount(t, f.s, "SELECT count(*) FROM artifacts")
	f.clock.advance(8 * 24 * time.Hour)
	scanRetention(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != artifacts || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 {
		t.Fatal("expiry erased SQL verification history")
	}
	sweep, err := retention.NewSweeper(f.s, f.blobs, blobs)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, _, err = sweep.SweepOnce(context.Background(), retention.DefaultPolicy(), 0, 100); err != nil {
			t.Fatal(err)
		}
	}
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE expired_at IS NOT NULL AND deleted_at IS NULL") != 0 {
		t.Fatal("pending SQL tombstones did not drain")
	}
}

func TestRetentionSchemaSevenUpgradePreservesPublishedHistory(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Read)
	original, err := f.s.ReadCollection(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	f.root = copyFixtureAtSchema(t, f.s, 7)
	f.restart(t)
	upgraded, err := f.s.ReadCollection(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID)
	if err != nil || len(upgraded.Files) != len(original.Files) || upgraded.VerifiedAt != original.VerifiedAt {
		t.Fatal("upgrade lost published identity", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE expired_at IS NOT NULL") != 0 {
		t.Fatal("upgrade invented expiry")
	}
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory") != 4+len(original.Files) {
		t.Fatal("upgrade did not inventory committed identities")
	}
	assertNoNewCompute(t, f)
}
