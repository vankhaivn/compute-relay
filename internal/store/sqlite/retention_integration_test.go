package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func TestRetentionResultExpirySweepAndRootBinding(t *testing.T) {
	ctx := context.Background()
	f, id, p, blobs := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	result, err := f.s.ReadCollection(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID)
	if err != nil || len(result.Files) == 0 {
		t.Fatal("missing verified results", err)
	}
	before := f.state(t, id)
	f.clock.advance(8 * 24 * time.Hour)
	if err := f.s.SetRetentionHold(ctx, "a", "attempt", string(id.AttemptID), "review", true, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	scanRetention(t, f)
	if f.state(t, id).Result != domain.ResultAvailable {
		t.Fatal("held result expired")
	}
	if err := f.s.SetRetentionHold(ctx, "a", "attempt", string(id.AttemptID), "review", false, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	scanRetention(t, f)
	after := f.state(t, id)
	if after.Result != domain.ResultExpired || after.Orchestration != before.Orchestration || after.Execution != before.Execution || after.ReleaseEvidence != before.ReleaseEvidence || after.Cancellation != before.Cancellation {
		t.Fatal("expiry changed remote or business outcome", after)
	}
	if _, err := f.s.ReadCollection(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID); !errors.Is(err, retention.ErrExpired) {
		t.Fatal("expiry indistinguishable from missing metadata", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != len(result.Files) || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 {
		t.Fatal("expiry erased verification history")
	}
	sweeper, err := retention.NewSweeper(f.s, f.blobs, blobs, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	var cursor int64
	for i := 0; i < 20; i++ {
		_, cursor, err = sweeper.SweepOnce(ctx, retention.DefaultPolicy(), cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if cursor == 0 {
			break
		}
	}
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE expired_at IS NOT NULL AND deleted_at IS NULL") != 0 {
		t.Fatal("pending deletions did not drain")
	}
	for _, file := range result.Files {
		if stream, err := blobs.Open(ctx, "a", file.Object.ID); err == nil {
			stream.Close()
			t.Fatal("expired result bytes still present")
		}
	}
	swapped, err := retention.NewSweeper(f.s, blobs, f.blobs, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := swapped.SweepOnce(ctx, retention.DefaultPolicy(), 0, 10); !errors.Is(err, retention.ErrChanged) {
		t.Fatal("swapped roots inherited deletion authority", err)
	}
	replacement, err := blobfs.New(filepath.Join(t.TempDir(), "replacement"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	changed, err := retention.NewSweeper(f.s, f.blobs, replacement, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := changed.SweepOnce(ctx, retention.DefaultPolicy(), 0, 10); !errors.Is(err, retention.ErrChanged) {
		t.Fatal("replacement store inherited tombstones", err)
	}
	f.restart(t)
	if _, err := f.s.ReadCollection(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID); !errors.Is(err, retention.ErrExpired) {
		t.Fatal("restart forgot expiry", err)
	}
	assertNoNewCompute(t, f)
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
