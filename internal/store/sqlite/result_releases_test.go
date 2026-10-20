package sqlite

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func resultReleaser(t *testing.T, f *dispatchFixture, results retention.BlobDeleter) *retention.Releaser {
	t.Helper()
	access, err := auth.New(f.s, f.s)
	requireNoError(t, err)
	sweeper, err := retention.NewSweeper(f.s, f.blobs, results)
	requireNoError(t, err)
	service, err := retention.NewReleaser(access, f.s, sweeper)
	requireNoError(t, err)
	return service
}

func releaseRequest(t *testing.T, f *dispatchFixture) retention.ReleaseRequest {
	t.Helper()
	var request retention.ReleaseRequest
	requireNoError(t, f.s.db.QueryRow(`SELECT attempt_id,artifact_id,sha256 FROM artifacts WHERE role='output' LIMIT 1`).
		Scan(&request.AttemptID, &request.ArtifactID, &request.SHA256))
	return request
}

func TestResultReleaseReclaimsExactPublicationAndPreservesHistory(t *testing.T) {
	ctx := context.Background()
	f, id, p, results := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, results, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	_, reader, _ := controlService(t, f, "a", auth.Read)
	_, foreign, _ := controlService(t, f, "b", auth.Operate)
	original, err := f.s.ReadCollection(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID)
	requireNoError(t, err)
	f.root = copyFixtureAtSchema(t, f.s, 37)
	f.restart(t)
	upgraded, err := f.s.ReadCollection(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID)
	requireNoError(t, err)
	if !reflect.DeepEqual(original, upgraded) || controlCount(t, f.s, "SELECT count(*) FROM result_releases") != 0 {
		t.Fatal("schema upgrade changed retained result evidence or invented a release")
	}
	service := resultReleaser(t, f, results)
	request := releaseRequest(t, f)
	for _, who := range []auth.Principal{reader, foreign} {
		if _, err := service.Release(ctx, who, "a", id.JobID, "release-original", request); !errors.Is(err, auth.ErrForbidden) {
			t.Fatal("release accepted missing/foreign authority", err)
		}
	}
	wrong := request
	wrong.SHA256 = domain.SHA256Digest(strings.Repeat("a", 64))
	if _, err := service.Release(ctx, actor, "a", id.JobID, "release-original", wrong); !errors.Is(err, retention.ErrChanged) {
		t.Fatal("release accepted wrong output hash", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM retention_expirations") != 0 {
		t.Fatal("rejected release expired bytes")
	}
	data := []byte("unrelated result remains")
	witness := domain.ObjectMetadata{WorkspaceID: "b", ID: "unrelated-result", Bytes: int64(len(data)), SHA256: provider.Digest(data)}
	_, err = results.Put(ctx, witness, bytes.NewReader(data))
	requireNoError(t, err)
	before := snapshotTableCounts(t, f.s, "jobs", "attempts", "artifacts", "collection_publications", "operations", "receipts")
	receipt, err := service.Release(ctx, actor, "a", id.JobID, "release-original", request)
	if err != nil || receipt.PendingFiles != 0 || receipt.Files != 5 || receipt.Bytes < 1 || receipt.ReclaimedBytes != receipt.Bytes {
		t.Fatal("immediate release did not reclaim verified bytes", receipt, err)
	}
	if f.state(t, id).Result != domain.ResultExpired {
		t.Fatal("release left result downloadable")
	}
	if _, err := f.s.ReadCollection(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID); !errors.Is(err, retention.ErrExpired) {
		t.Fatal("released result remained available", err)
	}
	after := snapshotTableCounts(t, f.s, "jobs", "attempts", "artifacts", "collection_publications", "operations", "receipts")
	if len(before) != len(after) {
		t.Fatal("history table set changed")
	}
	for key, value := range before {
		if after[key] != value {
			t.Fatal("release erased history", key)
		}
	}
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE kind='input' AND expired_at IS NOT NULL") != 0 {
		t.Fatal("result release expired inputs")
	}
	stream, err := results.Open(ctx, witness.WorkspaceID, witness.ID)
	requireNoError(t, err)
	got, err := io.ReadAll(stream)
	requireNoError(t, err)
	requireNoError(t, stream.Close())
	if !bytes.Equal(got, data) {
		t.Fatal("release changed an unrelated result")
	}
	f.restart(t)
	service = resultReleaser(t, f, results)
	replayed, err := service.Release(ctx, actor, "a", id.JobID, "release-original", request)
	if err != nil || replayed != receipt || controlCount(t, f.s, "SELECT count(*) FROM result_releases") != 1 {
		t.Fatal("lost-response replay changed release identity", replayed, err)
	}
	if _, err := service.Release(ctx, actor, "a", id.JobID, "release-original", wrong); !errors.Is(err, retention.ErrReleaseConflict) {
		t.Fatal("release key accepted changed input", err)
	}
	assertNoNewCompute(t, f)
}

type failingReleaseBlobs struct {
	*blobfs.Store
	fail  bool
	calls int
}

func (b *failingReleaseBlobs) Delete(ctx context.Context, expected domain.ObjectMetadata) (bool, error) {
	b.calls++
	if b.fail {
		return false, blobfs.ErrUnavailable
	}
	return b.Store.Delete(ctx, expected)
}

func TestResultReleaseCommitsBeforeDeletionAndResumesPendingFiles(t *testing.T) {
	ctx := context.Background()
	f, id, p, results := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, results, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Operate)
	request := releaseRequest(t, f)
	outputs := &failingReleaseBlobs{Store: results, fail: true}
	service := resultReleaser(t, f, outputs)
	_, err := f.s.db.Exec(`CREATE TRIGGER release_fault BEFORE INSERT ON result_releases BEGIN SELECT RAISE(ABORT,'synthetic release rollback'); END`)
	requireNoError(t, err)
	if _, err := service.Release(ctx, actor, "a", id.JobID, "release-original", request); err == nil {
		t.Fatal("release ignored failed intent commit")
	}
	if outputs.calls != 0 || f.state(t, id).Result != domain.ResultAvailable || controlCount(t, f.s, "SELECT count(*) FROM retention_expirations") != 0 {
		t.Fatal("failed intent leaked deletion or tombstones")
	}
	_, err = f.s.db.Exec("DROP TRIGGER release_fault")
	requireNoError(t, err)
	receipt, err := service.Release(ctx, actor, "a", id.JobID, "release-original", request)
	if err != nil || receipt.PendingFiles != 5 || receipt.ReclaimedBytes != 0 {
		t.Fatal("failed physical deletion was not reported pending", receipt, err)
	}
	f.restart(t)
	outputs.fail = false
	service = resultReleaser(t, f, outputs)
	recovered, err := service.Release(ctx, actor, "a", id.JobID, "release-original", request)
	if err != nil || recovered.PendingFiles != 0 || recovered.ReclaimedBytes != receipt.Bytes || recovered.ReleasedAt != receipt.ReleasedAt {
		t.Fatal("pending release did not resume", recovered, err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM retention_expirations") != 1 || controlCount(t, f.s, "SELECT count(*) FROM result_releases") != 1 {
		t.Fatal("recovery invented another release")
	}
	assertNoNewCompute(t, f)
}

func TestResultReleaseRefusesHeldWorkAndRecoversThroughOrdinaryRetention(t *testing.T) {
	ctx := t.Context()
	f, id, p, results := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, results, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Operate)
	request := releaseRequest(t, f)
	outputs := &failingReleaseBlobs{Store: results, fail: true}
	service := resultReleaser(t, f, outputs)
	_, err := f.s.db.Exec(`UPDATE work_leases SET held=1,generation=generation+1,fence=? WHERE kind='collection'`, strings.Repeat("b", 64))
	requireNoError(t, err)
	if _, err := service.Release(ctx, actor, "a", id.JobID, "release-original", request); !errors.Is(err, retention.ErrPinned) {
		t.Fatal("release ignored held collection work", err)
	}
	if outputs.calls != 0 || f.state(t, id).Result != domain.ResultAvailable {
		t.Fatal("pinned release deleted results")
	}
	_, err = f.s.db.Exec(`UPDATE work_leases SET held=0 WHERE kind='collection'`)
	requireNoError(t, err)
	pending, err := service.Release(ctx, actor, "a", id.JobID, "release-original", request)
	requireNoError(t, err)
	f.restart(t)
	// Recovery is independent of the consumer. The ordinary hourly worker can
	// finish committed deletion even while the seven-day age window is open.
	sweeper, err := retention.NewSweeper(f.s, f.blobs, results)
	requireNoError(t, err)
	_, _, err = sweeper.SweepOnce(ctx, retention.DefaultPolicy(), 0, 100)
	requireNoError(t, err)
	service = resultReleaser(t, f, results)
	recovered, err := service.Release(ctx, actor, "a", id.JobID, "release-original", request)
	requireNoError(t, err)
	if pending.PendingFiles != 5 || recovered.PendingFiles != 0 || recovered.ReclaimedBytes != pending.Bytes {
		t.Fatal("retention worker did not recover released bytes", pending, recovered)
	}
}

func TestResultReleaseAcknowledgesAlreadyExpiredPublication(t *testing.T) {
	f, id, p, results := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, results, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Operate)
	request := releaseRequest(t, f)
	f.clock.advance(8 * 24 * time.Hour)
	scanRetention(t, f)
	service := resultReleaser(t, f, results)
	receipt, err := service.Release(t.Context(), actor, "a", id.JobID, "release-after-expiry", request)
	if err != nil || receipt.PendingFiles != 0 || receipt.ReclaimedBytes != receipt.Bytes {
		t.Fatal("automatic expiry could not be acknowledged", receipt, err)
	}
}
