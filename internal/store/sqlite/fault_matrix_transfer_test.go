package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestFaultMatrixLargeTransferFailureRecoversWithoutCompute(t *testing.T) {
	ctx := context.Background()
	f, id, p, _ := collectionFixture(t)
	// A 16 MiB synthetic file spans many bounded transfer reads. Fail after its
	// first 8 MiB, then require an explicit same-pin retry after database reopen.
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<20)
	digest := provider.Digest(data)
	p.files["outputs/answer.json"] = data
	var manifest map[string]any
	if err := json.Unmarshal(p.files[collection.ManifestPath], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest["artifacts"] = []any{map[string]any{"path": "answer.json", "bytes": len(data), "sha256": digest}}
	var err error
	p.files[collection.ManifestPath], err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "large-results"), blobfs.Limits{MaxObjectBytes: 32 << 20, MaxTotalBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	p.mode = "partial"
	engine := collectionEngine(t, f, p, blobs, f.s)
	worked, err := engine.RunOnce(ctx)
	var failure collection.Failure
	if !worked || !errors.As(err, &failure) || p.fetches["outputs/answer.json"] != 1 || f.state(t, id).Result == domain.ResultAvailable {
		t.Fatal("large partial transfer was not rejected", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_snapshots") != 1 {
		t.Fatal("partial bytes escaped publication or lost the original pin")
	}
	if worked, err := engine.RunOnce(ctx); worked || err != nil {
		t.Fatal("failed collection was automatically retried", err)
	}
	lists, _ := p.counts()
	f.restart(t)
	f.clock.advance(time.Minute)
	p.mode = ""
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	if _, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCollect, "large-transfer-recovery", controlRequest(id.AttemptID)); err != nil {
		t.Fatal(err)
	}
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	after, _ := p.counts()
	if after != lists || p.fetches["outputs/answer.json"] != 2 {
		t.Fatal("recovery changed the catalog or repeated extra file transfers")
	}
	result, err := f.s.ReadCollection(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range result.Files {
		if file.Object.SHA256 != digest {
			continue
		}
		found = true
		stream, err := blobs.Open(ctx, "a", file.Object.ID)
		if err != nil {
			t.Fatal(err)
		}
		actual, readErr := io.ReadAll(io.LimitReader(stream, int64(len(data))+1))
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(actual, data) || file.Object.Bytes != int64(len(data)) {
			t.Fatal("recovered file differs from the original frozen bytes")
		}
	}
	if !found || f.state(t, id).Result != domain.ResultAvailable {
		t.Fatal("verified large file was not published")
	}
	assertNoNewCompute(t, f)
}
