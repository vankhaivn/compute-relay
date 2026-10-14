package sqlite

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestPublishedCollectionReadsPreserveSQLHistory(t *testing.T) {
	ctx := context.Background()
	f, id, provider, blobs := collectionFixture(t)
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	original, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCollect, "publication-read-original", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	runCollection(t, collectionEngine(t, f, provider, blobs, f.s))
	state := f.state(t, id)
	counts := map[string]int{}
	for _, table := range []string{"events", "artifacts", "collection_publications", "operations", "receipts"} {
		counts[table] = controlCount(t, f.s, "SELECT count(*) FROM "+table)
	}
	if counts["artifacts"] != 5 || counts["collection_publications"] != 1 {
		t.Fatal("missing atomic publication", counts)
	}
	access, err := auth.New(f.s, f.s)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := collection.NewReader(access, f.s, blobs)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Read(ctx, actor, "a", id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range result.Files {
		_, stream, err := reader.Open(ctx, actor, "a", id.JobID, id.AttemptID, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, stream)
		if err := stream.Close(); err != nil || readErr != nil {
			t.Fatal("read committed file", readErr, err)
		}
	}
	replay, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCollect, "publication-read-original", controlRequest(id.AttemptID))
	if err != nil || !replay.Replay || !reflect.DeepEqual(replay.Operation, original.Operation) || replay.Effect != original.Effect {
		t.Fatal("read changed original receipt", err)
	}
	if !reflect.DeepEqual(f.state(t, id), state) {
		t.Fatal("read changed durable outcome")
	}
	for table, before := range counts {
		if after := controlCount(t, f.s, "SELECT count(*) FROM "+table); after != before {
			t.Fatal("read changed durable history", table, before, after)
		}
	}
	f.clock.advance(8 * 24 * time.Hour)
	scanRetention(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 || controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 5 {
		t.Fatal("expiry erased publication history")
	}
}
