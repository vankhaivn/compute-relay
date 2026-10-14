package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
)

func servePublishedFixture(t *testing.T, f *dispatchFixture, blobs collection.BlobStore) (string, func()) {
	t.Helper()
	access, err := auth.New(f.s, f.s)
	if err != nil {
		t.Fatal(err)
	}
	results, err := collection.NewReader(access, f.s, blobs)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := objects.New(access, f.blobs, f.s)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := api.DefaultConfig()
	cfg.Listen, cfg.Results = listener.Addr().String(), results
	cfg.GlobalRate, cfg.WorkspaceRate = api.Rate{PerSecond: 10000, Burst: 10000}, api.Rate{PerSecond: 10000, Burst: 10000}
	server, err := api.NewServer(cfg, access, inputs, f.s.Ready)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	server.ErrorLog = log.New(io.Discard, "", 0)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			_ = server.Close()
			t.Error(err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	}
	t.Cleanup(stop)
	return "http://" + listener.Addr().String(), stop
}

func TestPublishedArtifactHTTPReusesImmutablePublication(t *testing.T) {
	ctx := context.Background()
	f, id, provider, blobs := collectionFixture(t)
	service, actor, token := controlService(t, f, "a", auth.Read, auth.Operate)
	original, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCollect, "artifact-http-original", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	endpoint, stop := servePublishedFixture(t, f, blobs)
	path := "/v1/workspaces/a/jobs/" + string(id.JobID) + "/artifacts"
	request := func(path string) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	query := "?attempt_id=" + string(id.AttemptID)
	response := request(path + query)
	if response.StatusCode == http.StatusOK {
		t.Fatal("unverified results exposed")
	}
	response.Body.Close()
	stop()
	runCollection(t, collectionEngine(t, f, provider, blobs, f.s))
	before := f.state(t, id)
	events := controlCount(t, f.s, "SELECT count(*) FROM events")
	lists, fetches := provider.counts()
	endpoint, stop = servePublishedFixture(t, f, blobs)
	type file struct {
		ID           string `json:"artifact_id"`
		Path, SHA256 string
		Bytes        int64
	}
	type page struct {
		SnapshotSHA256 string `json:"snapshot_sha256"`
		NextCursor     string `json:"next_cursor"`
		Total          int    `json:"total_artifacts"`
		Artifacts      []file
	}
	readPage := func(cursor string) page {
		t.Helper()
		response := request(path + query + "&limit=2&cursor=" + url.QueryEscape(cursor))
		defer response.Body.Close()
		var result page
		if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&result) != nil {
			t.Fatal("invalid artifact page", response.StatusCode)
		}
		return result
	}
	first := readPage("")
	if first.Total != 5 || len(first.Artifacts) != 2 || first.NextCursor == "" {
		t.Fatal(first)
	}
	stop()
	f.restart(t)
	endpoint, stop = servePublishedFixture(t, f, blobs)
	all := first.Artifacts
	for cursor := first.NextCursor; cursor != ""; {
		page := readPage(cursor)
		if page.SnapshotSHA256 != first.SnapshotSHA256 {
			t.Fatal("restart changed publication")
		}
		all = append(all, page.Artifacts...)
		cursor = page.NextCursor
	}
	if len(all) != 5 {
		t.Fatal("incomplete publication")
	}
	for _, file := range all {
		response := request(path + "/" + file.ID + "/content" + query)
		actual, err := io.ReadAll(response.Body)
		response.Body.Close()
		sum := sha256.Sum256(actual)
		if err != nil || response.StatusCode != 200 || int64(len(actual)) != file.Bytes || hex.EncodeToString(sum[:]) != file.SHA256 || !bytes.Equal(actual, provider.files[file.Path]) {
			t.Fatal("incorrect published bytes", err)
		}
	}
	response = request(path + "/" + all[0].ID + "?attempt_id=att_foreign")
	response.Body.Close()
	if response.StatusCode == 200 {
		t.Fatal("foreign attempt exposed committed file")
	}
	if !reflect.DeepEqual(f.state(t, id), before) || controlCount(t, f.s, "SELECT count(*) FROM events") != events {
		t.Fatal("delivery rewrote durable outcome")
	}
	if l, n := provider.counts(); l != lists || n != fetches {
		t.Fatal("delivery repeated provider collection")
	}
	stop()
	access, _ := auth.New(f.s, f.s)
	fresh, err := access.Authenticate(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	service, _, _ = controlService(t, f, "a", auth.Read, auth.Operate)
	replay, err := service.Submit(ctx, fresh, "a", id.JobID, domain.OperationCollect, "artifact-http-original", controlRequest(id.AttemptID))
	if err != nil || !replay.Replay || !reflect.DeepEqual(replay.Operation, original.Operation) || replay.Effect != original.Effect {
		t.Fatal("delivery changed receipt", err)
	}
	f.clock.advance(8 * 24 * time.Hour)
	scanRetention(t, f)
	endpoint, stop = servePublishedFixture(t, f, blobs)
	response = request(path + query)
	response.Body.Close()
	if response.StatusCode != 410 {
		t.Fatal("expiry did not return 410", response.StatusCode)
	}
	if err := f.s.RevokeToken(ctx, actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	response = request(path + query)
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatal("expiry bypassed current authority", response.StatusCode)
	}
	stop()
	assertNoNewCompute(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 || controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 5 {
		t.Fatal("delivery changed history")
	}
}
