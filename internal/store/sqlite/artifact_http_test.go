package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/appcli"
	"github.com/vankhaivn/compute-relay/internal/appclient"
	"github.com/vankhaivn/compute-relay/internal/artifactwire"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func servePublishedFixture(t *testing.T, f *dispatchFixture, blobs collection.BlobStore) (string, func()) {
	t.Helper()
	access, err := auth.New(f.s, f.s, nil)
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

func TestPublishedArtifactHTTPAndCLIReuseImmutableM3Publication(t *testing.T) {
	ctx := context.Background()
	f, id, provider, blobs := collectionFixture(t)
	service, actor, token := controlService(t, f, "a", auth.Read, auth.Operate)
	original, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCollect, "artifact-http-original", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	target := artifactwire.Target{WorkspaceID: "a", JobID: string(id.JobID), AttemptID: string(id.AttemptID)}
	endpoint, stop := servePublishedFixture(t, f, blobs)
	if _, err := appclient.ListArtifacts(ctx, endpoint, []byte(token), target, "", 2); err == nil {
		t.Fatal("accepted collection ticket exposed unverified results")
	}
	stop()
	runCollection(t, collectionEngine(t, f, provider, blobs, f.s))
	before := f.state(t, id)
	events := controlCount(t, f.s, "SELECT count(*) FROM events")
	providerLists, providerFetches := provider.counts()
	endpoint, stop = servePublishedFixture(t, f, blobs)
	first, err := appclient.ListArtifacts(ctx, endpoint, []byte(token), target, "", 2)
	if err != nil || first.Total != 5 || len(first.Artifacts) != 2 || first.NextCursor == "" {
		t.Fatal(first, err)
	}
	stop()
	f.restart(t)
	endpoint, stop = servePublishedFixture(t, f, blobs)
	all := append([]artifactwire.File{}, first.Artifacts...)
	cursor := first.NextCursor
	for cursor != "" {
		page, err := appclient.ListArtifacts(ctx, endpoint, []byte(token), target, cursor, 2)
		if err != nil || page.SnapshotSHA256 != first.SnapshotSHA256 {
			t.Fatal("restart changed published cursor", err)
		}
		all = append(all, page.Artifacts...)
		cursor = page.NextCursor
	}
	if len(all) != 5 {
		t.Fatal("incomplete publication")
	}
	private, err := statefs.PrivateDir(filepath.Join(t.TempDir(), "client"), true)
	if err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(private, "token")
	if statefs.WriteNew(tokenFile, []byte(token+"\n")) != nil {
		t.Fatal("cannot provision fixture application token")
	}
	for _, file := range all {
		output := filepath.Join(private, file.ID+".download")
		args := []string{"artifact", "download", "--url", endpoint, "--workspace", "a", "--token-file", tokenFile, "--job", target.JobID, "--attempt", target.AttemptID, "--id", file.ID, "--output", output}
		var out, diagnostic bytes.Buffer
		if code := appcli.RunArtifacts(ctx, args, &out, &diagnostic); code != 0 {
			t.Fatal("CLI download failed", code, diagnostic.String())
		}
		var receipt struct {
			artifactwire.Metadata
			Delivery string `json:"delivery"`
		}
		if json.Unmarshal(out.Bytes(), &receipt) != nil || receipt.Target != target || receipt.Artifact != file || receipt.Delivery != "verified-new-file" {
			t.Fatal("receipt changed identity")
		}
		actual, err := os.ReadFile(output)
		if err != nil || !bytes.Equal(actual, provider.files[file.Path]) || statefs.CheckFile(output) != nil {
			t.Fatal("application received incorrect or unprotected bytes", err)
		}
		out.Reset()
		diagnostic.Reset()
		if code := appcli.RunArtifacts(ctx, args, &out, &diagnostic); code == 0 || out.Len() != 0 {
			t.Fatal("download overwrote existing destination")
		}
	}
	foreign := target
	foreign.AttemptID = "att_foreign"
	if _, err := appclient.ArtifactMetadata(ctx, endpoint, []byte(token), foreign, all[0].ID); err == nil {
		t.Fatal("foreign attempt exposed a committed file")
	}
	if after := f.state(t, id); !reflect.DeepEqual(after, before) || controlCount(t, f.s, "SELECT count(*) FROM events") != events {
		t.Fatal("read-only delivery rewrote durable outcome/events")
	}
	if l, n := provider.counts(); l != providerLists || n != providerFetches {
		t.Fatal("local download repeated provider collection")
	}
	stop()
	// Recreate the service after reopen, retaining the original issued token/receipt.
	access, _ := auth.New(f.s, f.s, nil)
	fresh, err := access.Authenticate(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	service, _, _ = controlService(t, f, "a", auth.Read, auth.Operate)
	replay, err := service.Submit(ctx, fresh, "a", id.JobID, domain.OperationCollect, "artifact-http-original", controlRequest(id.AttemptID))
	if err != nil || !replay.Replay || !reflect.DeepEqual(replay.Operation, original.Operation) || replay.Effect != original.Effect {
		t.Fatal("download changed original collect receipt", err)
	}
	f.clock.advance(8 * 24 * time.Hour)
	scanRetention(t, f)
	endpoint, stop = servePublishedFixture(t, f, blobs)
	_, err = appclient.ListArtifacts(ctx, endpoint, []byte(token), target, "", 2)
	var failure *appclient.Failure
	if !errors.As(err, &failure) || failure.HTTPStatus != 410 || failure.RequestMayHaveCommitted {
		t.Fatal("expiry is not explicit read-only 410", err)
	}
	if err := f.s.RevokeToken(ctx, actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	_, err = appclient.ListArtifacts(ctx, endpoint, []byte(token), target, "", 2)
	if !errors.As(err, &failure) || failure.HTTPStatus != 401 {
		t.Fatal("expired metadata bypassed current token authority", err)
	}
	stop()
	assertNoNewCompute(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 1 || controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 5 {
		t.Fatal("delivery/expiry changed publication history")
	}
}
