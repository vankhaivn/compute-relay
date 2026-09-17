package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/appclient"
	"github.com/vankhaivn/compute-relay/internal/artifactwire"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

// This tier injects publication/source faults. The separate SQLite test creates
// publications through the real M3 collection engine, not this fixture repository.
type artifactReadFixture struct {
	result collection.Result
	bytes  map[domain.ObjectID][]byte
	fault  string
	expired atomic.Bool
	reads   atomic.Int64
	closed  atomic.Int64
	onClose func()
}

func (f *artifactReadFixture) ReadCollection(_ context.Context, w domain.WorkspaceID, _ string, j domain.JobID, a domain.AttemptID) (collection.Result, error) {
	f.reads.Add(1)
	if w != f.result.WorkspaceID || j != f.result.JobID || a != f.result.AttemptID {
		return collection.Result{}, collection.ErrNotFound
	}
	if f.expired.Load() {
		return collection.Result{}, retention.ErrExpired
	}
	r := f.result
	r.Files = append([]collection.File{}, r.Files...)
	return r, nil
}
func (f *artifactReadFixture) Put(context.Context, domain.ObjectMetadata, io.Reader) (domain.ObjectMetadata, error) {
	return domain.ObjectMetadata{}, errors.New("read-only fixture cannot publish")
}
func (f *artifactReadFixture) Open(_ context.Context, w domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	data, ok := f.bytes[id]
	if w != f.result.WorkspaceID || !ok {
		return nil, collection.ErrNotFound
	}
	copyData := append([]byte{}, data...)
	switch f.fault {
	case "short":
		copyData = copyData[:len(copyData)/2]
	case "extra":
		copyData = append(copyData, 'x')
	case "wrong":
		copyData[0] ^= 1
	}
	return &artifactFaultStream{reader: bytes.NewReader(copyData), owner: f}, nil
}

type artifactFaultStream struct {
	reader *bytes.Reader
	owner  *artifactReadFixture
}

func (s *artifactFaultStream) Read(p []byte) (int, error) {
	n, err := s.reader.Read(p)
	if s.reader.Len() == 0 && s.owner.fault == "final-read" {
		return n, errors.New("PRIVATE_SOURCE_ERROR")
	}
	return n, err
}
func (s *artifactFaultStream) Close() error {
	s.owner.closed.Add(1)
	if s.owner.onClose != nil {
		s.owner.onClose()
	}
	if s.owner.fault == "close" {
		return errors.New("PRIVATE_CLOSE_ERROR")
	}
	if s.owner.fault == "close-panic" {
		panic("PRIVATE_CLOSE_PANIC")
	}
	return nil
}

func artifactHTTPFixture(t *testing.T, payload []byte) (*fixture, *artifactReadFixture, artifactwire.Target) {
	t.Helper()
	target := artifactwire.Target{WorkspaceID: "a", JobID: "job_one", AttemptID: "att_one"}
	repo := &artifactReadFixture{bytes: map[domain.ObjectID][]byte{}, result: collection.Result{
		WorkspaceID: "a", JobID: "job_one", AttemptID: "att_one", Phase: "completed", VerifiedAt: time.Now().UTC(),
	}}
	for _, entry := range []struct{ id, path, role string; data []byte }{
		{"art_manifest", collection.ManifestPath, "manifest", []byte("{}")},
		{"art_output", "outputs/answer.bin", "output", payload},
	} {
		object := domain.ObjectMetadata{ID: domain.ObjectID(entry.id), WorkspaceID: "a", Bytes: int64(len(entry.data)), SHA256: provider.Digest(entry.data)}
		repo.result.Files = append(repo.result.Files, collection.File{ID: domain.ArtifactID(entry.id), Path: entry.path, Role: entry.role, Object: object})
		repo.bytes[object.ID] = entry.data
	}
	f := setupHTTP(t, nil, nil, func(server *http.Server) {
		h := server.Handler.(*handler)
		reader, err := collection.NewReader(h.auth, repo, repo)
		if err != nil {
			t.Fatal(err)
		}
		h.config.Results = reader
		server.ErrorLog = log.New(io.Discard, "", 0)
	})
	return f, repo, target
}

func TestArtifactHTTPExplicitTargetsPaginationAndSafeDownload(t *testing.T) {
	payload := bytes.Repeat([]byte("verified-large-file\n"), 1<<20)
	f, repo, target := artifactHTTPFixture(t, payload)
	ctx := context.Background()
	token := []byte(f.tokens["read"])
	first, err := appclient.ListArtifacts(ctx, f.url, token, target, "", 1)
	if err != nil || first.Total != 2 || first.NextCursor == "" || len(first.Artifacts) != 1 {
		t.Fatal(first, err)
	}
	last, err := appclient.ListArtifacts(ctx, f.url, token, target, first.NextCursor, 1)
	if err != nil || last.NextCursor != "" || last.Artifacts[0].ID != "art_output" {
		t.Fatal(last, err)
	}
	pin, err := appclient.ArtifactMetadata(ctx, f.url, token, target, "art_output")
	if err != nil {
		t.Fatal(err)
	}
	var dst bytes.Buffer
	if err := appclient.DownloadArtifact(ctx, f.url, token, pin, &dst); err != nil || !bytes.Equal(dst.Bytes(), payload) || repo.closed.Load() != 1 {
		t.Fatal("verified download failed", err, dst.Len())
	}
	prefix := "/v1/workspaces/a/jobs/job_one/artifacts"
	for _, tc := range []struct{ path, who string; code int }{
		{prefix, "a", 400},
		{prefix + "?attempt_id=att_one&attempt_id=att_one", "a", 400},
		{prefix + "?attempt_id=att_one&limit=01", "a", 400},
		{prefix + "?attempt_id=att_one&extra=x", "a", 400},
		{prefix + "?attempt_id=att_other", "a", 404},
		{prefix + "?attempt_id=att_one", "b", 403},
		{prefix + "?attempt_id=att_one", "", 401},
		{prefix + "/art_foreign?attempt_id=att_one", "a", 404},
		{prefix + "?attempt_id=att_one&cursor=ar1_wrong", "a", 409},
	} {
		resp, body := f.request(t, "GET", tc.path, tc.who, nil, nil)
		assertStatus(t, resp, body, tc.code)
	}
	resp, body := f.request(t, "HEAD", prefix+"?attempt_id=att_one", "a", nil, nil)
	if resp.StatusCode != 405 || len(body) != 0 || resp.Header.Get("Allow") != "GET" {
		t.Fatal("HEAD unexpectedly read bytes")
	}
	resp, body = f.request(t, "GET", prefix+"/art_output/content?attempt_id=att_one", "a", nil, func(r *http.Request) { r.Header.Set("Range", "bytes=0-2") })
	assertStatus(t, resp, body, 400)
	resp, body = f.request(t, "GET", prefix+"/art_manifest/content?attempt_id=att_one", "a", nil, nil)
	if resp.StatusCode != 200 || string(body) != "{}" || resp.Header.Get("Content-Type") != "application/octet-stream" || !strings.HasPrefix(resp.Header.Get("Content-Disposition"), "attachment;") || resp.Header.Get("X-Content-Type-Options") != "nosniff" || resp.ContentLength != -1 || resp.Trailer.Get(artifactwire.VerifiedTrailer) != "true" {
		t.Fatal("unsafe binary response", resp.Header)
	}
	repo.expired.Store(true)
	resp, body = f.request(t, "GET", prefix+"?attempt_id=att_one", "a", nil, nil)
	assertStatus(t, resp, body, 410)
	if !bytes.Contains(body, []byte("expired")) {
		t.Fatal("expiry disguised as absence")
	}
}

func TestArtifactHTTPLastByteFaultsNeverReceiveSuccessAcknowledgement(t *testing.T) {
	for _, mode := range []string{"short", "extra", "wrong", "final-read", "close", "close-panic", "revoke", "expire"} {
		t.Run(mode, func(t *testing.T) {
			f, repo, target := artifactHTTPFixture(t, bytes.Repeat([]byte("original"), 16384))
			pin, err := appclient.ArtifactMetadata(context.Background(), f.url, []byte(f.tokens["a"]), target, "art_output")
			if err != nil {
				t.Fatal(err)
			}
			repo.fault = mode
			if mode == "expire" {
				repo.onClose = func() { repo.expired.Store(true) }
			}
			if mode == "revoke" {
				repo.onClose = func() { _ = f.access.Revoke(context.Background(), f.tokenIDs["a"]) }
			}
			var dst bytes.Buffer
			err = appclient.DownloadArtifact(context.Background(), f.url, []byte(f.tokens["a"]), pin, &dst)
			if err == nil || repo.closed.Load() != 1 || bytes.Contains(dst.Bytes(), []byte("PRIVATE_")) {
				t.Fatal("failed bytes qualified or leaked diagnostics", err, repo.closed.Load())
			}
		})
	}
}

func TestArtifactHTTPEmptyFileAndUncomposedRoutes(t *testing.T) {
	f, _, target := artifactHTTPFixture(t, nil)
	pin, err := appclient.ArtifactMetadata(context.Background(), f.url, []byte(f.tokens["a"]), target, "art_output")
	if err != nil || appclient.DownloadArtifact(context.Background(), f.url, []byte(f.tokens["a"]), pin, io.Discard) != nil {
		t.Fatal("empty artifact lost final acknowledgement", err)
	}
	base := setupHTTP(t, nil, nil)
	resp, body := base.request(t, "GET", "/v1/workspaces/a/jobs/job_one/artifacts?attempt_id=att_one", "a", nil, nil)
	assertStatus(t, resp, body, 404)
	resp, body = f.request(t, "GET", "/v1/info", "a", nil, nil)
	var info struct{ Features []string `json:"features"` }
	if json.Unmarshal(body, &info) != nil || !strings.Contains(strings.Join(info.Features, ","), "artifact_download") {
		t.Fatal("composed capability missing", resp.StatusCode)
	}
}
