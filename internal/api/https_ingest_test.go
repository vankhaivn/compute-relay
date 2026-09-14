package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/httpsinput"
	"github.com/vankhaivn/compute-relay/internal/objects"
)

type ingestFetch func(context.Context, httpsinput.Request, func(context.Context, int64, io.Reader) error) error

func (f ingestFetch) Fetch(ctx context.Context, r httpsinput.Request, consume func(context.Context, int64, io.Reader) error) error {
	return f(ctx, r, consume)
}

func inputOption(t *testing.T, fetch objects.HTTPSFetcher) func(*http.Server) {
	t.Helper()
	return func(s *http.Server) {
		h := s.Handler.(*handler)
		var err error
		h.config.HTTPSInputs, err = objects.NewIngestor(h.objects, fetch)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func inputJSON(r *http.Request) { r.Header.Set("Content-Type", "application/json") }

func TestHTTPSIngestHTTPUsesExistingObjectBoundary(t *testing.T) {
	payload := "synthetic public input"
	digest := sha256.Sum256([]byte(payload))
	var calls atomic.Int32
	f := setupHTTP(t, func(c *Config) { c.MaxJSONBytes = 1024 }, nil, inputOption(t, ingestFetch(func(ctx context.Context, r httpsinput.Request, consume func(context.Context, int64, io.Reader) error) error {
		calls.Add(1)
		if r.URL != "https://example.com/data?q=source-canary" {
			t.Error("source changed")
		}
		return consume(ctx, int64(len(payload)), strings.NewReader(payload))
	})))
	encoded, err := json.Marshal(httpsinput.Request{URL: "https://example.com/data?q=source-canary", SHA256: hex.EncodeToString(digest[:])})
	if err != nil {
		t.Fatal(err)
	}
	response, body := f.request(t, "POST", "/v1/workspaces/a/objects/ingest", "a", strings.NewReader(string(encoded)), inputJSON)
	assertStatus(t, response, body, 201)
	if strings.Contains(string(body), "source-canary") {
		t.Fatal("URL echoed")
	}
	var meta domain.ObjectMetadata
	if err := json.Unmarshal(body, &meta); err != nil || !meta.Valid() || meta.Bytes != int64(len(payload)) || string(meta.SHA256) != hex.EncodeToString(digest[:]) {
		t.Fatal("invalid receipt", err)
	}
	location := response.Header.Get("Location")
	response, body = f.request(t, "GET", location, "a", nil, nil)
	assertStatus(t, response, body, 200)
	response, body = f.request(t, "GET", "/v1/workspaces/b/objects/"+string(meta.ID), "b", nil, nil)
	assertStatus(t, response, body, 404)
	stored, err := f.blobs.Open(context.Background(), "a", meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(stored)
	stored.Close()
	if err != nil || string(data) != payload || calls.Load() != 1 || f.catalog.ObjectCount() != 1 {
		t.Fatal("snapshot mismatch or implicit refetch", err)
	}
}

func TestHTTPSIngestHTTPRejectsBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	f := setupHTTP(t, nil, nil, inputOption(t, ingestFetch(func(context.Context, httpsinput.Request, func(context.Context, int64, io.Reader) error) error {
		calls.Add(1)
		return httpsinput.ErrFetch
	})))
	for _, tc := range []struct {
		name, who, workspace, body string
		status                     int
		change                     func(*http.Request)
	}{
		{"no auth", "", "a", `{"url":"https://example.com/data"}`, 401, inputJSON},
		{"read only", "read", "a", `{"url":"https://example.com/data"}`, 403, inputJSON},
		{"workspace", "a", "b", `{"url":"https://example.com/data"}`, 403, inputJSON},
		{"scheme", "a", "a", `{"url":"http://example.com/data"}`, 400, inputJSON},
		{"private", "a", "a", `{"url":"https://127.0.0.1/metadata"}`, 400, inputJSON},
		{"userinfo", "a", "a", `{"url":"https://user:source-canary@example.com/"}`, 400, inputJSON},
		{"headers", "a", "a", `{"url":"https://example.com/","headers":{"Authorization":"source-canary"}}`, 400, inputJSON},
		{"case", "a", "a", `{"URL":"https://example.com/"}`, 400, inputJSON},
		{"duplicate", "a", "a", `{"url":"https://example.com/","url":"https://example.com/"}`, 400, inputJSON},
		{"null", "a", "a", `{"url":null}`, 400, inputJSON},
		{"trailing", "a", "a", `{"url":"https://example.com/"} {}`, 400, inputJSON},
		{"big body", "a", "a", `{"url":"https://example.com/` + strings.Repeat("x", 200) + `"}`, 413, inputJSON},
		{"media", "a", "a", `{"url":"https://example.com/"}`, 415, nil},
		{"encoding", "a", "a", `{"url":"https://example.com/"}`, 415, func(r *http.Request) { inputJSON(r); r.Header.Set("Content-Encoding", "gzip") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, b := f.request(t, "POST", "/v1/workspaces/"+tc.workspace+"/objects/ingest", tc.who, strings.NewReader(tc.body), tc.change)
			assertStatus(t, r, b, tc.status)
			if strings.Contains(string(b), "source-canary") {
				t.Fatal("source credentials echoed")
			}
		})
	}
	if calls.Load() != 0 || f.catalog.ObjectCount() != 0 {
		t.Fatal("rejected input made network request")
	}
	disabled := setupHTTP(t, nil, nil)
	r, b := disabled.request(t, "POST", "/v1/workspaces/a/objects/ingest", "a", strings.NewReader(`{"url":"https://example.com/"}`), inputJSON)
	assertStatus(t, r, b, 403)
}

func TestHTTPSIngestHTTPMapsFailuresWithoutPublishing(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{
		{httpsinput.ErrURL, 400}, {httpsinput.ErrBlocked, 400}, {httpsinput.ErrFetch, 400},
		{httpsinput.ErrTimeout, 408}, {httpsinput.ErrLimit, 413}, {httpsinput.ErrDigest, 400},
		{httpsinput.ErrBusy, 429}, {errors.New("backend-source-canary"), 503},
	} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			f := setupHTTP(t, nil, nil, inputOption(t, ingestFetch(func(context.Context, httpsinput.Request, func(context.Context, int64, io.Reader) error) error {
				return tc.err
			})))
			r, b := f.request(t, "POST", "/v1/workspaces/a/objects/ingest", "a", strings.NewReader(`{"url":"https://example.com/"}`), inputJSON)
			assertStatus(t, r, b, tc.status)
			if f.catalog.ObjectCount() != 0 || strings.Contains(string(b), "canary") {
				t.Fatal("unsafe failure response")
			}
		})
	}
}

func TestHTTPSIngestHTTPRechecksRevocation(t *testing.T) {
	var f *fixture
	f = setupHTTP(t, nil, nil, inputOption(t, ingestFetch(func(ctx context.Context, _ httpsinput.Request, consume func(context.Context, int64, io.Reader) error) error {
		if err := f.access.Revoke(ctx, f.tokenIDs["a"]); err != nil {
			return err
		}
		return consume(ctx, 7, strings.NewReader("payload"))
	})))
	r, b := f.request(t, "POST", "/v1/workspaces/a/objects/ingest", "a", strings.NewReader(`{"url":"https://example.com/"}`), inputJSON)
	assertStatus(t, r, b, 401)
	if f.catalog.ObjectCount() != 0 {
		t.Fatal("revoked request committed")
	}
}
