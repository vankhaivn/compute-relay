package api

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/localinput"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/testsupport"
)

func importFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	inputs, err := localinput.New([]localinput.RootSpec{{Name: "source", Path: root, Workspaces: []string{"a"}}}, packaging.DefaultLimits(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	mem := testsupport.NewMemory(auth.Workspace{ID: "a", Enabled: true}, auth.Workspace{ID: "b", Enabled: true})
	access, _ := auth.New(mem, mem, nil)
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	svc, _ := objects.New(access, blobs, mem)
	importer, _ := objects.NewImporter(svc, inputs)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Listen = ln.Addr().String()
	cfg.LocalImports = importer
	cfg.GlobalRate = Rate{10000, 10000}
	cfg.WorkspaceRate = Rate{10000, 10000}
	server, err := NewServer(cfg, access, svc, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{server: server, access: access, catalog: mem, blobs: blobs, url: "http://" + ln.Addr().String(), tokens: map[string]string{}, tokenIDs: map[string]string{}}
	for _, who := range []string{"a", "b", "read"} {
		w := domain.WorkspaceID(who)
		scopes := []auth.Scope{auth.Read, auth.Write}
		if who == "read" {
			w = "a"
			scopes = []auth.Scope{auth.Read}
		}
		secret, record, err := access.Issue(context.Background(), w, scopes, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[who] = secret.Reveal()
		f.tokenIDs[who] = record.ID
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(ln) }()
	t.Cleanup(func() { _ = server.Close(); <-done; _ = inputs.Close(); _ = blobs.Close() })
	return f
}
func TestLocalImportHTTPAndStrictRequestBoundary(t *testing.T) {
	f := importFixture(t)
	url := "/v1/workspaces/a/objects/import"
	jsonMedia := func(r *http.Request) { r.Header.Set("Content-Type", "application/json") }
	valid := `{"root":"source","kind":"file","path":"file.txt"}`
	response, body := f.request(t, "POST", url, "a", strings.NewReader(valid), jsonMedia)
	assertStatus(t, response, body, 201)
	var meta domain.ObjectMetadata
	if err := json.Unmarshal(body, &meta); err != nil || !meta.Valid() {
		t.Fatal("invalid receipt", err)
	}
	response, body = f.request(t, "GET", response.Header.Get("Location"), "a", nil, nil)
	assertStatus(t, response, body, 200)
	for _, tc := range []struct {
		name, who, body string
		want            int
	}{
		{"no token", "", valid, 401}, {"read scope", "read", valid, 403}, {"wrong workspace", "b", valid, 403},
		{"root denied", "a", `{"root":"missing","kind":"file","path":"file.txt"}`, 403},
		{"absolute path", "a", `{"root":"source","kind":"file","path":"/etc/passwd"}`, 400},
		{"traversal", "a", `{"root":"source","kind":"file","path":"../outside"}`, 400},
		{"unknown field", "a", `{"root":"source","kind":"file","path":"file.txt","command":"do-not-run"}`, 400},
		{"duplicate root", "a", `{"root":"source","root":"source","kind":"file","path":"file.txt"}`, 400},
		{"null path", "a", `{"root":"source","kind":"file","path":null}`, 400},
		{"implicit project", "a", `{"root":"source","kind":"bundle","includes":["."]}`, 400},
		{"trailing JSON", "a", valid + ` {}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, b := f.request(t, "POST", url, tc.who, strings.NewReader(tc.body), jsonMedia)
			assertStatus(t, r, b, tc.want)
		})
	}
	response, body = f.request(t, "POST", url, "a", strings.NewReader(`{"root":"source","kind":"bundle","includes":["file.txt"]}`), jsonMedia)
	assertStatus(t, response, body, 201)
	if f.catalog.ObjectCount() != 2 {
		t.Fatal("rejected imports changed ownership metadata")
	}
	disabled := setupHTTP(t, nil, nil)
	response, body = disabled.request(t, "POST", url, "a", strings.NewReader(valid), jsonMedia)
	assertStatus(t, response, body, 403)
}
