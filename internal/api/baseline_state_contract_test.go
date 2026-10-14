package api_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/statefs"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type baselineContractRequest struct {
	Name     string          `json:"name"`
	Method   string          `json:"method"`
	Path     string          `json:"path"`
	Key      string          `json:"idempotency_key,omitempty"`
	Body     json.RawMessage `json:"body,omitempty"`
	Status   int             `json:"status"`
	Response json.RawMessage `json:"response"`
}

// Restore the immutable standalone format through database/sql. The historical
// dump and synthetic token are checked-in fixtures; no current Store internals or
// fixture generators are exposed to the HTTP contract test.
func baselineContractState(t *testing.T, script []byte) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := statefs.WriteNew(filepath.Join(root, ".compute-relay-state"), []byte("compute-relay/state-v1\n")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "runtime.db")
	if err := statefs.WriteNew(path, nil); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(t.Context(), "PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(t.Context(), string(script)); err != nil {
		t.Fatal("materialize immutable state", err)
	}
	return root
}
func baselineContractHandler(t *testing.T, store *sqlite.Store) http.Handler {
	t.Helper()
	access, err := auth.New(store, store)
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	inputs, err := objects.New(access, blobs, store)
	if err != nil {
		t.Fatal(err)
	}
	cfg := api.DefaultConfig()
	cfg.Jobs, err = admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Operations, err = operations.New(access, store, blobs, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Authorizations, err = executionauth.New(access, store, baselineContractClock{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := api.NewServer(cfg, access, inputs, store.Ready)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler
}

type baselineContractClock struct{}

func (baselineContractClock) Now() time.Time { return time.Now().UTC() }
func TestBaselineStateReadsAndReplaysUnchanged(t *testing.T) {
	directory := filepath.Join("..", "store", "sqlite", "testdata", "baseline-9591372")
	script, err := os.ReadFile(filepath.Join(directory, "runtime.sql"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(directory, "requests.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Token    string                    `json:"token"`
		Requests []baselineContractRequest `json:"requests"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open(context.Background(), baselineContractState(t, script), sqlite.DefaultOptions())
	if err != nil {
		t.Fatal("baseline state no longer opens", err)
	}
	defer store.Close()
	handler := baselineContractHandler(t, store)
	for _, want := range fixture.Requests {
		reason := map[string]string{"granted status": "account_capacity", "queued status": "workspace_fifo"}[want.Name]
		if reason != "" {
			var expected map[string]any
			if err = json.Unmarshal(want.Response, &expected); err != nil {
				t.Fatal(err)
			}
			expected["scheduling"] = map[string]string{"reason": reason}
			want.Response, err = json.Marshal(expected)
			if err != nil {
				t.Fatal(err)
			}
		}
		var body io.Reader
		if len(want.Body) > 0 {
			body = bytes.NewReader(want.Body)
		}
		request := httptest.NewRequest(want.Method, want.Path, body)
		request.Host = api.DefaultConfig().Listen
		request.Header.Set("Authorization", "Bearer "+fixture.Token)
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		if want.Key != "" {
			request.Header.Set("Idempotency-Key", want.Key)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		var got, expected any
		if err = json.Unmarshal(response.Body.Bytes(), &got); err != nil {
			t.Fatal("baseline returned non-JSON", want.Name, err)
		}
		if err = json.Unmarshal(want.Response, &expected); err != nil {
			t.Fatal(err)
		}
		if response.Code != want.Status || !reflect.DeepEqual(got, expected) {
			t.Errorf("%s: %s %s changed\nstatus %d want %d\n got %s\nwant %s", want.Name, want.Method, want.Path, response.Code, want.Status, response.Body.Bytes(), want.Response)
		}
	}
}
