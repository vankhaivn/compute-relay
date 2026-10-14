package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
)

// Historical state and receipts remain immutable fixtures for the upgrade test.
const baselineDir = "testdata/baseline-9591372"

type baselineFixture struct {
	Token    string            `json:"token"` // Authenticates only against this synthetic database.
	Requests []baselineRequest `json:"requests"`
}
type baselineRequest struct {
	Name     string          `json:"name"`
	Method   string          `json:"method"`
	Path     string          `json:"path"`
	Key      string          `json:"idempotency_key,omitempty"`
	Body     json.RawMessage `json:"body,omitempty"`
	Status   int             `json:"status"`
	Response json.RawMessage `json:"response"`
}

// loadStateScript materializes a dumped database as a new state root, as a restored
// installation would present it to the next binary.
func loadStateScript(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state")
	root, err := statefs.Open(path)
	requireNoError(t, err)
	defer root.Close()
	requireNoError(t, initializeRoot(path))
	requireNoError(t, statefs.WriteNew(filepath.Join(path, databaseName), nil))
	db, err := connect(filepath.Join(path, databaseName))
	requireNoError(t, err)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	requireNoError(t, err)
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), script); err != nil {
		t.Fatal("load dumped state", err)
	}
	return path
}

func baselineHandler(t *testing.T, s *Store) http.Handler {
	t.Helper()
	access, err := auth.New(s, s)
	requireNoError(t, err)
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	requireNoError(t, err)
	t.Cleanup(func() { _ = blobs.Close() })
	inputs, err := objects.New(access, blobs, s)
	requireNoError(t, err)
	cfg := api.DefaultConfig()
	if cfg.Jobs, err = admission.New(access, s, admission.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	if cfg.Operations, err = operations.New(access, s, blobs, admission.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	if cfg.Authorizations, err = executionauth.New(access, s, clock{}); err != nil {
		t.Fatal(err)
	}
	server, err := api.NewServer(cfg, access, inputs, s.Ready)
	requireNoError(t, err)
	return server.Handler
}

type clock struct{}

func (clock) Now() time.Time { return time.Now().UTC() }

func baselineCall(t *testing.T, handler http.Handler, token string, r baselineRequest) (int, json.RawMessage) {
	t.Helper()
	var body io.Reader
	if len(r.Body) > 0 {
		body = bytes.NewReader(r.Body)
	}
	req := httptest.NewRequest(r.Method, r.Path, body)
	req.Host = api.DefaultConfig().Listen
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if r.Key != "" {
		req.Header.Set("Idempotency-Key", r.Key)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	var compact bytes.Buffer
	if err := json.Compact(&compact, recorder.Body.Bytes()); err != nil {
		t.Fatalf("%s: non-JSON response %q", r.Name, recorder.Body.String())
	}
	return recorder.Code, compact.Bytes()
}

func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var left, right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}
