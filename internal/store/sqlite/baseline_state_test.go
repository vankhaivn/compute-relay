package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

// State written by the compatibility baseline. Installed applications upgrade such state in
// place, so later revisions must preserve these fields and exact replay receipts.
// The directory is write-once: a deliberate new baseline gets a new directory.
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

func TestBaselineStateReadsAndReplaysUnchanged(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(baselineDir, "runtime.sql"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(baselineDir, "requests.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture baselineFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), loadStateScript(t, string(script)), DefaultOptions())
	if err != nil {
		t.Fatal("baseline state no longer opens", err)
	}
	defer s.Close()
	handler := baselineHandler(t, s)
	for _, want := range fixture.Requests {
		// Scheduling is an intentional additive status contract. Keep the stored
		// baseline untouched and assert each new field as well as every old field.
		reason := map[string]string{"granted status": "account_capacity", "queued status": "workspace_fifo"}[want.Name]
		if reason != "" {
			var expected map[string]any
			if err := json.Unmarshal(want.Response, &expected); err != nil {
				t.Fatal(err)
			}
			expected["scheduling"] = map[string]string{"reason": reason}
			want.Response, err = json.Marshal(expected)
			if err != nil {
				t.Fatal(err)
			}
		}
		status, got := baselineCall(t, handler, fixture.Token, want)
		if status != want.Status || !sameJSON(t, got, want.Response) {
			t.Errorf("%s: %s %s changed\nstatus %d want %d\n got %s\nwant %s", want.Name, want.Method, want.Path, status, want.Status, got, want.Response)
		}
	}
}

// TestWriteBaselineState drives the real admission, authorization, dispatch, control and
// collection services on the fake provider, then records the resulting database and responses.
func TestWriteBaselineState(t *testing.T) {
	if os.Getenv("COMPUTE_RELAY_WRITE_BASELINE") != "1" {
		t.Skip("set COMPUTE_RELAY_WRITE_BASELINE=1 only when creating a new baseline directory")
	}
	if _, err := os.Stat(baselineDir); err == nil {
		t.Fatal("baseline fixtures are write-once; create a new directory instead")
	}
	ctx := context.Background()
	f := managedFixture(t, fake.DefaultScenario())
	// The fixture connection stands in for a checked connection whose selection is current.
	if _, err := f.s.db.Exec("UPDATE managed_connections SET current_profile=? WHERE connection_id='con_fixture'", f.profile.Binding.Profile); err != nil {
		t.Fatal(err)
	}
	access, err := auth.New(f.s, f.s, nil)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := access.Issue(ctx, "a", []auth.Scope{auth.Read, auth.Write, auth.Operate, auth.Execute}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := admission.New(access, f.s, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	grants, err := executionauth.New(access, f.s, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	controls, err := operations.New(access, f.s, f.blobs, f.clock, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	var requests []baselineRequest
	admit := func(name, mode string, grant bool) scheduler.Identity {
		t.Helper()
		body := dispatchJSON
		if mode != "" {
			body = strings.Replace(body, `"name":"dispatch-fixture"`, `"name":"dispatch-fixture","result_collection":"`+mode+`"`, 1)
		}
		receipt, err := jobs.Submit(ctx, principal, "a", "admit-"+name, []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		id := scheduler.Identity{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID}
		base := "/v1/workspaces/a/jobs"
		requests = append(requests,
			baselineRequest{Name: name + " status", Method: "GET", Path: base + "/" + string(id.JobID)},
			baselineRequest{Name: name + " admission replay", Method: "POST", Path: base, Key: "admit-" + name, Body: json.RawMessage(body)})
		if grant {
			request := authorizationRequest(id.AttemptID, 10)
			r, err := grants.Authorize(ctx, principal, "a", id.JobID, "grant-"+name, request)
			if err != nil {
				t.Fatal(err)
			}
			requests = append(requests,
				baselineRequest{Name: name + " authorization", Method: "GET", Path: base + "/" + string(id.JobID) + "/authorizations/" + string(r.AuthorizationID)},
				baselineRequest{Name: name + " authorization replay", Method: "POST", Path: base + "/" + string(id.JobID) + "/authorize", Key: "grant-" + name, Body: request})
		}
		return id
	}
	execute := func(id scheduler.Identity, advances int) {
		t.Helper()
		submittedControl(t, f, id)
		for i := 0; i < advances; i++ {
			if err := f.adapter.Advance(*f.journal(t, id).Remote); err != nil {
				t.Fatal(err)
			}
			if err := f.step(t); err != nil {
				t.Fatal(err)
			}
		}
	}

	execute(admit("awaiting-download", "manual", true), 2)
	collected := admit("collected", "manual", true)
	execute(collected, 2)
	collect := controlRequest(collected.AttemptID)
	ticket, err := controls.Submit(ctx, principal, "a", collected.JobID, domain.OperationCollect, "collect-collected", collect)
	if err != nil {
		t.Fatal(err)
	}
	results, err := blobfs.New(filepath.Join(t.TempDir(), "results"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()
	outputs := &collectionProvider{fetches: map[string]int{}, files: completedOutputs(t, f, f.journal(t, collected).Remote.Identity)}
	runCollection(t, collectionEngine(t, f, outputs, results, f.s))
	jobPath := "/v1/workspaces/a/jobs/" + string(collected.JobID)
	requests = append(requests,
		baselineRequest{Name: "collected collect replay", Method: "POST", Path: jobPath + "/collect", Key: "collect-collected", Body: collect},
		baselineRequest{Name: "collected operation", Method: "GET", Path: "/v1/workspaces/a/operations/" + string(ticket.Operation.ID)})
	execute(admit("running", "", true), 1)
	admit("granted", "", true)
	admit("queued", "", false)

	script := dumpState(t, f.s)
	s, err := Open(ctx, loadStateScript(t, script), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := baselineHandler(t, s)
	for i := range requests {
		requests[i].Status, requests[i].Response = baselineCall(t, handler, secret.Reveal(), requests[i])
		if requests[i].Status >= 300 {
			t.Fatalf("%s failed: %d %s", requests[i].Name, requests[i].Status, requests[i].Response)
		}
	}
	raw, err := json.MarshalIndent(baselineFixture{Token: secret.Reveal(), Requests: requests}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(baselineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(baselineDir, "runtime.sql"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(baselineDir, "requests.json"), append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func dumpState(t *testing.T, s *Store) string {
	t.Helper()
	var out strings.Builder
	var application, version int
	if err := s.db.QueryRow("PRAGMA application_id").Scan(&application); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	out.WriteString("PRAGMA application_id=" + strconv.Itoa(application) + ";\nPRAGMA user_version=" + strconv.Itoa(version) + ";\n")
	schema := func(kind string) []string {
		rows, err := s.db.Query("SELECT name,sql FROM sqlite_schema WHERE type=? AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid", kind)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var names []string
		for rows.Next() {
			var name, definition string
			if err := rows.Scan(&name, &definition); err != nil {
				t.Fatal(err)
			}
			names = append(names, name)
			out.WriteString(definition + ";\n")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return names
	}
	tables := schema("table")
	var sequence int
	if err := s.db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name='sqlite_sequence'").Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if sequence == 1 {
		tables = append(tables, "sqlite_sequence")
	}
	for _, table := range tables {
		columns, err := s.db.Query("SELECT name FROM pragma_table_info(?) ORDER BY cid", table)
		if err != nil {
			t.Fatal(err)
		}
		var literals []string
		for columns.Next() {
			var name string
			if err := columns.Scan(&name); err != nil {
				t.Fatal(err)
			}
			literals = append(literals, "quote("+quoteName(name)+")")
		}
		columns.Close()
		rows, err := s.db.Query("SELECT " + strings.Join(literals, "||','||") + " FROM " + quoteName(table))
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var values string
			if err := rows.Scan(&values); err != nil {
				t.Fatal(err)
			}
			out.WriteString("INSERT INTO " + quoteName(table) + " VALUES(" + values + ");\n")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	schema("index")
	schema("trigger")
	return out.String()
}

// loadStateScript materializes a dumped database as a new state root, as a restored
// installation would present it to the next binary.
func loadStateScript(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state")
	root, err := statefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := initializeRoot(path); err != nil {
		t.Fatal(err)
	}
	if err := statefs.WriteNew(filepath.Join(path, databaseName), nil); err != nil {
		t.Fatal(err)
	}
	db, err := connect(filepath.Join(path, databaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
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
	access, err := auth.New(s, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	inputs, err := objects.New(access, blobs, s)
	if err != nil {
		t.Fatal(err)
	}
	cfg := api.DefaultConfig()
	if cfg.Jobs, err = admission.New(access, s, admission.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	if cfg.Operations, err = operations.New(access, s, blobs, clock{}, admission.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	if cfg.Authorizations, err = executionauth.New(access, s, clock{}); err != nil {
		t.Fatal(err)
	}
	server, err := api.NewServer(cfg, access, inputs, s.Ready)
	if err != nil {
		t.Fatal(err)
	}
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

func quoteName(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
