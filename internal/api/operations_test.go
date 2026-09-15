package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// These are verified local fixture bytes, not a runnable bundle or a provider.
// HTTP controls never dispatch or execute the admitted command.
const controlBytes = "immutable control-only fixture; never executed"

type controlBlobs struct{ opens atomic.Int32 }

func (b *controlBlobs) Open(_ context.Context, w domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	b.opens.Add(1)
	if w != "a" || id != "code" {
		return nil, errors.New("unexpected object")
	}
	return io.NopCloser(strings.NewReader(controlBytes)), nil
}

func setupControlHTTP(t *testing.T, override operations.Repository) (*jobHTTP, *controlBlobs) {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "state"), sqlite.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	access, err := auth.New(store, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := objects.New(access, noUpload{}, store)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	f := &jobHTTP{store: store, tokens: map[string]string{}, tokenIDs: map[string]string{}}
	for _, w := range []domain.WorkspaceID{"a", "b"} {
		if err = store.PutWorkspace(ctx, auth.Workspace{ID: w, Enabled: true, AllowedProfiles: []string{"default-gpu"}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, who := range []string{"a", "b", "read", "write", "operate"} {
		w := domain.WorkspaceID("a")
		scopes := []auth.Scope{auth.Read, auth.Write, auth.Operate}
		if who == "b" {
			w = "b"
		} else if who != "a" {
			switch who {
			case "read":
				scopes = []auth.Scope{auth.Read}
			case "write":
				scopes = []auth.Scope{auth.Write}
			case "operate":
				scopes = []auth.Scope{auth.Operate}
			}
		}
		secret, record, err := access.Issue(ctx, w, scopes, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[who], f.tokenIDs[who] = secret.Reveal(), record.ID
	}
	if err = store.PutProfile(ctx, admission.DefaultProfile(domain.ProviderBinding{Profile: "default-gpu", ProviderInstanceID: "fake", ConfigurationRevision: "rev1"}, "account"), true); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(controlBytes))
	if err = store.CommitObject(ctx, domain.ObjectMetadata{ID: "code", WorkspaceID: "a", Bytes: int64(len(controlBytes)), SHA256: domain.SHA256Digest(hex.EncodeToString(digest[:]))}); err != nil {
		t.Fatal(err)
	}
	var repo operations.Repository = store
	if override != nil {
		repo = override
	}
	blobs := &controlBlobs{}
	controls, err := operations.New(access, repo, blobs, nil, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := api.DefaultConfig()
	cfg.Listen, cfg.Jobs, cfg.Operations = ln.Addr().String(), jobs, controls
	cfg.WorkspaceRate = api.Rate{PerSecond: 1000, Burst: 1000}
	cfg.GlobalRate = cfg.WorkspaceRate
	server, err := api.NewServer(cfg, access, obj, store.Ready)
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}
	f.url, f.handler = "http://"+ln.Addr().String(), server.Handler
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(ln) }()
	t.Cleanup(func() { _ = server.Close(); <-done })
	return f, blobs
}

type controlWire struct {
	ID        string `json:"operation_id"`
	Attempt   string `json:"attempt_id"`
	New       string `json:"new_attempt_id"`
	Kind      string `json:"kind"`
	Status    string `json:"status"`
	Effect    string `json:"effect"`
	Confirmed bool   `json:"remote_termination_confirmed"`
	Replay    bool   `json:"replay"`
	Links     struct {
		Self string `json:"self"`
		Job  string `json:"job"`
	} `json:"links"`
}

func decodeControl(t *testing.T, b []byte) controlWire {
	t.Helper()
	var result controlWire
	if err := json.Unmarshal(b, &result); err != nil {
		t.Fatal(err)
	}
	if result.ID == "" || result.Links.Self == "" || result.Links.Job == "" {
		t.Fatal("missing operation identity or links")
	}
	return result
}

func admitControlJob(t *testing.T, f *jobHTTP) admission.Receipt {
	t.Helper()
	_, b := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "control-job-01", jobBody, 202)
	var receipt admission.Receipt
	if err := json.Unmarshal(b, &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestControlHTTPCancelRetryReplayAndHistory(t *testing.T) {
	f, blobs := setupControlHTTP(t, nil)
	job := admitControlJob(t, f)
	body := `{"attempt_id":"` + string(job.AttemptID) + `","reason":"private-reason-canary"}`
	headers, b := f.call(t, "POST", job.Links.Self+"/cancel", "a", "cancel-key-01", body, 202)
	cancel := decodeControl(t, b)
	if cancel.Status != "succeeded" || cancel.Effect != "dispatch_prevented" || cancel.Confirmed || cancel.Replay || headers.Get("Location") != cancel.Links.Self || bytes.Contains(b, []byte("private-reason-canary")) {
		t.Fatal("local cancellation receipt misrepresented its effect or leaked the reason")
	}
	for _, key := range []string{"cancel-key-01", "cancel-key-02"} {
		_, b = f.call(t, "POST", job.Links.Self+"/cancel", "a", key, body, 202)
		replay := decodeControl(t, b)
		if !replay.Replay || replay.ID != cancel.ID {
			t.Fatal("cancellation replay/coalescing created another operation")
		}
	}
	f.call(t, "POST", job.Links.Self+"/cancel", "a", "cancel-key-01", strings.Replace(body, "private-reason-canary", "changed", 1), 409)
	_, b = f.call(t, "POST", job.Links.Self+"/reconcile", "a", "reconcile-key", body, 202)
	if op := decodeControl(t, b); op.Effect != "observation_refreshed" || op.Status != "succeeded" || op.Confirmed {
		t.Fatal("terminal reconciliation changed the attempt")
	}
	_, b = f.call(t, "POST", job.Links.Self+"/collect", "a", "collect-key-01", body, 409)
	if !bytes.Contains(b, []byte(`"compute_may_have_started":true`)) || !bytes.Contains(b, []byte("REMOTE_EXECUTION_UNRESOLVED")) {
		t.Fatal("collection without terminal execution did not preserve uncertainty")
	}
	if blobs.opens.Load() != 0 {
		t.Fatal("cancel, reconcile or rejected collect read payload bytes")
	}
	_, b = f.call(t, "POST", job.Links.Self+"/retry", "a", "retry-key-01", body, 202)
	retry := decodeControl(t, b)
	if retry.Kind != "retry_compute" || retry.Effect != "new_attempt_created" || retry.New == "" || retry.New == string(job.AttemptID) || retry.Attempt != string(job.AttemptID) || retry.Confirmed {
		t.Fatal("retry did not preserve the target and separate new attempt identity")
	}
	_, b = f.call(t, "POST", job.Links.Self+"/retry", "a", "retry-key-01", body, 202)
	replayed := decodeControl(t, b)
	if replayed.ID != retry.ID || replayed.New != retry.New || !replayed.Replay || blobs.opens.Load() != 1 {
		t.Fatal("retry replay created another attempt or re-read frozen bytes")
	}
	f.call(t, "POST", job.Links.Self+"/retry", "a", "retry-key-02", body, 409)
	_, b = f.call(t, "GET", job.Links.Self, "a", "", "", 200)
	if !bytes.Contains(b, []byte(`"active_attempt_id":"`+retry.New+`"`)) {
		t.Fatal("new active attempt not visible")
	}
	_, b = f.call(t, "GET", cancel.Links.Self, "read", "", "", 200)
	old := decodeControl(t, b)
	if old.ID != cancel.ID || old.Attempt != string(job.AttemptID) || old.Effect != "dispatch_prevented" || old.Replay {
		t.Fatal("historical operation was rewritten or GET returned a replay receipt")
	}
	for _, who := range []string{"read", "write"} {
		f.call(t, "POST", job.Links.Self+"/cancel", who, "forbidden-control", body, 403)
	}
	for _, who := range []string{"write", "operate"} {
		f.call(t, "GET", cancel.Links.Self, who, "", "", 403)
	}
	f.call(t, "GET", cancel.Links.Self, "b", "", "", 403)
	f.call(t, "GET", strings.Replace(cancel.Links.Self, "/a/", "/b/", 1), "b", "", "", 404)
	f.call(t, "POST", strings.Replace(job.Links.Self, "/a/", "/b/", 1)+"/cancel", "b", "foreign-target", body, 404)
	f.call(t, "GET", cancel.Links.Self, "", "", "", 401)
	if err := f.store.RevokeToken(context.Background(), f.tokenIDs["a"]); err != nil {
		t.Fatal(err)
	}
	f.call(t, "POST", job.Links.Self+"/cancel", "a", "cancel-key-01", body, 401)
	f.call(t, "GET", cancel.Links.Self, "a", "", "", 401)
}

func TestControlHTTPLostReceiptAndStrictRequestGuards(t *testing.T) {
	f, _ := setupControlHTTP(t, nil)
	job := admitControlJob(t, f)
	body := `{"attempt_id":"` + string(job.AttemptID) + `"}`
	path := job.Links.Self + "/cancel"
	for _, raw := range []string{"", "null", "{}", `{"attempt_id":null}`, `{"attempt_id":"a","attempt_id":"b"}`, `{"attempt_id":"a","reason":null}`, `{"attempt_id":"a","extra":true}`, body + "{}", strings.Replace(body, "}", `,"reason":"line\nbreak"}`, 1)} {
		f.call(t, "POST", path, "a", "strict-control-key", raw, 400)
	}
	f.call(t, "POST", path, "a", "oversized-control", strings.Repeat(" ", operations.MaxRequestBytes+1), 413)
	f.call(t, "POST", path, "a", "", body, 400)
	f.call(t, "POST", job.Links.Self+"/retry", "a", "reason-required", body, 400)
	f.call(t, "POST", job.Links.Self+"/retry", "a", "queued-no-retry", strings.TrimSuffix(body, "}")+`,"reason":"explicit retry"}`, 409)
	f.call(t, "GET", path, "a", "", "", 405)
	f.call(t, "POST", job.Links.Self+"/cleanup", "a", "not-a-cancel", body, 404)
	for _, tc := range []struct {
		name string
		edit func(*http.Request)
		want int
	}{
		{"duplicate key", func(r *http.Request) { r.Header.Add("Idempotency-Key", "other-key") }, 400},
		{"duplicate content type", func(r *http.Request) { r.Header.Add("Content-Type", "application/json") }, 415},
		{"encoding", func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, 415},
		{"charset", func(r *http.Request) { r.Header.Set("Content-Type", "application/json; charset=latin1") }, 415},
		{"origin", func(r *http.Request) { r.Header.Set("Origin", "https://outside.invalid") }, 403},
		{"host", func(r *http.Request) { r.Host = "outside.invalid:7331" }, 403},
		{"query", func(r *http.Request) { r.URL.RawQuery = "attempt_id=other" }, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", f.url+path, strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer "+f.tokens["a"])
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "guarded-control")
			tc.edit(r)
			w := httptest.NewRecorder()
			f.handler.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d: %s", w.Code, tc.want, w.Body)
			}
			wireSchema(t, "error", w.Body.Bytes())
		})
	}
	r := httptest.NewRequest("POST", f.url+path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+f.tokens["a"])
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "lost-control-receipt")
	broken := &lostAdmissionResponse{header: make(http.Header)}
	f.handler.ServeHTTP(broken, r)
	if broken.status != 202 || broken.header.Get("Location") == "" {
		t.Fatal("operation did not commit before response failure")
	}
	headers, b := f.call(t, "POST", path, "a", "lost-control-receipt", body, 202)
	if op := decodeControl(t, b); !op.Replay || headers.Get("Location") != broken.header.Get("Location") {
		t.Fatal("lost receipt created another operation")
	}
	f.call(t, "POST", headers.Get("Location"), "a", "", "", 405)
	_, b = f.call(t, "GET", "/v1/info", "a", "", "", 200)
	for _, feature := range []string{"job_cancel", "job_retry", "job_reconcile", "job_collect", "operation_status"} {
		if !bytes.Contains(b, []byte(`"`+feature+`"`)) {
			t.Fatal("configured feature not advertised", feature)
		}
	}
}

// Explicit test-only repository for safe-wire and uncertainty checks; production
// composition uses SQLite, never this fixture.
type controlRepository struct {
	record operations.Record
	err    error
}

func (c controlRepository) ReplayOperation(context.Context, domain.WorkspaceID, string, domain.OperationKind, string, string) (*operations.Record, error) {
	return nil, nil
}
func (c controlRepository) RetryInputs(context.Context, domain.WorkspaceID, string, domain.JobID, domain.AttemptID) (operations.RetryInputs, error) {
	return operations.RetryInputs{}, c.err
}
func (c controlRepository) AdmitOperation(context.Context, operations.Command) (operations.Record, error) {
	return c.record, c.err
}
func (c controlRepository) ReadOperation(context.Context, domain.WorkspaceID, string, domain.OperationID) (operations.Record, error) {
	return c.record, c.err
}

func TestControlHTTPManualRequiredAndSafeErrors(t *testing.T) {
	now := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	op, err := domain.NewOperation("op_fixture", "a", "job_fixture", "att_fixture", domain.OperationCancel, now)
	if err != nil {
		t.Fatal(err)
	}
	problem, err := domain.NewProblem(domain.CodeRemoteCancelUnsupported, "Cancellation is not verified for this provider binding.", domain.FailureStageOperation)
	if err != nil {
		t.Fatal(err)
	}
	problem = problem.WithRetrySemantics(false, true).WithRecommendedAction(domain.RecommendedActionInspectProvider)
	problem.Details = map[string]string{"internal": "private-provider-canary"}
	op, err = op.Transition(domain.OperationManualRequired, &problem, now)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := setupControlHTTP(t, controlRepository{record: operations.Record{Operation: op, Effect: operations.ManualRequired}})
	_, b := f.call(t, "GET", "/v1/workspaces/a/operations/op_fixture", "read", "", "", 200)
	view := decodeControl(t, b)
	if view.Confirmed || view.Status != "manual_required" || view.Effect != "manual_required" || bytes.Contains(b, []byte("private-provider-canary")) || bytes.Contains(b, []byte(`"details"`)) {
		t.Fatal("manual cancellation was represented as confirmed or leaked diagnostics")
	}
	for _, tc := range []struct {
		err    error
		status int
		code   string
		action string
	}{
		{operations.ErrUnresolved, 409, "REMOTE_EXECUTION_UNRESOLVED", "reconcile"},
		{operations.ErrCollect, 409, "ILLEGAL_STATE_TRANSITION", "collect"},
		{operations.ErrInputs, 409, "INPUT_NOT_FOUND", "fix_request"},
		{operations.ErrChanged, 409, "ILLEGAL_STATE_TRANSITION", "retry_read"},
		{errors.New("private-storage-canary"), 503, "STATE_STORE_UNAVAILABLE", "contact_operator"},
	} {
		t.Run(tc.code+tc.action, func(t *testing.T) {
			f, _ := setupControlHTTP(t, controlRepository{err: tc.err})
			_, b := f.call(t, "POST", "/v1/workspaces/a/jobs/job_fixture/cancel", "a", "safe-error-key", `{"attempt_id":"att_fixture"}`, tc.status)
			var envelope api.ErrorEnvelope
			if err := json.Unmarshal(b, &envelope); err != nil {
				t.Fatal(err)
			}
			if string(envelope.Error.Code) != tc.code || string(envelope.Error.RecommendedAction) != tc.action || !envelope.Error.ComputeMayHaveStarted || envelope.Error.SafeOperationRetry || bytes.Contains(b, []byte("private-storage-canary")) {
				t.Fatal("control uncertainty or safe next action was lost", string(b))
			}
		})
	}
}
