package api_test

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

// This port fixture checks HTTP authority and forwarding. Storage accounting,
// preview freshness and durable replay are exercised by the store tests.
type storageHTTPPort struct {
	mu        sync.Mutex
	calls     []storageHTTPCall
	err       error
	view      hoststorage.View
	preview   hoststorage.Preview
	operation hoststorage.Operation
}
type storageHTTPCall struct {
	method                                      string
	workspace                                   domain.WorkspaceID
	token, connection, orphan, fingerprint, key string
	operation                                   domain.OperationID
}

func (s *storageHTTPPort) record(c storageHTTPCall) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, c)
	return s.err
}
func (s *storageHTTPPort) snapshot() []storageHTTPCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]storageHTTPCall(nil), s.calls...)
}
func (s *storageHTTPPort) ConnectionStorage(_ context.Context, w domain.WorkspaceID, token, connection string) (hoststorage.View, error) {
	err := s.record(storageHTTPCall{method: "read", workspace: w, token: token, connection: connection})
	return s.view, err
}
func (s *storageHTTPPort) Preview(_ context.Context, w domain.WorkspaceID, token, connection, orphan string) (hoststorage.Preview, error) {
	err := s.record(storageHTTPCall{method: "preview", workspace: w, token: token, connection: connection, orphan: orphan})
	return s.preview, err
}
func (s *storageHTTPPort) Apply(_ context.Context, w domain.WorkspaceID, token, connection, orphan, fingerprint, key string) (hoststorage.Operation, error) {
	err := s.record(storageHTTPCall{method: "apply", workspace: w, token: token, connection: connection, orphan: orphan, fingerprint: fingerprint, key: key})
	return s.operation, err
}
func (s *storageHTTPPort) Operation(_ context.Context, w domain.WorkspaceID, token string, id domain.OperationID) (hoststorage.Operation, error) {
	err := s.record(storageHTTPCall{method: "operation", workspace: w, token: token, operation: id})
	return s.operation, err
}

const storageConnectionPath = "/v1/workspaces/a/connections/conn_storage/storage"

var storageOrphanID = "orphan_" + strings.Repeat("b", 64)
var storageFingerprint = strings.Repeat("c", 64)

func storageCleanupPath() string {
	return storageConnectionPath + "/orphans/" + storageOrphanID + "/cleanup"
}
func storageApplyBody() string { return `{"fingerprint":"` + storageFingerprint + `"}` }

func setupStorageHTTP(t *testing.T) (*jobHTTP, *storageHTTPPort) {
	t.Helper()
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	port := &storageHTTPPort{
		view:      hoststorage.View{SchemaVersion: 1, ObservedAt: &now, ScanState: "complete", Precision: "upper_bound", Workspaces: []hoststorage.WorkspaceReference{{WorkspaceID: "a", ReferencedBytes: 32}}, Retention: []hoststorage.RetentionItem{}, Orphans: []hoststorage.Orphan{{ID: storageOrphanID, Class: hoststorage.Volume, ChargedBytes: 32, Eligible: true}}, Budget: hoststorage.BudgetView{Configured: hoststorage.DefaultBudget(), Effective: hoststorage.DefaultBudget(), State: "below_soft"}},
		preview:   hoststorage.Preview{OrphanID: storageOrphanID, Class: hoststorage.Volume, ChargedBytes: 32, Eligible: true, Reason: "eligible", Fingerprint: storageFingerprint, ExpiresAt: now.Add(time.Minute)},
		operation: hoststorage.Operation{OperationID: "op_storage", Status: "queued", Result: "pending", Reason: "requested", CreatedAt: now, UpdatedAt: now},
	}
	port.view.Scope.ResourceID = "resource_" + strings.Repeat("a", 64)
	port.view.Scope.ConnectionRevision = 3
	f := setupJobHTTPWith(t, jobHTTPOptions{extraScopes: map[string][]auth.Scope{"manage": {auth.Manage}}, configure: func(cfg *api.Config, _ *auth.Service, _ *sqlite.Store) { cfg.Storage = port; cfg.StorageCleanup = port }})
	return f, port
}

func TestStorageHTTPReadContractAndAuthority(t *testing.T) {
	f, port := setupStorageHTTP(t)
	for _, tc := range []struct {
		who    string
		status int
	}{{"", 401}, {"b", 403}, {"manage", 403}} {
		f.call(t, http.MethodGet, storageConnectionPath, tc.who, "", "", tc.status)
	}
	f.call(t, http.MethodGet, "/v1/workspaces/a/connections/bad!/storage", "read", "", "", 400)
	if len(port.snapshot()) != 0 {
		t.Fatal("rejected storage read reached repository")
	}
	_, raw := f.call(t, http.MethodGet, storageConnectionPath, "read", "", "", 200)
	wireSchema(t, "connection-storage", raw)
	view := decodeHTTPJSON[hoststorage.View](t, raw)
	if !reflect.DeepEqual(view, port.view) {
		t.Fatal("storage HTTP response changed durable projection")
	}
	managedSafeResponse(t, raw)
	want := []storageHTTPCall{{method: "read", workspace: "a", token: f.tokenIDs["read"], connection: "conn_storage"}}
	if !reflect.DeepEqual(port.snapshot(), want) {
		t.Fatal("storage read crossed cleanup port or lost scoped identity", port.snapshot())
	}
}

func TestStorageHTTPCleanupAuthorityReceiptsAndForwarding(t *testing.T) {
	f, port := setupStorageHTTP(t)
	path := storageCleanupPath()
	opPath := "/v1/workspaces/a/storage-cleanup-operations/op_storage"
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, tc := range []struct {
			who    string
			status int
		}{{"", 401}, {"read", 403}, {"b", 403}} {
			f.call(t, method, path, tc.who, "storage-key", storageApplyBody(), tc.status)
		}
	}
	f.call(t, http.MethodGet, opPath, "", "", "", 401)
	f.call(t, http.MethodGet, opPath, "b", "", "", 403)
	f.call(t, http.MethodGet, opPath, "manage", "", "", 403)
	if len(port.snapshot()) != 0 {
		t.Fatal("rejected cleanup request reached controller")
	}
	_, raw := f.call(t, http.MethodGet, path, "manage", "", "", 200)
	if got := decodeHTTPJSON[hoststorage.Preview](t, raw); !reflect.DeepEqual(got, port.preview) {
		t.Fatal("preview changed at HTTP boundary", got)
	}
	headers, raw := f.call(t, http.MethodPost, path, "manage", "storage-key", storageApplyBody(), 202)
	if headers.Get("Location") != opPath || !reflect.DeepEqual(decodeHTTPJSON[hoststorage.Operation](t, raw), port.operation) {
		t.Fatal("cleanup admission lost its operation receipt")
	}
	_, replay := f.call(t, http.MethodPost, path, "manage", "storage-key", storageApplyBody(), 202)
	if string(replay) != string(raw) {
		t.Fatal("same-key forwarding changed controller receipt")
	}
	_, status := f.call(t, http.MethodGet, opPath, "read", "", "", 200)
	if !reflect.DeepEqual(decodeHTTPJSON[hoststorage.Operation](t, status), port.operation) {
		t.Fatal("operation read changed receipt")
	}
	managedSafeResponse(t, raw)
	want := []storageHTTPCall{
		{method: "preview", workspace: "a", token: f.tokenIDs["manage"], connection: "conn_storage", orphan: storageOrphanID},
		{method: "apply", workspace: "a", token: f.tokenIDs["manage"], connection: "conn_storage", orphan: storageOrphanID, fingerprint: storageFingerprint, key: "storage-key"},
		{method: "apply", workspace: "a", token: f.tokenIDs["manage"], connection: "conn_storage", orphan: storageOrphanID, fingerprint: storageFingerprint, key: "storage-key"},
		{method: "operation", workspace: "a", token: f.tokenIDs["read"], operation: "op_storage"},
	}
	if !reflect.DeepEqual(port.snapshot(), want) {
		t.Fatal("cleanup route lost exact target, actor, fingerprint or idempotency key", port.snapshot())
	}
}

func TestStorageHTTPCleanupStrictApplyBody(t *testing.T) {
	f, port := setupStorageHTTP(t)
	for _, body := range []string{
		`{}`, `null`, `[]`, `{"fingerprint":null}`, `{"fingerprint":7}`, `{"fingerprint":"short"}`,
		`{"fingerprint":"` + strings.Repeat("g", 64) + `"}`,
		`{"fingerprint":"` + storageFingerprint + `","fingerprint":"` + storageFingerprint + `"}`,
		`{"fingerprint":"` + storageFingerprint + `","target":"private-docker-id"}`,
		storageApplyBody() + ` {}`,
	} {
		f.call(t, http.MethodPost, storageCleanupPath(), "manage", "storage-key", body, 400)
	}
	f.call(t, http.MethodPost, storageCleanupPath(), "manage", "", storageApplyBody(), 400)
	f.call(t, http.MethodPost, storageCleanupPath(), "manage", "storage-key", `{"fingerprint":"`+strings.Repeat("a", 1024)+`"}`, 413)
	f.call(t, http.MethodGet, storageConnectionPath+"/orphans/short/cleanup", "manage", "", "", 400)
	if len(port.snapshot()) != 0 {
		t.Fatal("invalid cleanup authority reached controller")
	}
}

func TestStorageHTTPCleanupErrorsRemainPublicAndStable(t *testing.T) {
	f, port := setupStorageHTTP(t)
	for _, tc := range []struct {
		err    error
		status int
		code   domain.ErrorCode
		stage  domain.FailureStage
	}{
		{hoststorage.ErrInvalid, 400, domain.CodeInvalidRequest, domain.FailureStageValidation},
		{hoststorage.ErrUnverified, 409, domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation},
		{hoststorage.ErrIneligible, 409, domain.CodeIllegalStateTransition, domain.FailureStageOperation},
		{hoststorage.ErrUnknown, 503, domain.CodeProviderStateUnknown, domain.FailureStageObservation},
		{admission.ErrConflict, 409, domain.CodeIdempotencyConflict, domain.FailureStageOperation},
		{connections.ErrNotFound, 404, domain.CodeInvalidRequest, domain.FailureStageValidation},
		{errors.New("private-storage-failure"), 503, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime},
	} {
		port.mu.Lock()
		port.err = errors.Join(tc.err, errors.New("private-docker-id private-credential-canary"))
		port.mu.Unlock()
		for _, route := range []struct{ method, path, who, key, body string }{
			{http.MethodGet, storageCleanupPath(), "manage", "", ""},
			{http.MethodPost, storageCleanupPath(), "manage", "storage-key", storageApplyBody()},
			{http.MethodGet, "/v1/workspaces/a/storage-cleanup-operations/op_storage", "read", "", ""},
		} {
			_, raw := f.call(t, route.method, route.path, route.who, route.key, route.body, tc.status)
			problem := decodeHTTPJSON[api.ErrorEnvelope](t, raw).Error
			if problem.Code != tc.code || problem.Stage != tc.stage || problem.SafeOperationRetry {
				t.Fatal("cleanup error lost stable envelope", problem)
			}
			for _, canary := range []string{"private-storage-failure", "private-docker-id", "private-credential-canary", storageFingerprint} {
				if strings.Contains(string(raw), canary) {
					t.Fatal("cleanup refusal exposed private evidence")
				}
			}
		}
	}
}

func TestStorageHTTPReadErrorsAndMethodBoundaries(t *testing.T) {
	f, port := setupStorageHTTP(t)
	for _, tc := range []struct {
		err    error
		status int
		code   domain.ErrorCode
		stage  domain.FailureStage
	}{
		{hoststorage.ErrUnverified, 409, domain.CodeIllegalStateTransition, domain.FailureStageObservation},
		{connections.ErrNotFound, 404, domain.CodeInvalidRequest, domain.FailureStageValidation},
		{errors.New("private-machine-id"), 503, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime},
	} {
		port.mu.Lock()
		port.err = errors.Join(tc.err, errors.New("private-docker-id private-credential-canary"))
		port.mu.Unlock()
		_, raw := f.call(t, http.MethodGet, storageConnectionPath, "read", "", "", tc.status)
		problem := decodeHTTPJSON[api.ErrorEnvelope](t, raw).Error
		if problem.Code != tc.code || problem.Stage != tc.stage {
			t.Fatal("storage read error lost stable envelope", problem)
		}
		for _, canary := range []string{"private-machine-id", "private-docker-id", "private-credential-canary"} {
			if strings.Contains(string(raw), canary) {
				t.Fatal("storage read refusal leaked private evidence")
			}
		}
	}
	before := len(port.snapshot())
	for _, path := range []string{storageConnectionPath, storageCleanupPath(), "/v1/workspaces/a/storage-cleanup-operations/op_storage"} {
		request, err := http.NewRequest(http.MethodHead, f.url+path, nil)
		requireNoError(t, err)
		request.Header.Set("Authorization", "Bearer "+f.tokens["read"])
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
		requireNoError(t, err)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusMethodNotAllowed || !strings.Contains(response.Header.Get("Allow"), "GET") {
			t.Fatal("storage HEAD lost GET-only boundary", response.StatusCode, response.Header)
		}
	}
	if len(port.snapshot()) != before {
		t.Fatal("HEAD invoked storage controller")
	}
}
