package api_test

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

// noCleanup composes shared operation admission without provider preview.
type noCleanup struct{ operations.Repository }

// This operation-port fixture exercises cleanup wire and refusal checks.
type cleanupRepository struct {
	operations.Repository
	record operations.Record
	err    error
}

func (c cleanupRepository) ReplayOperation(context.Context, domain.WorkspaceID, string, domain.OperationKind, string, string) (*operations.Record, error) {
	return nil, nil
}
func (c cleanupRepository) AdmitOperation(context.Context, operations.Command) (operations.Record, error) {
	return c.record, c.err
}
func (c cleanupRepository) ReadOperation(context.Context, domain.WorkspaceID, string, domain.OperationID) (operations.Record, error) {
	return c.record, c.err
}

func cleanupRecord(t *testing.T, status domain.OperationStatus, effect operations.Effect) operations.Record {
	t.Helper()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	op, err := domain.NewOperation("op_cleanup", "a", "job_fixture", "att_fixture", domain.OperationCleanup, now)
	requireNoError(t, err)
	if status == domain.OperationRunning || status == domain.OperationSucceeded {
		if op, err = op.Transition(domain.OperationRunning, nil, now); err != nil {
			t.Fatal(err)
		}
	}
	if status != domain.OperationAccepted && status != domain.OperationRunning {
		var failure *domain.Problem
		if status == domain.OperationFailed {
			p, err := domain.NewProblem(domain.CodeUnsupportedCapability, "remote cleanup is not supported for this attempt's provider binding; owned resources were preserved", domain.FailureStageValidation)
			requireNoError(t, err)
			p = p.WithRecommendedAction(domain.RecommendedActionInspectProvider)
			p.Details = map[string]string{"internal": "private-provider-canary"}
			failure = &p
		}
		if op, err = op.Transition(status, failure, now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	r := operations.Record{Operation: op, Effect: effect}
	if r.Validate() != nil {
		t.Fatal("invalid cleanup fixture", status, effect)
	}
	return r
}

func TestCleanupHTTPReceiptsSatisfyPublishedContract(t *testing.T) {
	schema := transportControlSchema(t, "schemas/control-operation.v1alpha1.schema.json", "")
	for _, c := range []struct {
		status domain.OperationStatus
		effect operations.Effect
	}{
		{domain.OperationAccepted, operations.Pending},
		{domain.OperationRunning, operations.Pending},
		{domain.OperationSucceeded, operations.RemoteDeleted},
		{domain.OperationSucceeded, operations.AlreadyAbsent},
		{domain.OperationFailed, operations.Failed},
	} {
		t.Run(string(c.status)+"/"+string(c.effect), func(t *testing.T) {
			f, _ := setupControlsHTTP(t, nil, cleanupRepository{record: cleanupRecord(t, c.status, c.effect)})
			body := `{"attempt_id":"att_fixture","discard_uncollected":true,"reason":"private-reason-canary"}`
			headers, raw := f.call(t, "POST", "/v1/workspaces/a/jobs/job_fixture/cleanup", "a", "cleanup-contract", body, 202)
			_, read := f.call(t, "GET", headers.Get("Location"), "read", "", "", 200)
			for _, b := range [][]byte{raw, read} {
				value := decodeHTTPJSON[any](t, b)
				if err := schema.Validate(value); err != nil {
					t.Fatal("cleanup receipt violates the published contract", err, string(b))
				}
				view := decodeControl(t, b)
				if view.Kind != "cleanup" || view.Status != string(c.status) || view.Effect != string(c.effect) || view.Confirmed || view.Links.Self != headers.Get("Location") {
					t.Fatal("cleanup receipt misrepresented its state", string(b))
				}
				for _, canary := range []string{"private-reason-canary", "private-provider-canary", `"details"`, "res_staging", "discard_uncollected"} {
					if bytes.Contains(b, []byte(canary)) {
						t.Fatal("cleanup receipt leaked private data", canary)
					}
				}
			}
		})
	}
}

func TestCleanupHTTPRefusalsUseStableProblems(t *testing.T) {
	for _, c := range []struct {
		err    error
		status int
		code   string
		action string
	}{
		{operations.ErrUncollected, 409, "ILLEGAL_STATE_TRANSITION", "collect"},
		{operations.ErrUnresolved, 409, "REMOTE_EXECUTION_UNRESOLVED", "reconcile"},
		{operations.ErrBusy, 409, "ILLEGAL_STATE_TRANSITION", "retry_read"},
		{operations.ErrState, 409, "ILLEGAL_STATE_TRANSITION", "retry_read"},
		{operations.ErrConflict, 409, "IDEMPOTENCY_CONFLICT", ""},
		{operations.ErrNotFound, 404, "INVALID_REQUEST", ""},
		{errors.New("private-storage-canary"), 503, "STATE_STORE_UNAVAILABLE", "contact_operator"},
	} {
		t.Run(c.code+c.action, func(t *testing.T) {
			f, _ := setupControlsHTTP(t, nil, cleanupRepository{err: c.err})
			_, b := f.call(t, "POST", "/v1/workspaces/a/jobs/job_fixture/cleanup", "a", "cleanup-refusal", `{"attempt_id":"att_fixture"}`, c.status)
			envelope := decodeHTTPJSON[api.ErrorEnvelope](t, b)
			if string(envelope.Error.Code) != c.code || c.action != "" && string(envelope.Error.RecommendedAction) != c.action || bytes.Contains(b, []byte("private-storage-canary")) {
				t.Fatal("cleanup refusal lost its stable problem", string(b))
			}
		})
	}
}

func TestCleanupHTTPAdmissionGuardsWithStore(t *testing.T) {
	f, _ := setupControlHTTP(t, nil)
	job := admitControlJob(t, f)
	path := job.Links.Self + "/cleanup"
	body := `{"attempt_id":"` + string(job.AttemptID) + `"}`
	_, b := f.call(t, "POST", path, "a", "cleanup-queued", body, 409)
	if !bytes.Contains(b, []byte(`"ILLEGAL_STATE_TRANSITION"`)) {
		t.Fatal("queued attempt cleanup was not refused as a state transition", string(b))
	}
	for _, raw := range []string{"", "{}", `{"attempt_id":null}`, `{"attempt_id":"a","attempt_id":"b"}`, strings.TrimSuffix(body, "}") + `,"discard_uncollected":"true"}`, strings.TrimSuffix(body, "}") + `,"accept_name_based_delete":true}`, strings.TrimSuffix(body, "}") + `,"accept_name_based_delete":false}`, strings.TrimSuffix(body, "}") + `,"extra":true}`, body + "{}"} {
		f.call(t, "POST", path, "a", "cleanup-strict", raw, 400)
	}
	f.call(t, "POST", path, "a", "", body, 400)
	f.call(t, "POST", path, "read", "cleanup-read-scope", body, 403)
	f.call(t, "POST", path, "b", "cleanup-foreign", body, 403)
	head := httptest.NewRequest("HEAD", f.url+path, nil)
	head.Header.Set("Authorization", "Bearer "+f.tokens["a"])
	response := httptest.NewRecorder()
	f.handler.ServeHTTP(response, head)
	if response.Code != 405 || response.Header().Get("Allow") != "GET, POST" {
		t.Fatal("cleanup HEAD lost methods", response.Code, response.Header().Get("Allow"))
	}
	f.call(t, "GET", path, "a", "", "", 400)
	query := "?attempt_id=" + string(job.AttemptID)
	_, preview := f.call(t, "GET", path+query, "a", "", "", 200)
	wireSchema(t, "cleanup-preview", preview)
	for _, invalid := range []string{"?attempt_id=", "?attempt_id=a&attempt_id=b", query + "&discard_uncollected=1", query + "&discard_uncollected=true&discard_uncollected=false", query + "&remote_ref=foreign", query + "&discard_uncollected=", query + "&attempt_id=x", query + "&x=y;z"} {
		f.call(t, "GET", path+invalid, "a", "", "", 400)
	}
	f.call(t, "POST", path+query, "a", "query-not-permit", body, 400)
	f.call(t, "GET", path+query, "a", "", "{}", 400)
	for _, q := range []string{"&accept_name_based_delete=true", "&accept_name_based_delete=false", "&accept_name_based_delete=1", "&accept_name_based_delete=true&accept_name_based_delete=false", "&accept_name_based_delete="} {
		f.call(t, "GET", path+"?attempt_id="+string(job.AttemptID)+q, "a", "", "", 400)
	}
	f.call(t, "GET", path+"?attempt_id="+string(job.AttemptID), "read", "", "", 403)
	f.call(t, "GET", "/v1/workspaces/a/operations/op_missing", "a", "", "", 404)
	_, b = f.call(t, "GET", "/v1/info", "a", "", "", 200)
	if !bytes.Contains(b, []byte(`"`+api.FeatureJobCleanup+`"`)) {
		t.Fatal("configured cleanup not advertised")
	}

	off, _ := setupControlsHTTP(t, nil, noCleanup{})
	offJob := admitControlJob(t, off)
	off.call(t, "POST", offJob.Links.Self+"/cleanup", "a", "cleanup-unconfigured", `{"attempt_id":"`+string(offJob.AttemptID)+`"}`, 409)
	_, b = off.call(t, "GET", "/v1/info", "a", "", "", 200)
	if !bytes.Contains(b, []byte(`"`+api.FeatureJobCleanup+`"`)) || bytes.Contains(b, []byte(`"`+api.FeatureJobCleanupPreview+`"`)) {
		t.Fatal("shared cleanup admission or preview capability misrepresented")
	}
}
