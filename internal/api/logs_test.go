package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

type unavailableLogRepo struct{ calls int }

func (r *unavailableLogRepo) ReadLogAttempt(_ context.Context, w domain.WorkspaceID, _ string, j domain.JobID, a domain.AttemptID) (joblogs.Attempt, error) {
	r.calls++
	if w != "a" || j != "job" || a != "attempt" {
		return joblogs.Attempt{}, joblogs.ErrNotFound
	}
	return joblogs.Attempt{}, nil
}
func TestJobLogsRouteQueryAuthenticationAndUnavailable(t *testing.T) {
	f := setupHTTP(t, nil, nil)
	repo := &unavailableLogRepo{}
	reader, err := joblogs.NewReader(f.access, repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.server.Handler.(*handler).config.Logs = reader
	base := "/v1/workspaces/a/jobs/job/logs"
	for _, tc := range []struct {
		suffix, method, who, body string
		status                    int
	}{
		{"?attempt_id=attempt", "GET", "read", "", 200},
		{"?attempt_id=attempt&limit=1&cursor=opaque", "GET", "a", "", 200},
		{"?attempt_id=attempt", "GET", "", "", 401},
		{"?attempt_id=attempt", "GET", "b", "", 403},
		{"?attempt_id=missing", "GET", "a", "", 404},
		{"", "GET", "a", "", 400},
		{"?attempt_id=attempt&attempt_id=attempt", "GET", "a", "", 400},
		{"?attempt_id=attempt&cursor=x&cursor=x", "GET", "a", "", 400},
		{"?attempt_id=attempt&limit=1&limit=1", "GET", "a", "", 400},
		{"?attempt_id=attempt&limit=0", "GET", "a", "", 400},
		{"?attempt_id=attempt&limit=101", "GET", "a", "", 400},
		{"?attempt_id=attempt&limit=01", "GET", "a", "", 400},
		{"?attempt_id=attempt&unknown=x", "GET", "a", "", 400},
		{"?attempt_id=attempt&cursor=" + strings.Repeat("x", 513), "GET", "a", "", 400},
		{"?attempt_id=attempt", "GET", "a", "body", 400},
		{"?attempt_id=attempt", "POST", "a", "", 405},
	} {
		t.Run(tc.method+tc.suffix+tc.who+tc.body, func(t *testing.T) {
			before := repo.calls
			response, raw := f.request(t, tc.method, base+tc.suffix, tc.who, strings.NewReader(tc.body), nil)
			if response.StatusCode != tc.status {
				t.Fatalf("status %d want %d: %s", response.StatusCode, tc.status, raw)
			}
			if tc.status == 200 {
				var page struct {
					APIVersion   string   `json:"api_version"`
					WorkspaceID  string   `json:"workspace_id"`
					JobID        string   `json:"job_id"`
					AttemptID    string   `json:"attempt_id"`
					Source       string   `json:"source"`
					Availability string   `json:"availability"`
					Lines        []string `json:"lines"`
					NextCursor   string   `json:"next_cursor"`
					Truncated    bool     `json:"truncated"`
				}
				if json.Unmarshal(raw, &page) != nil || page.APIVersion != "compute-connector/v1alpha1" || page.WorkspaceID != "a" || page.JobID != "job" || page.AttemptID != "attempt" || page.Availability != "unavailable" || page.Source != "provider" || page.Lines == nil || len(page.Lines) > 0 || page.Truncated || page.NextCursor != "" {
					t.Fatal(string(raw))
				}
			} else if tc.status != 404 && repo.calls != before {
				t.Fatal("invalid request reached repository")
			}
		})
	}
	response, raw := f.request(t, http.MethodGet, "/v1/info", "a", nil, nil)
	if response.StatusCode != 200 || !strings.Contains(string(raw), `"job_logs"`) {
		t.Fatal(string(raw))
	}
	if err := f.access.Revoke(context.Background(), f.tokenIDs["a"]); err != nil {
		t.Fatal(err)
	}
	response, _ = f.request(t, "GET", base+"?attempt_id=attempt", "a", nil, nil)
	if response.StatusCode != 401 {
		t.Fatal("revoked token read logs")
	}
}

type cursorLogRepo struct{ target joblogs.Attempt }

func (r cursorLogRepo) ReadLogAttempt(context.Context, domain.WorkspaceID, string, domain.JobID, domain.AttemptID) (joblogs.Attempt, error) {
	return r.target, nil
}

type cursorLogResolver struct{ adapter provider.Provider }

func (r cursorLogResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	return r.adapter, nil
}

type cursorLogProvider struct {
	provider.Provider
	err error
}

func (p cursorLogProvider) Describe() provider.Descriptor {
	return provider.Descriptor{InstanceID: "fixture"}
}
func (p cursorLogProvider) VerifyBinding(context.Context, provider.BindingSnapshot) error { return nil }
func (p cursorLogProvider) ReadLogs(context.Context, provider.RemoteReference, provider.PageRequest) (provider.LogPage, error) {
	return provider.LogPage{}, p.err
}
func TestJobLogsCursorErrorsAreStableAndSanitized(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{provider.ErrLogCursorInvalid, 400, "INVALID_REQUEST"},
		{provider.ErrLogCursorReset, 409, "LOG_CURSOR_RESET"},
		{provider.LogReadFailure{Reason: "stream_timeout"}, 503, "STATE_STORE_UNAVAILABLE"},
		{fmt.Errorf("private-provider-canary: %w", provider.LogReadFailure{Reason: "stream_http_403"}), 503, "STATE_STORE_UNAVAILABLE"},
		{provider.LogReadFailure{Reason: "private-provider-canary"}, 503, "STATE_STORE_UNAVAILABLE"},
		{errors.New("private-provider-canary resource-secret"), 503, "STATE_STORE_UNAVAILABLE"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			f := setupHTTP(t, nil, nil)
			job := fake.ExampleJob("fixture")
			job.Identity.WorkspaceID = "a"
			job.Identity.JobID = "job"
			job.Identity.AttemptID = "attempt"
			target := joblogs.Attempt{Binding: provider.BindingSnapshot{Binding: job.Binding, AccountScope: "fixture_account", CredentialRef: "fixture_credential"}, Remote: &provider.RemoteReference{Identity: job.Identity, Resource: "original-resource", Version: "1"}}
			reader, err := joblogs.NewReader(f.access, cursorLogRepo{target}, cursorLogResolver{cursorLogProvider{err: tc.err}})
			if err != nil {
				t.Fatal(err)
			}
			f.server.Handler.(*handler).config.Logs = reader
			response, raw := f.request(t, "GET", "/v1/workspaces/a/jobs/job/logs?attempt_id=attempt&cursor=opaque", "read", nil, nil)
			var envelope ErrorEnvelope
			if response.StatusCode != tc.status || json.Unmarshal(raw, &envelope) != nil || string(envelope.Error.Code) != tc.code || strings.Contains(string(raw), "private-provider") || strings.Contains(string(raw), "original-resource") {
				t.Fatal(response.StatusCode, string(raw))
			}
			var failure provider.LogReadFailure
			if errors.As(tc.err, &failure) && failure.Valid() && (envelope.Error.Message != failure.Error() || envelope.Error.Stage != domain.FailureStageObservation) {
				t.Fatal("fixed read diagnostic was lost", string(raw))
			}
		})
	}
}
