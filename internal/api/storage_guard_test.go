package api_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type storageRefusal struct {
	calls atomic.Int32
	err   error
}

func (g *storageRefusal) PreflightAdmission(ctx context.Context, _ domain.WorkspaceID, _, _ string, _ admission.Request) (context.Context, *admission.Receipt, error) {
	g.calls.Add(1)
	return ctx, nil, g.err
}

func TestStorageAdmissionPreservesStudioErrorEnvelope(t *testing.T) {
	for _, tc := range []struct {
		reason string
		cause  error
	}{
		{"host_storage_limit", hoststorage.ErrHardLimit},
		{"host_storage_unknown", hoststorage.ErrUnknown},
		{"host_storage_unknown", hoststorage.ErrIneligible},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			guard := &storageRefusal{err: errors.Join(tc.cause, errors.New("private-provider-canary"))}
			f := setupJobHTTPWith(t, jobHTTPOptions{profiles: true, configure: func(cfg *api.Config, _ *auth.Service, _ *sqlite.Store) { cfg.Jobs.WithPreflight(guard) }})
			f.call(t, "POST", "/v1/workspaces/a/jobs", "read", "storage-forbidden", jobBody, 403)
			if guard.calls.Load() != 0 {
				t.Fatal("unauthorized request reached storage preflight")
			}
			_, raw := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "storage-refused", jobBody, 422)
			wireSchema(t, "error", raw)
			response := decodeHTTPJSON[api.ErrorEnvelope](t, raw)
			p := response.Error
			if guard.calls.Load() != 1 || p.Code != domain.CodeResourceRequirementUnsatisfied || p.Stage != domain.FailureStageValidation || p.ComputeMayHaveStarted || p.SafeOperationRetry || p.RecommendedAction != domain.RecommendedActionWait || len(p.Details) != 1 || p.Details[domain.ReasonDetail] != tc.reason || strings.Contains(string(raw), "private-provider-canary") {
				t.Fatalf("storage refusal changed existing envelope semantics: %+v", p)
			}
		})
	}
}

func TestStorageRetryRefusalPreservesStudioErrorEnvelope(t *testing.T) {
	for _, tc := range []struct {
		cause  error
		reason string
	}{
		{hoststorage.ErrHardLimit, "host_storage_limit"},
		{hoststorage.ErrUnknown, "host_storage_unknown"},
		{hoststorage.ErrIneligible, "host_storage_unknown"},
	} {
		t.Run(tc.cause.Error(), func(t *testing.T) {
			f, _ := setupControlHTTP(t, controlRepository{err: errors.Join(tc.cause, errors.New("private-provider-canary"))})
			_, raw := f.call(t, "POST", "/v1/workspaces/a/jobs/job_fixture/retry", "operate", "storage-retry", `{"attempt_id":"att_fixture","reason":"explicit retry"}`, 422)
			wireSchema(t, "error", raw)
			p := decodeHTTPJSON[api.ErrorEnvelope](t, raw).Error
			if p.Code != domain.CodeResourceRequirementUnsatisfied || p.Stage != domain.FailureStageValidation || p.ComputeMayHaveStarted || p.RecommendedAction != domain.RecommendedActionWait || p.Details[domain.ReasonDetail] != tc.reason || strings.Contains(string(raw), "private-provider-canary") {
				t.Fatalf("unsafe retry storage refusal: %+v", p)
			}
		})
	}
}
