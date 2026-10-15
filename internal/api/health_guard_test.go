package api_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type unhealthyAdmission struct{ calls atomic.Int32 }

func (g *unhealthyAdmission) PreflightAdmission(ctx context.Context, _ domain.WorkspaceID, _, _ string, _ admission.Request) (context.Context, *admission.Receipt, error) {
	g.calls.Add(1)
	return ctx, nil, admission.ErrHostUnhealthy
}

func TestHostHealthAdmissionPreservesErrorEnvelope(t *testing.T) {
	guard := &unhealthyAdmission{}
	f := setupJobHTTPWith(t, jobHTTPOptions{profiles: true, configure: func(cfg *api.Config, _ *auth.Service, _ *sqlite.Store) {
		cfg.Jobs.WithPreflight(guard)
	}})
	f.call(t, "POST", "/v1/workspaces/a/jobs", "read", "health-forbidden", jobBody, 403)
	if guard.calls.Load() != 0 {
		t.Fatal("unauthorized request reached health preflight")
	}
	_, raw := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "health-refused", jobBody, 422)
	wireSchema(t, "error", raw)
	response := decodeHTTPJSON[api.ErrorEnvelope](t, raw)
	if guard.calls.Load() != 1 || response.Error.Code != domain.CodeResourceRequirementUnsatisfied || response.Error.ComputeMayHaveStarted || response.Error.SafeOperationRetry || !strings.Contains(response.Error.Message, "host health") {
		t.Fatal("health refusal changed public error semantics", response)
	}
}
