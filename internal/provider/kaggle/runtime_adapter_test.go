package kaggle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestRuntimePreparePlanCheckDoesNotRepeatGPUQuotaRead(t *testing.T) {
	c, plan, _ := executionFixture(t)
	plan.Job.Specification = []byte(strings.Replace(executionSpec, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`, 1))
	plan.Job.SpecificationSHA256 = provider.Digest(plan.Job.Specification)
	plan.Job.Required = append(plan.Job.Required, domain.CapabilityGPU)
	plan.VerifyAfterStart = []domain.CapabilityName{domain.CapabilityGPU}
	policy := DefaultExecutionPolicy()
	policy.MachineShape = "NvidiaTeslaT4"
	p := &RuntimeAdapter{config: c, binding: provider.BindingSnapshot{Binding: plan.Job.Binding}, policy: policy}
	// No monitor is configured: a quota read would fail the validated plan check.
	if err := p.validPlan(context.Background(), plan); err != nil {
		t.Fatal("plan check repeated provider validation", err)
	}
	changed := plan.Clone()
	changed.VerifyAfterStart = nil
	if err := p.validPlan(context.Background(), changed); !errors.Is(err, ErrRuntimeBinding) {
		t.Fatal("changed plan accepted", err)
	}
	changed = plan.Clone()
	changed.Job.Binding.ConfigurationRevision = "other"
	if err := p.validPlan(context.Background(), changed); !errors.Is(err, ErrRuntimeBinding) {
		t.Fatal("changed binding accepted", err)
	}

}

func TestRuntimeVerifyPreservesFailureClass(t *testing.T) {
	for _, tc := range []struct {
		name, authentication, account string
		code                          domain.ErrorCode
	}{
		{"provider_unavailable", "unavailable", "not_checked", domain.CodeProviderUnreachable},
		{"credential_rejected", "failed", "not_checked", domain.CodeProviderAuthFailed},
		{"credential_unavailable", "failed", "not_checked", domain.CodeConfigurationInvalid},
		{"access_denied", "unavailable", "not_checked", domain.CodeProviderAccessDenied},
		{"account_mismatch", "verified", "mismatch", domain.CodeRemoteIdentityMismatch},
		{"invalid_response", "unavailable", "not_checked", domain.CodeProviderStateUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			resolver, _ := fixtureCredentials([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "synthetic-token", true })
			preflight, err := New(c, resolver)
			if err != nil {
				t.Fatal(err)
			}
			preflight.run = func(context.Context, Config, Mode, []byte, bool) (Report, error) {
				r := baseline(ReadOnly)
				r.Authentication, r.AccountBinding, r.Problem = tc.authentication, tc.account, tc.name
				return r, nil
			}
			adapter := &RuntimeAdapter{preflight: preflight}
			var problem domain.Problem
			if err := adapter.Verify(context.Background(), adapter.binding); !errors.As(err, &problem) || problem.Code != tc.code {
				t.Fatalf("Verify: %v; expected %s", err, tc.code)
			}
		})
	}
}

func TestRuntimeVerifyTransportAndQuotaUncertainty(t *testing.T) {
	c := testConfig(t)
	resolver, _ := fixtureCredentials([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "synthetic-token", true })
	preflight, err := New(c, resolver)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &RuntimeAdapter{preflight: preflight}
	for _, cause := range []error{errors.New("private-response-canary"), context.DeadlineExceeded} {
		preflight.run = func(context.Context, Config, Mode, []byte, bool) (Report, error) { return Report{}, cause }
		var problem domain.Problem
		if err := adapter.Verify(context.Background(), adapter.binding); !errors.As(err, &problem) || problem.Code != domain.CodeProviderUnreachable || strings.Contains(err.Error(), "canary") {
			t.Fatal("transport failure mislabeled or exposed", err)
		}
	}
	preflight.run = func(context.Context, Config, Mode, []byte, bool) (Report, error) {
		r := verified()
		r.Quota, r.Problem = "unavailable", "quota_unavailable"
		return r, nil
	}
	if err := adapter.Verify(context.Background(), adapter.binding); err != nil {
		t.Fatal("quota loss blocked existing run", err)
	}
	changed := adapter.binding
	changed.AccountScope = "other"
	var problem domain.Problem
	if err := adapter.Verify(context.Background(), changed); !errors.As(err, &problem) || problem.Code != domain.CodeRemoteIdentityMismatch {
		t.Fatal("binding mismatch hidden", err)
	}
}
