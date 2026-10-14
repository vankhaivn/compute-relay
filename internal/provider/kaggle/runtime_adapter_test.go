package kaggle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
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
