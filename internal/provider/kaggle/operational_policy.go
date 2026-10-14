package kaggle

import (
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (e *Executor) checkOperationalReference(ref provider.RemoteReference) error {
	if e == nil || ref.Validate() != nil || ref.Identity != e.plan.Job.Identity || ref.Version != "1" {
		return ErrExecutionIdentity
	}
	var recorded kernelReference
	if closedObject([]byte(ref.Resource), &recorded) != nil || !positiveDecimal(recorded.KernelID) || recorded.Reference != e.request.Owner+"/"+e.request.Slug || recorded.SourceSHA256 != e.request.SourceSHA256 {
		return ErrExecutionIdentity
	}
	return nil
}

// TimeoutBudgets exposes frozen layers without inventing a remote clock or timer.
// A control-call timeout or exceeded local deadline cannot become remote timed_out
// or release evidence. M3 owns intent/state; the verified result owns payload outcome.
type TimeoutBudgets struct {
	RemoteWallSeconds        int64
	SetupSeconds             int64
	FinalizationGraceSeconds int64
	ControlCall              time.Duration
	ProviderEnforcement      domain.CapabilitySupport
}

func (e *Executor) TimeoutBudgets() (TimeoutBudgets, error) {
	if e == nil {
		return TimeoutBudgets{}, ErrConfig
	}
	request, err := admission.Parse(e.plan.Job.Specification)
	if err != nil {
		return TimeoutBudgets{}, ErrConfig
	}
	t := request.Spec().Timeouts
	return TimeoutBudgets{RemoteWallSeconds: t.RemoteWallSeconds, SetupSeconds: t.SetupSeconds,
		FinalizationGraceSeconds: t.FinalizationGraceSeconds, ControlCall: e.policy.Timeout,
		ProviderEnforcement: domain.CapabilitySupportUnknown}, nil
}
