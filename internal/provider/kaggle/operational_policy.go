package kaggle

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// OperationalDescriptor describes this implementation, not live account support.
// It cannot register an incomplete Provider or turn a local check into M1 evidence.
// Every call returns independent capability/condition slices.
func OperationalDescriptor(c Config) (provider.Descriptor, error) {
	if c.Validate() != nil {
		return provider.Descriptor{}, ErrConfig
	}
	names := []domain.CapabilityName{
		domain.CapabilityBatchExecution, domain.CapabilityPython, domain.CapabilityShell,
		domain.CapabilityGPU, domain.CapabilityPrivateInputStaging,
		domain.CapabilityRemoteCancellation, domain.CapabilityLogsWhileRunning,
		domain.CapabilityLogsAfterCompletion, domain.CapabilityQuotaReporting,
		domain.CapabilityExecutionTimeout, domain.CapabilityRemoteNetworkControl,
		domain.CapabilityStrongIdentity, domain.CapabilityCustomContainer,
		domain.CapabilityRetainedSessions,
	}
	d := provider.Descriptor{Type: "kaggle", InstanceID: domain.ProviderInstanceID(c.InstanceID), Version: ClientVersion + "/sdk-" + SDKVersion}
	for _, name := range names {
		cap := domain.CapabilityStatus{Name: name, Support: domain.CapabilitySupportUnknown,
			Evidence: domain.EvidenceImplementedOffline, ClientVersion: d.Version,
			Reason: "offline components exist; full integration and account-scoped live acceptance remain required"}
		switch name {
		case domain.CapabilityRemoteCancellation:
			cap.Support = domain.CapabilitySupportUnsupported
			cap.Reason = "this batch reference has no verified kernel-session ID; manual cancellation is required"
		case domain.CapabilityCustomContainer, domain.CapabilityRetainedSessions:
			cap.Support = domain.CapabilitySupportUnsupported
			cap.Reason = "not implemented by this finite batch adapter"
		case domain.CapabilityLogsAfterCompletion:
			cap.Support = domain.CapabilitySupportSupported
			cap.Reason = "bounded version-scoped provider log snapshots; not verified payload artifacts"
			cap.Conditions = []string{"exact kernel identity and explicit log field required; live availability is unverified"}
		case domain.CapabilityLogsWhileRunning:
			cap.Reason = "delayed snapshots may be returned; live SSE streaming is not implemented or claimed"
		case domain.CapabilityQuotaReporting:
			cap.Support = domain.CapabilitySupportSupported
			cap.Reason = "explicit account-scoped read with conservative units, reservations and freshness"
			cap.Conditions = []string{"complete free-quota fields required; not a reservation or live-account capability claim"}
		case domain.CapabilityExecutionTimeout:
			cap.Reason = "requested SDK wall limit and runner budgets exist; provider enforcement is unverified"
		case domain.CapabilityStrongIdentity:
			cap.Reason = "kernel ID, source and version are checked; same-version session reruns are not attestable"
		}
		d.Capabilities = append(d.Capabilities, cap)
	}
	return d, nil
}

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

var _ provider.Canceller = (*Executor)(nil)

// Cancel never guesses a session ID from a kernel ID, creates an interactive
// session, deletes resources or invokes a provider. M3 persists the manual-required
// operation and retains possible activity; this result is not termination evidence.
func (e *Executor) Cancel(ctx context.Context, ref provider.RemoteReference, operation domain.OperationID) (provider.CancellationOutcome, error) {
	if err := ctx.Err(); err != nil {
		return provider.CancellationOutcome{}, err
	}
	if err := e.checkOperationalReference(ref); err != nil {
		return provider.CancellationOutcome{}, err
	}
	if !operation.Valid() {
		return provider.CancellationOutcome{}, ErrConfig
	}
	return provider.CancellationOutcome{Status: domain.CancellationManual}, nil
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
