package provider

import "github.com/vankhaivn/compute-relay/internal/domain"

// Capabilities is an adapter's behavior declaration. GPU, running logs and
// quota have a meaningful unknown state; other fields describe implemented paths.
type Capabilities struct {
	BatchExecution       bool
	Python               bool
	Shell                bool
	GPU                  domain.CapabilitySupport
	PrivateInputStaging  bool
	RemoteCancellation   bool
	RemoteCleanup        bool
	LogsWhileRunning     domain.CapabilitySupport
	LogsAfterCompletion  bool
	QuotaReporting       domain.CapabilitySupport
	ExecutionTimeout     bool
	SeparateStart        bool
	EphemeralSecrets     bool
	AbsoluteDeadline     bool
	RemoteNetworkControl bool
	StrongIdentity       bool
	Accelerators         []string
	GPUShape             string
}

func (c Capabilities) Support(name domain.CapabilityName) domain.CapabilitySupport {
	boolSupport := func(ok bool) domain.CapabilitySupport {
		if ok {
			return domain.CapabilitySupportSupported
		}
		return domain.CapabilitySupportUnsupported
	}
	switch name {
	case domain.CapabilityBatchExecution:
		return boolSupport(c.BatchExecution)
	case domain.CapabilityPython:
		return boolSupport(c.Python)
	case domain.CapabilityShell:
		return boolSupport(c.Shell)
	case domain.CapabilityGPU:
		return c.GPU
	case domain.CapabilityPrivateInputStaging:
		return boolSupport(c.PrivateInputStaging)
	case domain.CapabilityRemoteCancellation:
		return boolSupport(c.RemoteCancellation)
	case domain.CapabilityRemoteCleanup:
		return boolSupport(c.RemoteCleanup)
	case domain.CapabilityLogsWhileRunning:
		return c.LogsWhileRunning
	case domain.CapabilityLogsAfterCompletion:
		return boolSupport(c.LogsAfterCompletion)
	case domain.CapabilityQuotaReporting:
		return c.QuotaReporting
	case domain.CapabilityExecutionTimeout:
		return boolSupport(c.ExecutionTimeout)
	case domain.CapabilitySeparateStart:
		return boolSupport(c.SeparateStart)
	case domain.CapabilityEphemeralSecrets:
		return boolSupport(c.EphemeralSecrets)
	case domain.CapabilityAbsoluteDeadline:
		return boolSupport(c.AbsoluteDeadline)
	case domain.CapabilityRemoteNetworkControl:
		return boolSupport(c.RemoteNetworkControl)
	case domain.CapabilityStrongIdentity:
		return boolSupport(c.StrongIdentity)
	case domain.CapabilityCustomContainer, domain.CapabilityRetainedSessions:
		return domain.CapabilitySupportUnsupported
	default:
		return domain.CapabilitySupportUnknown
	}
}

func (c Capabilities) Valid() bool {
	return c.BatchExecution && c.GPU.Valid() && c.LogsWhileRunning.Valid() && c.QuotaReporting.Valid()
}
