package domain

// MaxRemoteWallSeconds is the provider-neutral ceiling for one finite remote wall budget.
// Each provider advertises its own maximum, never above this value.
const MaxRemoteWallSeconds int64 = 7 * 24 * 60 * 60

// CapabilityName is a provider-neutral behavior exposed by a provider instance.
type CapabilityName string

const (
	CapabilityBatchExecution       CapabilityName = "batch_execution"
	CapabilityPython               CapabilityName = "python"
	CapabilityShell                CapabilityName = "shell"
	CapabilityGPU                  CapabilityName = "gpu"
	CapabilityPrivateInputStaging  CapabilityName = "private_input_staging"
	CapabilityRemoteCancellation   CapabilityName = "remote_cancellation"
	CapabilityRemoteCleanup        CapabilityName = "remote_cleanup"
	CapabilityLogsWhileRunning     CapabilityName = "logs_while_running"
	CapabilityLogsAfterCompletion  CapabilityName = "logs_after_completion"
	CapabilityQuotaReporting       CapabilityName = "quota_reporting"
	CapabilityExecutionTimeout     CapabilityName = "execution_timeout"
	CapabilitySeparateStart        CapabilityName = "separate_prepare_start"
	CapabilityEphemeralSecrets     CapabilityName = "ephemeral_workload_secrets"
	CapabilityAbsoluteDeadline     CapabilityName = "absolute_execution_deadline"
	CapabilityRemoteNetworkControl CapabilityName = "remote_network_control"
	CapabilityStrongIdentity       CapabilityName = "strong_execution_identity"
	CapabilityCustomContainer      CapabilityName = "custom_container"
	CapabilityRetainedSessions     CapabilityName = "retained_sessions"
)

// CapabilitySupport separates a capability conclusion from the evidence used to reach it.
type CapabilitySupport string

const (
	CapabilitySupportSupported   CapabilitySupport = "supported"
	CapabilitySupportUnsupported CapabilitySupport = "unsupported"
	CapabilitySupportUnknown     CapabilitySupport = "unknown"
)

func (support CapabilitySupport) Valid() bool {
	switch support {
	case CapabilitySupportSupported, CapabilitySupportUnsupported, CapabilitySupportUnknown:
		return true
	default:
		return false
	}
}

// EvidenceLevel records what was actually exercised or reviewed.
type EvidenceLevel string

const (
	EvidencePlanned            EvidenceLevel = "planned"
	EvidenceImplementedOffline EvidenceLevel = "implemented_offline"
	EvidenceDocumentedUpstream EvidenceLevel = "documented_upstream"
	EvidencePassedLive         EvidenceLevel = "passed_live"
	EvidenceNotTested          EvidenceLevel = "not_tested"
	EvidenceBlockedEnvironment EvidenceLevel = "blocked_environment"
)

func (level EvidenceLevel) Valid() bool {
	switch level {
	case EvidencePlanned, EvidenceImplementedOffline, EvidenceDocumentedUpstream, EvidencePassedLive, EvidenceNotTested, EvidenceBlockedEnvironment:
		return true
	default:
		return false
	}
}
