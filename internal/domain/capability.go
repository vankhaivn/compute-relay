package domain

import (
	"fmt"
	"strings"
	"time"
)

// CapabilityName is a provider-neutral behavior exposed by a provider instance.
type CapabilityName string

const (
	CapabilityBatchExecution       CapabilityName = "batch_execution"
	CapabilityPython               CapabilityName = "python"
	CapabilityShell                CapabilityName = "shell"
	CapabilityGPU                  CapabilityName = "gpu"
	CapabilityPrivateInputStaging  CapabilityName = "private_input_staging"
	CapabilityRemoteCancellation   CapabilityName = "remote_cancellation"
	CapabilityLogsWhileRunning     CapabilityName = "logs_while_running"
	CapabilityLogsAfterCompletion  CapabilityName = "logs_after_completion"
	CapabilityQuotaReporting       CapabilityName = "quota_reporting"
	CapabilityExecutionTimeout     CapabilityName = "execution_timeout"
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

// CapabilityStatus is one evidence-bearing capability conclusion.
type CapabilityStatus struct {
	Name           CapabilityName
	Support        CapabilitySupport
	Reason         string
	Conditions     []string
	Evidence       EvidenceLevel
	CheckedAt      time.Time
	ClientVersion  string
	AccountChecked bool
}

// Validate rejects internally contradictory capability claims.
func (status CapabilityStatus) Validate() error {
	if strings.TrimSpace(string(status.Name)) == "" {
		return fmt.Errorf("capability name must not be empty")
	}
	if !status.Support.Valid() {
		return fmt.Errorf("invalid capability support %q", status.Support)
	}
	if !status.Evidence.Valid() {
		return fmt.Errorf("invalid capability evidence %q", status.Evidence)
	}
	if status.Support != CapabilitySupportUnknown {
		switch status.Evidence {
		case EvidencePlanned, EvidenceNotTested, EvidenceBlockedEnvironment:
			return fmt.Errorf("capability %q cannot be %q with evidence %q", status.Name, status.Support, status.Evidence)
		}
	}
	if status.Evidence == EvidenceDocumentedUpstream || status.Evidence == EvidencePassedLive {
		if status.CheckedAt.IsZero() {
			return fmt.Errorf("capability %q requires an evidence timestamp", status.Name)
		}
	}
	if status.Evidence == EvidencePassedLive && !status.AccountChecked {
		return fmt.Errorf("live capability evidence must record an account-scoped check")
	}
	for _, condition := range status.Conditions {
		if strings.TrimSpace(condition) == "" {
			return fmt.Errorf("capability %q contains an empty condition", status.Name)
		}
	}
	return nil
}
