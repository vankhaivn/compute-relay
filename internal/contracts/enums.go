package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func validateDomainEnums(root string) error {
	path := filepath.Join(root, "api", "schemas", "common.v1alpha1.schema.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read common schema: %w", err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("decode common schema: %w", err)
	}
	definitions, ok := document["$defs"].(map[string]any)
	if !ok {
		return fmt.Errorf("common schema has no object $defs")
	}

	for name, expected := range domainEnums() {
		definition, ok := definitions[name].(map[string]any)
		if !ok {
			return fmt.Errorf("common schema has no %q definition", name)
		}
		raw, ok := definition["enum"].([]any)
		if !ok {
			return fmt.Errorf("common schema definition %q has no enum", name)
		}
		actual := make([]string, 0, len(raw))
		for _, value := range raw {
			text, ok := value.(string)
			if !ok {
				return fmt.Errorf("common schema definition %q contains a non-string enum", name)
			}
			actual = append(actual, text)
		}
		if !sameUniqueStrings(actual, expected) {
			return fmt.Errorf("common schema enum %q = %#v, want domain values %#v", name, actual, expected)
		}
	}
	return nil
}

func domainEnums() map[string][]string {
	return map[string][]string{
		"orchestration_state": stringsOf(
			domain.OrchestrationQueued, domain.OrchestrationPreparing, domain.OrchestrationDispatching,
			domain.OrchestrationSubmitted, domain.OrchestrationRunning, domain.OrchestrationCollecting,
			domain.OrchestrationBlocked, domain.OrchestrationReconciling, domain.OrchestrationCancelling,
			domain.OrchestrationNeedsAttention, domain.OrchestrationSucceeded, domain.OrchestrationFailed,
			domain.OrchestrationCancelled, domain.OrchestrationTimedOut,
		),
		"execution_state": stringsOf(
			domain.ExecutionNotSubmitted, domain.ExecutionUnknown, domain.ExecutionQueued,
			domain.ExecutionStarting, domain.ExecutionRunning, domain.ExecutionSucceeded,
			domain.ExecutionFailed, domain.ExecutionCancelled, domain.ExecutionTimedOut,
		),
		"result_state": stringsOf(
			domain.ResultNotAvailable, domain.ResultCollecting, domain.ResultAvailable,
			domain.ResultIncomplete, domain.ResultInvalid, domain.ResultExpired,
		),
		"cancellation_state": stringsOf(
			domain.CancellationNotRequested, domain.CancellationRequested, domain.CancellationRequesting,
			domain.CancellationAccepted, domain.CancellationPrevented, domain.CancellationConfirmed,
			domain.CancellationTooLate, domain.CancellationManual,
		),
		"remote_activity_state": stringsOf(
			domain.RemoteActivityNotStarted, domain.RemoteActivityUnknown, domain.RemoteActivityPossible,
			domain.RemoteActivityActive, domain.RemoteActivityInactive,
		),
		"release_evidence": stringsOf(
			domain.ReleaseEvidenceUnknown, domain.ReleaseEvidenceNotApplicable,
			domain.ReleaseEvidenceNotObservable, domain.ReleaseEvidenceProviderReported,
			domain.ReleaseEvidenceConfirmed,
		),
		"error_code": errorCodeStrings(domain.AllErrorCodes()),
		"failure_stage": stringsOf(
			domain.FailureStageValidation, domain.FailureStageAuthentication,
			domain.FailureStageInputPreparation, domain.FailureStageProviderPreparation,
			domain.FailureStageSubmission, domain.FailureStageExecution,
			domain.FailureStageObservation, domain.FailureStageResults,
			domain.FailureStageOperation, domain.FailureStageLocalRuntime,
		),
		"recommended_action": stringsOf(
			domain.RecommendedActionNone, domain.RecommendedActionFixRequest,
			domain.RecommendedActionConfigure, domain.RecommendedActionWait,
			domain.RecommendedActionRetryRead, domain.RecommendedActionReconcile,
			domain.RecommendedActionCollect, domain.RecommendedActionRetryComputeExplicitly,
			domain.RecommendedActionInspectProvider, domain.RecommendedActionFreeSpace,
			domain.RecommendedActionContactOperator,
		),
		"capability_support": stringsOf(
			domain.CapabilitySupportSupported, domain.CapabilitySupportUnsupported,
			domain.CapabilitySupportUnknown,
		),
		"evidence_level": stringsOf(
			domain.EvidencePlanned, domain.EvidenceImplementedOffline,
			domain.EvidenceDocumentedUpstream, domain.EvidencePassedLive,
			domain.EvidenceNotTested, domain.EvidenceBlockedEnvironment,
		),
		"operation_kind": stringsOf(
			domain.OperationCancel, domain.OperationRetryCompute, domain.OperationReconcile,
			domain.OperationCollect, domain.OperationCleanup,
		),
		"operation_status": stringsOf(
			domain.OperationAccepted, domain.OperationRunning, domain.OperationSucceeded,
			domain.OperationFailed, domain.OperationManualRequired,
		),
		"event_type": stringsOf(
			domain.EventJobAccepted, domain.EventInputsReady, domain.EventProviderStagingReady,
			domain.EventSubmissionIntentRecorded, domain.EventSubmissionAccepted,
			domain.EventSubmissionUnknown, domain.EventExecutionObserved,
			domain.EventCancellationRequested, domain.EventCollectionFailed,
			domain.EventArtifactVerified, domain.EventJobCompleted,
			domain.EventSchedulerClaimed, domain.EventSchedulerDeferred, domain.EventSchedulerReleased,
			domain.EventPreparationIntended, domain.EventPreparationObserved, domain.EventSubmissionRejected, domain.EventReconciliationDeferred,
		),
		"capability_name": stringsOf(
			domain.CapabilityBatchExecution, domain.CapabilityPython, domain.CapabilityShell,
			domain.CapabilityGPU, domain.CapabilityPrivateInputStaging,
			domain.CapabilityRemoteCancellation, domain.CapabilityLogsWhileRunning,
			domain.CapabilityLogsAfterCompletion, domain.CapabilityQuotaReporting,
			domain.CapabilityExecutionTimeout, domain.CapabilityRemoteNetworkControl,
			domain.CapabilityStrongIdentity, domain.CapabilityCustomContainer,
			domain.CapabilityRetainedSessions,
		),
	}
}

func sameUniqueStrings(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	actualSet := make(map[string]struct{}, len(actual))
	for _, value := range actual {
		if _, exists := actualSet[value]; exists {
			return false
		}
		actualSet[value] = struct{}{}
	}
	expectedSet := make(map[string]struct{}, len(expected))
	for _, value := range expected {
		if _, exists := expectedSet[value]; exists {
			return false
		}
		expectedSet[value] = struct{}{}
	}
	for value := range actualSet {
		if _, exists := expectedSet[value]; !exists {
			return false
		}
	}
	return true
}

func stringsOf[T ~string](values ...T) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value)
	}
	return result
}

func errorCodeStrings(values []domain.ErrorCode) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value)
	}
	return result
}
