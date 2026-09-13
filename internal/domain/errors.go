package domain

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrorCategory groups stable public error codes without collapsing their exact meaning.
type ErrorCategory string

const (
	ErrorCategoryValidation                  ErrorCategory = "validation"
	ErrorCategoryAuthenticationAuthorization ErrorCategory = "authentication_authorization"
	ErrorCategoryInputPreparation            ErrorCategory = "input_preparation"
	ErrorCategoryProviderPreparation         ErrorCategory = "provider_preparation"
	ErrorCategorySubmission                  ErrorCategory = "submission"
	ErrorCategoryExecution                   ErrorCategory = "execution"
	ErrorCategoryObservation                 ErrorCategory = "observation"
	ErrorCategoryResults                     ErrorCategory = "results"
	ErrorCategoryOperations                  ErrorCategory = "operations"
	ErrorCategoryLocalRuntime                ErrorCategory = "local_runtime"
)

// ErrorCode is a stable machine-readable failure code.
type ErrorCode string

const (
	CodeInvalidJobSpec                 ErrorCode = "INVALID_JOB_SPEC"
	CodeUnsupportedCapability          ErrorCode = "UNSUPPORTED_CAPABILITY"
	CodeInvalidInputPath               ErrorCode = "INVALID_INPUT_PATH"
	CodeResourceRequirementUnsatisfied ErrorCode = "RESOURCE_REQUIREMENT_UNSATISFIED"
	CodeRuntimeAuthRequired            ErrorCode = "RUNTIME_AUTH_REQUIRED"
	CodeWorkspaceForbidden             ErrorCode = "WORKSPACE_FORBIDDEN"
	CodeProviderAuthFailed             ErrorCode = "PROVIDER_AUTH_FAILED"
	CodeProviderAccessDenied           ErrorCode = "PROVIDER_ACCESS_DENIED"
	CodeInputNotFound                  ErrorCode = "INPUT_NOT_FOUND"
	CodeInputFetchFailed               ErrorCode = "INPUT_FETCH_FAILED"
	CodeInputChanged                   ErrorCode = "INPUT_CHANGED"
	CodeInputDigestMismatch            ErrorCode = "INPUT_DIGEST_MISMATCH"
	CodeInputTooLarge                  ErrorCode = "INPUT_TOO_LARGE"
	CodeStagingFailed                  ErrorCode = "STAGING_FAILED"
	CodeStagingNotReady                ErrorCode = "STAGING_NOT_READY"
	CodePrivateStagingUnavailable      ErrorCode = "PRIVATE_STAGING_UNAVAILABLE"
	CodeProviderStorageLimit           ErrorCode = "PROVIDER_STORAGE_LIMIT"
	CodeProviderRejected               ErrorCode = "PROVIDER_REJECTED"
	CodeProviderSubmissionUnknown      ErrorCode = "PROVIDER_SUBMISSION_UNKNOWN"
	CodeProviderRateLimited            ErrorCode = "PROVIDER_RATE_LIMITED"
	CodeQuotaExhausted                 ErrorCode = "QUOTA_EXHAUSTED"
	CodeDependencySetupFailed          ErrorCode = "DEPENDENCY_SETUP_FAILED"
	CodeCommandFailed                  ErrorCode = "COMMAND_FAILED"
	CodeRemoteTimeout                  ErrorCode = "REMOTE_TIMEOUT"
	CodeResourceExhausted              ErrorCode = "RESOURCE_EXHAUSTED"
	CodeProviderExecutionLost          ErrorCode = "PROVIDER_EXECUTION_LOST"
	CodeProviderUnreachable            ErrorCode = "PROVIDER_UNREACHABLE"
	CodeProviderStateUnknown           ErrorCode = "PROVIDER_STATE_UNKNOWN"
	CodeRemoteIdentityMismatch         ErrorCode = "REMOTE_IDENTITY_MISMATCH"
	CodeResultManifestMissing          ErrorCode = "RESULT_MANIFEST_MISSING"
	CodeArtifactMissing                ErrorCode = "ARTIFACT_MISSING"
	CodeArtifactDigestMismatch         ErrorCode = "ARTIFACT_DIGEST_MISMATCH"
	CodeArtifactCollectionFailed       ErrorCode = "ARTIFACT_COLLECTION_FAILED"
	CodeRemoteCancelUnsupported        ErrorCode = "REMOTE_CANCEL_UNSUPPORTED"
	CodeRemoteExecutionUnresolved      ErrorCode = "REMOTE_EXECUTION_UNRESOLVED"
	CodeIllegalStateTransition         ErrorCode = "ILLEGAL_STATE_TRANSITION"
	CodeIdempotencyConflict            ErrorCode = "IDEMPOTENCY_CONFLICT"
	CodeStateStoreUnavailable          ErrorCode = "STATE_STORE_UNAVAILABLE"
	CodeDiskLimitExceeded              ErrorCode = "DISK_LIMIT_EXCEEDED"
	CodeStateDirectoryLocked           ErrorCode = "STATE_DIRECTORY_LOCKED"
	CodeConfigurationInvalid           ErrorCode = "CONFIGURATION_INVALID"
)

var errorCategories = map[ErrorCode]ErrorCategory{
	CodeInvalidJobSpec:                 ErrorCategoryValidation,
	CodeUnsupportedCapability:          ErrorCategoryValidation,
	CodeInvalidInputPath:               ErrorCategoryValidation,
	CodeResourceRequirementUnsatisfied: ErrorCategoryValidation,
	CodeRuntimeAuthRequired:            ErrorCategoryAuthenticationAuthorization,
	CodeWorkspaceForbidden:             ErrorCategoryAuthenticationAuthorization,
	CodeProviderAuthFailed:             ErrorCategoryAuthenticationAuthorization,
	CodeProviderAccessDenied:           ErrorCategoryAuthenticationAuthorization,
	CodeInputNotFound:                  ErrorCategoryInputPreparation,
	CodeInputFetchFailed:               ErrorCategoryInputPreparation,
	CodeInputChanged:                   ErrorCategoryInputPreparation,
	CodeInputDigestMismatch:            ErrorCategoryInputPreparation,
	CodeInputTooLarge:                  ErrorCategoryInputPreparation,
	CodeStagingFailed:                  ErrorCategoryProviderPreparation,
	CodeStagingNotReady:                ErrorCategoryProviderPreparation,
	CodePrivateStagingUnavailable:      ErrorCategoryProviderPreparation,
	CodeProviderStorageLimit:           ErrorCategoryProviderPreparation,
	CodeProviderRejected:               ErrorCategorySubmission,
	CodeProviderSubmissionUnknown:      ErrorCategorySubmission,
	CodeProviderRateLimited:            ErrorCategorySubmission,
	CodeQuotaExhausted:                 ErrorCategorySubmission,
	CodeDependencySetupFailed:          ErrorCategoryExecution,
	CodeCommandFailed:                  ErrorCategoryExecution,
	CodeRemoteTimeout:                  ErrorCategoryExecution,
	CodeResourceExhausted:              ErrorCategoryExecution,
	CodeProviderExecutionLost:          ErrorCategoryExecution,
	CodeProviderUnreachable:            ErrorCategoryObservation,
	CodeProviderStateUnknown:           ErrorCategoryObservation,
	CodeRemoteIdentityMismatch:         ErrorCategoryObservation,
	CodeResultManifestMissing:          ErrorCategoryResults,
	CodeArtifactMissing:                ErrorCategoryResults,
	CodeArtifactDigestMismatch:         ErrorCategoryResults,
	CodeArtifactCollectionFailed:       ErrorCategoryResults,
	CodeRemoteCancelUnsupported:        ErrorCategoryOperations,
	CodeRemoteExecutionUnresolved:      ErrorCategoryOperations,
	CodeIllegalStateTransition:         ErrorCategoryOperations,
	CodeIdempotencyConflict:            ErrorCategoryOperations,
	CodeStateStoreUnavailable:          ErrorCategoryLocalRuntime,
	CodeDiskLimitExceeded:              ErrorCategoryLocalRuntime,
	CodeStateDirectoryLocked:           ErrorCategoryLocalRuntime,
	CodeConfigurationInvalid:           ErrorCategoryLocalRuntime,
}

// Category returns the stable category for a known code.
func (code ErrorCode) Category() (ErrorCategory, bool) {
	category, ok := errorCategories[code]
	return category, ok
}

// Valid reports whether code belongs to the public taxonomy.
func (code ErrorCode) Valid() bool {
	_, ok := code.Category()
	return ok
}

// AllErrorCodes returns a deterministic copy of the registered taxonomy.
func AllErrorCodes() []ErrorCode {
	codes := make([]ErrorCode, 0, len(errorCategories))
	for code := range errorCategories {
		codes = append(codes, code)
	}
	sort.Slice(codes, func(left, right int) bool { return codes[left] < codes[right] })
	return codes
}

// FailureStage identifies where a problem occurred.
type FailureStage string

const (
	FailureStageValidation          FailureStage = "validation"
	FailureStageAuthentication      FailureStage = "authentication"
	FailureStageInputPreparation    FailureStage = "input_preparation"
	FailureStageProviderPreparation FailureStage = "provider_preparation"
	FailureStageSubmission          FailureStage = "submission"
	FailureStageExecution           FailureStage = "execution"
	FailureStageObservation         FailureStage = "observation"
	FailureStageResults             FailureStage = "results"
	FailureStageOperation           FailureStage = "operation"
	FailureStageLocalRuntime        FailureStage = "local_runtime"
)

func (stage FailureStage) Valid() bool {
	switch stage {
	case FailureStageValidation, FailureStageAuthentication, FailureStageInputPreparation,
		FailureStageProviderPreparation, FailureStageSubmission, FailureStageExecution,
		FailureStageObservation, FailureStageResults, FailureStageOperation, FailureStageLocalRuntime:
		return true
	default:
		return false
	}
}

// RecommendedAction distinguishes safe observation/transfer recovery from new compute.
type RecommendedAction string

const (
	RecommendedActionNone                   RecommendedAction = "none"
	RecommendedActionFixRequest             RecommendedAction = "fix_request"
	RecommendedActionConfigure              RecommendedAction = "configure"
	RecommendedActionWait                   RecommendedAction = "wait"
	RecommendedActionRetryRead              RecommendedAction = "retry_read"
	RecommendedActionReconcile              RecommendedAction = "reconcile"
	RecommendedActionCollect                RecommendedAction = "collect"
	RecommendedActionRetryComputeExplicitly RecommendedAction = "retry_compute_explicitly"
	RecommendedActionInspectProvider        RecommendedAction = "inspect_provider"
	RecommendedActionFreeSpace              RecommendedAction = "free_space"
	RecommendedActionContactOperator        RecommendedAction = "contact_operator"
)

func (action RecommendedAction) Valid() bool {
	switch action {
	case RecommendedActionNone, RecommendedActionFixRequest, RecommendedActionConfigure,
		RecommendedActionWait, RecommendedActionRetryRead, RecommendedActionReconcile,
		RecommendedActionCollect, RecommendedActionRetryComputeExplicitly,
		RecommendedActionInspectProvider, RecommendedActionFreeSpace,
		RecommendedActionContactOperator:
		return true
	default:
		return false
	}
}

func defaultFailureStage(category ErrorCategory) FailureStage {
	switch category {
	case ErrorCategoryValidation:
		return FailureStageValidation
	case ErrorCategoryAuthenticationAuthorization:
		return FailureStageAuthentication
	case ErrorCategoryInputPreparation:
		return FailureStageInputPreparation
	case ErrorCategoryProviderPreparation:
		return FailureStageProviderPreparation
	case ErrorCategorySubmission:
		return FailureStageSubmission
	case ErrorCategoryExecution:
		return FailureStageExecution
	case ErrorCategoryObservation:
		return FailureStageObservation
	case ErrorCategoryResults:
		return FailureStageResults
	case ErrorCategoryOperations:
		return FailureStageOperation
	case ErrorCategoryLocalRuntime:
		return FailureStageLocalRuntime
	default:
		return ""
	}
}

// Problem is the provider-neutral structured error carried by domain and application layers.
type Problem struct {
	Code                  ErrorCode
	Message               string
	Stage                 FailureStage
	SafeOperationRetry    bool
	ComputeMayHaveStarted bool
	RecommendedAction     RecommendedAction
	Details               map[string]string
	cause                 error
}

// NewProblem creates a validated problem with conservative retry semantics.
func NewProblem(code ErrorCode, message string, stage FailureStage) (Problem, error) {
	problem := Problem{
		Code:              code,
		Message:           strings.TrimSpace(message),
		Stage:             stage,
		RecommendedAction: RecommendedActionNone,
	}
	if err := problem.Validate(); err != nil {
		return Problem{}, err
	}
	return problem, nil
}

// WithRetrySemantics returns a copy with explicit control-operation and compute ambiguity.
func (problem Problem) WithRetrySemantics(safeOperationRetry, computeMayHaveStarted bool) Problem {
	problem.SafeOperationRetry = safeOperationRetry
	problem.ComputeMayHaveStarted = computeMayHaveStarted
	return problem
}

// WithRecommendedAction returns a copy with one explicit next action.
func (problem Problem) WithRecommendedAction(action RecommendedAction) Problem {
	problem.RecommendedAction = action
	return problem
}

// WithDetail returns a copy with one bounded diagnostic field.
func (problem Problem) WithDetail(key, value string) Problem {
	cloned := make(map[string]string, len(problem.Details)+1)
	for existingKey, existingValue := range problem.Details {
		cloned[existingKey] = existingValue
	}
	cloned[key] = value
	problem.Details = cloned
	return problem
}

// WithCause returns a copy that unwraps to cause without exposing it as public details.
func (problem Problem) WithCause(cause error) Problem {
	problem.cause = cause
	return problem
}

// Validate checks the stable code, stage, message, action, and retry invariants.
func (problem Problem) Validate() error {
	if !problem.Code.Valid() {
		return fmt.Errorf("unknown error code %q", problem.Code)
	}
	if strings.TrimSpace(problem.Message) == "" {
		return errors.New("problem message must not be empty")
	}
	if !problem.Stage.Valid() {
		return fmt.Errorf("invalid failure stage %q", problem.Stage)
	}
	category, _ := problem.Code.Category()
	if expected := defaultFailureStage(category); problem.Stage != expected {
		return fmt.Errorf("error code %q requires failure stage %q, got %q", problem.Code, expected, problem.Stage)
	}
	if !problem.RecommendedAction.Valid() {
		return fmt.Errorf("invalid recommended action %q", problem.RecommendedAction)
	}
	if problem.ComputeMayHaveStarted && problem.SafeOperationRetry && problem.RecommendedAction == RecommendedActionRetryComputeExplicitly {
		return errors.New("ambiguous compute cannot be marked safe for automatic operation retry")
	}
	for key := range problem.Details {
		if strings.TrimSpace(key) == "" {
			return errors.New("problem detail keys must not be empty")
		}
	}
	return nil
}

func (problem Problem) Error() string {
	return fmt.Sprintf("%s: %s", problem.Code, problem.Message)
}

func (problem Problem) Unwrap() error {
	return problem.cause
}
