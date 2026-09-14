package domain

import (
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestErrorTaxonomyCategories(t *testing.T) {
	t.Parallel()

	expected := map[ErrorCategory][]ErrorCode{
		ErrorCategoryValidation: {
			CodeInvalidJobSpec, CodeUnsupportedCapability, CodeInvalidInputPath,
			CodeResourceRequirementUnsatisfied, CodeInvalidRequest,
		},
		ErrorCategoryAuthenticationAuthorization: {
			CodeRuntimeAuthRequired, CodeWorkspaceForbidden, CodeProviderAuthFailed,
			CodeProviderAccessDenied,
		},
		ErrorCategoryInputPreparation: {
			CodeInputNotFound, CodeInputFetchFailed, CodeInputChanged,
			CodeInputDigestMismatch, CodeInputTooLarge,
		},
		ErrorCategoryProviderPreparation: {
			CodeStagingFailed, CodeStagingNotReady, CodePrivateStagingUnavailable,
			CodeProviderStorageLimit,
		},
		ErrorCategorySubmission: {
			CodeProviderRejected, CodeProviderSubmissionUnknown, CodeProviderRateLimited,
			CodeQuotaExhausted,
		},
		ErrorCategoryExecution: {
			CodeDependencySetupFailed, CodeCommandFailed, CodeRemoteTimeout,
			CodeResourceExhausted, CodeProviderExecutionLost,
		},
		ErrorCategoryObservation: {
			CodeProviderUnreachable, CodeProviderStateUnknown, CodeRemoteIdentityMismatch,
		},
		ErrorCategoryResults: {
			CodeResultManifestMissing, CodeArtifactMissing, CodeArtifactDigestMismatch,
			CodeArtifactCollectionFailed,
		},
		ErrorCategoryOperations: {
			CodeRemoteCancelUnsupported, CodeRemoteExecutionUnresolved,
			CodeIllegalStateTransition, CodeIdempotencyConflict,
		},
		ErrorCategoryLocalRuntime: {
			CodeStateStoreUnavailable, CodeDiskLimitExceeded, CodeStateDirectoryLocked,
			CodeConfigurationInvalid, CodeRequestLimitExceeded,
		},
	}

	var wantAll []ErrorCode
	for category, codes := range expected {
		for _, code := range codes {
			gotCategory, ok := code.Category()
			if !ok {
				t.Fatalf("code %q is not registered", code)
			}
			if gotCategory != category {
				t.Fatalf("code %q category = %q, want %q", code, gotCategory, category)
			}
			wantAll = append(wantAll, code)
		}
	}
	sort.Slice(wantAll, func(left, right int) bool { return wantAll[left] < wantAll[right] })
	if got := AllErrorCodes(); !reflect.DeepEqual(got, wantAll) {
		t.Fatalf("AllErrorCodes() = %#v, want %#v", got, wantAll)
	}
	if ErrorCode("SOMETHING_NEW").Valid() {
		t.Fatal("unknown code reported as valid")
	}
}

func TestProblemPreservesRetryAndCauseSemantics(t *testing.T) {
	t.Parallel()

	cause := errors.New("connection reset")
	problem, err := NewProblem(
		CodeProviderSubmissionUnknown,
		"The submission response was lost; the remote execution may exist.",
		FailureStageSubmission,
	)
	if err != nil {
		t.Fatalf("NewProblem() error = %v", err)
	}
	problem = problem.
		WithRetrySemantics(false, true).
		WithRecommendedAction(RecommendedActionReconcile).
		WithDetail("provider_instance", "instance_01").
		WithCause(cause)

	if err := problem.Validate(); err != nil {
		t.Fatalf("Problem.Validate() error = %v", err)
	}
	if problem.SafeOperationRetry {
		t.Fatal("ambiguous submission unexpectedly marked safe to retry")
	}
	if !problem.ComputeMayHaveStarted {
		t.Fatal("ambiguous submission lost compute ambiguity")
	}
	if problem.RecommendedAction != RecommendedActionReconcile {
		t.Fatalf("recommended action = %q", problem.RecommendedAction)
	}
	if !errors.Is(problem, cause) {
		t.Fatal("Problem does not unwrap its internal cause")
	}
	if category, _ := problem.Code.Category(); category != ErrorCategorySubmission {
		t.Fatalf("category = %q, want submission", category)
	}
}

func TestProblemDetailsAreCopyOnWrite(t *testing.T) {
	t.Parallel()

	base, err := NewProblem(CodeInvalidJobSpec, "invalid job", FailureStageValidation)
	if err != nil {
		t.Fatal(err)
	}
	first := base.WithDetail("field", "execution.command")
	second := first.WithDetail("reason", "empty")

	if len(base.Details) != 0 {
		t.Fatalf("base details mutated: %#v", base.Details)
	}
	if _, ok := first.Details["reason"]; ok {
		t.Fatalf("first details mutated: %#v", first.Details)
	}
	if second.Details["field"] != "execution.command" || second.Details["reason"] != "empty" {
		t.Fatalf("second details = %#v", second.Details)
	}
}

func TestProblemRejectsUnsafeAutomaticComputeRetry(t *testing.T) {
	t.Parallel()

	problem, err := NewProblem(CodeProviderSubmissionUnknown, "ambiguous", FailureStageSubmission)
	if err != nil {
		t.Fatal(err)
	}
	problem = problem.
		WithRetrySemantics(true, true).
		WithRecommendedAction(RecommendedActionRetryComputeExplicitly)
	if err := problem.Validate(); err == nil {
		t.Fatal("unsafe retry semantics accepted")
	}
}

func TestProblemRejectsMismatchedFailureStage(t *testing.T) {
	t.Parallel()

	if _, err := NewProblem(CodeProviderSubmissionUnknown, "ambiguous", FailureStageExecution); err == nil {
		t.Fatal("submission error accepted with execution stage")
	}
}
