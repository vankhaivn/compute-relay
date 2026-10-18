package collection

import (
	"errors"
	"fmt"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestCollectionFailureSanitizesExistingFailureReasons(t *testing.T) {
	for _, input := range []Failure{
		{Code: domain.CodeArtifactCollectionFailed, Invalid: true, Reason: "private_provider_canary"},
		{Code: domain.CodeArtifactMissing, Invalid: true, Reason: "artifact_output_missing"},
	} {
		got := collectionFailure(input)
		if got.Code != input.Code || got.Invalid != input.Invalid || got.Reason != "" {
			t.Fatal("changed existing failure semantics or retained arbitrary reason", got)
		}
	}
}

func TestCollectionFailurePreservesOnlyBoundedCollectionProblemReason(t *testing.T) {
	for _, code := range []domain.ErrorCode{domain.CodeArtifactCollectionFailed, domain.CodeArtifactMissing} {
		problem := provider.Problem(code, domain.FailureStageResults, "private message").WithDetail(domain.ReasonDetail, "artifact_read_failed").WithCause(errors.New("private cause"))
		got := collectionFailure(fmt.Errorf("private wrapper: %w", problem))
		want := ""
		if code == domain.CodeArtifactCollectionFailed {
			want = "artifact_read_failed"
		}
		if got.Code != domain.CodeArtifactCollectionFailed || got.Invalid || got.Reason != want {
			t.Fatal("unexpected collection translation", got)
		}
	}
}
