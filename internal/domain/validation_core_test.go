package domain

import (
	"math"
	"strings"
	"testing"
	"time"
)

const testDigest SHA256Digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestAllDeclaredEvidenceStagesAndActionsAreValid(t *testing.T) {
	t.Parallel()

	for _, level := range []EvidenceLevel{
		EvidencePlanned,
		EvidenceImplementedOffline,
		EvidenceDocumentedUpstream,
		EvidencePassedLive,
		EvidenceNotTested,
		EvidenceBlockedEnvironment,
	} {
		if !level.Valid() {
			t.Fatalf("declared evidence level %q is invalid", level)
		}
	}
	if EvidenceLevel("invented").Valid() {
		t.Fatal("unknown evidence level is valid")
	}

	for _, stage := range []FailureStage{
		FailureStageValidation,
		FailureStageAuthentication,
		FailureStageInputPreparation,
		FailureStageProviderPreparation,
		FailureStageSubmission,
		FailureStageExecution,
		FailureStageObservation,
		FailureStageResults,
		FailureStageOperation,
		FailureStageLocalRuntime,
	} {
		if !stage.Valid() {
			t.Fatalf("declared failure stage %q is invalid", stage)
		}
	}
	if FailureStage("invented").Valid() {
		t.Fatal("unknown failure stage is valid")
	}

	for _, action := range []RecommendedAction{
		RecommendedActionNone,
		RecommendedActionFixRequest,
		RecommendedActionConfigure,
		RecommendedActionWait,
		RecommendedActionRetryRead,
		RecommendedActionReconcile,
		RecommendedActionCollect,
		RecommendedActionRetryComputeExplicitly,
		RecommendedActionInspectProvider,
		RecommendedActionFreeSpace,
		RecommendedActionContactOperator,
	} {
		if !action.Valid() {
			t.Fatalf("declared recommended action %q is invalid", action)
		}
	}
	if RecommendedAction("invented").Valid() {
		t.Fatal("unknown recommended action is valid")
	}
}

func TestProblemValidationFailures(t *testing.T) {
	t.Parallel()

	valid, err := NewProblem(CodeInvalidJobSpec, "invalid", FailureStageValidation)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		problem Problem
	}{
		{name: "unknown code", problem: Problem{Code: "UNKNOWN", Message: "x", Stage: FailureStageValidation, RecommendedAction: RecommendedActionNone}},
		{name: "blank message", problem: Problem{Code: CodeInvalidJobSpec, Message: " ", Stage: FailureStageValidation, RecommendedAction: RecommendedActionNone}},
		{name: "invalid stage", problem: Problem{Code: CodeInvalidJobSpec, Message: "x", Stage: "bad", RecommendedAction: RecommendedActionNone}},
		{name: "mismatched stage", problem: Problem{Code: CodeInvalidJobSpec, Message: "x", Stage: FailureStageExecution, RecommendedAction: RecommendedActionNone}},
		{name: "invalid action", problem: Problem{Code: CodeInvalidJobSpec, Message: "x", Stage: FailureStageValidation, RecommendedAction: "bad"}},
		{name: "empty detail key", problem: valid.WithDetail(" ", "value")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.problem.Validate(); err == nil {
				t.Fatal("invalid problem accepted")
			}
		})
	}

	if got := valid.Error(); !strings.Contains(got, string(CodeInvalidJobSpec)) {
		t.Fatalf("Error() = %q", got)
	}
}

func TestProviderBindingAndJobValidationFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC)
	valid := Job{
		ID:                   JobID("job_01"),
		WorkspaceID:          WorkspaceID("ws_01"),
		Name:                 "job",
		SpecificationVersion: "compute-connector/v1alpha1",
		SpecificationDigest:  testDigest,
		Binding: ProviderBinding{
			Profile:               "default-gpu",
			ProviderInstanceID:    ProviderInstanceID("provider_01"),
			ConfigurationRevision: "revision_01",
		},
		ActiveAttemptID: AttemptID("attempt_01"),
		CreatedAt:       now,
	}

	mutations := []struct {
		name   string
		mutate func(*Job)
	}{
		{"job ID", func(job *Job) { job.ID = "bad/id" }},
		{"workspace ID", func(job *Job) { job.WorkspaceID = "bad/id" }},
		{"name", func(job *Job) { job.Name = " " }},
		{"spec version", func(job *Job) { job.SpecificationVersion = "" }},
		{"spec digest", func(job *Job) { job.SpecificationDigest = "bad" }},
		{"profile", func(job *Job) { job.Binding.Profile = "" }},
		{"provider ID", func(job *Job) { job.Binding.ProviderInstanceID = "bad/id" }},
		{"config revision", func(job *Job) { job.Binding.ConfigurationRevision = "" }},
		{"active attempt", func(job *Job) { job.ActiveAttemptID = "bad/id" }},
		{"created time", func(job *Job) { job.CreatedAt = time.Time{} }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			t.Parallel()
			job := valid
			mutation.mutate(&job)
			if _, err := NewJob(job); err == nil {
				t.Fatal("invalid job accepted")
			}
		})
	}
}

func TestAttemptValidationAndTransitionFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 13, 13, 0, 0, 0, time.UTC)
	valid, err := NewAttempt(AttemptID("attempt_01"), JobID("job_01"), 1, now)
	if err != nil {
		t.Fatal(err)
	}

	mutations := []struct {
		name   string
		mutate func(*Attempt)
	}{
		{"attempt ID", func(attempt *Attempt) { attempt.ID = "bad/id" }},
		{"job ID", func(attempt *Attempt) { attempt.JobID = "bad/id" }},
		{"number", func(attempt *Attempt) { attempt.Number = 0 }},
		{"revision", func(attempt *Attempt) { attempt.Revision = 0 }},
		{"created time", func(attempt *Attempt) { attempt.CreatedAt = time.Time{} }},
		{"updated time", func(attempt *Attempt) { attempt.UpdatedAt = time.Time{} }},
		{"time order", func(attempt *Attempt) { attempt.UpdatedAt = now.Add(-time.Second) }},
		{"state", func(attempt *Attempt) { attempt.State.Orchestration = "bad" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			t.Parallel()
			attempt := valid
			mutation.mutate(&attempt)
			if err := attempt.Validate(); err == nil {
				t.Fatal("invalid attempt accepted")
			}
		})
	}

	next := valid.State
	next.Orchestration = OrchestrationPreparing
	if _, err := valid.Transition(next, time.Time{}); err == nil {
		t.Fatal("zero transition time accepted")
	}
	overflow := valid
	overflow.Revision = math.MaxUint64
	if _, err := overflow.Transition(next, now.Add(time.Second)); err == nil {
		t.Fatal("revision overflow accepted")
	}
}
