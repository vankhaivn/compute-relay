package api

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestDispatchProblemWireShapeAndRedaction(t *testing.T) {
	at := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	a, err := domain.NewAttempt("attempt", "job", 1, at)
	requireNoError(t, err)
	p, err := domain.NewProblem(domain.CodeProviderSubmissionUnknown, "Submission may have been accepted; reconcile the recorded identity without resubmitting.", domain.FailureStageSubmission)
	requireNoError(t, err)
	p = p.WithRetrySemantics(false, true).WithRecommendedAction(domain.RecommendedActionReconcile).
		WithDetail("private", "sensitive-detail-canary").WithCause(errors.New("internal-cause-canary"))
	record := admission.Record{
		Job:     domain.Job{ID: "job", WorkspaceID: "workspace", Name: "fixture", SpecificationVersion: "compute-connector/v1alpha1", CreatedAt: at},
		Attempt: a, Problem: &p,
		Profile: admission.Profile{CredentialRef: "file:/credential-canary", AccountScope: "account-canary"},
	}
	record.Attempt.State.Orchestration = domain.OrchestrationReconciling
	record.Attempt.State.Execution = domain.ExecutionUnknown
	record.Attempt.State.RemoteActivity = domain.RemoteActivityPossible
	encoded, err := json.Marshal(jobStatus(record))
	requireNoError(t, err)
	if strings.Contains(string(encoded), "canary") {
		t.Fatal("internal details, cause or binding leaked")
	}
	var wire map[string]any
	if err = json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	problem, ok := wire["problem"].(map[string]any)
	if !ok || problem["code"] != string(domain.CodeProviderSubmissionUnknown) || problem["compute_may_have_started"] != true || problem["safe_operation_retry"] != false || problem["recommended_action"] != "reconcile" {
		t.Fatal("wire lost safe recovery semantics")
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("source path unavailable")
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	schema, err := compiler.Compile(filepath.Join(filepath.Dir(source), "..", "..", "api", "schemas", "job-status.v1alpha1.schema.json"))
	requireNoError(t, err)
	if err = schema.Validate(wire); err != nil {
		t.Fatal("actual problem response failed existing schema", err)
	}
	// A closed reason is public; unsafe text in the same key is dropped with the other details.
	for reason, public := range map[string]bool{"staging_not_confirmed": true, "https://reason-canary.invalid": false, "Reason canary": false} {
		reasoned := p.WithDetail(domain.ReasonDetail, reason)
		record.Problem = &reasoned
		encoded, err := json.Marshal(jobStatus(record))
		requireNoError(t, err)
		var wire map[string]any
		if strings.Contains(string(encoded), "canary") || json.Unmarshal(encoded, &wire) != nil || schema.Validate(wire) != nil {
			t.Fatal("reasoned problem leaked or failed the schema", reason)
		}
		details, exists := wire["problem"].(map[string]any)["details"]
		if exists != public || public && len(details.(map[string]any)) != 1 || public && details.(map[string]any)["reason"] != reason {
			t.Fatal("unexpected public details", reason, details)
		}
	}
	record.Problem = nil
	if _, exists := jobStatus(record)["problem"]; exists {
		t.Fatal("absent problem was fabricated")
	}
}

func TestPreparationProgressWireShape(t *testing.T) {
	at := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	a, err := domain.NewAttempt("attempt", "job", 1, at)
	requireNoError(t, err)
	a.State.Orchestration = domain.OrchestrationPreparing
	record := admission.Record{
		Job:     domain.Job{ID: "job", WorkspaceID: "workspace", Name: "fixture", SpecificationVersion: "compute-connector/v1alpha1", CreatedAt: at},
		Attempt: a,
		Preparation: &domain.PreparationStatus{Progress: &domain.PreparationProgress{
			Scope: "staged_input_bytes", Generation: 2, BytesCompleted: 512, BytesTotal: 2048, ObservedAt: at}},
	}
	encoded, err := json.Marshal(jobStatus(record))
	requireNoError(t, err)
	var wire map[string]any
	if err = json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	_, source, _, _ := runtime.Caller(0)
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	schema, err := compiler.Compile(filepath.Join(filepath.Dir(source), "..", "..", "api", "schemas", "job-status.v1alpha1.schema.json"))
	requireNoError(t, err)
	if err = schema.Validate(wire); err != nil {
		t.Fatal("preparation progress failed the status schema", err)
	}
	progress := wire["preparation"].(map[string]any)["progress"].(map[string]any)
	if progress["bytes_completed"] != float64(512) || progress["bytes_total"] != float64(2048) {
		t.Fatalf("wrong wire progress: %v", progress)
	}
	record.Preparation = nil
	if _, exists := jobStatus(record)["preparation"]; exists {
		t.Fatal("absent preparation progress was fabricated")
	}
}
