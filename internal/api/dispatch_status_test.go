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
	if err != nil {
		t.Fatal(err)
	}
	p, err := domain.NewProblem(domain.CodeProviderSubmissionUnknown, "Submission may have been accepted; reconcile the recorded identity without resubmitting.", domain.FailureStageSubmission)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if err = schema.Validate(wire); err != nil {
		t.Fatal("actual problem response failed existing schema", err)
	}
	record.Problem = nil
	if _, exists := jobStatus(record)["problem"]; exists {
		t.Fatal("absent problem was fabricated")
	}
}
