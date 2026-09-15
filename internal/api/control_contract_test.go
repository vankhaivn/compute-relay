package api

import (
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

func controlSchema(t *testing.T, name string) *jsonschema.Schema {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("source path unavailable")
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	file, fragment, _ := strings.Cut(name, "#")
	path := filepath.ToSlash(filepath.Join(filepath.Dir(source), "..", "..", "api", "schemas", file))
	if !strings.HasPrefix(path, "/") {
		path = "/" + path // Absolute Windows drive path in a file URI.
	}
	location := (&url.URL{Scheme: "file", Path: path, Fragment: fragment}).String()
	schema, err := c.Compile(location)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

// Check the actual serializer, not a parallel fixture-only response model. Every
// supported kind/status/effect combination must agree with Record.Validate.
func TestControlWireSchemaMatchesRecordSemantics(t *testing.T) {
	schema := controlSchema(t, "control-operation.v1alpha1.schema.json")
	at := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	problem, err := domain.NewProblem(domain.CodeRemoteExecutionUnresolved, "Reconcile the recorded identity without resubmitting.", domain.FailureStageOperation)
	if err != nil {
		t.Fatal(err)
	}
	problem = problem.WithRetrySemantics(false, true).WithRecommendedAction(domain.RecommendedActionReconcile).
		WithDetail("private", "detail-canary").WithCause(errors.New("cause-canary"))
	kinds := []domain.OperationKind{domain.OperationCancel, domain.OperationRetryCompute, domain.OperationReconcile, domain.OperationCollect}
	statuses := []domain.OperationStatus{domain.OperationAccepted, domain.OperationRunning, domain.OperationSucceeded, domain.OperationFailed, domain.OperationManualRequired}
	effects := []operations.Effect{operations.Pending, operations.DispatchPrevented, operations.CancellationRequested,
		operations.CancellationConfirmed, operations.TooLate, operations.ObservationRefreshed, operations.CollectionRequested,
		operations.ResultsAvailable, operations.NewAttemptCreated, operations.ManualRequired, operations.Failed}
	for _, kind := range kinds {
		for _, status := range statuses {
			for _, effect := range effects {
				for _, newAttempt := range []domain.AttemptID{"", "next"} {
					for _, confirmed := range []bool{false, true} {
						r := operations.Record{Operation: domain.Operation{ID: "operation", WorkspaceID: "workspace", JobID: "job", AttemptID: "source",
							Kind: kind, Status: status, Revision: 1, CreatedAt: at, UpdatedAt: at}, Effect: effect, NewAttemptID: newAttempt, TerminationConfirmed: confirmed}
						if status == domain.OperationFailed || status == domain.OperationManualRequired {
							r.Operation.Failure = &problem
						}
						wire := serializedControl(t, r)
						if valid := schema.Validate(wire) == nil; valid != (r.Validate() == nil) {
							t.Fatalf("schema/record disagreement: %s/%s/%s new=%q confirmed=%v: schema=%v record=%v", kind, status, effect, newAttempt, confirmed, schema.Validate(wire), r.Validate())
						}
					}
				}
			}
		}
	}
	r := operations.Record{Operation: domain.Operation{ID: "operation", WorkspaceID: "workspace", JobID: "job", AttemptID: "source",
		Kind: domain.OperationCancel, Status: domain.OperationSucceeded, Revision: 1, CreatedAt: at, UpdatedAt: at}, Effect: operations.DispatchPrevented, Replay: true}
	wire := serializedControl(t, r)
	if err := schema.Validate(wire); err != nil {
		t.Fatal("replayed receipt rejected", err)
	}
	wire["reason"] = "must-not-be-public"
	if schema.Validate(wire) == nil {
		t.Fatal("private reason admitted into the public response")
	}
}

func serializedControl(t *testing.T, r operations.Record) map[string]any {
	t.Helper()
	data, err := json.Marshal(controlView(r))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "canary") {
		t.Fatal("internal details or cause leaked into the wire contract")
	}
	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestControlRequestSchemasMatchStrictParser(t *testing.T) {
	for _, kind := range []domain.OperationKind{domain.OperationCancel, domain.OperationRetryCompute, domain.OperationReconcile, domain.OperationCollect} {
		name := "control-request.v1alpha1.schema.json"
		if kind == domain.OperationRetryCompute {
			name += "#/$defs/retry"
		}
		schema := controlSchema(t, name)
		for _, raw := range []string{`{"attempt_id":"source"}`, `{"attempt_id":"source","reason":"explicit retry"}`,
			`{}`, `null`, `{"attempt_id":null}`, `{"attempt_id":"source","reason":null}`,
			`{"attempt_id":"source","reason":" "}`, `{"attempt_id":"source","reason":"\u00a0"}`,
			`{"attempt_id":"source","reason":"\n"}`, `{"attempt_id":"source","reason":"\u0085"}`,
			`{"attempt_id":"source","force":true}`, `{"attempt_id":"../source"}`,
			`{"attempt_id":"source","reason":"` + strings.Repeat("x", 512) + `"}`,
			`{"attempt_id":"source","reason":"` + strings.Repeat("x", 513) + `"}`} {
			var value any
			if err := json.Unmarshal([]byte(raw), &value); err != nil {
				t.Fatal(err)
			}
			_, parseErr := operations.Parse(kind, []byte(raw))
			if (schema.Validate(value) == nil) != (parseErr == nil) {
				t.Fatalf("schema/parser disagreement: %s request=%s schema=%v parse=%v", kind, raw, schema.Validate(value), parseErr)
			}
		}
	}
}
