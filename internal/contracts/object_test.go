package contracts

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestObjectAndHTTPErrorWireShapesMatchSchemas(t *testing.T) {
	root := repositoryRoot(t)
	cases := []struct {
		schema string
		value  any
	}{
		{"object", domain.ObjectMetadata{ID: "obj_wire", WorkspaceID: "workspace", Bytes: 0, SHA256: domain.SHA256Digest(strings.Repeat("a", 64))}},
		{"error", api.ErrorEnvelope{Error: api.ErrorBody{Code: domain.CodeInvalidRequest, Message: "invalid declaration", Stage: domain.FailureStageValidation, RequestID: "req_example", RecommendedAction: domain.RecommendedActionFixRequest}}},
		{"error", api.ErrorEnvelope{Error: api.ErrorBody{Code: domain.CodeRequestLimitExceeded, Message: "local request limit", Stage: domain.FailureStageLocalRuntime, RequestID: "req_example", RecommendedAction: domain.RecommendedActionWait}}},
	}
	for _, tc := range cases {
		compiler := jsonschema.NewCompiler()
		compiler.DefaultDraft(jsonschema.Draft2020)
		compiler.AssertFormat()
		schema, err := compiler.Compile(filepath.Join(root, "api", "schemas", tc.schema+".v1alpha1.schema.json"))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(tc.value)
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err = json.Unmarshal(encoded, &value); err != nil {
			t.Fatal(err)
		}
		if err = schema.Validate(value); err != nil {
			t.Fatal("actual Go wire shape rejected", err)
		}
	}
}

func TestImplementationStatusDoesNotUpgradeJobRoutes(t *testing.T) {
	for _, id := range []string{"getHealth", "getReadiness", "getRuntimeInfo", "uploadObject", "getObject"} {
		if operationStatus(id) != "implemented-offline" {
			t.Fatal(id)
		}
	}
	for _, id := range []string{"createJob", "retryJob", "cancelJob", "getJob", "unknown"} {
		if operationStatus(id) != "planned" {
			t.Fatal("unimplemented operation upgraded", id)
		}
	}
}
