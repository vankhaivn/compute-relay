// Package contracts validates the repository's versioned JSON Schema and OpenAPI files.
package contracts

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	manifestPath = "api/contract-manifest.json"
	lockPath     = "api/contract.lock.json"
)

type manifest struct {
	ManifestVersion int            `json:"manifest_version"`
	OpenAPI         string         `json:"openapi"`
	SharedSchemas   []string       `json:"shared_schemas"`
	Roots           []contractRoot `json:"roots"`
}

type contractRoot struct {
	Schema  string   `json:"schema"`
	Valid   []string `json:"valid"`
	Invalid []string `json:"invalid"`
}

// Check validates schemas, examples, local-only references, OpenAPI, domain enums, and lock freshness.
func Check(root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}

	var manifest manifest
	if err := readStrictJSON(filepath.Join(root, filepath.FromSlash(manifestPath)), &manifest); err != nil {
		return fmt.Errorf("read contract manifest: %w", err)
	}
	if err := manifest.validate(root); err != nil {
		return err
	}

	apiRoot := filepath.Join(root, "api")
	if err := validateLocalRefs(apiRoot); err != nil {
		return err
	}
	if err := validateSchemasAndExamples(root, manifest); err != nil {
		return err
	}
	if err := validateDomainEnums(root); err != nil {
		return err
	}
	if err := validateOpenAPI(root, manifest.OpenAPI); err != nil {
		return err
	}
	if err := ValidateLock(root); err != nil {
		return err
	}
	return nil
}

func (manifest manifest) validate(root string) error {
	if manifest.ManifestVersion != 1 {
		return fmt.Errorf("unsupported contract manifest version %d", manifest.ManifestVersion)
	}
	if _, err := resolveContractPath(root, "api", manifest.OpenAPI); err != nil {
		return fmt.Errorf("openapi path: %w", err)
	}
	if len(manifest.SharedSchemas) == 0 {
		return errors.New("contract manifest must list shared schemas explicitly")
	}
	if len(manifest.Roots) == 0 {
		return errors.New("contract manifest must contain root schemas")
	}

	accounted := make(map[string]struct{}, len(manifest.SharedSchemas)+len(manifest.Roots))
	for _, schema := range manifest.SharedSchemas {
		if err := registerManifestPath(root, accounted, schema, "shared schema"); err != nil {
			return err
		}
	}
	for index, contract := range manifest.Roots {
		if err := registerManifestPath(root, accounted, contract.Schema, fmt.Sprintf("root schema %d", index)); err != nil {
			return err
		}
		if len(contract.Valid) == 0 || len(contract.Invalid) == 0 {
			return fmt.Errorf("schema %q must have both valid and invalid fixtures", contract.Schema)
		}
		seenExamples := map[string]struct{}{}
		for _, example := range append(append([]string{}, contract.Valid...), contract.Invalid...) {
			if _, err := resolveContractPath(root, "api", example); err != nil {
				return fmt.Errorf("schema %q example %q: %w", contract.Schema, example, err)
			}
			if _, exists := seenExamples[example]; exists {
				return fmt.Errorf("schema %q lists duplicate example %q", contract.Schema, example)
			}
			seenExamples[example] = struct{}{}
		}
	}

	var discovered []string
	err := filepath.WalkDir(filepath.Join(root, "api", "schemas"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".schema.json") {
			return nil
		}
		relative, err := filepath.Rel(filepath.Join(root, "api"), path)
		if err != nil {
			return err
		}
		discovered = append(discovered, filepath.ToSlash(relative))
		return nil
	})
	if err != nil {
		return fmt.Errorf("discover schemas: %w", err)
	}
	sort.Strings(discovered)
	for _, schema := range discovered {
		if _, exists := accounted[schema]; !exists {
			return fmt.Errorf("schema %q is not declared in contract-manifest.json", schema)
		}
	}
	if len(discovered) != len(accounted) {
		return fmt.Errorf("manifest accounts for %d schemas, repository contains %d", len(accounted), len(discovered))
	}
	return nil
}

func registerManifestPath(root string, accounted map[string]struct{}, path, label string) error {
	if !strings.HasPrefix(path, "schemas/") || !strings.HasSuffix(path, ".schema.json") {
		return fmt.Errorf("%s path %q must be beneath schemas/ and end in .schema.json", label, path)
	}
	if _, err := resolveContractPath(root, "api", path); err != nil {
		return fmt.Errorf("%s %q: %w", label, path, err)
	}
	if _, exists := accounted[path]; exists {
		return fmt.Errorf("schema %q is listed more than once", path)
	}
	accounted[path] = struct{}{}
	return nil
}

func validateSchemasAndExamples(root string, manifest manifest) error {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()

	for _, contract := range manifest.Roots {
		schemaPath, _ := resolveContractPath(root, "api", contract.Schema)
		schema, err := compiler.Compile(schemaPath)
		if err != nil {
			return fmt.Errorf("compile schema %q: %w", contract.Schema, err)
		}
		for _, example := range contract.Valid {
			value, err := readJSONValue(filepath.Join(root, "api", filepath.FromSlash(example)))
			if err != nil {
				return fmt.Errorf("read valid example %q: %w", example, err)
			}
			if err := schema.Validate(value); err != nil {
				return fmt.Errorf("valid example %q rejected by %q: %w", example, contract.Schema, err)
			}
		}
		for _, example := range contract.Invalid {
			value, err := readJSONValue(filepath.Join(root, "api", filepath.FromSlash(example)))
			if err != nil {
				return fmt.Errorf("read invalid example %q: %w", example, err)
			}
			if err := schema.Validate(value); err == nil {
				return fmt.Errorf("invalid example %q was accepted by %q", example, contract.Schema)
			}
		}
	}
	return nil
}

func validateOpenAPI(root, relative string) error {
	path, err := resolveContractPath(root, "api", relative)
	if err != nil {
		return fmt.Errorf("openapi path: %w", err)
	}

	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true
	document, err := loader.LoadFromFile(path)
	if err != nil {
		return fmt.Errorf("load OpenAPI document: %w", err)
	}
	if document.OpenAPI != "3.1.1" {
		return fmt.Errorf("OpenAPI version = %q, want 3.1.1", document.OpenAPI)
	}
	if status, ok := document.Extensions["x-implementation-status"]; !ok || status != "planned" {
		return errors.New("OpenAPI root must explicitly declare x-implementation-status=planned")
	}
	if err := document.Validate(context.Background()); err != nil {
		return fmt.Errorf("validate OpenAPI document: %w", err)
	}

	expectedOperations := map[string]struct{}{
		"getHealth": {}, "getReadiness": {}, "getRuntimeInfo": {},
		"validateJob": {}, "createJob": {}, "getJob": {},
		"cancelJob": {}, "retryJob": {}, "reconcileJob": {},
		"collectJob": {}, "getOperation": {},
		"uploadObject": {}, "getObject": {}, "importObject": {}, "ingestObject": {},
	}
	observedOperations := make(map[string]struct{}, len(expectedOperations))
	for path, pathItem := range document.Paths.Map() {
		for method, operation := range pathItem.Operations() {
			if operation == nil || strings.TrimSpace(operation.OperationID) == "" {
				return fmt.Errorf("OpenAPI operation %s %s has no operationId", strings.ToUpper(method), path)
			}
			if status, ok := operation.Extensions["x-implementation-status"]; !ok || status != operationStatus(operation.OperationID) {
				return fmt.Errorf("OpenAPI operation %q must declare x-implementation-status=%s", operation.OperationID, operationStatus(operation.OperationID))
			}
			if _, exists := observedOperations[operation.OperationID]; exists {
				return fmt.Errorf("duplicate OpenAPI operationId %q", operation.OperationID)
			}
			observedOperations[operation.OperationID] = struct{}{}
		}
	}
	if !mapsEqual(expectedOperations, observedOperations) {
		return fmt.Errorf("OpenAPI operation inventory = %v, want %v", sortedKeys(observedOperations), sortedKeys(expectedOperations))
	}
	return nil
}

// Handler-level evidence does not imply production CLI composition or provider dispatch.
// Control operations remain planned until their separate acceptance gates pass.
func operationStatus(id string) string {
	switch id {
	case "getHealth", "getReadiness", "getRuntimeInfo", "uploadObject", "getObject", "importObject", "ingestObject", "createJob", "validateJob", "getJob":
		return "implemented-offline"
	default:
		return "planned"
	}
}

func mapsEqual(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, ok := right[key]; !ok {
			return false
		}
	}
	return true
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
