package connections

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

type request struct {
	ProviderType  string            `json:"provider_type,omitempty"`
	Label         string            `json:"label,omitempty"`
	Credentials   map[string]string `json:"credentials,omitempty"`
	Action        string            `json:"action,omitempty"`
	Revision      int64             `json:"expected_revision,omitempty"`
	Configuration *Configuration    `json:"configuration,omitempty"`
}

func (request) String() string   { return "[connection request redacted]" }
func (request) GoString() string { return "[connection request redacted]" }

var schemasOnce sync.Once
var requestSchemas map[string]*jsonschema.Schema
var schemaErr error
var providerTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

type noNetworkLoader struct{}

func (noNetworkLoader) Load(string) (any, error) { return nil, ErrUnavailable }
func compileRequests() {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(noNetworkLoader{})
	for _, name := range []string{"common.v1alpha1.schema.json", "connection-request.v1alpha1.schema.json"} {
		raw, err := schemas.Files.ReadFile(name)
		if err != nil {
			schemaErr = ErrUnavailable
			return
		}
		var value any
		if json.Unmarshal(raw, &value) != nil || compiler.AddResource("https://contracts.compute-relay.invalid/"+name, value) != nil {
			schemaErr = ErrUnavailable
			return
		}
	}
	requestSchemas = map[string]*jsonschema.Schema{}
	for _, kind := range []string{"create", "action"} {
		schema, err := compiler.Compile("https://contracts.compute-relay.invalid/connection-request.v1alpha1.schema.json#/$defs/" + kind)
		if err != nil {
			schemaErr = ErrUnavailable
			return
		}
		requestSchemas[kind] = schema
	}
}
func parse(raw []byte, kind string) (request, []byte, error) {
	if _, err := jsonwire.Object(raw, MaxRequestBytes); err != nil {
		return request{}, nil, ErrRequest
	}
	schemasOnce.Do(compileRequests)
	if schemaErr != nil {
		return request{}, nil, schemaErr
	}
	var value any
	if json.Unmarshal(raw, &value) != nil || requestSchemas[kind].Validate(value) != nil {
		return request{}, nil, ErrRequest
	}
	var req request
	if json.Unmarshal(raw, &req) != nil || len(req.Label) > 128 || kind == "create" && strings.TrimSpace(req.Label) == "" {
		return request{}, nil, ErrRequest
	}
	for key, secret := range req.Credentials {
		if len(key) > 64 || len(secret) > 16384 {
			return request{}, nil, ErrRequest
		}
	}
	if kind == "create" {
		req.Action = "create"
	}
	// Canonical identity includes semantic action; Go JSON map ordering is deterministic.
	canonical, err := json.Marshal(req)
	if err != nil || len(canonical) > MaxRequestBytes {
		return request{}, nil, ErrRequest
	}
	return req, canonical, nil
}
