package contracts

import (
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Root schemas also cover projections/receipts. Route-specific references must not
// accidentally accept a different operation simply because it is valid at the root.
func TestManagedRouteContractsRejectOtherOperationBodies(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	root := filepath.Join(repositoryRoot(t), "api")
	cases := []struct{ schema, fragment, valid, invalid string }{
		{"connection-request", "create", "connection-request.create.valid.json", "connection-request.check.valid.json"},
		{"connection-request", "action", "connection-request.replace_credential.valid.json", "connection-request.create.valid.json"},
		{"connection", "connection", "connection.ready.valid.json", "connection.page.valid.json"},
		{"connection", "page", "connection.page.valid.json", "connection.ready.valid.json"},
		{"execution-authorization", "request", "execution-authorization.request.valid.json", "execution-authorization.granted.valid.json"},
		{"execution-authorization", "receipt", "execution-authorization.consumed.valid.json", "execution-authorization.request.valid.json"},
	}
	for _, tc := range cases {
		t.Run(tc.schema+"/"+tc.fragment, func(t *testing.T) {
			schema, err := compiler.Compile(filepath.Join(root, "schemas", tc.schema+".v1alpha1.schema.json") + "#/$defs/" + tc.fragment)
			if err != nil {
				t.Fatal(err)
			}
			for _, fixture := range []struct {
				path  string
				valid bool
			}{{tc.valid, true}, {tc.invalid, false}} {
				value, err := readJSONValue(filepath.Join(root, "examples", fixture.path))
				if err != nil {
					t.Fatal(err)
				}
				if err = schema.Validate(value); (err == nil) != fixture.valid {
					t.Fatalf("%s valid=%v, want %v", fixture.path, err == nil, fixture.valid)
				}
			}
		})
	}
}
