package api_test

import (
	"encoding/json"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func transportControlSchema(t *testing.T, file, fragment string) *jsonschema.Schema {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("source path unavailable")
	}
	path := filepath.ToSlash(filepath.Join(filepath.Dir(source), "..", "..", "api", filepath.FromSlash(file)))
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	schema, err := c.Compile((&url.URL{Scheme: "file", Path: path, Fragment: fragment}).String())
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestControlHTTPResponsesSatisfyPublishedContracts(t *testing.T) {
	f, _ := setupControlHTTP(t, nil)
	job := admitControlJob(t, f)
	operation := transportControlSchema(t, "schemas/control-operation.v1alpha1.schema.json", "")
	errorSchema := transportControlSchema(t, "schemas/error.v1alpha1.schema.json", "")
	info := transportControlSchema(t, "openapi.json", "/paths/~1v1~1info/get/responses/200/content/application~1json/schema")
	validate := func(schema *jsonschema.Schema, raw []byte) {
		t.Helper()
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(value); err != nil {
			t.Fatal("actual loopback HTTP response violates the published contract", err)
		}
	}
	body := `{"attempt_id":"` + string(job.AttemptID) + `","reason":"explicit contract fixture"}`
	for _, action := range []string{"cancel", "reconcile", "retry"} {
		headers, raw := f.call(t, "POST", job.Links.Self+"/"+action, "a", "contract-"+action, body, 202)
		validate(operation, raw)
		if headers.Get("Location") != decodeControl(t, raw).Links.Self {
			t.Fatal("receipt Location does not match its public self link")
		}
		_, raw = f.call(t, "GET", headers.Get("Location"), "a", "", "", 200)
		validate(operation, raw)
		_, raw = f.call(t, "POST", job.Links.Self+"/"+action, "a", "contract-"+action, body, 202)
		validate(operation, raw)
	}
	_, raw := f.call(t, "POST", job.Links.Self+"/collect", "a", "contract-collect", body, 409)
	validate(errorSchema, raw)
	_, raw = f.call(t, "GET", "/v1/info", "a", "", "", 200)
	validate(info, raw)
}
