package admission

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

const validJob = `{"api_version":"compute-connector/v1alpha1","name":"unit","profile":"default-gpu","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","main.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":2,"finalization_grace_seconds":2}}`

func TestStrictSpecAndCanonicalIdentity(t *testing.T) {
	req, err := Parse([]byte(validJob))
	if err != nil {
		t.Fatal(err)
	}
	reordered := map[string]any{}
	if err = json.Unmarshal([]byte(validJob), &reordered); err != nil {
		t.Fatal(err)
	}
	b, _ := json.MarshalIndent(reordered, "", "  ")
	equivalent, err := Parse(b)
	if err != nil || req.Hash() != equivalent.Hash() {
		t.Fatal("format changed identity", err)
	}
	copy := req.Canonical()
	copy[0] = 'x'
	spec := req.Spec()
	spec.Execution.Command[0] = "mutated"
	if req.Canonical()[0] != '{' || req.Spec().Execution.Command[0] != "python" {
		t.Fatal("request aliases mutable memory")
	}
	if strings.Contains(fmt.Sprintf("%v %#v", req, req), "main.py") {
		t.Fatal("request leaked by formatting")
	}
	defaults := strings.Replace(validJob, `"name":"unit"`, `"name":"unit","labels":{}`, 1)
	other, err := Parse([]byte(defaults))
	if err != nil || other.Hash() == req.Hash() {
		t.Fatal("field presence lost")
	}
}
func TestSpecRejectsInvalidAndContradictoryData(t *testing.T) {
	changes := [][2]string{
		{`"name":"unit"`, `"name":"unit","Name":"alias"`},
		{`"name":"unit"`, `"name":"unit","name":"duplicate"`},
		{`"name":"unit"`, `"name":null`},
		{`"name":"unit"`, `"name":" "`},
		{`"command":["python","main.py"]`, `"command":[]`},
		{`"command":["python","main.py"]`, `"command":["python","main.py"],"environment":{"CC_JOB_ID":"override"}`},
		{`"command":["python","main.py"]`, `"command":["python","main.py"],"environment":{"PATH":"override"}`},
		{`"command":["python","main.py"]`, `"command":["python","bad\u0000argument"]`},
		{`"accelerator":"cpu"`, `"accelerator":"cpu","minimum_gpu_count":1`},
		{`"accelerator":"cpu"`, `"accelerator":"gpu"`},
		{`"setup_seconds":2`, `"setup_seconds":9`},
		{`"remote_wall_seconds":10`, `"remote_wall_seconds":1.5`},
		{`"path":"answer.json"`, `"path":"../escape"`},
		{`"path":"answer.json"`, `"path":"C:\\escape"`},
		{`"object_id":"code"`, `"object_id":"../other"`},
	}
	for _, c := range changes {
		raw := strings.Replace(validJob, c[0], c[1], 1)
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", c[1])
		}
	}
	for _, url := range []string{"http://example.com/data", "https://user:secret@example.com/data", "https://127.0.0.1/data", "https://example.com/data?token=secret"} {
		input, _ := json.Marshal([]Input{{Name: "x", Target: "x", Source: Source{Kind: "https", URL: url}}})
		raw := strings.Replace(validJob, `"inputs":[]`, `"inputs":`+string(input), 1)
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatal("unsafe durable URL", url)
		}
	}
	for _, inputs := range [][]Input{
		{{Name: "x", Target: "a", Source: Source{Kind: "object", ObjectID: "data"}}, {Name: "X", Target: "b", Source: Source{Kind: "object", ObjectID: "data"}}},
		{{Name: "x", Target: "a", Source: Source{Kind: "object", ObjectID: "data"}}, {Name: "y", Target: "a/b", Source: Source{Kind: "object", ObjectID: "data"}}},
	} {
		b, _ := json.Marshal(inputs)
		raw := strings.Replace(validJob, `"inputs":[]`, `"inputs":`+string(b), 1)
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatal("colliding input accepted")
		}
	}
}
