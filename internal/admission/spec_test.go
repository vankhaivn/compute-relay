package admission

import (
	"encoding/json"
	"errors"
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

func TestCollectionPolicyDoesNotNormalizeHistoricalRequests(t *testing.T) {
	old, err := Parse([]byte(validJob))
	if err != nil {
		t.Fatal(err)
	}
	if old.Spec().CollectionMode() != "automatic" || strings.Contains(string(old.Canonical()), "result_collection") {
		t.Fatal("historical canonical bytes gained a default")
	}
	for _, mode := range []string{"automatic", "manual"} {
		r, err := Parse([]byte(strings.Replace(validJob, `"name":"unit"`, `"name":"unit","result_collection":"`+mode+`"`, 1)))
		if err != nil || r.Spec().CollectionMode() != mode || r.Hash() == old.Hash() {
			t.Fatal(mode, err)
		}
	}
	for _, value := range []string{`null`, `true`, `"on-demand"`, `{}`} {
		if _, err := Parse([]byte(strings.Replace(validJob, `"name":"unit"`, `"name":"unit","result_collection":`+value, 1))); err == nil {
			t.Fatal("accepted invalid policy", value)
		}
	}
}

func TestRetentionPolicyFrozenInCanonicalRequest(t *testing.T) {
	withRetention := func(value string) []byte {
		return []byte(strings.Replace(validJob, `"name":"unit"`, `"name":"unit","retention":`+value, 1))
	}
	old, err := Parse([]byte(validJob))
	if err != nil || old.Spec().Retention != nil || strings.Contains(string(old.Canonical()), `"retention"`) {
		t.Fatal("omitted retention changed historical request", err)
	}
	for _, tc := range []struct {
		name        string
		value       string
		after       *int64
		uncollected *int64
	}{
		{"delete after collection", `{"after_collection_seconds":0}`, ptrInt64(0), nil},
		{"keep after collection", `{"after_collection_seconds":3600}`, ptrInt64(3600), nil},
		{"expire uncollected", `{"uncollected_max_seconds":60}`, nil, ptrInt64(60)},
		{"both", `{"after_collection_seconds":0,"uncollected_max_seconds":60}`, ptrInt64(0), ptrInt64(60)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := Parse(withRetention(tc.value))
			if err != nil || req.Spec().Retention == nil || req.Hash() == old.Hash() {
				t.Fatal("valid retention not frozen", err)
			}
			s := req.Spec()
			if !sameOptionalInt64(s.Retention.AfterCollectionSeconds, tc.after) || !sameOptionalInt64(s.Retention.UncollectedMaxSeconds, tc.uncollected) {
				t.Fatalf("decoded retention differs: %+v", s.Retention)
			}
			reparsed, err := Parse(req.Canonical())
			if err != nil || reparsed.Hash() != req.Hash() {
				t.Fatal("canonical retention did not round-trip", err)
			}
			if s.Retention.AfterCollectionSeconds != nil {
				*s.Retention.AfterCollectionSeconds = 99
			}
			if s.Retention.UncollectedMaxSeconds != nil {
				*s.Retention.UncollectedMaxSeconds = 99
			}
			if req.Hash() != reparsed.Hash() || string(req.Canonical()) != string(reparsed.Canonical()) {
				t.Fatal("caller mutation changed frozen retention")
			}
		})
	}
	zero, _ := Parse(withRetention(`{"after_collection_seconds":0}`))
	hour, _ := Parse(withRetention(`{"after_collection_seconds":3600}`))
	if zero.Hash() == hour.Hash() {
		t.Fatal("distinct retention policies share idempotency identity")
	}
	for _, value := range []string{
		`null`, `{}`, `[]`, `{"after_collection_seconds":null}`,
		`{"uncollected_max_seconds":null}`, `{"after_collection_seconds":-1}`,
		`{"after_collection_seconds":2592001}`, `{"uncollected_max_seconds":0}`,
		`{"uncollected_max_seconds":2592001}`, `{"after_collection_seconds":1.5}`,
		`{"after_collection_seconds":"0"}`, `{"after_collection_seconds":0,"unknown":1}`,
	} {
		if _, err := Parse(withRetention(value)); !errors.Is(err, ErrSpec) {
			t.Fatalf("accepted invalid retention %s: %v", value, err)
		}
	}
}

func ptrInt64(v int64) *int64 { return &v }

func sameOptionalInt64(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
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

func TestHTTPSInputRejectedForNewAdmissionAndReadableHistorically(t *testing.T) {
	input, _ := json.Marshal([]Input{{Name: "x", Target: "x", Source: Source{Kind: "https", URL: "https://example.com/input"}}})
	raw := strings.Replace(validJob, `"inputs":[]`, `"inputs":`+string(input), 1)
	req, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal("historical request unreadable", err)
	}
	if !errors.Is(req.Spec().CheckInputs(), ErrUnsupportedInput) {
		t.Fatal("HTTPS input accepted for admission")
	}
	object, err := Parse([]byte(validJob))
	if err != nil || object.Spec().CheckInputs() != nil {
		t.Fatal("object inputs rejected", err)
	}
}
