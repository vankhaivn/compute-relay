package admission

import (
	"errors"
	"strings"
	"testing"
)

func TestOptionalObjectExpectationsPreserveOmittedCanonicalRequest(t *testing.T) {
	old, err := Parse([]byte(validJob))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(old.Canonical()), `"sha256"`) || strings.Contains(string(old.Canonical()), `"bytes"`) || old.Spec().Bundle.Bytes != nil {
		t.Fatal("omitted expectations changed the historical request")
	}
	with := strings.Replace(validJob, `"bundle":{"object_id":"code"}`, `"bundle":{"object_id":"code","sha256":"`+strings.Repeat("a", 64)+`","bytes":7}`, 1)
	with = strings.Replace(with, `"inputs":[]`, `"inputs":[{"name":"data","source":{"kind":"object","object_id":"data","sha256":"`+strings.Repeat("b", 64)+`","bytes":0},"target":"data.bin"}]`, 1)
	parsed, err := Parse([]byte(with))
	if err != nil || parsed.Hash() == old.Hash() || parsed.Spec().Bundle.Bytes == nil || *parsed.Spec().Bundle.Bytes != 7 || parsed.Spec().Inputs[0].Source.Bytes == nil || *parsed.Spec().Inputs[0].Source.Bytes != 0 {
		t.Fatal("paired expectations were not frozen", err)
	}
	for _, change := range []string{
		`"bundle":{"object_id":"code","sha256":"` + strings.Repeat("a", 64) + `"}`,
		`"bundle":{"object_id":"code","bytes":7}`,
		`"bundle":{"object_id":"code","sha256":"` + strings.Repeat("a", 64) + `","bytes":-1}`,
		`"bundle":{"object_id":"code","sha256":"` + strings.Repeat("A", 64) + `","bytes":7}`,
	} {
		raw := strings.Replace(validJob, `"bundle":{"object_id":"code"}`, change, 1)
		if _, err := Parse([]byte(raw)); !errors.Is(err, ErrSpec) {
			t.Fatalf("accepted incomplete or invalid bundle expectation: %v", err)
		}
	}
	for _, source := range []string{
		`{"kind":"object","object_id":"data","sha256":"` + strings.Repeat("b", 64) + `"}`,
		`{"kind":"object","object_id":"data","bytes":0}`,
		`{"kind":"object","object_id":"data","sha256":"` + strings.Repeat("b", 64) + `","bytes":-1}`,
	} {
		raw := strings.Replace(validJob, `"inputs":[]`, `"inputs":[{"name":"data","source":`+source+`,"target":"data.bin"}]`, 1)
		if _, err := Parse([]byte(raw)); !errors.Is(err, ErrSpec) {
			t.Fatalf("accepted incomplete or invalid input expectation: %v", err)
		}
	}
}
