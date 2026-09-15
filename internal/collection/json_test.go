package collection

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStrictManifestJSON(t *testing.T) {
	for _, raw := range []string{`{}`, `{"nested":[1,{"unicode":"\ud83d\ude00"}]}`, `{"literal":"\\ud800"}`} {
		if _, err := strictValue([]byte(raw)); err != nil {
			t.Fatal("valid JSON rejected", raw, err)
		}
	}
	invalid := []string{`null`, `[]`, `{"a":1,"a":2}`, `{"a":1,"\u0061":2}`, `{"a":{"x":1,"x":2}}`, `{"x":"\ud800"}`, `{"x":"\udc00"}`, `{"x":"\ud800\ud800"}`, `{"x":"\ud800x"}`, `{} {}`, `{"x":`, string([]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}), `{"x":` + strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34) + "}", `{"x":"` + strings.Repeat("a", MaxManifestBytes) + `"}`}
	for i, raw := range invalid {
		if _, err := strictValue([]byte(raw)); err == nil {
			t.Fatalf("invalid JSON %d accepted", i)
		}
	}
}
func FuzzStrictManifestJSON(f *testing.F) {
	for _, raw := range []string{`{}`, `{"a":[1,2]}`, `{"x":"\ud800"}`, `{"a":1,"\u0061":2}`} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		value, err := strictValue(raw)
		if err == nil {
			if !json.Valid(raw) || value == nil {
				t.Fatal("accepted invalid manifest JSON")
			}
		}
	})
}
