package jsonwire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestObjectRejectsAmbiguousOrUnboundedJSON(t *testing.T) {
	bad := []string{"", "null", "[]", `{"x":1,"x":2}`, `{"x":{"a":1,"\u0061":2}}`, `{"x":"\ud800"}`, "{\"x\":\"\xff\"}", `{} {}`, `{"x":NaN}`, `{"x":` + strings.Repeat("[", 34) + `0` + strings.Repeat("]", 34) + `}`}
	for _, raw := range bad {
		if _, err := Object([]byte(raw), 4096); err == nil {
			t.Fatalf("accepted invalid object %q", raw)
		}
	}
	if _, err := Object([]byte(`{"x":1}`), 2); err == nil {
		t.Fatal("ignored byte limit")
	}
}
func TestObjectPreservesFutureFieldsAndExactNumbers(t *testing.T) {
	m, err := Object([]byte(`{"known":9007199254740993,"future":{"items":[true,null,"text"]}}`), 4096)
	if err != nil || string(m["known"]) != "9007199254740993" {
		t.Fatal(m, err)
	}
	if Fields(m, []string{"known"}, []string{"future"}) != nil {
		t.Fatal("valid field set rejected")
	}
	for _, m := range []map[string]json.RawMessage{{}, {"Known": json.RawMessage(`1`)}, {"known": json.RawMessage(`null`)}, {"known": json.RawMessage(`1`), "extra": json.RawMessage(`2`)}} {
		if Fields(m, []string{"known"}, nil) == nil {
			t.Fatal("invalid field set accepted")
		}
	}
}
