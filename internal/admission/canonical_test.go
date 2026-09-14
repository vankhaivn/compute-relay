package admission

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestCanonicalJSON(t *testing.T) {
	for _, raw := range []string{`{"b":1.0,"a":"\u0061"}`, ` { "a": "a", "b": 1e0 } `, `{"a":"a","b":1}`} {
		b, _, err := canonicalJSON([]byte(raw))
		if err != nil || string(b) != `{"a":"a","b":1}` {
			t.Fatalf("%q %v", b, err)
		}
	}
	for _, raw := range []string{`{"x":1,"x":2}`, `{"x":1,"\u0078":2}`, `{"o":{"x":1,"x":2}}`, `{"x":"\ud800"}`, `{"x":"\udc00"}`, `{"x":"\ud800\u0041"}`, `{"x":1} {}`, `{"x":1.5}`, `{"x":9007199254740993}`, `{"x":1e999999999999}`, `{"x":null`, "\xff", `[]`, `null`, `{` + strings.Repeat(`"x":{`, 18) + `"x":0` + strings.Repeat(`}`, 18) + `}`} {
		if b, _, err := canonicalJSON([]byte(raw)); err == nil {
			t.Fatalf("invalid accepted %q => %q", raw, b)
		}
	}
	a, _, err := canonicalJSON([]byte(`{"x":"\ud83d\ude00","q":"\\uD800"}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := canonicalJSON([]byte(`{"q":"\\uD800","x":"😀"}`))
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("valid unicode mismatch %s %s %v", a, b, err)
	}
	if _, _, err := canonicalJSON(bytes.Repeat([]byte(" "), MaxRequestBytes+1)); err == nil {
		t.Fatal("oversized input")
	}
}
func TestIdempotencyKeyBounds(t *testing.T) {
	for _, key := range []string{"short", " leading-space", "abcdefgh\n", strings.Repeat("x", 257), "abcdeféé"} {
		if _, err := KeyDigest(key); err == nil {
			t.Fatalf("accepted %q", key)
		}
	}
	for _, key := range []string{"key-0001", strings.Repeat("a", 256)} {
		h, err := KeyDigest(key)
		if err != nil || len(h) != 64 {
			t.Fatal(h, err)
		}
	}
}
func FuzzCanonicalJSON(f *testing.F) {
	for _, s := range []string{`{}`, `{"x":1}`, `{"x":"\ud800"}`, `{"x":1e100000}`, `{"a":[{}]}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		a, _, err := canonicalJSON(b)
		if err != nil {
			return
		}
		c, _, err := canonicalJSON(a)
		if err != nil || !bytes.Equal(a, c) {
			t.Fatal("canonicalization not idempotent", fmt.Sprint(err))
		}
	})
}

func TestCanonicalExpansionIsBounded(t *testing.T) {
	raw := []byte(`{"value":"` + strings.Repeat("<", MaxRequestBytes/2) + `"}`)
	if _, _, err := canonicalJSON(raw); err == nil {
		t.Fatal("escaped canonical request exceeds storage bound")
	}
}
