package localinput

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/vankhaivn/compute-relay/internal/packaging"
)

// UnmarshalJSON preserves the strict request contract even without a runtime schema
// dependency. Duplicate keys, nulls and unknown keys cannot choose different roots in
// different parsers. The HTTP boundary still owns the input-size budget.
func (r *Request) UnmarshalJSON(b []byte) error {
	d := json.NewDecoder(bytes.NewReader(b))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return packaging.ErrInvalid
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return packaging.ErrInvalid
		}
		seen[key] = true
		switch key {
		case "root", "kind", "path", "includes", "sha256":
		default:
			return packaging.ErrInvalid
		}
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return packaging.ErrInvalid
		}
	}
	if token, err := d.Token(); err != nil || token != json.Delim('}') {
		return packaging.ErrInvalid
	}
	if _, err := d.Token(); err != io.EOF {
		return packaging.ErrInvalid
	}
	type plain Request
	var result plain
	if err := json.Unmarshal(b, &result); err != nil {
		return packaging.ErrInvalid
	}
	*r = Request(result)
	return nil
}
