// Package jsonwire checks bounded JSON before typed decoding can discard ambiguity.
// It is not a canonicalization scheme or a replacement for semantic validation.
package jsonwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

var ErrInvalid = errors.New("invalid bounded JSON object")

// Object rejects duplicate decoded keys, invalid Unicode, trailing documents and
// excessive nesting. Unknown fields remain available for the caller's own policy.
func Object(raw []byte, limit int) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > limit || !utf8.Valid(raw) {
		return nil, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	tokens := 0
	var value func(int) error
	value = func(depth int) error {
		tokens++
		if depth > 32 || tokens > 100000 {
			return ErrInvalid
		}
		t, err := d.Token()
		if err != nil {
			return ErrInvalid
		}
		if s, ok := t.(string); ok && strings.ContainsRune(s, utf8.RuneError) {
			return ErrInvalid
		}
		switch t {
		case json.Delim('{'):
			keys := map[string]bool{}
			for d.More() {
				k, err := d.Token()
				s, ok := k.(string)
				if err != nil || !ok || keys[s] || strings.ContainsRune(s, utf8.RuneError) {
					return ErrInvalid
				}
				keys[s] = true
				if value(depth+1) != nil {
					return ErrInvalid
				}
			}
			t, err = d.Token()
			if err != nil || t != json.Delim('}') {
				return ErrInvalid
			}
		case json.Delim('['):
			for d.More() {
				if value(depth+1) != nil {
					return ErrInvalid
				}
			}
			t, err = d.Token()
			if err != nil || t != json.Delim(']') {
				return ErrInvalid
			}
		case json.Delim('}'), json.Delim(']'):
			return ErrInvalid
		}
		return nil
	}
	if value(0) != nil || d.Decode(new(any)) != io.EOF {
		return nil, ErrInvalid
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, ErrInvalid
	}
	return object, nil
}

// Fields enforces an exact spelling and presence policy on an already checked object.
func Fields(object map[string]json.RawMessage, required, optional []string) error {
	allowed := make(map[string]bool, len(required)+len(optional))
	for _, key := range required {
		raw, exists := object[key]
		if !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return ErrInvalid
		}
		allowed[key] = true
	}
	for _, key := range optional {
		allowed[key] = true
	}
	for key, raw := range object {
		if !allowed[key] || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return ErrInvalid
		}
	}
	return nil
}
