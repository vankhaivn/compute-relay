package collection

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"unicode/utf8"
)

// strictValue preserves no duplicate decoded key and accepts no replacement of an
// invalid Unicode surrogate. json.Unmarshal alone silently repairs those strings.
func strictValue(raw []byte) (any, error) {
	if len(raw) == 0 || len(raw) > MaxManifestBytes || !utf8.Valid(raw) || !pairedEscapes(raw) {
		return nil, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	budget := 100000
	if err := walkJSON(d, 0, &budget); err != nil {
		return nil, ErrInvalid
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, ErrInvalid
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return nil, ErrInvalid
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, ErrInvalid
	}
	return value, nil
}
func walkJSON(d *json.Decoder, depth int, budget *int) error {
	*budget--
	if depth > 32 || *budget < 0 {
		return ErrInvalid
	}
	token, err := d.Token()
	if err != nil {
		return ErrInvalid
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := map[string]bool{}
		for d.More() {
			token, err := d.Token()
			key, ok := token.(string)
			if err != nil || !ok || keys[key] {
				return ErrInvalid
			}
			keys[key] = true
			if err := walkJSON(d, depth+1, budget); err != nil {
				return err
			}
		}
		token, err = d.Token()
		if err != nil || token != json.Delim('}') {
			return ErrInvalid
		}
	case '[':
		for d.More() {
			if err := walkJSON(d, depth+1, budget); err != nil {
				return err
			}
		}
		token, err = d.Token()
		if err != nil || token != json.Delim(']') {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func pairedEscapes(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n < 0xd800 || n > 0xdbff {
			continue
		}
		if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}
