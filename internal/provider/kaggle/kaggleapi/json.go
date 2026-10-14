package kaggleapi

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// strictJSON rejects nested duplicate keys, trailing data, invalid UTF-8 and
// unpaired escaped surrogates that encoding/json otherwise silently replaces.
func strictJSON(data []byte) error {
	if !utf8.Valid(data) || !validEscapedUnicode(data) {
		return ErrResponse
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		if depth > 64 {
			return ErrResponse
		}
		token, err := decoder.Token()
		if err != nil {
			return ErrResponse
		}
		delimiter, compound := token.(json.Delim)
		if !compound {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				key, err := decoder.Token()
				text, ok := key.(string)
				if err != nil || !ok || seen[text] {
					return ErrResponse
				}
				seen[text] = true
				if value(depth+1) != nil {
					return ErrResponse
				}
			}
		case '[':
			for decoder.More() {
				if value(depth+1) != nil {
					return ErrResponse
				}
			}
		default:
			return ErrResponse
		}
		end, err := decoder.Token()
		if err != nil || delimiter == '{' && end != json.Delim('}') || delimiter == '[' && end != json.Delim(']') {
			return ErrResponse
		}
		return nil
	}
	if value(0) != nil {
		return ErrResponse
	}
	if _, err := decoder.Token(); err != io.EOF {
		return ErrResponse
	}
	return nil
}

func strictObject(data []byte) (map[string]json.RawMessage, error) {
	if strictJSON(data) != nil {
		return nil, ErrResponse
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil || object == nil {
		return nil, ErrResponse
	}
	return object, nil
}

func validEscapedUnicode(data []byte) bool {
	quoted := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			return false
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return false
		}
		unit, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if unit >= 0xdc00 && unit <= 0xdfff {
			return false
		}
		if unit >= 0xd800 && unit <= 0xdbff {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

func unsignedDecimal(text string, maximum int64) (int64, bool) {
	if text == "" {
		return 0, false
	}
	for _, b := range []byte(text) {
		if b < '0' || b > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(text, 10, 64)
	return n, err == nil && n >= 0 && n <= maximum
}

func decodeObject(object map[string]json.RawMessage, out any) error {
	data, err := json.Marshal(object)
	if err != nil || exactFieldCase(data, reflect.TypeOf(out)) != nil || json.Unmarshal(data, out) != nil {
		return ErrResponse
	}
	return nil
}

// encoding/json accepts case-insensitive aliases for tagged fields. The wire
// contract does not: unknown fields stay ignored, but aliases of a known field
// must not replace missing evidence or override a canonical false/private flag.
func exactFieldCase(raw []byte, target reflect.Type) error {
	for target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if bytes.Equal(raw, []byte("null")) {
		return nil
	}
	switch target.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			return ErrResponse
		}
		for key, value := range fields {
			for i := 0; i < target.NumField(); i++ {
				field := target.Field(i)
				name := strings.SplitN(field.Tag.Get("json"), ",", 2)[0]
				if name == "" || name == "-" {
					continue
				}
				if key == name {
					if exactFieldCase(value, field.Type) != nil {
						return ErrResponse
					}
					break
				}
				if strings.EqualFold(key, name) {
					return ErrResponse
				}
			}
		}
	case reflect.Slice:
		element := target.Elem()
		for element.Kind() == reflect.Pointer {
			element = element.Elem()
		}
		if element.Kind() != reflect.Struct {
			return nil
		}
		var values []json.RawMessage
		if json.Unmarshal(raw, &values) != nil {
			return ErrResponse
		}
		for _, value := range values {
			if exactFieldCase(value, element) != nil {
				return ErrResponse
			}
		}
	}
	return nil
}

// ID accepts the exact positive int64 represented as either a JSON number or
// protobuf JSON string. Floating-point, sign, leading-zero and boolean values fail.
type ID string

func (id *ID) UnmarshalJSON(raw []byte) error {
	text := string(raw)
	if len(raw) > 0 && raw[0] == '"' {
		if json.Unmarshal(raw, &text) != nil {
			return ErrResponse
		}
	}
	n, ok := unsignedDecimal(text, 1<<63-1)
	if !ok || n == 0 || strconv.FormatInt(n, 10) != text {
		return ErrResponse
	}
	*id = ID(text)
	return nil
}
