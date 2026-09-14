package admission

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

const MaxRequestBytes = 1 << 20
const CanonicalVersion = "compute-relay/job-request/v1"

// canonicalJSON is a versioned, integer-only JSON normalization, not RFC 8785.
// Object order, whitespace and equivalent integer spellings are immaterial. Array
// order, strings and presence/absence of optional fields remain significant.
func canonicalJSON(raw []byte) ([]byte, any, error) {
	if len(raw) == 0 || len(raw) > MaxRequestBytes || !validUnicode(raw) {
		return nil, nil, ErrSpec
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	v, err := readValue(d, 0)
	if err != nil {
		return nil, nil, ErrSpec
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, nil, ErrSpec
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, nil, ErrSpec
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) > MaxRequestBytes {
		return nil, nil, ErrSpec
	}
	return b, v, nil
}

func readValue(d *json.Decoder, depth int) (any, error) {
	if depth > 16 {
		return nil, ErrSpec
	}
	t, err := d.Token()
	if err != nil {
		return nil, ErrSpec
	}
	switch t := t.(type) {
	case json.Delim:
		switch t {
		case '{':
			m := map[string]any{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return nil, ErrSpec
				}
				key, ok := k.(string)
				if !ok {
					return nil, ErrSpec
				}
				if _, ok = m[key]; ok {
					return nil, ErrSpec
				}
				v, err := readValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				m[key] = v
			}
			close, err := d.Token()
			if err != nil || close != json.Delim('}') {
				return nil, ErrSpec
			}
			return m, nil
		case '[':
			a := []any{}
			for d.More() {
				v, err := readValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			close, err := d.Token()
			if err != nil || close != json.Delim(']') {
				return nil, ErrSpec
			}
			return a, nil
		}
		return nil, ErrSpec
	case json.Number:
		// All numbers in this job schema are bounded integers. Bound exponent parsing
		// before using big.Rat so malicious exponent strings cannot allocate huge bignums.
		s := string(t)
		if len(s) > 64 {
			return nil, ErrSpec
		}
		if i := strings.IndexAny(s, "eE"); i >= 0 {
			e, err := strconv.Atoi(s[i+1:])
			if err != nil || e < -20 || e > 20 {
				return nil, ErrSpec
			}
		}
		n, ok := new(big.Rat).SetString(s)
		if !ok || !n.IsInt() || !n.Num().IsInt64() {
			return nil, ErrSpec
		}
		// Keep JSON-schema validators and language-neutral consumers within exact integer range.
		x := n.Num().Int64()
		if x < -9007199254740991 || x > 9007199254740991 {
			return nil, ErrSpec
		}
		return json.Number(strconv.FormatInt(x, 10)), nil
	default:
		return t, nil
	}
}

// encoding/json replaces malformed UTF-8/surrogates. Reject them before decoding so
// two distinct invalid requests cannot collapse onto one accepted request identity.
func validUnicode(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	inString := false
	for i := 0; i < len(raw); i++ {
		if raw[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || raw[i] != '\\' {
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
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return !inString
}

func digest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

// KeyDigest scopes the eventual record to a workspace and operation in the store.
// Neither raw keys nor hashes should be emitted in ordinary API logs or errors.
func KeyDigest(key string) (string, error) {
	if len(key) < 8 || len(key) > 256 {
		return "", ErrKey
	}
	for _, c := range []byte(key) {
		if c < 33 || c > 126 {
			return "", ErrKey
		}
	}
	return digest([]byte(key)), nil
}
