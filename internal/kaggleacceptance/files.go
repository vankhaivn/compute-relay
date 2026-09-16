package kaggleacceptance

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func randomNonce() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", ErrState
	}
	return hex.EncodeToString(raw[:]), nil
}

// Reject duplicate/unknown/case-alias/missing fields in small internally generated
// records. Exact round-tripping also rejects lossy type conversion. It is integrity
// checking inside the trusted operator boundary, not a signed evidence format.
func decodeRecord(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > 16384 {
		return ErrState
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 16 {
			return ErrState
		}
		token, err := d.Token()
		if err != nil {
			return ErrState
		}
		switch token {
		case json.Delim('{'):
			seen := map[string]bool{}
			for d.More() {
				token, err := d.Token()
				key, ok := token.(string)
				if err != nil || !ok || seen[key] {
					return ErrState
				}
				seen[key] = true
				if err := walk(depth+1); err != nil {
					return err
				}
			}
			_, err = d.Token()
		case json.Delim('['):
			for d.More() {
				if err := walk(depth+1); err != nil {
					return err
				}
			}
			_, err = d.Token()
		}
		if err != nil {
			return ErrState
		}
		return nil
	}
	if walk(0) != nil || d.Decode(new(any)) != io.EOF {
		return ErrState
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil {
		return ErrState
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		return ErrState
	}
	var original, normalized any
	if json.Unmarshal(raw, &original) != nil || json.Unmarshal(encoded, &normalized) != nil || !reflect.DeepEqual(original, normalized) {
		return ErrState
	}
	return nil
}

func readRecord(path string, target any) error {
	if err := statefs.CheckFile(path); err != nil {
		return ErrState
	}
	before, err := os.Lstat(path)
	if err != nil || before.Size() > 16384 {
		return ErrState
	}
	f, err := os.Open(path)
	if err != nil {
		return ErrState
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) {
		return ErrState
	}
	raw, err := io.ReadAll(io.LimitReader(f, 16385))
	if err != nil {
		return ErrState
	}
	return decodeRecord(raw, target)
}
func writeRecord(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 16384 {
		return ErrState
	}
	// Create-only and flush before any dependent effect. A partial record blocks
	// future work rather than being silently replaced or treated as absent.
	if err := statefs.WriteNew(path, raw); err != nil {
		return ErrState
	}
	return statefs.SyncDir(filepath.Dir(path))
}
