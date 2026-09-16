// Package runnerassets exposes the existing remote Python runner as inert,
// checksum-verified source data. It never extracts or executes a workload locally.
package runnerassets

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

//go:embed all:python/relay_runner assets.lock.json
var assets embed.FS

var sourceNames = [...]string{"__init__.py", "contract.py", "files.py", "main.py", "process.py"}

// Sources returns a fresh map of the five locked runner modules. Adapters may
// encode these trusted assets into a remote script, not substitute uploaded code.
// The original Python source and its reviewed lock remain the source of truth.
func Sources() (map[string]string, error) {
	var lock struct {
		Version int `json:"lock_version"`
		Files   []struct {
			Path   string `json:"path"`
			Bytes  int    `json:"bytes"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	raw, err := assets.ReadFile("assets.lock.json")
	if err != nil || json.Unmarshal(raw, &lock) != nil || lock.Version != 1 {
		return nil, errors.New("remote runner lock unavailable")
	}
	result := make(map[string]string, len(sourceNames))
	for _, name := range sourceNames {
		path := "python/relay_runner/" + name
		data, err := assets.ReadFile(path)
		if err != nil {
			return nil, errors.New("remote runner source unavailable")
		}
		sum := sha256.Sum256(data)
		matched := 0
		for _, entry := range lock.Files {
			if entry.Path == "runner/"+path {
				if entry.Bytes != len(data) || entry.SHA256 != hex.EncodeToString(sum[:]) {
					return nil, errors.New("remote runner source differs from lock")
				}
				matched++
			}
		}
		if matched != 1 || !strings.HasSuffix(string(data), "\n") {
			return nil, errors.New("remote runner source identity is invalid")
		}
		result[name] = string(data)
	}
	return result, nil
}
