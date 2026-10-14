// Package runnerassets exposes the existing remote Python runner as inert,
// embedded source data. It never extracts or executes a workload locally.
package runnerassets

import (
	"embed"
	"errors"
	"strings"
)

// Enumerate exact files, including __init__.py, without embedding __pycache__
// or unrelated local files created between builds.
//
//go:embed python/relay_runner/__init__.py python/relay_runner/contract.py
//go:embed python/relay_runner/files.py python/relay_runner/main.py
//go:embed python/relay_runner/process.py
//go:embed python/relay_runner/deadline.py
//go:embed python/relay_runner/secrets.py
var assets embed.FS

var sourceNames = [...]string{"__init__.py", "contract.py", "files.py", "main.py", "process.py", "deadline.py", "secrets.py"}

// Sources returns a fresh map of the embedded runner modules. Adapters may
// encode these trusted assets into a remote script, not substitute uploaded code.
func Sources() (map[string]string, error) {
	result := make(map[string]string, len(sourceNames))
	for _, name := range sourceNames {
		data, err := assets.ReadFile("python/relay_runner/" + name)
		if err != nil || !strings.HasSuffix(string(data), "\n") {
			return nil, errors.New("remote runner source unavailable")
		}
		result[name] = string(data)
	}
	return result, nil
}
