// Package gpusmoke exposes a fixed, finite remote workload as inert source.
// Importing it does not start Python, allocate a device or read credentials.
package gpusmoke

import _ "embed"

//go:embed main.py
var mainSource string

//go:embed calculation.py
var calculationSource string

func Sources() map[string]string {
	return map[string]string{"main.py": mainSource, "calculation.py": calculationSource}
}
