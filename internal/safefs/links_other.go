//go:build !linux && !darwin && !windows

package safefs

import "os"

// Unsupported hosts fail closed; no platform support is inferred from a compile.
func singleLink(*os.File, os.FileInfo) bool { return false }
