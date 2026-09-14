// Package safefs provides read-only, rooted access to operator-allowed local inputs.
// It is not a sandbox against the host administrator or changes to mounted filesystems.
package safefs

import (
	"errors"
	"os"
	"strings"
)

var (
	ErrNotFound    = errors.New("local input not found")
	ErrPath        = errors.New("unsafe or unsupported relative path")
	ErrChanged     = errors.New("input changed while snapshotting")
	ErrUnavailable = errors.New("local input unavailable")
)

// ValidPath defines a portable ASCII path subset. Dot is allowed only for opening the
// root, never for selecting an entire project implicitly. Reserved names are rejected on
// every host so Linux-created bundles do not change meaning on Windows/macOS.
func ValidPath(name string) bool {
	if name == "." {
		return true
	}
	if len(name) == 0 || len(name) > 1024 {
		return false
	}
	for _, c := range []byte(name) {
		if c < 32 || c >= 127 || strings.ContainsRune(`\:*?"<>|`, rune(c)) {
			return false
		}
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." || len(part) > 100 || strings.TrimRight(part, " .") != part {
			return false
		}
		base := strings.ToUpper(strings.SplitN(part, ".", 2)[0])
		switch base {
		case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
			return false
		}
		if len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '0' && base[3] <= '9' {
			return false
		}
	}
	return true
}

// Unchanged compares the opened object, not just a path spelling. Digest comparison in
// packaging additionally detects same-size changes with a restored timestamp.
func Unchanged(before, after os.FileInfo) bool {
	return before != nil && after != nil && os.SameFile(before, after) && before.Mode() == after.Mode() && before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
}
