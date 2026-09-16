package runtimehost

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

// Resolve existing parent aliases before checking protected store ancestry.
// The operator owns the path and must not race directory replacement. This is
// not a hostile same-user filesystem sandbox or a cross-resource transaction.
func tokenDestination(root, output string) (string, error) {
	absolute, err := filepath.Abs(output)
	if err != nil || statefs.CheckDir(filepath.Dir(absolute)) != nil {
		return "", ErrRequest
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil || statefs.CheckDir(parent) != nil {
		return "", ErrRequest
	}
	absolute = filepath.Join(parent, filepath.Base(absolute))
	relative, err := filepath.Rel(root, absolute)
	if err != nil {
		return "", ErrRequest
	}
	first := strings.ToLower(strings.Split(filepath.ToSlash(relative), "/")[0])
	if first == "state" || first == "inputs" || first == "results" || relative == "." {
		return "", ErrRequest
	}
	if _, err := os.Lstat(absolute); !errors.Is(err, os.ErrNotExist) {
		return "", ErrRequest
	}
	return absolute, nil
}
