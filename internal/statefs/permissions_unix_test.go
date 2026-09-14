//go:build linux || darwin

package statefs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRejectBroadUnixPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "public")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if r, err := Open(path); !errors.Is(err, ErrUnsafe) {
		if r != nil {
			r.Close()
		}
		t.Fatal("public directory accepted", err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o755 {
		t.Fatal("operator permissions changed")
	}
}
