//go:build windows

package statefs

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestRejectBroadWindowsDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err := setDACL(path, "D:P(A;OICI;FA;;;WD)"); err != nil {
		t.Fatal(err)
	}
	if r, err := Open(path); !errors.Is(err, ErrUnsafe) {
		if r != nil {
			r.Close()
		}
		t.Fatal("world-readable DACL accepted", err)
	}
}
