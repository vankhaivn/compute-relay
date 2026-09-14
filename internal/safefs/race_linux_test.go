//go:build linux

package safefs

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

func TestSymlinkSwapNeverReadsOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	sub := filepath.Join(root, "sub")
	held := filepath.Join(root, "held")
	_ = os.Mkdir(sub, 0o700)
	_ = os.WriteFile(filepath.Join(sub, "file"), []byte("inside"), 0o600)
	_ = os.WriteFile(filepath.Join(outside, "file"), []byte("outside-canary"), 0o600)
	r, err := OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			_ = os.Rename(sub, held)
			_ = os.Symlink(outside, sub)
			_ = os.Remove(sub)
			_ = os.Rename(held, sub)
		}
	}()
	for i := 0; i < 300; i++ {
		f, err := r.Open("sub/file")
		if err != nil {
			continue
		}
		b, err := io.ReadAll(f)
		f.Close()
		if err != nil || string(b) != "inside" {
			t.Errorf("root escape: %q %v", b, err)
		}
	}
	wg.Wait()
	if err := syscall.Mkfifo(filepath.Join(root, "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	if f, err := r.Open("fifo"); err == nil {
		f.Close()
		t.Fatal("FIFO accepted")
	}
}
