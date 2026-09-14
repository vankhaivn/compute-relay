//go:build !linux

package safefs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// The pinned production Go toolchain supplies traversal-resistant os.Root. Linux uses
// openat/O_NOFOLLOW so every component can additionally be rejected without following it.
type Root struct {
	mu     sync.RWMutex
	root   *os.Root
	closed bool
}

func OpenRoot(path string) (*Root, error) {
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrUnavailable
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, ErrUnavailable
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		_ = root.Close()
		return nil, ErrChanged
	}
	return &Root{root: root}, nil
}
func (r *Root) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.root.Close()
}
func (r *Root) Open(name string) (*os.File, error) {
	if !ValidPath(name) {
		return nil, ErrPath
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, ErrUnavailable
	}
	var before os.FileInfo
	prefix := ""
	for _, part := range strings.Split(name, "/") {
		prefix = filepath.Join(prefix, part)
		info, err := r.root.Lstat(prefix)
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 || (!info.IsDir() && !info.Mode().IsRegular()) {
			return nil, ErrPath
		}
		before = info
	}
	f, err := r.root.Open(filepath.FromSlash(name))
	if err != nil {
		return nil, ErrPath
	}
	info, err := f.Stat()
	after, pathErr := r.root.Lstat(filepath.FromSlash(name))
	if err != nil || pathErr != nil || !Unchanged(before, info) || !Unchanged(info, after) || (!info.IsDir() && !info.Mode().IsRegular()) || !singleLink(f, info) {
		_ = f.Close()
		return nil, ErrChanged
	}
	return f, nil
}
