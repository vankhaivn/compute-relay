//go:build linux

package safefs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// Root holds a read-only directory descriptor. Every path component is opened relative
// to that descriptor with O_NOFOLLOW; a symlink swap cannot redirect a read outside it.
type Root struct {
	mu     sync.RWMutex
	file   *os.File
	closed bool
}

func OpenRoot(path string) (*Root, error) {
	path = filepath.Clean(path)
	before, err := os.Lstat(path)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, ErrUnavailable
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrUnavailable
	}
	f := os.NewFile(uintptr(fd), "input-root")
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) {
		_ = f.Close()
		return nil, ErrChanged
	}
	return &Root{file: f}, nil
}
func (r *Root) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.file.Close()
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
	parent := int(r.file.Fd())
	var owned *os.File
	defer func() {
		if owned != nil {
			_ = owned.Close()
		}
	}()
	parts := strings.Split(name, "/")
	for i, part := range parts {
		flags := syscall.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_CLOEXEC | syscall.O_NONBLOCK
		if i < len(parts)-1 {
			flags |= syscall.O_DIRECTORY
		}
		fd, err := syscall.Openat(parent, part, flags, 0)
		if errors.Is(err, syscall.ENOENT) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, ErrPath
		}
		f := os.NewFile(uintptr(fd), "local-input")
		info, err := f.Stat()
		if err != nil || (!info.IsDir() && !info.Mode().IsRegular()) {
			_ = f.Close()
			return nil, ErrPath
		}
		if info.Mode().IsRegular() && info.Sys().(*syscall.Stat_t).Nlink != 1 {
			_ = f.Close()
			return nil, ErrPath
		}
		if i == len(parts)-1 {
			return f, nil
		}
		if owned != nil {
			_ = owned.Close()
		}
		owned = f
		parent = fd
	}
	return nil, ErrPath
}
