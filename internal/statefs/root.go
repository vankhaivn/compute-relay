// Package statefs protects operator-owned state roots with OS locks and private
// permissions. It is not a sandbox against the host administrator or same-user writes.
package statefs

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrLocked      = errors.New("state directory is already in use")
	ErrUnsafe      = errors.New("state path or permissions are unsafe")
	ErrUnavailable = errors.New("state filesystem unavailable")
)

// Root must remain open until every database handle is closed. Never unlink its lock
// file: replacing an inode would allow two processes to acquire different locks.
type Root struct {
	Path   string
	file   *os.File
	mu     sync.Mutex
	closed bool
}

// PrivateDir creates only the requested final directory (parents must exist), or
// validates an existing one. exclusive refuses existing paths without modifying them.
func PrivateDir(path string, exclusive bool) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil || path == "" {
		return "", ErrUnsafe
	}
	err = os.Mkdir(absolute, 0o700)
	created := err == nil
	if err != nil && (exclusive || !errors.Is(err, os.ErrExist)) {
		return "", ErrUnavailable
	}
	if err := secureNewRoot(absolute, created); err != nil {
		return "", err
	}
	if err := CheckDir(absolute); err != nil {
		return "", err
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", ErrUnavailable
	}
	return absolute, nil
}

func Open(path string) (*Root, error) {
	absolute, err := PrivateDir(path, false)
	if err != nil {
		return nil, err
	}
	lockPath := filepath.Join(absolute, "runtime.lock")
	if err := CheckFile(lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err := CheckFile(lockPath); err != nil {
		f.Close()
		return nil, err
	}
	if err := lock(f); err != nil {
		f.Close()
		return nil, err
	}
	return &Root{Path: absolute, file: f}, nil
}

func (r *Root) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return errors.Join(unlock(r.file), r.file.Close())
}

func CheckDir(path string) error  { return check(path, true) }
func CheckFile(path string) error { return check(path, false) }
func check(path string, directory bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.ErrNotExist
	}
	if err != nil {
		return ErrUnavailable
	}
	if info.Mode()&os.ModeSymlink != 0 || (directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) || !privateMode(info.Mode()) {
		return ErrUnsafe
	}
	return checkACL(path)
}

// WriteNew publishes only small internal files. Callers own the parent and keep
// incomplete files private until their separate commit/manifest boundary.
func WriteNew(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return ErrUnavailable
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return ErrUnavailable
	}
	if err := f.Sync(); err != nil {
		return ErrUnavailable
	}
	if err := f.Close(); err != nil {
		return ErrUnavailable
	}
	return nil
}

func SyncFile(path string) error {
	if err := CheckFile(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return ErrUnavailable
	}
	defer f.Close()
	return f.Sync()
}
